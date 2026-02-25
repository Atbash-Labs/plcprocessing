#!/usr/bin/env python3
"""
Database client for live read-only queries against project databases.

Connects to MySQL, MSSQL, and PostgreSQL databases defined in the Ignition
project's databaseConnections. Credentials are loaded from db_credentials.json
(never exposed to AI tools).
"""

import json
import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Statements that are allowed (read-only)
_ALLOWED_PREFIXES = re.compile(
    r"^\s*(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN|WITH)\b", re.IGNORECASE
)

# Statements that are explicitly forbidden
_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE|MERGE|CALL)\b",
    re.IGNORECASE,
)

DEFAULT_ROW_LIMIT = 100


@dataclass
class ConnectionInfo:
    """Resolved connection metadata (no password)."""

    name: str
    database_type: str
    url: str
    username: str
    enabled: bool
    has_credentials: bool = False


class DatabaseClient:
    """Manages connections to project databases for live read-only queries."""

    def __init__(
        self,
        credentials_path: Optional[str] = None,
        neo4j_graph=None,
    ):
        self._credentials_path = credentials_path or self._default_credentials_path()
        self._graph = neo4j_graph
        self._connections: Dict[str, Any] = {}  # cached DB connections
        self._conn_meta: Dict[str, Dict] = {}  # connection metadata from Neo4j

    @staticmethod
    def _default_credentials_path() -> str:
        from dotenv import load_dotenv

        load_dotenv()
        env_dir = os.environ.get("DOTENV_PATH", "")
        if env_dir and os.path.isfile(env_dir):
            return os.path.join(os.path.dirname(env_dir), "db_credentials.json")
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "db_credentials.json",
        )

    # ------------------------------------------------------------------
    # Credential loading
    # ------------------------------------------------------------------

    def _load_credentials(self) -> Dict[str, Dict]:
        """Load credentials from db_credentials.json."""
        p = Path(self._credentials_path)
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read db_credentials.json: %s", exc)
            return {}

    def _get_credentials(self, connection_name: str) -> Tuple[str, str]:
        """Return (username, password) for *connection_name*."""
        creds = self._load_credentials().get(connection_name, {})
        return creds.get("username", ""), creds.get("password", "")

    # ------------------------------------------------------------------
    # Connection metadata from Neo4j
    # ------------------------------------------------------------------

    def _load_connection_meta(self) -> Dict[str, Dict]:
        """Load DatabaseConnection nodes from Neo4j."""
        if self._conn_meta:
            return self._conn_meta
        if not self._graph:
            return {}
        try:
            with self._graph.session() as session:
                result = session.run(
                    """
                    MATCH (d:DatabaseConnection)
                    RETURN d.name AS name, d.database_type AS database_type,
                           d.url AS url, d.username AS username,
                           d.enabled AS enabled,
                           d.validation_query AS validation_query
                """
                )
                for record in result:
                    self._conn_meta[record["name"]] = dict(record)
        except Exception as exc:
            logger.warning("Failed to load DB connections from Neo4j: %s", exc)
        return self._conn_meta

    def refresh_metadata(self):
        """Force-reload connection metadata from Neo4j."""
        self._conn_meta = {}
        self._load_connection_meta()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _parse_url(self, url: str, db_type: str) -> Dict[str, Any]:
        """Parse an Ignition-style DB URL into host, port, database."""
        host = url
        port = None
        database = ""

        if "/" in url:
            host_part, database = url.split("/", 1)
        else:
            host_part = url

        if ":" in host_part:
            host, port_str = host_part.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                host = host_part

        if port is None:
            defaults = {"MYSQL": 3306, "MSSQL": 1433, "POSTGRESQL": 5432}
            port = defaults.get(db_type.upper(), 3306)

        return {"host": host, "port": port, "database": database}

    def _connect(self, connection_name: str):
        """Create a new DB connection."""
        meta = self._load_connection_meta().get(connection_name)
        if not meta:
            raise ValueError(f"Unknown database connection: {connection_name}")

        db_type = (meta.get("database_type") or "").upper()
        url = meta.get("url", "")
        parsed = self._parse_url(url, db_type)

        username, password = self._get_credentials(connection_name)
        if not username:
            username = meta.get("username", "")

        if db_type == "MYSQL":
            import mysql.connector

            return mysql.connector.connect(
                host=parsed["host"],
                port=parsed["port"],
                database=parsed["database"],
                user=username,
                password=password,
                connect_timeout=10,
                autocommit=True,
            )
        elif db_type == "MSSQL":
            import pyodbc

            driver = self._find_odbc_driver()
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={parsed['host']},{parsed['port']};"
                f"DATABASE={parsed['database']};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
                f"LoginTimeout=10;"
            )
            return pyodbc.connect(conn_str, autocommit=True)
        elif db_type in ("POSTGRESQL", "POSTGRES"):
            import psycopg2

            return psycopg2.connect(
                host=parsed["host"],
                port=parsed["port"],
                dbname=parsed["database"],
                user=username,
                password=password,
                connect_timeout=10,
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    @staticmethod
    def _find_odbc_driver() -> str:
        """Find the best available ODBC driver for SQL Server."""
        import pyodbc

        preferred = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server",
        ]
        available = pyodbc.drivers()
        for drv in preferred:
            if drv in available:
                return drv
        if available:
            return available[0]
        raise RuntimeError("No ODBC drivers found for SQL Server")

    def get_connection(self, connection_name: str):
        """Get a cached connection or create a new one."""
        conn = self._connections.get(connection_name)
        if conn is not None:
            try:
                # Quick liveness check
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return conn
            except Exception:
                self._connections.pop(connection_name, None)

        conn = self._connect(connection_name)
        self._connections[connection_name] = conn
        return conn

    def close_all(self):
        """Close all cached connections."""
        for name, conn in list(self._connections.items()):
            try:
                conn.close()
            except Exception:
                pass
        self._connections.clear()

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    @staticmethod
    def validate_readonly(sql: str) -> None:
        """Raise ValueError if the SQL is not a read-only statement."""
        stripped = sql.strip().rstrip(";").strip()
        if not _ALLOWED_PREFIXES.match(stripped):
            raise ValueError(
                "Only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH statements are allowed"
            )
        if _FORBIDDEN_KEYWORDS.search(stripped):
            raise ValueError(
                "Query contains forbidden keywords (INSERT, UPDATE, DELETE, etc.)"
            )

    def execute_query(
        self,
        connection_name: str,
        sql: str,
        params: Optional[Dict] = None,
        row_limit: int = DEFAULT_ROW_LIMIT,
    ) -> Dict[str, Any]:
        """Execute a read-only query and return results.

        Returns:
            Dict with 'columns', 'rows' (list of dicts), 'row_count', 'truncated'.
        """
        self.validate_readonly(sql)

        limited_sql = sql.rstrip().rstrip(";")
        # Append LIMIT/TOP if not already present
        if row_limit and "LIMIT" not in limited_sql.upper():
            meta = self._load_connection_meta().get(connection_name, {})
            db_type = (meta.get("database_type") or "").upper()
            if db_type == "MSSQL":
                if "TOP" not in limited_sql.upper():
                    limited_sql = limited_sql.replace(
                        "SELECT", f"SELECT TOP {row_limit}", 1
                    )
            else:
                limited_sql = f"{limited_sql} LIMIT {row_limit + 1}"

        conn = self.get_connection(connection_name)
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(limited_sql, tuple(params.values()))
            else:
                cursor.execute(limited_sql)

            if cursor.description is None:
                return {"columns": [], "rows": [], "row_count": 0, "truncated": False}

            columns = [desc[0] for desc in cursor.description]
            rows_raw = cursor.fetchmany(row_limit + 1)
            truncated = len(rows_raw) > row_limit
            rows_raw = rows_raw[:row_limit]

            rows = []
            for row in rows_raw:
                rows.append(
                    {col: self._serialize_value(val) for col, val in zip(columns, row)}
                )

            return {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated,
            }
        finally:
            cursor.close()

    @staticmethod
    def _serialize_value(val: Any) -> Any:
        """Convert DB values to JSON-safe types."""
        if val is None:
            return None
        if isinstance(val, (int, float, bool, str)):
            return val
        if isinstance(val, bytes):
            return val.hex()
        return str(val)

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def describe_schema(
        self, connection_name: str, schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """List tables and their columns for a database connection."""
        meta = self._load_connection_meta().get(connection_name)
        if not meta:
            raise ValueError(f"Unknown database connection: {connection_name}")

        db_type = (meta.get("database_type") or "").upper()

        if db_type == "MYSQL":
            sql = """
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                       COLUMN_KEY, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
        elif db_type == "MSSQL":
            schema_filter = schema or "dbo"
            sql = f"""
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                       COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME),
                                      COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
                       COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_filter}'
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
        elif db_type in ("POSTGRESQL", "POSTGRES"):
            schema_filter = schema or "public"
            sql = f"""
                SELECT table_name, column_name, data_type, is_nullable,
                       column_default
                FROM information_schema.columns
                WHERE table_schema = '{schema_filter}'
                ORDER BY table_name, ordinal_position
            """
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        result = self.execute_query(connection_name, sql, row_limit=2000)

        tables: Dict[str, List[Dict]] = {}
        for row in result["rows"]:
            table = row.get("TABLE_NAME") or row.get("table_name", "")
            tables.setdefault(table, []).append(row)

        return {
            "connection": connection_name,
            "database_type": db_type,
            "table_count": len(tables),
            "tables": {
                tbl: {
                    "columns": [
                        {
                            "name": c.get("COLUMN_NAME") or c.get("column_name"),
                            "type": c.get("DATA_TYPE") or c.get("data_type"),
                            "nullable": c.get("IS_NULLABLE") or c.get("is_nullable"),
                        }
                        for c in cols
                    ]
                }
                for tbl, cols in tables.items()
            },
        }

    # ------------------------------------------------------------------
    # Listing / status
    # ------------------------------------------------------------------

    def list_connections(self) -> List[ConnectionInfo]:
        """Return all known database connections with status."""
        meta = self._load_connection_meta()
        creds = self._load_credentials()
        out = []
        for name, m in meta.items():
            out.append(
                ConnectionInfo(
                    name=name,
                    database_type=m.get("database_type", ""),
                    url=m.get("url", ""),
                    username=m.get("username", ""),
                    enabled=m.get("enabled", True),
                    has_credentials=bool(creds.get(name, {}).get("password")),
                )
            )
        return out

    def test_connection(self, connection_name: str) -> Dict[str, Any]:
        """Test connectivity by running the validation query."""
        meta = self._load_connection_meta().get(connection_name)
        if not meta:
            return {"success": False, "error": f"Unknown connection: {connection_name}"}

        validation_query = meta.get("validation_query") or "SELECT 1"
        try:
            conn = self._connect(connection_name)
            cursor = conn.cursor()
            cursor.execute(validation_query)
            cursor.fetchone()
            cursor.close()
            conn.close()
            return {"success": True}
        except Exception as exc:
            return {"success": False, "error": str(exc)}
