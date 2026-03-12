#!/usr/bin/env python3
"""
MCP (Model Context Protocol) server exposing PLC/SCADA ontology tools.

Exposes the same tools that the Claude troubleshooting agent uses internally,
so any MCP-compatible client (Claude Desktop, Cursor, external agents) can:
  - Query the Neo4j ontology graph
  - Read live Ignition tags and history
  - List/manage anomaly events
  - Run anomaly scoring
  - Execute read-only database queries
  - Search and explore the graph

Run:
    python scripts/mcp_server.py                  # stdio transport (default)
    python scripts/mcp_server.py --transport sse   # SSE transport on port 8080
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

# Ensure scripts/ is on path for sibling imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

from mcp.server.fastmcp import FastMCP

# File logging (stdout is reserved for MCP protocol over stdio)
_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "mcp_server.log")
logging.basicConfig(
    filename=os.path.abspath(_log_path),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    "plc-ontology",
    instructions="PLC/SCADA ontology tools for troubleshooting, anomaly monitoring, and graph exploration.",
)

logger.info("MCP server initialized (pid=%d)", os.getpid())

# ---------------------------------------------------------------------------
#  Lazy singletons (connect on first use)
# ---------------------------------------------------------------------------

_graph = None
_api_client = None
_ontology_tools = None


def _get_graph():
    global _graph
    if _graph is None:
        from neo4j_ontology import get_ontology_graph
        _graph = get_ontology_graph()
    return _graph


def _get_api_client():
    global _api_client
    if _api_client is None:
        from ignition_api_client import IgnitionApiClient
        _api_client = IgnitionApiClient()
    return _api_client


def _get_ontology_tools():
    global _ontology_tools
    if _ontology_tools is None:
        from claude_client import OntologyTools
        _ontology_tools = OntologyTools(_get_graph(), api_client=_get_api_client())
    return _ontology_tools


def _json(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


def _log_call(name: str, args: dict, result: str) -> str:
    """Log tool call and return result passthrough."""
    preview = result[:200] + "..." if len(result) > 200 else result
    logger.info("TOOL %s args=%s result=%s", name, json.dumps(args, default=str)[:300], preview)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  Graph exploration (core)
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
def get_schema() -> str:
    """Get the Neo4j database schema: node labels, relationship types, properties, and patterns. Call this first to understand what data exists."""
    return _get_ontology_tools().execute("get_schema", {})


@mcp.tool()
def run_query(query: str, params: Optional[dict] = None) -> str:
    """Execute a read-only Cypher query against the Neo4j ontology graph. Results limited to 50 rows. Use toLower() and CONTAINS for text search."""
    args = {"query": query}
    if params:
        args["params"] = params
    return _get_ontology_tools().execute("run_query", args)


@mcp.tool()
def get_node(label: str, name: str) -> str:
    """Get a specific node by label and name, including all properties and immediate relationships."""
    return _get_ontology_tools().execute("get_node", {"label": label, "name": name})


@mcp.tool()
def search_nodes(query: str, node_types: Optional[List[str]] = None, limit: int = 25) -> str:
    """Case-insensitive keyword search across graph nodes. Returns matching nodes with type and key properties."""
    from graph_api import GraphAPI
    api = GraphAPI(_get_graph())
    return _json(api.search_nodes(query, node_types=node_types, limit=limit))


@mcp.tool()
def get_neighbors(node_name: str, node_type: str, hops: int = 1, max_nodes: int = 50) -> str:
    """Get the neighborhood of a node (1-3 hops). Returns nodes and edges for graph exploration."""
    from graph_api import GraphAPI
    api = GraphAPI(_get_graph())
    return _json(api.get_neighbors(node_id=node_name, node_type=node_type, hops=hops, max_nodes=max_nodes))


@mcp.tool()
def create_mapping(aoi_name: str, scada_name: str, mapping_type: str, description: str) -> str:
    """Create a MAPS_TO_SCADA relationship between a PLC AOI and a SCADA component (UDT or Equipment)."""
    return _get_ontology_tools().execute("create_mapping", {
        "aoi_name": aoi_name, "scada_name": scada_name,
        "mapping_type": mapping_type, "description": description,
    })


# ═══════════════════════════════════════════════════════════════════════════
#  Live Ignition API
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
def get_current_time() -> str:
    """Get the current server date/time in local and UTC. Call before query_tag_history to know 'now'."""
    return _get_ontology_tools().execute("get_current_time", {})


@mcp.tool()
def browse_tags(path: str = "", depth: int = 1) -> str:
    """Browse the Ignition tag tree to discover tag folders and tag paths. Start with path='' to see top-level folders, then drill into subfolders. Use the full paths returned here when calling read_tag or query_tag_history."""
    api = _get_api_client()
    entities = api.browse_entities(path=path, depth=depth)
    return _json(entities)


@mcp.tool()
def read_tag(path: str) -> str:
    """Read a single Ignition tag's current value, quality, and timestamp from the live gateway. Path must be the full tag path including folders, e.g. '[default]Final_Process/FinalProduct_Temperature'. Use browse_tags first to discover available paths."""
    return _get_ontology_tools().execute("read_tag", {"path": path})


@mcp.tool()
def read_tags(paths: List[str]) -> str:
    """Read multiple Ignition tags at once. Returns value, quality, and timestamp for each. Paths must be full tag paths including folders, e.g. '[default]Final_Process/FinalProduct_Temperature'. Use browse_tags to discover paths."""
    return _get_ontology_tools().execute("read_tags", {"paths": paths})


@mcp.tool()
def query_tag_history(
    tag_paths: List[str],
    start_date: str,
    end_date: str,
    return_size: int = 100,
    aggregation_mode: str = "Average",
    return_format: str = "Wide",
    interval_minutes: Optional[int] = None,
) -> str:
    """Query historical values of Ignition tags over a time range. Tag paths must be full paths including folders, e.g. '[default]Final_Process/FinalProduct_Temperature'. Use browse_tags to discover paths. Call get_current_time first to construct relative time ranges."""
    args: dict = {
        "tag_paths": tag_paths, "start_date": start_date, "end_date": end_date,
        "return_size": return_size, "aggregation_mode": aggregation_mode,
        "return_format": return_format,
    }
    if interval_minutes is not None:
        args["interval_minutes"] = interval_minutes
    return _get_ontology_tools().execute("query_tag_history", args)


@mcp.tool()
def get_gateway_status() -> str:
    """Get Ignition gateway health: version, uptime, platform, active connections."""
    return _get_ontology_tools().execute("get_gateway_status", {})


@mcp.tool()
def get_alarm_status() -> str:
    """Get current state of all alarm notification pipelines on the Ignition gateway."""
    return _get_ontology_tools().execute("get_alarm_status", {})


# ═══════════════════════════════════════════════════════════════════════════
#  Anomaly monitoring
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
def list_anomaly_events(
    limit: int = 100,
    state: Optional[str] = None,
    severity: Optional[str] = None,
    run_id: Optional[str] = None,
) -> str:
    """List persisted anomaly events from the monitoring agent. Filter by state (active/acknowledged/cleared), severity (info/warning/critical), or run ID."""
    graph = _get_graph()
    events = graph.list_anomaly_events(limit=limit, state=state, severity=severity, run_id=run_id)
    return _json({"success": True, "events": events})


@mcp.tool()
def get_anomaly_event(event_id: str) -> str:
    """Get full details of a single anomaly event by its event ID."""
    graph = _get_graph()
    event = graph.get_anomaly_event(event_id)
    if event:
        return _json({"success": True, "event": event})
    return _json({"error": f"Not found: {event_id}"})


@mcp.tool()
def ack_anomaly_event(event_id: str, note: str = "") -> str:
    """Acknowledge an anomaly event, optionally with a note."""
    graph = _get_graph()
    with graph.session() as session:
        row = session.run(
            "MATCH (e:AnomalyEvent {event_id: $eid}) "
            "SET e.state='acknowledged', e.acknowledged_at=datetime(), "
            "e.ack_note=$note, e.updated_at=datetime() RETURN count(e) AS cnt",
            eid=event_id, note=note,
        ).single()
        if not row or row["cnt"] == 0:
            return _json({"error": f"Not found: {event_id}"})
    return _json({"success": True, "eventId": event_id})


@mcp.tool()
def clear_anomaly_event(event_id: str, note: str = "") -> str:
    """Clear an acknowledged anomaly event."""
    graph = _get_graph()
    with graph.session() as session:
        row = session.run(
            "MATCH (e:AnomalyEvent {event_id: $eid}) "
            "SET e.state='cleared', e.cleared_at=datetime(), "
            "e.clear_note=$note, e.updated_at=datetime() RETURN count(e) AS cnt",
            eid=event_id, note=note,
        ).single()
        if not row or row["cnt"] == 0:
            return _json({"error": f"Not found: {event_id}"})
    return _json({"success": True, "eventId": event_id})


@mcp.tool()
def deep_analyze_event(event_id: str) -> str:
    """Run LLM-powered deep triage on an existing anomaly event. Returns root cause analysis, probable causes, and recommended checks."""
    from anomaly_monitor import AgentCoordinator
    coordinator = AgentCoordinator(config={})
    return _json(coordinator.deep_analyze(event_id))


@mcp.tool()
def get_agent_status(run_id: str) -> str:
    """Get the status of an anomaly monitoring agent run."""
    graph = _get_graph()
    with graph.session() as session:
        row = session.run(
            "MATCH (r:AgentRun {run_id: $rid}) RETURN r LIMIT 1",
            rid=run_id,
        ).single()
        if not row:
            return _json({"error": f"Run not found: {run_id}"})
        props = dict(row["r"])
        return _json({
            "success": True,
            "status": props.get("status"),
            "metrics": {
                "cycleCount": props.get("cycle_count", 0),
                "lastCycleMs": props.get("last_cycle_ms", 0),
            },
            "lastHeartbeatAt": props.get("last_heartbeat_at"),
            "run": props,
        })


@mcp.tool()
def cleanup_anomaly_events(retention_days: int = 14) -> str:
    """Delete anomaly events older than the specified retention period."""
    graph = _get_graph()
    deleted = graph.cleanup_anomaly_events(retention_days)
    return _json({"success": True, "deleted": deleted})


# ═══════════════════════════════════════════════════════════════════════════
#  Anomaly scoring (stateless)
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
def compute_deviation_scores(
    current_value: float,
    history_values: List[float],
    prev_value: Optional[float] = None,
    thresholds: Optional[dict] = None,
) -> str:
    """Run deterministic anomaly scoring on a value against its history. Returns z-score, MAD score, delta rate, drift score, category (normal/spike/drift/stuck/deviation), and whether it's a candidate anomaly."""
    from anomaly_rules import compute_deviation_scores as _compute
    return _json(_compute(current_value, history_values, prev_value=prev_value, thresholds=thresholds))


# ═══════════════════════════════════════════════════════════════════════════
#  Database queries
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
def list_db_connections() -> str:
    """List all database connections defined in the Ignition project (name, type, URL)."""
    return _get_ontology_tools().execute("list_db_connections", {})


@mcp.tool()
def describe_db_schema(connection_name: str) -> str:
    """List all tables and columns for a database connection."""
    return _get_ontology_tools().execute("describe_db_schema", {"connection_name": connection_name})


@mcp.tool()
def execute_db_query(connection_name: str, query: str) -> str:
    """Execute a read-only SQL query (SELECT/SHOW/DESCRIBE/EXPLAIN only). Results limited to 100 rows."""
    return _get_ontology_tools().execute("execute_db_query", {"connection_name": connection_name, "query": query})


# ═══════════════════════════════════════════════════════════════════════════
#  MES / RCA tools
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
def get_batch_context(batch_name: str) -> str:
    """Get full batch context for root cause analysis: batch details, deviations, materials, production order."""
    return _get_ontology_tools().execute("get_batch_context", {"batch_name": batch_name})


@mcp.tool()
def get_equipment_rca(equipment_name: str) -> str:
    """Get equipment root cause analysis context: equipment details with full PLC/SCADA chain."""
    return _get_ontology_tools().execute("get_equipment_rca", {"equipment_name": equipment_name})


@mcp.tool()
def get_ccp_context(ccp_name: str) -> str:
    """Get Critical Control Point monitoring context."""
    return _get_ontology_tools().execute("get_ccp_context", {"ccp_name": ccp_name})


@mcp.tool()
def search_by_symptom(symptom_text: str) -> str:
    """Search the ontology by operator observations/symptoms to find related entities."""
    return _get_ontology_tools().execute("search_by_symptom", {"symptom_text": symptom_text})


@mcp.tool()
def trace_tag_impact(tag_path: str) -> str:
    """Trace a PLC tag upstream to its business impact (equipment, batches, CCPs, deviations)."""
    return _get_ontology_tools().execute("trace_tag_impact", {"tag_path": tag_path})


@mcp.tool()
def get_open_deviations() -> str:
    """Get all open process deviations with full context (batch, equipment, CCP)."""
    return _get_ontology_tools().execute("get_open_deviations", {})


# ═══════════════════════════════════════════════════════════════════════════
#  Entry point
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PLC/SCADA Ontology MCP Server")
    parser.add_argument("--transport", choices=["stdio", "sse"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    if args.transport == "sse":
        mcp.settings.host = args.host
        mcp.settings.port = args.port

    mcp.run(transport=args.transport)
