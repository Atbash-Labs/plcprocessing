#!/usr/bin/env python3
"""
Post-backup enrichment with live Ignition API data.

After ignition_ontology.py creates entities in Neo4j from the backup,
this module optionally calls the live Ignition API to add runtime metadata:

- Tag values:       live_value, live_quality, live_timestamp on Equipment/ScadaTag nodes
- Connection health: OPC connection status on Equipment/Device nodes
- Gateway metadata:  version, uptime, platform on a Gateway node
- Alarm states:      pipeline states linked to relevant entities

This is a read-only, additive step — it updates existing Neo4j nodes
with additional properties rather than creating new node types.
"""

import json
import logging
from typing import Optional

from ignition_api_client import IgnitionApiClient
from neo4j_ontology import OntologyGraph

logger = logging.getLogger(__name__)


class LiveEnricher:
    """Enriches the Neo4j ontology graph with live data from the Ignition API."""

    def __init__(self, graph: OntologyGraph, api_client: IgnitionApiClient):
        self.graph = graph
        self.api = api_client

    def enrich_all(self, verbose: bool = False) -> dict:
        """Run all enrichment steps and return a summary.

        Returns:
            Dict with counts of updates made by each step.
        """
        summary = {}

        summary["gateway"] = self.enrich_gateway_metadata(verbose)
        summary["connections"] = self.enrich_connection_health(verbose)
        summary["tags"] = self.enrich_tag_values(verbose)
        summary["alarms"] = self.enrich_alarm_states(verbose)

        if verbose:
            total = sum(summary.values())
            print(f"[OK] Live enrichment complete – {total} updates applied")
            for step, count in summary.items():
                print(f"  {step}: {count}")

        return summary

    # ------------------------------------------------------------------ #
    #  Gateway metadata
    # ------------------------------------------------------------------ #

    def enrich_gateway_metadata(self, verbose: bool = False) -> int:
        """Store gateway version, uptime, platform info on a Gateway node."""
        overview = self.api.get_gateway_overview()
        if overview is None:
            if verbose:
                print("[WARN] Could not fetch gateway overview – skipping")
            return 0

        with self.graph.session() as session:
            session.run("""
                MERGE (g:Gateway {name: 'IgnitionGateway'})
                SET g.live_version      = $version,
                    g.live_state        = $state,
                    g.live_platform     = $platform,
                    g.live_uptime_ms    = $uptime_ms,
                    g.live_edition      = $edition,
                    g.live_updated_at   = datetime()
            """, {
                "version": overview.version,
                "state": overview.state,
                "platform": overview.platform,
                "uptime_ms": overview.uptime_ms,
                "edition": overview.edition,
            })

        if verbose:
            print(f"[OK] Gateway: {overview.version} ({overview.state}), "
                  f"uptime {_fmt_uptime(overview.uptime_ms)}")
        return 1

    # ------------------------------------------------------------------ #
    #  Connection health
    # ------------------------------------------------------------------ #

    def enrich_connection_health(self, verbose: bool = False) -> int:
        """Check OPC / DB connection status and store on relevant nodes."""
        connections = self.api.get_connections()
        if not connections:
            if verbose:
                print("[WARN] No connections returned – skipping")
            return 0

        updated = 0
        with self.graph.session() as session:
            for conn in connections:
                result = session.run("""
                    OPTIONAL MATCH (e:Equipment)
                    WHERE toLower(e.name) CONTAINS toLower($conn_name)
                    WITH collect(e) AS equips, $conn_name AS cn, $status AS st, $server_type AS stype
                    FOREACH (e IN equips |
                        SET e.live_connection_name   = cn,
                            e.live_connection_status = st,
                            e.live_connection_type   = stype,
                            e.live_conn_updated_at   = datetime()
                    )
                    RETURN size(equips) AS matched
                """, {
                    "conn_name": conn.name,
                    "status": conn.status,
                    "server_type": conn.server_type or "",
                })
                record = result.single()
                matched = record["matched"] if record else 0
                updated += matched

                if verbose:
                    print(f"  Connection '{conn.name}': {conn.status} "
                          f"({conn.server_type or 'N/A'}) – matched {matched} equipment nodes")

        if verbose:
            print(f"[OK] Updated {updated} equipment nodes with connection health")
        return updated

    # ------------------------------------------------------------------ #
    #  Tag values
    # ------------------------------------------------------------------ #

    def enrich_tag_values(self, verbose: bool = False) -> int:
        """Read live values for tags associated with Equipment or ScadaTag nodes."""
        tag_paths = self._collect_tag_paths()
        if not tag_paths:
            if verbose:
                print("[INFO] No tag paths found in the graph – skipping tag enrichment")
            return 0

        if verbose:
            print(f"[INFO] Reading live values for {len(tag_paths)} tag paths...")

        updated = 0
        for path in tag_paths:
            tv = self.api.read_tag(path)
            if tv.error:
                if verbose:
                    logger.debug("Tag %s: %s", path, tv.error)
                continue

            with self.graph.session() as session:
                # Update ScadaTag nodes
                result = session.run("""
                    MATCH (t:ScadaTag)
                    WHERE t.name = $name OR t.opc_item_path = $path
                    SET t.live_value     = $value,
                        t.live_quality   = $quality,
                        t.live_timestamp = $timestamp,
                        t.live_data_type = $data_type,
                        t.live_updated_at = datetime()
                    RETURN count(t) AS cnt
                """, {
                    "name": path,
                    "path": path,
                    "value": _safe_value(tv.value),
                    "quality": tv.quality,
                    "timestamp": tv.timestamp or "",
                    "data_type": tv.data_type or "",
                })
                record = result.single()
                updated += record["cnt"] if record else 0

        if verbose:
            print(f"[OK] Updated {updated} tag nodes with live values")
        return updated

    def _collect_tag_paths(self) -> list:
        """Gather tag paths from ScadaTag and Equipment nodes in the graph."""
        paths = []
        with self.graph.session() as session:
            result = session.run("""
                MATCH (t:ScadaTag)
                WHERE t.opc_item_path IS NOT NULL AND t.opc_item_path <> ''
                RETURN t.opc_item_path AS path
                UNION
                MATCH (t:ScadaTag)
                WHERE t.name IS NOT NULL AND t.name <> ''
                RETURN t.name AS path
            """)
            seen = set()
            for record in result:
                p = record["path"]
                if p and p not in seen:
                    paths.append(p)
                    seen.add(p)
        return paths

    # ------------------------------------------------------------------ #
    #  Alarm pipeline states
    # ------------------------------------------------------------------ #

    def enrich_alarm_states(self, verbose: bool = False) -> int:
        """Read alarm pipeline states and store as properties."""
        pipelines = self.api.get_alarm_pipelines()
        if not pipelines:
            if verbose:
                print("[INFO] No alarm pipelines returned – skipping")
            return 0

        updated = 0
        with self.graph.session() as session:
            for pipeline in pipelines:
                p_name = pipeline.get("name", "")
                if not p_name:
                    continue

                session.run("""
                    MERGE (ap:AlarmPipeline {name: $name})
                    SET ap.live_state     = $state,
                        ap.live_enabled   = $enabled,
                        ap.live_updated_at = datetime(),
                        ap += $extra
                """, {
                    "name": p_name,
                    "state": pipeline.get("state", ""),
                    "enabled": pipeline.get("enabled", True),
                    "extra": {k: _safe_value(v) for k, v in pipeline.items()
                              if k not in ("name", "state", "enabled")
                              and isinstance(v, (str, int, float, bool))},
                })
                updated += 1

                if verbose:
                    print(f"  Alarm pipeline '{p_name}': {pipeline.get('state', 'N/A')}")

        if verbose:
            print(f"[OK] Updated {updated} alarm pipelines")
        return updated


# ------------------------------------------------------------------ #
#  Helpers
# ------------------------------------------------------------------ #

def _safe_value(v):
    """Convert a value to a type Neo4j can store."""
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return v
    return json.dumps(v, default=str)


def _fmt_uptime(ms: Optional[int]) -> str:
    if ms is None:
        return "N/A"
    secs = ms // 1000
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


# ------------------------------------------------------------------ #
#  CLI
# ------------------------------------------------------------------ #

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrich Neo4j ontology with live Ignition API data"
    )
    parser.add_argument("--api-url", help="Ignition gateway URL (or set IGNITION_API_URL)")
    parser.add_argument("--api-token", help="API token (or set IGNITION_API_TOKEN)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--step",
        choices=["gateway", "connections", "tags", "alarms", "all"],
        default="all",
        help="Which enrichment step(s) to run (default: all)",
    )

    args = parser.parse_args()

    from neo4j_ontology import get_ontology_graph

    graph = get_ontology_graph()
    api_client = IgnitionApiClient(base_url=args.api_url, api_token=args.api_token)

    if not api_client.is_configured:
        print("[ERROR] No API URL configured. Pass --api-url or set IGNITION_API_URL.")
        return

    enricher = LiveEnricher(graph, api_client)

    if args.step == "all":
        enricher.enrich_all(verbose=args.verbose)
    elif args.step == "gateway":
        enricher.enrich_gateway_metadata(verbose=args.verbose)
    elif args.step == "connections":
        enricher.enrich_connection_health(verbose=args.verbose)
    elif args.step == "tags":
        enricher.enrich_tag_values(verbose=args.verbose)
    elif args.step == "alarms":
        enricher.enrich_alarm_states(verbose=args.verbose)


if __name__ == "__main__":
    main()
