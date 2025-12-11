#!/usr/bin/env python3
"""
LLM-based analyzer for Ignition SCADA configurations using Anthropic's Claude API.
Generates semantic understanding of tags, UDTs, views, and data flows.
Stores results in Neo4j graph database.

Uses tool calls to query existing ontology data, enabling Claude to build
on existing knowledge rather than starting from scratch.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

from ignition_parser import IgnitionParser, IgnitionBackup
from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient, get_claude_client


class IgnitionOntologyAnalyzer:
    """Analyzes Ignition configurations using Claude to generate semantic ontologies."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5-20250929",
        graph: Optional[OntologyGraph] = None,
        client: Optional[ClaudeClient] = None,
    ):
        """Initialize the analyzer with Anthropic API and Neo4j connection."""
        load_dotenv()

        # Use provided client or create one
        if client:
            self._client = client
            self._owns_client = False
        else:
            self._client = ClaudeClient(
                api_key=api_key, model=model, graph=graph, enable_tools=True
            )
            self._owns_client = True

    @property
    def graph(self) -> OntologyGraph:
        """Access the Neo4j graph."""
        return self._client.graph

    def close(self):
        """Close resources if we own them."""
        if self._owns_client and self._client:
            self._client.close()
            self._client = None

    def analyze_backup(
        self, backup: IgnitionBackup, verbose: bool = False
    ) -> Dict[str, Any]:
        """Analyze an Ignition backup and store ontology in Neo4j."""

        if verbose:
            print(f"[INFO] Analyzing Ignition backup...")

        # Build context for LLM
        context = self._build_analysis_context(backup)

        # Generate analysis with tool support
        analysis = self._query_llm(context, verbose)

        # Store in Neo4j
        # Store UDTs from Claude's analysis (with semantic descriptions)
        for udt_name, udt_purpose in analysis.get("udt_semantics", {}).items():
            self.graph.create_udt(udt_name, udt_purpose, backup.file_path)

        # Also ensure UDTs from backup definitions exist (for view mapping)
        for udt_def in backup.udt_definitions:
            # Create if not already in Claude's analysis
            if udt_def.name not in analysis.get("udt_semantics", {}):
                self.graph.create_udt(udt_def.name, "", backup.file_path)

        # Store equipment instances
        for equip in analysis.get("equipment_instances", []):
            self.graph.create_equipment(
                equip.get("name", ""),
                equip.get("type", ""),
                equip.get("purpose", ""),
                equip.get("udt_name"),  # Link to UDT if specified
            )

        # Store views
        for view_name, view_purpose in analysis.get("view_purposes", {}).items():
            self.graph.create_view(view_name, "", view_purpose)

        # Automatically map views to UDTs based on tag bindings
        view_udt_mappings = self._extract_view_udt_mappings(backup, verbose)
        mappings_created = 0
        for view_name, udt_names in view_udt_mappings.items():
            for udt_name in udt_names:
                success = self.graph.create_view_udt_mapping(
                    view_name, udt_name, "displays"
                )
                if verbose:
                    if success:
                        print(f"[OK] Mapped View '{view_name}' -> UDT '{udt_name}'")
                        mappings_created += 1
                    else:
                        print(
                            f"[WARN] Failed to map View '{view_name}' -> UDT '{udt_name}' (nodes not found)"
                        )

        if verbose:
            print(f"[OK] Stored Ignition ontology in Neo4j")
            print(f"[INFO] Created {mappings_created} view-to-UDT mappings")

        ontology = {
            "source": "ignition",
            "source_file": backup.file_path,
            "version": backup.version,
            "summary": {
                "udt_definitions": len(backup.udt_definitions),
                "udt_instances": len(backup.udt_instances),
                "tags": len(backup.tags),
                "windows": len(backup.windows),
                "named_queries": len(backup.named_queries),
            },
            "analysis": analysis,
        }

        return ontology

    def _build_analysis_context(self, backup: IgnitionBackup) -> str:
        """Build context string for LLM analysis."""
        parts = []

        parts.append("# Ignition SCADA Configuration Analysis")
        parts.append("")

        # UDT Definitions
        if backup.udt_definitions:
            parts.append("## UDT (User Defined Type) Definitions")
            for udt in backup.udt_definitions:
                parent = f" extends {udt.parent_name}" if udt.parent_name else ""
                parts.append(f"\n### {udt.name}{parent}")

                if udt.parameters:
                    parts.append("Parameters:")
                    for pname, param in udt.parameters.items():
                        parts.append(f"  - {pname}: {param.data_type}")

                if udt.members:
                    parts.append("Members:")
                    for member in udt.members:
                        parts.append(
                            f"  - {member.name}: {member.data_type} [{member.tag_type}]"
                        )
            parts.append("")

        # UDT Instances
        if backup.udt_instances:
            parts.append("## UDT Instances (Tag Configurations)")
            for inst in backup.udt_instances:
                parts.append(f"- {inst.name}: {inst.type_id}")
                if inst.parameters:
                    for pname, pval in inst.parameters.items():
                        if pval:
                            parts.append(f"    {pname} = {pval}")
            parts.append("")

        # Standalone Tags
        if backup.tags:
            parts.append("## Standalone Tags")
            for tag in backup.tags:
                parts.append(f"- {tag.name}: {tag.tag_type}")
                if tag.query:
                    parts.append(f"    Query: {tag.query[:200]}...")
                if tag.datasource:
                    parts.append(f"    Datasource: {tag.datasource}")
            parts.append("")

        # Windows/Views
        if backup.windows:
            parts.append("## Views/Windows")
            for window in backup.windows[:10]:  # Limit to first 10
                parts.append(f"\n### {window.name} ({window.path})")
                self._describe_components(window.components, parts, indent=0)
            parts.append("")

        # Named Queries
        if backup.named_queries:
            parts.append("## Named Queries")
            for query in backup.named_queries:
                folder = f" ({query.folder_path})" if query.folder_path else ""
                parts.append(f"- {query.name}{folder}")
            parts.append("")

        # Tag references from UI
        parser = IgnitionParser()
        tag_refs = parser.get_all_tag_references(backup)
        if tag_refs:
            parts.append("## Tag References in UI Bindings")
            for ref in sorted(tag_refs)[:30]:  # Limit
                parts.append(f"- {ref}")
            parts.append("")

        return "\n".join(parts)

    def _describe_components(self, components: List, parts: List[str], indent: int):
        """Recursively describe UI components."""
        for comp in components:
            prefix = "  " * indent
            parts.append(f"{prefix}- {comp.component_type}: {comp.name}")

            for binding in comp.bindings:
                parts.append(
                    f"{prefix}    binding: {binding.property_path} <- [{binding.binding_type}] {binding.target}"
                )

            if comp.children:
                self._describe_components(comp.children, parts, indent + 1)

    def _extract_view_udt_mappings(
        self, backup: IgnitionBackup, verbose: bool = False
    ) -> Dict[str, set]:
        """Extract which views reference which UDTs based on tag bindings.

        Analyzes tag bindings in views to determine which UDT types each view uses.
        Uses multiple strategies:
        1. Direct tag instance matching
        2. UDT member name matching (for parameterized views like {TagPath}/HMI_MotorControl)
        3. View name to UDT name matching by convention

        Returns:
            Dict mapping view names to sets of UDT names they reference
        """
        if verbose:
            print(f"\n[DEBUG] === View-to-UDT Mapping Analysis ===")
            print(f"[DEBUG] Found {len(backup.udt_instances)} UDT instances")
            print(f"[DEBUG] Found {len(backup.windows)} windows/views")
            print(f"[DEBUG] Found {len(backup.udt_definitions)} UDT definitions")

        # Build a map of tag paths to their UDT types (normalized names)
        tag_to_udt = {}

        # Map UDT instances to their type (normalized to base name)
        for inst in backup.udt_instances:
            # Normalize type_id: "Types/MotorReversingControl" -> "MotorReversingControl"
            udt_name = self._normalize_udt_name(inst.type_id)

            # Tag path is typically the instance name
            tag_to_udt[inst.name] = udt_name
            # Also add with common path prefixes
            tag_to_udt[f"[default]{inst.name}"] = udt_name
            tag_to_udt[f"[.]/{inst.name}"] = udt_name

            if verbose:
                print(
                    f"[DEBUG] UDT instance: '{inst.name}' -> type '{udt_name}' (raw: {inst.type_id})"
                )

        # Build member-to-UDT map from UDT definitions
        # Maps member names like "HMI_MotorControl" to their parent UDT "MotorReversingControl"
        member_to_udt = {}
        for udt_def in backup.udt_definitions:
            for member in udt_def.members:
                member_to_udt[member.name] = udt_def.name
                if verbose:
                    print(
                        f"[DEBUG] UDT member: '{member.name}' -> parent UDT '{udt_def.name}'"
                    )

        # Map view names to UDTs they reference
        view_udt_map: Dict[str, set] = {}

        for window in backup.windows:
            view_name = window.name
            udts_used = set()

            # Extract all tag references from this view
            tag_refs = self._get_component_tag_refs(window.components)

            if verbose and tag_refs:
                print(f"[DEBUG] View '{view_name}' has {len(tag_refs)} tag bindings")
                for ref in list(tag_refs)[:5]:  # Show first 5
                    print(f"[DEBUG]   - {ref}")
                if len(tag_refs) > 5:
                    print(f"[DEBUG]   ... and {len(tag_refs) - 5} more")

            for tag_ref in tag_refs:
                # Strategy 1: Direct tag instance matching
                udt_type = self._resolve_tag_to_udt(tag_ref, tag_to_udt, backup)
                if udt_type:
                    udts_used.add(udt_type)
                    continue

                # Strategy 2: UDT member matching for parameterized views
                # e.g., "{TagPath}/HMI_MotorControl/iStatus" -> look for "HMI_MotorControl" in member_to_udt
                matched_udt = self._match_tag_to_udt_member(
                    tag_ref, member_to_udt, verbose
                )
                if matched_udt:
                    udts_used.add(matched_udt)

            if udts_used:
                view_udt_map[view_name] = udts_used
                if verbose:
                    print(
                        f"[INFO] View '{view_name}' uses UDTs: {', '.join(udts_used)}"
                    )
            elif verbose and tag_refs:
                print(
                    f"[DEBUG] View '{view_name}' has tag bindings but no UDT matches found"
                )

        if verbose:
            print(f"[DEBUG] === End View-to-UDT Analysis ===\n")

        return view_udt_map

    def _match_tag_to_udt_member(
        self, tag_ref: str, member_to_udt: Dict[str, str], verbose: bool = False
    ) -> Optional[str]:
        """Match a tag reference to a UDT via member name.

        For parameterized views like "{TagPath}/HMI_MotorControl/iStatus",
        extract path segments and look for matches in member_to_udt map.
        """
        # Split on / and . to get all path segments
        # "{TagPath}/HMI_MotorControl/iStatus" -> ["TagPath", "HMI_MotorControl", "iStatus"]
        cleaned = (
            tag_ref.replace("{", "")
            .replace("}", "")
            .replace("[default]", "")
            .replace("[.]", "")
        )
        segments = []
        for part in cleaned.split("/"):
            segments.extend(part.split("."))

        for segment in segments:
            if segment in member_to_udt:
                if verbose:
                    print(
                        f"[DEBUG]   Matched member '{segment}' -> UDT '{member_to_udt[segment]}'"
                    )
                return member_to_udt[segment]

        return None

    def _normalize_udt_name(self, type_id: str) -> str:
        """Normalize a UDT type_id to its base name.

        Handles formats like:
        - "Types/MotorReversingControl" -> "MotorReversingControl"
        - "com.example/MyUDT" -> "MyUDT"
        - "MotorReversingControl" -> "MotorReversingControl"
        """
        if "/" in type_id:
            return type_id.split("/")[-1]
        return type_id

    def _get_component_tag_refs(self, components: List) -> set:
        """Recursively extract all tag references from UI components."""
        refs = set()
        for comp in components:
            for binding in comp.bindings:
                if binding.binding_type == "tag" and binding.target:
                    refs.add(binding.target)
            if comp.children:
                refs.update(self._get_component_tag_refs(comp.children))
        return refs

    def _resolve_tag_to_udt(
        self, tag_ref: str, tag_to_udt: Dict[str, str], backup: IgnitionBackup
    ) -> Optional[str]:
        """Resolve a tag reference to its UDT type (normalized name).

        Tag references can be in various formats:
        - [default]Equipment/Motor01.Status
        - [.]Equipment/Motor01.Command
        - Motor01.Running
        """
        # Direct lookup (already normalized in tag_to_udt)
        if tag_ref in tag_to_udt:
            return tag_to_udt[tag_ref]

        # Try extracting the base tag (before any member access)
        # e.g., "[default]Equipment/Motor01.Status" -> "Motor01"
        parts = tag_ref.replace("[default]", "").replace("[.]", "").split("/")

        for part in parts:
            # Split off any member access
            base_tag = part.split(".")[0]

            # Look for this in our UDT instances
            for inst in backup.udt_instances:
                if inst.name == base_tag or inst.name.endswith("/" + base_tag):
                    # Return normalized name
                    return self._normalize_udt_name(inst.type_id)

        return None

    def _query_llm(self, context: str, verbose: bool = False) -> Dict[str, Any]:
        """Query Claude API for analysis with tool support."""

        system_prompt = """You are an expert in industrial automation and SCADA systems, specializing in Ignition by Inductive Automation. Your task is to analyze Ignition configurations and generate semantic ontologies.

You have access to tools to query the existing ontology database:
- get_schema: Discover what node types exist (AOI, UDT, Tag, etc.)
- run_query: Execute Cypher queries to explore existing data
- get_node: Get details of specific components

USE THESE TOOLS to explore what PLC components already exist. This helps you:
- Identify how SCADA UDTs might map to existing PLC AOIs
- Find relationships between SCADA and PLC components
- Use consistent terminology with the PLC layer
- Build on existing knowledge

Focus on the industrial/operational meaning, not just the technical structure. Identify patterns like:
- Equipment templates (motors, valves, sensors)
- HMI patterns (dashboards, control panels, data displays)
- Data pathways (OPC to tag to UI binding)
- Hierarchical organization (areas, lines, equipment)"""

        user_prompt = f"""Analyze this Ignition SCADA configuration and generate a semantic ontology.

FIRST, use the available tools to explore existing data:
1. Use get_schema to see the current graph structure
2. Query for AOI nodes to understand PLC components
3. Look for existing UDT or Equipment nodes

THEN, provide your analysis as a structured JSON object with these fields:
- "system_purpose": string describing what this SCADA system monitors/controls
- "udt_semantics": object mapping UDT names to their industrial purpose
- "equipment_instances": array of {{name, type, purpose, plc_connection}} for each UDT instance
- "data_flows": array describing how data moves from PLC to UI
- "view_purposes": object mapping view names to their operational purpose
- "tag_categories": object grouping tags by their function (control, status, setpoint, etc.)
- "integration_points": array of external system connections (OPC servers, databases, etc.)
- "operational_patterns": array of identified patterns in the configuration

Be concise but informative. Focus on industrial/operational semantics.

## Configuration to Analyze:

{context}"""

        if verbose:
            print("[INFO] Querying Claude API with tool support...")

        result = self._client.query_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=20000,
            use_tools=True,
            verbose=verbose,
        )

        if verbose and result.get("tool_calls"):
            print(f"[INFO] Claude made {len(result['tool_calls'])} tool calls")

        if result.get("data"):
            return result["data"]
        else:
            return {
                "error": result.get("error", "Unknown error"),
                "raw_response": result.get("raw_text", "")[:1000],
            }

    def get_all_udts(self) -> List[Dict]:
        """Get all UDTs from Neo4j."""
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (u:UDT)
                RETURN u.name as name, u.purpose as purpose, u.source_file as source_file
            """
            )
            return [dict(r) for r in result]

    def get_all_equipment(self) -> List[Dict]:
        """Get all equipment from Neo4j."""
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (e:Equipment)
                OPTIONAL MATCH (e)-[:INSTANCE_OF]->(u:UDT)
                RETURN e.name as name, e.type as type, e.purpose as purpose,
                       u.name as udt_name
            """
            )
            return [dict(r) for r in result]

    def get_all_views(self) -> List[Dict]:
        """Get all views from Neo4j."""
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (v:View)
                RETURN v.name as name, v.path as path, v.purpose as purpose
            """
            )
            return [dict(r) for r in result]


def main():
    """CLI for Ignition ontology analyzer."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze Ignition backup JSON and generate semantic ontology (stored in Neo4j)"
    )
    parser.add_argument("input", nargs="?", help="Path to Ignition backup JSON file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--model", default="claude-sonnet-4-5-20250929", help="Claude model"
    )
    parser.add_argument(
        "--no-tools", action="store_true", help="Disable Neo4j tool calls"
    )
    parser.add_argument(
        "--list-udts", action="store_true", help="List all UDTs in Neo4j"
    )
    parser.add_argument(
        "--list-equipment", action="store_true", help="List all equipment in Neo4j"
    )
    parser.add_argument(
        "--list-views", action="store_true", help="List all views in Neo4j"
    )
    parser.add_argument("--export", metavar="FILE", help="Export analysis to JSON file")

    args = parser.parse_args()

    client = ClaudeClient(model=args.model, enable_tools=not args.no_tools)
    analyzer = IgnitionOntologyAnalyzer(client=client)

    try:
        if args.list_udts:
            udts = analyzer.get_all_udts()
            print(f"\n[INFO] Found {len(udts)} UDTs:\n")
            for udt in udts:
                print(f"  {udt['name']}: {udt.get('purpose', 'N/A')[:80]}...")

        elif args.list_equipment:
            equipment = analyzer.get_all_equipment()
            print(f"\n[INFO] Found {len(equipment)} equipment instances:\n")
            for eq in equipment:
                udt = f" (UDT: {eq['udt_name']})" if eq.get("udt_name") else ""
                print(f"  {eq['name']}: {eq.get('type', 'N/A')}{udt}")

        elif args.list_views:
            views = analyzer.get_all_views()
            print(f"\n[INFO] Found {len(views)} views:\n")
            for v in views:
                print(f"  {v['name']}: {v.get('purpose', 'N/A')[:60]}...")

        elif args.input:
            # Parse backup
            ignition_parser = IgnitionParser()
            backup = ignition_parser.parse_file(args.input)

            print(
                f"[INFO] Parsed: {len(backup.udt_definitions)} UDTs, {len(backup.udt_instances)} instances, {len(backup.windows)} views"
            )

            # Analyze and store in Neo4j
            ontology = analyzer.analyze_backup(backup, verbose=args.verbose)

            # Export if requested
            if args.export:
                with open(args.export, "w", encoding="utf-8") as f:
                    json.dump(ontology, f, indent=2)
                print(f"[OK] Exported analysis to {args.export}")
            else:
                print("\n=== Ignition Ontology ===")
                print(
                    f"System Purpose: {ontology['analysis'].get('system_purpose', 'N/A')}"
                )
                print(
                    f"UDTs: {list(ontology['analysis'].get('udt_semantics', {}).keys())}"
                )

        else:
            parser.print_help()

    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
