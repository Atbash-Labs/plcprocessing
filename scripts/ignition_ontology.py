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
        client: Optional[ClaudeClient] = None
    ):
        """Initialize the analyzer with Anthropic API and Neo4j connection."""
        load_dotenv()
        
        # Use provided client or create one
        if client:
            self._client = client
            self._owns_client = False
        else:
            self._client = ClaudeClient(
                api_key=api_key,
                model=model,
                graph=graph,
                enable_tools=True
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

    def analyze_backup(self, backup: IgnitionBackup, verbose: bool = False) -> Dict[str, Any]:
        """Analyze an Ignition backup and store ontology in Neo4j."""

        if verbose:
            print(f"[INFO] Analyzing Ignition backup...")

        # Build context for LLM
        context = self._build_analysis_context(backup)

        # Generate analysis with tool support
        analysis = self._query_llm(context, verbose)

        # Store in Neo4j
        # Store UDTs
        for udt_name, udt_purpose in analysis.get('udt_semantics', {}).items():
            self.graph.create_udt(udt_name, udt_purpose, backup.file_path)
        
        # Store equipment instances
        for equip in analysis.get('equipment_instances', []):
            self.graph.create_equipment(
                equip.get('name', ''),
                equip.get('type', ''),
                equip.get('purpose', ''),
                equip.get('udt_name'),  # Link to UDT if specified
            )
        
        # Store views
        for view_name, view_purpose in analysis.get('view_purposes', {}).items():
            self.graph.create_view(view_name, '', view_purpose)
        
        if verbose:
            print(f"[OK] Stored Ignition ontology in Neo4j")

        ontology = {
            'source': 'ignition',
            'source_file': backup.file_path,
            'version': backup.version,
            'summary': {
                'udt_definitions': len(backup.udt_definitions),
                'udt_instances': len(backup.udt_instances),
                'tags': len(backup.tags),
                'windows': len(backup.windows),
                'named_queries': len(backup.named_queries)
            },
            'analysis': analysis
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
                        parts.append(f"  - {member.name}: {member.data_type} [{member.tag_type}]")
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
                parts.append(f"{prefix}    binding: {binding.property_path} <- [{binding.binding_type}] {binding.target}")

            if comp.children:
                self._describe_components(comp.children, parts, indent + 1)

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
            verbose=verbose
        )

        if verbose and result.get("tool_calls"):
            print(f"[INFO] Claude made {len(result['tool_calls'])} tool calls")

        if result.get("data"):
            return result["data"]
        else:
            return {
                "error": result.get("error", "Unknown error"),
                "raw_response": result.get("raw_text", "")[:1000]
            }

    def get_all_udts(self) -> List[Dict]:
        """Get all UDTs from Neo4j."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (u:UDT)
                RETURN u.name as name, u.purpose as purpose, u.source_file as source_file
            """)
            return [dict(r) for r in result]

    def get_all_equipment(self) -> List[Dict]:
        """Get all equipment from Neo4j."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (e:Equipment)
                OPTIONAL MATCH (e)-[:INSTANCE_OF]->(u:UDT)
                RETURN e.name as name, e.type as type, e.purpose as purpose,
                       u.name as udt_name
            """)
            return [dict(r) for r in result]

    def get_all_views(self) -> List[Dict]:
        """Get all views from Neo4j."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (v:View)
                RETURN v.name as name, v.path as path, v.purpose as purpose
            """)
            return [dict(r) for r in result]


def main():
    """CLI for Ignition ontology analyzer."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze Ignition backup JSON and generate semantic ontology (stored in Neo4j)"
    )
    parser.add_argument('input', nargs='?', help='Path to Ignition backup JSON file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--model', default='claude-sonnet-4-5-20250929', help='Claude model')
    parser.add_argument('--no-tools', action='store_true',
                       help='Disable Neo4j tool calls')
    parser.add_argument('--list-udts', action='store_true', help='List all UDTs in Neo4j')
    parser.add_argument('--list-equipment', action='store_true', help='List all equipment in Neo4j')
    parser.add_argument('--list-views', action='store_true', help='List all views in Neo4j')
    parser.add_argument('--export', metavar='FILE', help='Export analysis to JSON file')

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
                udt = f" (UDT: {eq['udt_name']})" if eq.get('udt_name') else ""
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

            print(f"[INFO] Parsed: {len(backup.udt_definitions)} UDTs, {len(backup.udt_instances)} instances, {len(backup.windows)} views")

            # Analyze and store in Neo4j
            ontology = analyzer.analyze_backup(backup, verbose=args.verbose)

            # Export if requested
            if args.export:
                with open(args.export, 'w', encoding='utf-8') as f:
                    json.dump(ontology, f, indent=2)
                print(f"[OK] Exported analysis to {args.export}")
            else:
                print("\n=== Ignition Ontology ===")
                print(f"System Purpose: {ontology['analysis'].get('system_purpose', 'N/A')}")
                print(f"UDTs: {list(ontology['analysis'].get('udt_semantics', {}).keys())}")
        
        else:
            parser.print_help()
    
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
