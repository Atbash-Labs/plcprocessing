#!/usr/bin/env python3
"""
Unified ontology merger that combines L5X (PLC) and Ignition (SCADA) ontologies.
Creates a comprehensive system ontology showing how PLCs and HMI work together.
Stores results in Neo4j graph database.

Uses tool calls to query existing ontology data, enabling Claude to build
on existing knowledge rather than starting from scratch.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

from neo4j_ontology import OntologyGraph, get_ontology_graph, import_json_ontology
from claude_client import ClaudeClient, get_claude_client


class UnifiedOntologyMerger:
    """Merges PLC and SCADA ontologies into a unified system view."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5-20250929",
        graph: Optional[OntologyGraph] = None,
        client: Optional[ClaudeClient] = None
    ):
        """Initialize with Anthropic API and Neo4j connection."""
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

    def merge_from_neo4j(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Generate unified analysis from data already in Neo4j.
        Assumes both PLC and SCADA data have been imported.
        """
        # Get all AOIs (PLC)
        aois = self.graph.get_all_aois()
        
        # Get visualization data (includes UDTs, Equipment, Views)
        viz_data = self.graph.get_graph_for_visualization()
        
        if verbose:
            print(f"[INFO] Found {len(aois)} AOIs in Neo4j")
            print(f"[INFO] Found {len(viz_data['nodes'])} total nodes")
        
        # Build context for LLM
        context = self._build_merge_context_from_neo4j(aois, viz_data)
        
        # Query LLM for unified analysis with tool support
        unified_analysis = self._query_merge_llm(context, verbose)
        
        # Store unified analysis in Neo4j
        self.graph.create_system_overview(
            overview=unified_analysis.get('system_overview', ''),
            safety_architecture=unified_analysis.get('safety_architecture'),
            control_responsibilities=unified_analysis.get('control_responsibilities'),
        )
        
        # Store PLC-to-SCADA mappings
        for mapping in unified_analysis.get('plc_to_scada_mappings', []):
            self.graph.create_plc_scada_mapping(
                mapping.get('plc_component', ''),
                mapping.get('scada_component', ''),
                mapping.get('mapping_type', ''),
                mapping.get('description', ''),
            )
        
        # Store end-to-end flows
        for flow in unified_analysis.get('end_to_end_flows', []):
            flow_name = flow.get('flow_name', flow.get('name', 'Unknown'))
            self.graph.create_end_to_end_flow(flow_name, flow)
        
        if verbose:
            print(f"[OK] Stored unified analysis in Neo4j")
        
        return unified_analysis

    def merge_ontologies(self, l5x_ontology_path: str, ignition_ontology_path: str,
                         verbose: bool = False) -> Dict[str, Any]:
        """
        Merge L5X and Ignition ontologies into unified system ontology.
        Imports both to Neo4j and generates unified analysis.
        """
        # Import both ontologies to Neo4j
        if verbose:
            print(f"[INFO] Importing L5X ontology from {l5x_ontology_path}...")
        import_json_ontology(l5x_ontology_path, self.graph)
        
        if verbose:
            print(f"[INFO] Importing Ignition ontology from {ignition_ontology_path}...")
        import_json_ontology(ignition_ontology_path, self.graph)
        
        # Generate unified analysis
        return self.merge_from_neo4j(verbose)

    def _build_merge_context_from_neo4j(self, aois: List[Dict], viz_data: Dict) -> str:
        """Build context for LLM merge analysis from Neo4j data."""
        parts = []

        parts.append("# System Integration Analysis: PLC + SCADA")
        parts.append("")

        # L5X (PLC) summary
        parts.append("## PLC Layer (Rockwell L5X)")
        parts.append("")

        for aoi in aois[:8]:  # Limit
            name = aoi.get('name', 'Unknown')
            analysis = aoi.get('analysis', {})
            purpose = analysis.get('purpose', 'No description')

            parts.append(f"### AOI: {name}")
            parts.append(f"Purpose: {purpose[:500]}")

            # Key tags
            tags = analysis.get('tags', {})
            if tags:
                parts.append("Key tags:")
                for tag_name, tag_desc in list(tags.items())[:10]:
                    desc_str = tag_desc if isinstance(tag_desc, str) else str(tag_desc)
                    parts.append(f"  - {tag_name}: {desc_str[:100]}")

            # Relationships
            rels = analysis.get('relationships', [])
            if rels:
                parts.append("Key relationships:")
                for rel in rels[:5]:
                    parts.append(f"  - {rel.get('from')} -> {rel.get('to')}: {rel.get('relationship_type')}")

            parts.append("")

        # SCADA summary from visualization data
        parts.append("\n## SCADA Layer")
        parts.append("")

        scada_nodes = [n for n in viz_data['nodes'] if n['type'] in ('udt', 'equipment', 'view')]
        
        if scada_nodes:
            parts.append("### SCADA Components")
            for node in scada_nodes[:15]:
                purpose = node['details'].get('purpose', '')
                parts.append(f"- {node['label']} ({node['type']}): {purpose[:100]}")
            parts.append("")

        return "\n".join(parts)

    def _query_merge_llm(self, context: str, verbose: bool = False) -> Dict[str, Any]:
        """Query LLM to generate unified analysis with tool support."""

        system_prompt = """You are an expert industrial automation architect who understands both PLC programming and SCADA/HMI systems. Your task is to analyze PLC and SCADA ontologies together and create a UNIFIED SYSTEM ONTOLOGY.

You have access to tools to query AND MODIFY the ontology database:
- get_schema: Discover all node types (AOI, UDT, Equipment, View, etc.)
- run_query: Execute Cypher queries to explore data and relationships
- get_node: Get details of specific components
- create_mapping: CREATE a MAPS_TO_SCADA relationship between PLC and SCADA components

USE THESE TOOLS to explore the existing ontology and CREATE MAPPINGS. This helps you:
- Identify accurate PLC-to-SCADA mappings
- Understand the full data flow from field to HMI
- Find integration points between systems
- Build on existing knowledge

Focus on creating a holistic view that an automation engineer could use to understand the ENTIRE system."""

        user_prompt = f"""Analyze the PLC and SCADA ontologies and create a UNIFIED SYSTEM ONTOLOGY.

STEP 1: Explore the graph:
- Use get_schema to see what node types exist
- Query for AOI nodes: MATCH (a:AOI) RETURN a.name
- Query for UDT nodes: MATCH (u:UDT) RETURN u.name
- Look at their purposes to understand what matches

STEP 2: CREATE MAPPINGS using the create_mapping tool:
For each PLC component (AOI) that has a corresponding SCADA component (UDT), 
call create_mapping with the EXACT node names. Examples:
- Motor_Reversing (AOI) might map to MotorReversingControl (UDT)
- Valve_Solenoid (AOI) might map to ValveSolenoidControl (UDT)
- IO_DigitalInput (AOI) might map to DigitalInput (UDT)

Use the EXACT names from your queries - no fuzzy matching, use the real node names.

STEP 3: Provide your analysis as JSON with these fields:
- "system_overview": High-level description of what this automation system does
- "plc_to_scada_mappings": Array of the mappings you created (for reference)
- "end_to_end_flows": Array describing complete data paths from sensors through PLC logic to operator displays
- "equipment_hierarchy": Object describing the physical/logical equipment organization
- "control_responsibilities": Object mapping which layer (PLC vs SCADA) handles what functions
- "integration_points": Array of specific tag/data connections between systems
- "operational_modes": Description of how operators interact with the system through different modes
- "safety_architecture": How safety is distributed between PLC and SCADA layers
- "recommendations": Array of observations about the system architecture

Output ONLY valid JSON, no markdown formatting. Be comprehensive but concise.

## Context:

{context}"""

        if verbose:
            print("[INFO] Querying Claude for unified analysis with tool support...")

        result = self._client.query_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=16000,
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
                "raw": result.get("raw_text", "")
            }

    def get_system_overview(self) -> Optional[Dict]:
        """Get the stored system overview from Neo4j."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (s:SystemOverview {id: 'main'})
                RETURN s.overview as overview, s.safety_architecture as safety,
                       s.control_responsibilities as control
            """)
            record = result.single()
            if record:
                return {
                    'overview': record['overview'],
                    'safety_architecture': record['safety'],
                    'control_responsibilities': record['control'],
                }
        return None

    def get_plc_scada_mappings(self) -> List[Dict]:
        """Get all PLC-to-SCADA mappings from Neo4j."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (plc:AOI)-[r:MAPS_TO_SCADA]->(scada)
                RETURN plc.name as plc_component, 
                       coalesce(scada.name, 'unknown') as scada_component,
                       r.mapping_type as mapping_type,
                       r.description as description
            """)
            return [dict(r) for r in result]


def main():
    """CLI for unified ontology merger."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Merge L5X and Ignition ontologies into unified system ontology (stored in Neo4j)"
    )
    parser.add_argument('--merge', nargs=2, metavar=('L5X_JSON', 'IGNITION_JSON'),
                       help='Import and merge two JSON ontology files')
    parser.add_argument('--analyze', action='store_true',
                       help='Generate unified analysis from data already in Neo4j')
    parser.add_argument('--overview', action='store_true',
                       help='Show stored system overview')
    parser.add_argument('--mappings', action='store_true',
                       help='Show PLC-to-SCADA mappings')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--model', default='claude-sonnet-4-5-20250929', help='Claude model')
    parser.add_argument('--no-tools', action='store_true',
                       help='Disable Neo4j tool calls')
    parser.add_argument('--export', metavar='FILE',
                       help='Export unified analysis to JSON file')

    args = parser.parse_args()

    client = ClaudeClient(model=args.model, enable_tools=not args.no_tools)
    merger = UnifiedOntologyMerger(client=client)

    try:
        if args.merge:
            l5x_path, ignition_path = args.merge
            unified = merger.merge_ontologies(l5x_path, ignition_path, verbose=args.verbose)
            
            print(f"\n[OK] Created unified ontology")
            print(f"System Overview: {unified.get('system_overview', 'N/A')[:200]}...")
            
            if args.export:
                with open(args.export, 'w', encoding='utf-8') as f:
                    json.dump(unified, f, indent=2)
                print(f"[OK] Exported to {args.export}")
        
        elif args.analyze:
            unified = merger.merge_from_neo4j(verbose=args.verbose)
            
            print(f"\n[OK] Generated unified analysis")
            print(f"System Overview: {unified.get('system_overview', 'N/A')[:200]}...")
            
            if args.export:
                with open(args.export, 'w', encoding='utf-8') as f:
                    json.dump(unified, f, indent=2)
                print(f"[OK] Exported to {args.export}")
        
        elif args.overview:
            overview = merger.get_system_overview()
            if overview:
                print("\n=== System Overview ===")
                print(overview.get('overview', 'N/A'))
                print("\n=== Safety Architecture ===")
                print(overview.get('safety_architecture', 'N/A'))
                print("\n=== Control Responsibilities ===")
                print(overview.get('control_responsibilities', 'N/A'))
            else:
                print("[INFO] No system overview found. Run --analyze first.")
        
        elif args.mappings:
            mappings = merger.get_plc_scada_mappings()
            if mappings:
                print(f"\n[INFO] Found {len(mappings)} PLC-to-SCADA mappings:\n")
                for m in mappings:
                    print(f"  {m['plc_component']} -> {m['scada_component']}")
                    print(f"    Type: {m['mapping_type']}")
                    print(f"    {m['description']}")
                    print()
            else:
                print("[INFO] No mappings found. Run --merge or --analyze first.")
        
        else:
            parser.print_help()
    
    finally:
        merger.close()


if __name__ == "__main__":
    main()
