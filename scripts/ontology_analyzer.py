#!/usr/bin/env python3
"""
LLM-based analyzer for PLC code using Anthropic's Claude API.
Generates semantic understanding of tags and logic.
Stores results in Neo4j graph database.

Uses tool calls to query existing ontology data, enabling Claude to build
on existing knowledge rather than starting from scratch.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path

from sc_parser import SCParser, SCFile, Tag
from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient, get_claude_client


class OntologyAnalyzer:
    """Analyzes PLC code using Claude to generate semantic ontologies."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5-20250929",
        graph: Optional[OntologyGraph] = None,
        client: Optional[ClaudeClient] = None,
    ):
        """
        Initialize the analyzer.

        Args:
            api_key: Anthropic API key (uses env var if not provided)
            model: Claude model to use
            graph: Optional Neo4j connection (uses client's if not provided)
            client: Optional ClaudeClient (created if not provided)
        """
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

    def analyze_sc_file(
        self, sc_file: SCFile, verbose: bool = False, skip_ai: bool = False
    ) -> Dict[str, Any]:
        """Analyze a parsed SC file and store ontology in Neo4j.

        Args:
            sc_file: Parsed SC file
            verbose: Print detailed progress
            skip_ai: If True, skip AI analysis and just create the AOI node with pending status
        """

        if verbose:
            print(f"[INFO] {'Creating' if skip_ai else 'Analyzing'} {sc_file.name}...")

        if skip_ai:
            # Just create the node without AI analysis
            analysis = {}
        else:
            # Build context for LLM
            context = self._build_analysis_context(sc_file)
            # Generate analysis using Claude with tool support
            analysis = self._query_llm(context, sc_file.name, verbose)

        # Structure the response
        ontology = {
            "name": sc_file.name,
            "type": sc_file.type,
            "source_file": sc_file.file_path,
            "metadata": {
                "revision": sc_file.revision,
                "vendor": sc_file.vendor,
                "description": sc_file.description,
            },
            "analysis": analysis,
        }

        # Store in Neo4j (with semantic_status='pending' if skip_ai)
        self.graph.create_aoi(
            name=ontology["name"],
            aoi_type=ontology["type"],
            source_file=ontology["source_file"],
            metadata=ontology["metadata"],
            analysis=ontology["analysis"],
            semantic_status="pending" if skip_ai else "complete",
        )

        if verbose:
            print(f"[OK] {'Created' if skip_ai else 'Stored'} {sc_file.name} in Neo4j")

        return ontology

    def _build_analysis_context(self, sc_file: SCFile) -> str:
        """Build context string for LLM analysis."""

        context_parts = []

        # Header
        context_parts.append(f"# PLC Component: {sc_file.name}")
        context_parts.append(f"Type: {sc_file.type}")
        if sc_file.description:
            context_parts.append(f"Description: {sc_file.description}")
        context_parts.append("")

        # Input parameters
        if sc_file.input_tags:
            context_parts.append("## Input Parameters")
            for tag in sc_file.input_tags:
                desc = f" // {tag.description}" if tag.description else ""
                context_parts.append(f"- {tag.name}: {tag.data_type}{desc}")
            context_parts.append("")

        # Output parameters
        if sc_file.output_tags:
            context_parts.append("## Output Parameters")
            for tag in sc_file.output_tags:
                desc = f" // {tag.description}" if tag.description else ""
                context_parts.append(f"- {tag.name}: {tag.data_type}{desc}")
            context_parts.append("")

        # InOut parameters
        if sc_file.inout_tags:
            context_parts.append("## InOut Parameters")
            for tag in sc_file.inout_tags:
                desc = f" // {tag.description}" if tag.description else ""
                context_parts.append(f"- {tag.name}: {tag.data_type}{desc}")
            context_parts.append("")

        # Local tags (summarize if many)
        if sc_file.local_tags:
            context_parts.append("## Local Variables")
            for tag in sc_file.local_tags[:10]:  # First 10
                desc = f" // {tag.description}" if tag.description else ""
                context_parts.append(f"- {tag.name}: {tag.data_type}{desc}")
            if len(sc_file.local_tags) > 10:
                context_parts.append(
                    f"... and {len(sc_file.local_tags) - 10} more local variables"
                )
            context_parts.append("")

        # Logic rungs (sample key ones)
        if sc_file.routines:
            context_parts.append("## Logic Implementation")
            for routine in sc_file.routines:
                context_parts.append(
                    f"### Routine: {routine['name']} ({routine['type']})"
                )
                for rung in routine["rungs"][:15]:  # First 15 rungs
                    if rung.comment:
                        context_parts.append(f"\nRung {rung.number}: {rung.comment}")
                    else:
                        context_parts.append(f"\nRung {rung.number}:")
                    context_parts.append(f"```\n{rung.logic}\n```")

                if len(routine["rungs"]) > 15:
                    context_parts.append(
                        f"\n... and {len(routine['rungs']) - 15} more rungs"
                    )
                context_parts.append("")

        return "\n".join(context_parts)

    def _query_llm(
        self, context: str, component_name: str, verbose: bool = False
    ) -> Dict[str, Any]:
        """Query Claude API for analysis with tool support."""

        system_prompt = """You are an expert PLC (Programmable Logic Controller) engineer specializing in analyzing industrial control logic. Your task is to analyze PLC code and generate semantic ontologies that explain what tags (variables) mean and how the PLC manipulates them.

You have access to tools that let you query the existing ontology database:
- get_schema: Discover what node types and relationships exist
- run_query: Execute Cypher queries to explore existing data
- get_node: Get details of a specific node

USE THESE TOOLS to explore what already exists before analyzing new components. This helps you:
- Maintain consistent naming and descriptions
- Identify relationships to existing components
- Build on existing knowledge rather than starting from scratch
- Use established terminology and relationship types from the codebase

For each PLC component, provide:
1. **Functional Purpose**: High-level description of what this component does
2. **Tag Semantics**: For each important tag, explain its semantic meaning
3. **Relationships**: How tags influence each other (use consistent relationship types)
4. **Control Patterns**: Identify patterns (use existing pattern names when applicable)
5. **Data Flow**: Key paths showing how inputs lead to outputs
6. **Safety-Critical Elements**: Any tags or logic related to safety"""

        user_prompt = f"""Analyze this PLC component and generate a semantic ontology.

FIRST, use the available tools to explore the existing ontology:
1. Use get_schema to understand what data exists
2. Query for similar AOIs or tags that might be related
3. Check existing control patterns and relationship types for consistency

THEN, provide your analysis as a structured JSON object with these fields:
- "purpose": string describing the functional purpose
- "tags": object mapping tag names to their semantic descriptions
- "relationships": array of {{from, to, relationship_type, description}} objects
- "control_patterns": array of {{pattern, description}} objects (use existing pattern names when possible)
- "data_flows": array of {{path, description}} objects
- "safety_critical": array of {{element, criticality, reason}} objects

Be concise but informative. Focus on the "why" and "what" rather than just restating the syntax.

## Component to Analyze:

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
            # Handle error
            error_msg = result.get("error", "Unknown error")
            print(f"[WARNING] Failed to parse JSON response: {error_msg}")
            return {
                "purpose": f"Analysis failed - {error_msg}",
                "raw_response": result.get("raw_text", ""),
                "tags": {},
                "relationships": [],
                "control_patterns": [],
                "data_flows": [],
                "safety_critical": [],
            }

    def analyze_directory(
        self,
        directory: str,
        pattern: str = "*.aoi.sc",
        verbose: bool = False,
        skip_ai: bool = False,
    ) -> List[Dict[str, Any]]:
        """Analyze all SC files in a directory and store in Neo4j.

        Args:
            directory: Directory path
            pattern: File pattern to match
            verbose: Print detailed progress
            skip_ai: If True, skip AI analysis (for incremental mode)
        """

        dir_path = Path(directory)
        sc_files = list(dir_path.rglob(pattern))

        if not sc_files:
            print(f"[WARNING] No files matching '{pattern}' found in {directory}")
            return []

        action = "import" if skip_ai else "analyze"
        print(f"[INFO] Found {len(sc_files)} files to {action}")

        parser = SCParser()
        ontologies = []

        for i, sc_path in enumerate(sc_files, 1):
            print(f"\n[{i}/{len(sc_files)}] Processing {sc_path.name}...")

            try:
                # Parse SC file
                sc_file = parser.parse_file(str(sc_path))

                # Analyze with LLM and store in Neo4j
                ontology = self.analyze_sc_file(sc_file, verbose, skip_ai=skip_ai)
                ontologies.append(ontology)

                print(f"[OK] Completed {sc_path.name}")

            except Exception as e:
                print(f"[ERROR] Failed to process {sc_path.name}: {e}")
                continue

        return ontologies

    def get_all_ontologies(self) -> List[Dict[str, Any]]:
        """Retrieve all AOI ontologies from Neo4j."""
        return self.graph.get_all_aois()

    def get_ontology(self, name: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific AOI ontology from Neo4j."""
        return self.graph.get_aoi(name)


def main():
    """CLI for ontology analyzer."""
    import sys
    import argparse
    from dotenv import load_dotenv

    # Load .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Analyze PLC .sc files and generate semantic ontologies using Claude (stored in Neo4j)"
    )
    parser.add_argument("input", nargs="?", help="Path to .sc file or directory")
    parser.add_argument(
        "-p",
        "--pattern",
        default="*.aoi.sc",
        help="File pattern for directory mode (default: *.aoi.sc)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--model", default="claude-sonnet-4-5-20250929", help="Claude model to use"
    )
    parser.add_argument("--list", action="store_true", help="List all AOIs in Neo4j")
    parser.add_argument("--get", metavar="NAME", help="Get a specific AOI from Neo4j")
    parser.add_argument(
        "--export", metavar="FILE", help="Export all ontologies to JSON file"
    )
    parser.add_argument(
        "--no-tools",
        action="store_true",
        help="Disable Neo4j tool calls (analyze without context)",
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI analysis, only create AOI nodes with pending status (for incremental mode)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show semantic analysis status for AOIs",
    )

    args = parser.parse_args()

    # Initialize analyzer
    try:
        client = ClaudeClient(model=args.model, enable_tools=not args.no_tools)
        analyzer = OntologyAnalyzer(client=client)
    except ValueError as e:
        print(f"[ERROR] {e}")
        print("[INFO] Please set ANTHROPIC_API_KEY in .env file or environment")
        sys.exit(1)

    try:
        if args.status:
            # Show semantic analysis status
            status = analyzer.graph.get_semantic_status_counts()
            print("\n=== Semantic Analysis Status (AOIs) ===\n")
            aoi_counts = status.get("AOI", {})
            pending = aoi_counts.get("pending", 0) + aoi_counts.get(None, 0)
            complete = aoi_counts.get("complete", 0)
            in_progress = aoi_counts.get("in_progress", 0)
            total = pending + complete + in_progress
            if total > 0:
                pct = (complete / total * 100) if total > 0 else 0
                print(f"  AOI:  {complete}/{total} complete ({pct:.0f}%)")
                if pending > 0:
                    print(f"        {pending} pending")
                if in_progress > 0:
                    print(f"        {in_progress} in progress")
            else:
                print("  No AOIs found. Import PLC files first.")
            print()

        elif args.list:
            # List all AOIs
            aois = analyzer.get_all_ontologies()
            print(f"\n[INFO] Found {len(aois)} AOIs in Neo4j:\n")
            for aoi in aois:
                print(
                    f"  - {aoi['name']}: {aoi.get('analysis', {}).get('purpose', 'N/A')[:80]}..."
                )

        elif args.get:
            # Get specific AOI
            aoi = analyzer.get_ontology(args.get)
            if aoi:
                print(json.dumps(aoi, indent=2))
            else:
                print(f"[ERROR] AOI '{args.get}' not found in Neo4j")

        elif args.export:
            # Export to JSON
            aois = analyzer.get_all_ontologies()
            with open(args.export, "w", encoding="utf-8") as f:
                json.dump(aois, f, indent=2)
            print(f"[OK] Exported {len(aois)} AOIs to {args.export}")

        elif args.input:
            input_path = Path(args.input)

            # Process directory or single file
            if input_path.is_dir():
                ontologies = analyzer.analyze_directory(
                    str(input_path),
                    pattern=args.pattern,
                    verbose=args.verbose,
                    skip_ai=args.skip_ai,
                )
                action = "Imported" if args.skip_ai else "Analyzed"
                print(f"\n[OK] {action} {len(ontologies)} files and stored in Neo4j")
                if args.skip_ai:
                    print(
                        "[INFO] Use incremental analyzer to add semantic descriptions"
                    )

            elif input_path.is_file():
                # Parse and analyze single file
                sc_parser = SCParser()
                sc_file = sc_parser.parse_file(str(input_path))
                ontology = analyzer.analyze_sc_file(
                    sc_file, verbose=args.verbose, skip_ai=args.skip_ai
                )

                # Print summary
                print(f"\n=== Ontology: {ontology['name']} ===")
                if not args.skip_ai:
                    print(f"Purpose: {ontology['analysis'].get('purpose', 'N/A')}")
                action = "Created" if args.skip_ai else "Stored"
                print(f"\n[OK] {action} in Neo4j")

            else:
                print(f"[ERROR] Input path not found: {args.input}")
                sys.exit(1)

        else:
            parser.print_help()

    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
