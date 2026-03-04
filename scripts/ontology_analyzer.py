#!/usr/bin/env python3
"""
LLM-based analyzer for PLC code using Anthropic's Claude API.
Generates semantic understanding of tags and logic.
Stores results in Neo4j graph database.

Supports both Rockwell (.sc) and Siemens (.st) PLC file formats.

Uses tool calls to query existing ontology data, enabling Claude to build
on existing knowledge rather than starting from scratch.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path

from sc_parser import SCParser, SCFile, Tag
from siemens_parser import SiemensSTParser
from tia_xml_parser import TiaXmlParser
from siemens_project_parser import SiemensProjectParser, TiaProject
from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient, get_claude_client
from rockwell_export import (
    detect_rockwell_format,
    parse_rockwell_file,
    parse_rockwell_directory,
    find_rockwell_files,
    is_rockwell_file,
    ROCKWELL_EXTENSIONS,
)


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

        # Logic implementation (rungs for RLL, raw content for ST)
        if sc_file.routines:
            context_parts.append("## Logic Implementation")
            for routine in sc_file.routines:
                visibility = routine.get("visibility", "")
                vis_str = f" [{visibility}]" if visibility else ""
                context_parts.append(
                    f"### Routine: {routine['name']} ({routine['type']}){vis_str}"
                )

                # Ladder logic rungs
                if routine.get("rungs"):
                    for rung in routine["rungs"][:15]:  # First 15 rungs
                        if rung.comment:
                            context_parts.append(
                                f"\nRung {rung.number}: {rung.comment}"
                            )
                        else:
                            context_parts.append(f"\nRung {rung.number}:")
                        context_parts.append(f"```\n{rung.logic}\n```")

                    if len(routine["rungs"]) > 15:
                        context_parts.append(
                            f"\n... and {len(routine['rungs']) - 15} more rungs"
                        )

                # Structured Text body (Siemens methods / programs)
                elif routine.get("raw_content"):
                    # Include method-local variables if present
                    local_tags = routine.get("local_tags", [])
                    if local_tags:
                        context_parts.append("Local variables:")
                        for tag in local_tags[:10]:
                            desc = f" // {tag.description}" if tag.description else ""
                            context_parts.append(f"- {tag.name}: {tag.data_type}{desc}")

                    # Truncate very long method bodies
                    raw = routine["raw_content"]
                    if len(raw) > 2000:
                        raw = (
                            raw[:2000]
                            + f"\n... ({len(routine['raw_content'])} chars total)"
                        )
                    context_parts.append(f"```st\n{raw}\n```")

                context_parts.append("")

        return "\n".join(context_parts)

    def _query_llm(
        self, context: str, component_name: str, verbose: bool = False
    ) -> Dict[str, Any]:
        """Query Claude API for analysis with tool support."""

        system_prompt = """You are an expert PLC (Programmable Logic Controller) engineer specializing in analyzing industrial control logic from both Rockwell/Allen-Bradley and Siemens platforms. Your task is to analyze PLC code and generate semantic ontologies that explain what tags (variables) mean and how the PLC manipulates them.

You understand both platforms:
- Rockwell: Add-On Instructions (AOIs), UDTs, ladder logic (RLL), structured text (ST)
- Siemens: Function Blocks (CLASS), Types (STRUCT/UDT), Programs (PROGRAM), Configurations, Methods, SCL/ST

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
        siemens: bool = False,
        tia_xml: bool = False,
    ) -> List[Dict[str, Any]]:
        """Analyze all SC, ST, or TIA XML files in a directory and store in Neo4j.

        Args:
            directory: Directory path
            pattern: File pattern to match
            verbose: Print detailed progress
            skip_ai: If True, skip AI analysis (for incremental mode)
            siemens: If True, use Siemens .st parser instead of Rockwell .sc parser
            tia_xml: If True, use TIA Portal XML parser
        """

        dir_path = Path(directory)
        sc_files = list(dir_path.rglob(pattern))

        if not sc_files:
            print(f"[WARNING] No files matching '{pattern}' found in {directory}")
            return []

        action = "import" if skip_ai else "analyze"
        if tia_xml:
            platform = "Siemens TIA XML"
        elif siemens:
            platform = "Siemens"
        else:
            platform = "Rockwell"
        print(f"[INFO] Found {len(sc_files)} {platform} files to {action}")

        ontologies = []
        parsed_files: List[SCFile] = []  # Collect for cross-referencing

        if tia_xml:
            tia_parser = TiaXmlParser()
            block_count = 0
            for i, xml_path in enumerate(sc_files, 1):
                print(f"\n[{i}/{len(sc_files)}] Processing {xml_path.name}...")

                try:
                    parsed_blocks = tia_parser.parse_file(str(xml_path))

                    if not parsed_blocks:
                        print(f"[WARNING] No parseable blocks in {xml_path.name}")
                        continue

                    for sc_file in parsed_blocks:
                        block_count += 1
                        parsed_files.append(sc_file)
                        ontology = self.analyze_sc_file(
                            sc_file, verbose, skip_ai=skip_ai
                        )
                        ontologies.append(ontology)
                        print(f"  [OK] {sc_file.type}: {sc_file.name}")

                    print(
                        f"[OK] Completed {xml_path.name} ({len(parsed_blocks)} blocks)"
                    )

                except Exception as e:
                    print(f"[ERROR] Failed to process {xml_path.name}: {e}")
                    continue

            if block_count > 0:
                print(
                    f"\n[INFO] Processed {block_count} blocks from {len(sc_files)} files"
                )

        elif siemens:
            siemens_parser = SiemensSTParser()
            block_count = 0
            for i, st_path in enumerate(sc_files, 1):
                print(f"\n[{i}/{len(sc_files)}] Processing {st_path.name}...")

                try:
                    # Siemens .st files can contain multiple blocks
                    parsed_blocks = siemens_parser.parse_file(str(st_path))

                    if not parsed_blocks:
                        print(f"[WARNING] No parseable blocks in {st_path.name}")
                        continue

                    for sc_file in parsed_blocks:
                        block_count += 1
                        parsed_files.append(sc_file)
                        ontology = self.analyze_sc_file(
                            sc_file, verbose, skip_ai=skip_ai
                        )
                        ontologies.append(ontology)
                        print(f"  [OK] {sc_file.type}: {sc_file.name}")

                    print(
                        f"[OK] Completed {st_path.name} ({len(parsed_blocks)} blocks)"
                    )

                except Exception as e:
                    print(f"[ERROR] Failed to process {st_path.name}: {e}")
                    continue

            if block_count > 0:
                print(
                    f"\n[INFO] Processed {block_count} blocks from {len(sc_files)} files"
                )
        else:
            parser = SCParser()
            for i, sc_path in enumerate(sc_files, 1):
                print(f"\n[{i}/{len(sc_files)}] Processing {sc_path.name}...")

                try:
                    # Parse SC file
                    sc_file = parser.parse_file(str(sc_path))
                    parsed_files.append(sc_file)

                    # Analyze with LLM and store in Neo4j
                    ontology = self.analyze_sc_file(sc_file, verbose, skip_ai=skip_ai)
                    ontologies.append(ontology)

                    print(f"[OK] Completed {sc_path.name}")

                except Exception as e:
                    print(f"[ERROR] Failed to process {sc_path.name}: {e}")
                    continue

        # --- Cross-reference pass ---
        if parsed_files:
            xref_count = self.extract_cross_references(parsed_files, verbose=verbose)
            if xref_count:
                print(f"\n[INFO] Created {xref_count} cross-reference relationships")

        return ontologies

    # ------------------------------------------------------------------
    # Cross-reference extraction
    # ------------------------------------------------------------------

    # IEC 61131-3 primitive types (not user-defined, skip during matching)
    _PRIMITIVE_TYPES = frozenset(
        {
            "BOOL",
            "BYTE",
            "WORD",
            "DWORD",
            "LWORD",
            "SINT",
            "INT",
            "DINT",
            "LINT",
            "USINT",
            "UINT",
            "UDINT",
            "ULINT",
            "REAL",
            "LREAL",
            "TIME",
            "DATE",
            "TIME_OF_DAY",
            "TOD",
            "DATE_AND_TIME",
            "DT",
            "STRING",
            "WSTRING",
            "CHAR",
            "WCHAR",
            "TIMER",
            "COUNTER",
            "CONTROL",
            # Rockwell-specific
            "BIT",
            "ROUTINE",
            "ALARM_DIGITAL",
            "ALARM_ANALOG",
            "MESSAGE",
            "MOTION_GROUP",
            "MOTION_INSTRUCTION",
            "COORDINATE_SYSTEM",
            "AXIS",
            "MODULE",
        }
    )

    def extract_cross_references(
        self,
        sc_files: List[SCFile],
        verbose: bool = False,
    ) -> int:
        """
        Analyse a batch of parsed SCFile objects and create dependency
        relationships in Neo4j for every case where one AOI/FB's variable
        declarations reference another AOI/FB or UDT by type.

        Also checks against AOI names already in Neo4j, so cross-references
        to previously-ingested components are captured.

        Args:
            sc_files:  List of parsed SCFile objects (from sc_parser or siemens_parser).
            verbose:   Print progress.

        Returns:
            Number of dependency relationships created.
        """
        import re

        if verbose:
            print("\n[INFO] Extracting cross-references between AOIs / FBs / UDTs...")

        # 1. Build lookup: name → type  (AOI, UDT, FB, PROGRAM, CONFIGURATION)
        #    from the files we just parsed
        local_lookup: Dict[str, str] = {}
        for sf in sc_files:
            local_lookup[sf.name] = sf.type  # e.g. "HIPPSController" → "FB"

        # 2. Also pull existing AOI names from Neo4j
        try:
            existing = self.graph.get_all_aoi_names()  # [{name, type}, ...]
            for entry in existing:
                if entry["name"] not in local_lookup:
                    local_lookup[entry["name"]] = entry.get("type", "AOI")
        except Exception:
            pass  # Neo4j may not be reachable; degrade gracefully

        if verbose:
            print(
                f"[INFO] Known components: {len(local_lookup)} "
                f"({sum(1 for t in local_lookup.values() if t in ('AOI', 'FB'))} AOI/FB, "
                f"{sum(1 for t in local_lookup.values() if t == 'UDT')} UDT, "
                f"{sum(1 for t in local_lookup.values() if t not in ('AOI', 'FB', 'UDT'))} other)"
            )

        # 3. Walk every tag in every parsed file
        dependencies: List[Dict[str, str]] = []
        seen: set = set()  # (from, to, via_tag) dedup

        for sf in sc_files:
            all_tags = sf.input_tags + sf.output_tags + sf.inout_tags + sf.local_tags
            for tag in all_tags:
                dtype = tag.data_type.strip()

                # Strip STRING length specifiers like STRING[20]
                bare = re.sub(r"\[.*\]$", "", dtype).strip()

                # Skip primitives and self-references
                if bare.upper() in self._PRIMITIVE_TYPES:
                    continue
                if bare == sf.name:
                    continue

                if bare in local_lookup:
                    target_type = local_lookup[bare]
                    # Classify the relationship
                    if target_type == "UDT":
                        rel_type = "USES_TYPE"
                    else:
                        rel_type = "INSTANTIATES"

                    key = (sf.name, bare, tag.name)
                    if key not in seen:
                        seen.add(key)
                        dependencies.append(
                            {
                                "from_aoi": sf.name,
                                "to_aoi": bare,
                                "rel_type": rel_type,
                                "via_tag": tag.name,
                                "description": (
                                    f"{sf.name}.{tag.name} is of type {bare}"
                                ),
                            }
                        )

            # 4. Scan routine logic text for AOI call patterns (Rockwell ladder)
            #    Pattern: AOI_Name(instance_var, ...)
            for routine in sf.routines:
                logic_text = ""
                if routine.get("rungs"):
                    logic_text = " ".join(r.logic for r in routine["rungs"] if r.logic)
                elif routine.get("raw_content"):
                    logic_text = routine["raw_content"]

                if not logic_text:
                    continue

                for candidate_name, candidate_type in local_lookup.items():
                    if candidate_name == sf.name:
                        continue
                    # Rockwell: AOI_Name(instance, ...)
                    # Siemens:  instance.MethodName(...)
                    # Look for AOI name followed by '(' — indicates a call
                    pattern = r"\b" + re.escape(candidate_name) + r"\s*\("
                    if re.search(pattern, logic_text):
                        key = (sf.name, candidate_name, "__logic__")
                        if key not in seen:
                            seen.add(key)
                            rel = (
                                "USES_TYPE"
                                if candidate_type == "UDT"
                                else "INSTANTIATES"
                            )
                            dependencies.append(
                                {
                                    "from_aoi": sf.name,
                                    "to_aoi": candidate_name,
                                    "rel_type": rel,
                                    "via_tag": "",
                                    "description": (
                                        f"{sf.name} calls {candidate_name} in "
                                        f"routine '{routine.get('name', '?')}'"
                                    ),
                                }
                            )

        if not dependencies:
            if verbose:
                print("[INFO] No cross-references found")
            return 0

        # 5. Store in Neo4j
        if verbose:
            inst = sum(1 for d in dependencies if d["rel_type"] == "INSTANTIATES")
            uses = sum(1 for d in dependencies if d["rel_type"] == "USES_TYPE")
            print(
                f"[INFO] Found {len(dependencies)} dependencies "
                f"({inst} INSTANTIATES, {uses} USES_TYPE)"
            )
            for dep in dependencies:
                arrow = (
                    "--INSTANTIATES-->"
                    if dep["rel_type"] == "INSTANTIATES"
                    else "--USES_TYPE-->"
                )
                via = f" (via {dep['via_tag']})" if dep["via_tag"] else ""
                print(f"  {dep['from_aoi']} {arrow} {dep['to_aoi']}{via}")

        count = self.graph.create_aoi_dependencies_batch(dependencies)
        return count

    def cross_reference_directory(
        self,
        directory: str,
        pattern: str = "*.aoi.sc",
        siemens: bool = False,
        tia_xml: bool = False,
        verbose: bool = False,
    ) -> int:
        """
        Re-parse files in a directory and extract cross-references only
        (no AI analysis, no AOI node creation — just dependency edges).

        Useful for re-running cross-referencing on already-ingested data.
        """
        dir_path = Path(directory)
        files = list(dir_path.rglob(pattern))

        if not files:
            print(f"[WARNING] No files matching '{pattern}' in {directory}")
            return 0

        parsed: List[SCFile] = []

        if tia_xml:
            tia_parser = TiaXmlParser()
            for f in files:
                try:
                    blocks = tia_parser.parse_file(str(f))
                    parsed.extend(blocks)
                except Exception as e:
                    if verbose:
                        print(f"[WARNING] Could not parse {f.name}: {e}")
        elif siemens:
            siemens_parser = SiemensSTParser()
            for f in files:
                try:
                    blocks = siemens_parser.parse_file(str(f))
                    parsed.extend(blocks)
                except Exception as e:
                    if verbose:
                        print(f"[WARNING] Could not parse {f.name}: {e}")
        else:
            parser = SCParser()
            for f in files:
                try:
                    parsed.append(parser.parse_file(str(f)))
                except Exception as e:
                    if verbose:
                        print(f"[WARNING] Could not parse {f.name}: {e}")

        if not parsed:
            print("[WARNING] No parseable files found")
            return 0

        if tia_xml:
            platform = "Siemens TIA XML"
        elif siemens:
            platform = "Siemens"
        else:
            platform = "Rockwell"
        print(f"[INFO] Parsed {len(parsed)} {platform} blocks from {len(files)} files")
        return self.extract_cross_references(parsed, verbose=verbose)

    # ------------------------------------------------------------------
    # Rockwell multi-format ingestion (L5X, L5K, ACD)
    # ------------------------------------------------------------------

    def analyze_rockwell_file(
        self,
        file_path: str,
        verbose: bool = False,
        skip_ai: bool = False,
    ) -> List[Dict[str, Any]]:
        """Analyze a Rockwell PLC file (L5X, L5K, or ACD) and store in Neo4j.

        Auto-detects the file format and parses it into SCFile objects,
        then runs the same analysis pipeline as .sc files.

        Args:
            file_path: Path to Rockwell PLC file
            verbose: Print detailed progress
            skip_ai: Skip AI analysis (import only)

        Returns:
            List of ontology dicts for each component found.
        """
        fmt = detect_rockwell_format(file_path)
        if not fmt:
            print(f"[ERROR] Not a recognized Rockwell file: {file_path}")
            return []

        print(f"[INFO] Detected Rockwell {fmt} format: {Path(file_path).name}")

        parsed_files = parse_rockwell_file(file_path)
        if not parsed_files:
            print(f"[WARNING] No components found in {file_path}")
            return []

        action = "import" if skip_ai else "analyze"
        print(f"[INFO] Found {len(parsed_files)} components to {action}")

        ontologies = []
        for i, sc_file in enumerate(parsed_files, 1):
            print(f"\n[{i}/{len(parsed_files)}] Processing {sc_file.type}: {sc_file.name}...")
            try:
                ontology = self.analyze_sc_file(sc_file, verbose, skip_ai=skip_ai)
                ontologies.append(ontology)
                print(f"  [OK] {sc_file.type}: {sc_file.name}")
            except Exception as e:
                print(f"  [ERROR] Failed {sc_file.name}: {e}")
                continue

        # Cross-reference pass
        if parsed_files:
            xref_count = self.extract_cross_references(parsed_files, verbose=verbose)
            if xref_count:
                print(f"\n[INFO] Created {xref_count} cross-reference relationships")

        return ontologies

    def analyze_rockwell_directory(
        self,
        directory: str,
        verbose: bool = False,
        skip_ai: bool = False,
    ) -> List[Dict[str, Any]]:
        """Analyze all Rockwell PLC files in a directory.

        Finds and processes all .L5X, .L5K, and .ACD files.

        Args:
            directory: Directory path
            verbose: Print detailed progress
            skip_ai: Skip AI analysis

        Returns:
            List of ontology dicts.
        """
        files = find_rockwell_files(directory)
        if not files:
            print(f"[WARNING] No Rockwell PLC files found in {directory}")
            return []

        print(f"[INFO] Found {len(files)} Rockwell PLC file(s)")

        all_ontologies = []
        all_parsed: List[SCFile] = []

        for fp, fmt in files:
            print(f"\n{'='*60}")
            print(f"  [{fmt}] {Path(fp).name}")
            print(f"{'='*60}")

            parsed_files = parse_rockwell_file(fp, format_hint=fmt)
            if not parsed_files:
                continue

            action = "import" if skip_ai else "analyze"
            print(f"[INFO] {len(parsed_files)} components to {action}")

            for i, sc_file in enumerate(parsed_files, 1):
                try:
                    ontology = self.analyze_sc_file(
                        sc_file, verbose, skip_ai=skip_ai
                    )
                    all_ontologies.append(ontology)
                    all_parsed.append(sc_file)
                    print(f"  [{i}/{len(parsed_files)}] {sc_file.type}: {sc_file.name}")
                except Exception as e:
                    print(f"  [ERROR] {sc_file.name}: {e}")

        # Cross-reference pass across all files
        if all_parsed:
            xref_count = self.extract_cross_references(all_parsed, verbose=verbose)
            if xref_count:
                print(f"\n[INFO] Created {xref_count} cross-reference relationships")

        return all_ontologies

    # ------------------------------------------------------------------
    # TIA Portal full-project ingestion
    # ------------------------------------------------------------------

    def ingest_tia_project(
        self,
        project_dir: str,
        verbose: bool = False,
        skip_ai: bool = False,
    ) -> Dict[str, Any]:
        """Ingest an entire Siemens TIA Portal project into the ontology.

        Parses the full project structure (PLCs + HMIs) and stores everything
        in Neo4j, including interlinks between devices.

        Args:
            project_dir: Path to the TIA Portal export directory
            verbose: Print detailed progress
            skip_ai: If True, skip AI analysis for PLC blocks

        Returns:
            Summary dict with counts of ingested items
        """
        print(f"[INFO] Parsing TIA Portal project: {project_dir}")
        parser = SiemensProjectParser()
        project = parser.parse_project(project_dir)

        summary: Dict[str, int] = {
            "plc_devices": 0,
            "hmi_devices": 0,
            "blocks": 0,
            "plc_tag_tables": 0,
            "plc_tags": 0,
            "plc_types": 0,
            "hmi_connections": 0,
            "hmi_tag_tables": 0,
            "hmi_alarms": 0,
            "hmi_alarm_classes": 0,
            "hmi_scripts": 0,
            "hmi_screens": 0,
            "hmi_text_lists": 0,
            "cross_references": 0,
        }

        # --- 1. Create top-level TiaProject node ---
        print(f"\n[INFO] Creating TiaProject: {project.name}")
        self.graph.create_tia_project(
            name=project.name,
            directory=project.directory,
        )

        # --- 2. Process PLC devices ---
        for plc in project.plc_devices:
            print(f"\n[INFO] Processing PLC: {plc.name} ({plc.dir_name})")
            self.graph.create_plc_device(
                name=plc.name,
                project_name=project.name,
                dir_name=plc.dir_name,
            )
            summary["plc_devices"] += 1

            # 2a. Ingest PLC blocks (OB/FB/FC/DB) via existing pipeline
            if plc.blocks:
                print(f"  [INFO] Ingesting {len(plc.blocks)} PLC blocks...")
                all_parsed: List[SCFile] = []
                for sc_file in plc.blocks:
                    try:
                        ontology = self.analyze_sc_file(
                            sc_file, verbose=verbose, skip_ai=skip_ai
                        )
                        # Link AOI to PLC device
                        self.graph.link_aoi_to_plc_device(
                            aoi_name=sc_file.name,
                            plc_name=plc.name,
                            project_name=project.name,
                        )
                        all_parsed.append(sc_file)
                        summary["blocks"] += 1
                        if verbose:
                            print(f"    [OK] {sc_file.type}: {sc_file.name}")
                    except Exception as e:
                        print(f"    [ERROR] Block {sc_file.name}: {e}")

                # Cross-references between blocks
                if all_parsed:
                    xref = self.extract_cross_references(all_parsed, verbose=verbose)
                    summary["cross_references"] += xref

            # 2b. Ingest PLC tag tables
            if plc.tag_tables:
                print(f"  [INFO] Ingesting {len(plc.tag_tables)} PLC tag tables...")
                for tt in plc.tag_tables:
                    self.graph.create_plc_tag_table(
                        name=tt.name,
                        plc_name=plc.name,
                        project_name=project.name,
                    )
                    summary["plc_tag_tables"] += 1

                    for tag in tt.tags:
                        self.graph.create_plc_tag(
                            name=tag.name,
                            table_name=tt.name,
                            plc_name=plc.name,
                            project_name=project.name,
                            data_type=tag.data_type,
                            logical_address=tag.logical_address,
                            comment=tag.comment,
                        )
                        summary["plc_tags"] += 1

                    if verbose:
                        print(f"    [OK] TagTable '{tt.name}': {len(tt.tags)} tags")

            # 2c. Ingest PLC types (UDTs)
            if plc.types:
                print(f"  [INFO] Ingesting {len(plc.types)} PLC types/UDTs...")
                for plc_type in plc.types:
                    members = [
                        {
                            "name": m.name,
                            "data_type": m.data_type,
                            "description": m.description or "",
                        }
                        for m in plc_type.members
                    ]
                    self.graph.create_plc_type(
                        name=plc_type.name,
                        plc_name=plc.name,
                        project_name=project.name,
                        members=members,
                        is_failsafe=plc_type.is_failsafe,
                    )
                    summary["plc_types"] += 1
                    if verbose:
                        print(f"    [OK] Type '{plc_type.name}': {len(plc_type.members)} members")

            # 2d. Block metadata (JSON files for ProDiag etc.)
            if plc.block_metadata and verbose:
                print(f"  [INFO] Found {len(plc.block_metadata)} block metadata files")
                for meta in plc.block_metadata:
                    block_name = meta.get("Name", "?")
                    block_type = meta.get("Type", "?")
                    lang = meta.get("ProgrammingLanguage", "?")
                    print(f"    Meta: {block_name} ({block_type}) [{lang}]")

        # --- 3. Process HMI devices ---
        for hmi in project.hmi_devices:
            print(f"\n[INFO] Processing HMI: {hmi.name} ({hmi.dir_name})")
            self.graph.create_hmi_device(
                name=hmi.name,
                project_name=project.name,
                dir_name=hmi.dir_name,
            )
            summary["hmi_devices"] += 1

            # 3a. HMI connections (interlinks to PLCs)
            if hmi.connections:
                print(f"  [INFO] Ingesting {len(hmi.connections)} connections...")
                for conn in hmi.connections:
                    self.graph.create_hmi_connection(
                        name=conn.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                        partner=conn.partner,
                        station=conn.station,
                        communication_driver=conn.communication_driver,
                        node=conn.node,
                        address=conn.address,
                    )
                    summary["hmi_connections"] += 1
                    if verbose:
                        print(f"    [OK] {conn.name} -> {conn.partner}")

            # 3b. HMI alarm classes
            if hmi.alarm_classes:
                print(f"  [INFO] Ingesting {len(hmi.alarm_classes)} alarm classes...")
                for ac in hmi.alarm_classes:
                    self.graph.create_hmi_alarm_class(
                        name=ac.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                        priority=ac.priority,
                        state_machine=ac.state_machine,
                        is_system=ac.is_system,
                    )
                    summary["hmi_alarm_classes"] += 1

            # 3c. HMI alarms (analog + discrete)
            if hmi.alarms:
                print(f"  [INFO] Ingesting {len(hmi.alarms)} alarms...")
                for alarm in hmi.alarms:
                    self.graph.create_hmi_alarm(
                        name=alarm.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                        alarm_type=alarm.alarm_type,
                        alarm_class=alarm.alarm_class,
                        origin=alarm.origin,
                        priority=alarm.priority,
                        raised_state_tag=alarm.raised_state_tag,
                        trigger_bit_address=alarm.trigger_bit_address,
                        trigger_mode=alarm.trigger_mode,
                        condition=alarm.condition,
                        condition_value=alarm.condition_value,
                    )
                    summary["hmi_alarms"] += 1

                    # Link alarm to PLC tags if we can resolve the reference
                    tag_ref = alarm.raised_state_tag or alarm.trigger_bit_address
                    if tag_ref:
                        self.graph.link_alarm_to_plc_tag(
                            alarm_name=alarm.name,
                            hmi_name=hmi.name,
                            project_name=project.name,
                            tag_reference=tag_ref,
                        )

                analog = sum(1 for a in hmi.alarms if a.alarm_type == "Analog")
                discrete = sum(1 for a in hmi.alarms if a.alarm_type == "Discrete")
                if verbose:
                    print(f"    [OK] {analog} analog, {discrete} discrete alarms")

            # 3d. HMI tag tables
            if hmi.tag_tables:
                print(f"  [INFO] Ingesting {len(hmi.tag_tables)} HMI tag tables...")
                for tt in hmi.tag_tables:
                    self.graph.create_hmi_tag_table(
                        name=tt.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                        folder=tt.folder,
                    )
                    summary["hmi_tag_tables"] += 1

            # 3e. HMI scripts
            if hmi.scripts:
                print(f"  [INFO] Ingesting {len(hmi.scripts)} HMI scripts...")
                for script in hmi.scripts:
                    self.graph.create_hmi_script(
                        name=script.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                        script_file=script.script_file,
                        functions=script.functions,
                        script_text=script.script_text,
                    )
                    summary["hmi_scripts"] += 1
                    if verbose:
                        funcs = ", ".join(script.functions[:5])
                        more = f" +{len(script.functions) - 5}" if len(script.functions) > 5 else ""
                        print(f"    [OK] {script.name}: {funcs}{more}")

            # 3f. HMI screens
            if hmi.screens:
                print(f"  [INFO] Ingesting {len(hmi.screens)} HMI screens...")
                for screen in hmi.screens:
                    self.graph.create_hmi_screen(
                        name=screen.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                        folder=screen.folder,
                    )
                    summary["hmi_screens"] += 1

            # 3g. HMI text lists
            if hmi.text_lists:
                print(f"  [INFO] Ingesting {len(hmi.text_lists)} HMI text lists...")
                for tl in hmi.text_lists:
                    self.graph.create_hmi_text_list(
                        name=tl.name,
                        hmi_name=hmi.name,
                        project_name=project.name,
                    )
                    summary["hmi_text_lists"] += 1

        # --- 4. Print summary ---
        print(f"\n{'=' * 60}")
        print(f"  TIA Project Ingestion Complete: {project.name}")
        print(f"{'=' * 60}")
        print(f"  PLC Devices:      {summary['plc_devices']}")
        print(f"    Blocks (AOIs):  {summary['blocks']}")
        print(f"    PLC Tag Tables: {summary['plc_tag_tables']}")
        print(f"    PLC Tags:       {summary['plc_tags']}")
        print(f"    Types/UDTs:     {summary['plc_types']}")
        print(f"    Cross-refs:     {summary['cross_references']}")
        print(f"  HMI Devices:      {summary['hmi_devices']}")
        print(f"    Connections:    {summary['hmi_connections']}")
        print(f"    Alarm Classes:  {summary['hmi_alarm_classes']}")
        print(f"    Alarms:         {summary['hmi_alarms']}")
        print(f"    HMI Tag Tables: {summary['hmi_tag_tables']}")
        print(f"    Scripts:        {summary['hmi_scripts']}")
        print(f"    Screens:        {summary['hmi_screens']}")
        print(f"    Text Lists:     {summary['hmi_text_lists']}")
        print()

        return summary

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
        description="Analyze PLC files and generate semantic ontologies using Claude (stored in Neo4j). "
        "Supports Rockwell (L5X, L5K, ACD), Siemens (ST, TIA XML), and pre-exported .sc files."
    )
    parser.add_argument(
        "input", nargs="?",
        help="Path to PLC file (.L5X, .L5K, .ACD, .sc, .st) or directory",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        default=None,
        help="File pattern for directory mode (default: *.aoi.sc, or *.st with --siemens)",
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
        "--siemens",
        action="store_true",
        help="Use Siemens ST parser instead of Rockwell SC parser",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show semantic analysis status for AOIs",
    )
    parser.add_argument(
        "--tia-xml",
        action="store_true",
        help="Use Siemens TIA Portal XML parser (for Openness XML exports)",
    )
    parser.add_argument(
        "--cross-ref",
        action="store_true",
        help="Extract cross-references (INSTANTIATES / USES_TYPE) between AOIs/FBs/UDTs from parsed files",
    )
    parser.add_argument(
        "--tia-project",
        action="store_true",
        help="Parse entire Siemens TIA Portal project structure (PLCs + HMIs + interlinks)",
    )
    parser.add_argument(
        "--rockwell",
        action="store_true",
        help="Force Rockwell mode: auto-detect and process L5X, L5K, and ACD files",
    )

    args = parser.parse_args()

    # Resolve default pattern based on platform
    if args.pattern is None:
        if args.tia_xml:
            args.pattern = "*.xml"
        elif args.siemens:
            args.pattern = "*.st"
        else:
            args.pattern = "*.aoi.sc"

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

        elif args.tia_project and args.input:
            # Full TIA Portal project ingestion
            input_path = Path(args.input)
            if not input_path.is_dir():
                print("[ERROR] --tia-project requires a project directory as input")
                sys.exit(1)
            summary = analyzer.ingest_tia_project(
                str(input_path),
                verbose=args.verbose,
                skip_ai=args.skip_ai,
            )
            total = sum(summary.values())
            print(f"[OK] Ingested {total} items into Neo4j")

        elif args.cross_ref and args.input:
            # Standalone cross-reference extraction (re-parse, no AI)
            input_path = Path(args.input)
            if not input_path.is_dir():
                print("[ERROR] --cross-ref requires a directory as input")
                sys.exit(1)
            is_siemens = args.siemens
            count = analyzer.cross_reference_directory(
                str(input_path),
                pattern=args.pattern,
                siemens=is_siemens,
                verbose=args.verbose,
            )
            print(f"\n[OK] Created {count} cross-reference relationships")

        elif args.input:
            input_path = Path(args.input)

            # Auto-detect format from flags / file extension
            is_tia_xml = args.tia_xml or input_path.suffix.lower() == ".xml"
            is_siemens = args.siemens or input_path.suffix.lower() == ".st"
            is_rockwell_native = (
                args.rockwell
                or input_path.suffix.lower() in ROCKWELL_EXTENSIONS
                or (input_path.is_file() and is_rockwell_file(str(input_path)))
            )

            # Process directory or single file
            if input_path.is_dir():
                if is_rockwell_native or args.rockwell:
                    # Check if directory contains native Rockwell files
                    rockwell_files = find_rockwell_files(str(input_path))
                    if rockwell_files:
                        ontologies = analyzer.analyze_rockwell_directory(
                            str(input_path),
                            verbose=args.verbose,
                            skip_ai=args.skip_ai,
                        )
                        action = "Imported" if args.skip_ai else "Analyzed"
                        print(
                            f"\n[OK] {action} {len(ontologies)} Rockwell components "
                            f"and stored in Neo4j"
                        )
                        if args.skip_ai:
                            print(
                                "[INFO] Use incremental analyzer to add semantic descriptions"
                            )
                    else:
                        # Fall back to .sc file pattern
                        ontologies = analyzer.analyze_directory(
                            str(input_path),
                            pattern=args.pattern,
                            verbose=args.verbose,
                            skip_ai=args.skip_ai,
                        )
                        action = "Imported" if args.skip_ai else "Analyzed"
                        print(
                            f"\n[OK] {action} {len(ontologies)} Rockwell blocks "
                            f"and stored in Neo4j"
                        )
                else:
                    ontologies = analyzer.analyze_directory(
                        str(input_path),
                        pattern=args.pattern,
                        verbose=args.verbose,
                        skip_ai=args.skip_ai,
                        siemens=is_siemens,
                        tia_xml=is_tia_xml,
                    )
                    action = "Imported" if args.skip_ai else "Analyzed"
                    if is_tia_xml:
                        platform = "Siemens TIA XML"
                    elif is_siemens:
                        platform = "Siemens"
                    else:
                        platform = "Rockwell"
                    print(
                        f"\n[OK] {action} {len(ontologies)} {platform} blocks "
                        f"and stored in Neo4j"
                    )
                    if args.skip_ai:
                        print(
                            "[INFO] Use incremental analyzer to add semantic descriptions"
                        )

            elif input_path.is_file():
                if is_rockwell_native and not is_tia_xml and not is_siemens:
                    # Native Rockwell file (L5X, L5K, ACD)
                    ontologies = analyzer.analyze_rockwell_file(
                        str(input_path),
                        verbose=args.verbose,
                        skip_ai=args.skip_ai,
                    )
                    action = "Imported" if args.skip_ai else "Analyzed"
                    print(
                        f"\n[OK] {action} {len(ontologies)} components "
                        f"and stored in Neo4j"
                    )

                elif is_tia_xml:
                    # TIA Portal XML file
                    tia_parser = TiaXmlParser()
                    parsed_blocks = tia_parser.parse_file(str(input_path))
                    if not parsed_blocks:
                        print(f"[WARNING] No parseable blocks in {input_path.name}")
                    else:
                        for sc_file in parsed_blocks:
                            ontology = analyzer.analyze_sc_file(
                                sc_file, verbose=args.verbose, skip_ai=args.skip_ai
                            )
                            print(
                                f"\n=== Ontology: {ontology['name']} ({ontology['type']}) ==="
                            )
                            if not args.skip_ai:
                                print(
                                    f"Purpose: {ontology['analysis'].get('purpose', 'N/A')}"
                                )
                            action = "Created" if args.skip_ai else "Stored"
                            print(f"[OK] {action} in Neo4j")

                elif is_siemens:
                    # Siemens .st file — may contain multiple blocks
                    siemens_parser = SiemensSTParser()
                    parsed_blocks = siemens_parser.parse_file(str(input_path))
                    if not parsed_blocks:
                        print(f"[WARNING] No parseable blocks in {input_path.name}")
                    else:
                        for sc_file in parsed_blocks:
                            ontology = analyzer.analyze_sc_file(
                                sc_file, verbose=args.verbose, skip_ai=args.skip_ai
                            )
                            print(
                                f"\n=== Ontology: {ontology['name']} ({ontology['type']}) ==="
                            )
                            if not args.skip_ai:
                                print(
                                    f"Purpose: {ontology['analysis'].get('purpose', 'N/A')}"
                                )
                            action = "Created" if args.skip_ai else "Stored"
                            print(f"[OK] {action} in Neo4j")
                else:
                    # Rockwell .sc file (pre-exported)
                    sc_parser_inst = SCParser()
                    sc_file = sc_parser_inst.parse_file(str(input_path))
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
