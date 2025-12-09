#!/usr/bin/env python3
"""
LLM-based analyzer for Ignition SCADA configurations using Anthropic's Claude API.
Generates semantic understanding of tags, UDTs, views, and data flows.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
import anthropic
from dotenv import load_dotenv

from ignition_parser import IgnitionParser, IgnitionBackup


class IgnitionOntologyAnalyzer:
    """Analyzes Ignition configurations using Claude to generate semantic ontologies."""

    def __init__(self, api_key: Optional[str] = None, model: str = "claude-sonnet-4-5-20250929"):
        """Initialize the analyzer with Anthropic API."""
        load_dotenv()
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model

    def analyze_backup(self, backup: IgnitionBackup, verbose: bool = False) -> Dict[str, Any]:
        """Analyze an Ignition backup and generate ontology."""

        if verbose:
            print(f"[INFO] Analyzing Ignition backup...")

        # Build context for LLM
        context = self._build_analysis_context(backup)

        # Generate analysis
        analysis = self._query_llm(context, verbose)

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
        """Query Claude API for analysis."""

        system_prompt = """You are an expert in industrial automation and SCADA systems, specializing in Ignition by Inductive Automation. Your task is to analyze Ignition configurations and generate semantic ontologies that explain:

1. What each UDT (User Defined Type) represents in the industrial process
2. How tag instances map to physical equipment or process data
3. The purpose of views/windows and how they present data to operators
4. Data flow from PLCs through tags to UI displays
5. Relationships between different system components

Focus on the industrial/operational meaning, not just the technical structure. Identify patterns like:
- Equipment templates (motors, valves, sensors)
- HMI patterns (dashboards, control panels, data displays)
- Data pathways (OPC to tag to UI binding)
- Hierarchical organization (areas, lines, equipment)"""

        user_prompt = f"""Analyze this Ignition SCADA configuration and generate a semantic ontology:

{context}

Provide your analysis as a structured JSON object with these fields:
- "system_purpose": string describing what this SCADA system monitors/controls
- "udt_semantics": object mapping UDT names to their industrial purpose
- "equipment_instances": array of {{name, type, purpose, plc_connection}} for each UDT instance
- "data_flows": array describing how data moves from PLC to UI
- "view_purposes": object mapping view names to their operational purpose
- "tag_categories": object grouping tags by their function (control, status, setpoint, etc.)
- "integration_points": array of external system connections (OPC servers, databases, etc.)
- "operational_patterns": array of identified patterns in the configuration

Be concise but informative. Focus on industrial/operational semantics."""

        if verbose:
            print("[INFO] Querying Claude API...")

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=20000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )

            response_text = message.content[0].text

            # Extract JSON from response
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()

            return json.loads(response_text)

        except json.JSONDecodeError as e:
            print(f"[WARNING] Failed to parse JSON: {e}")
            return {"error": "JSON parsing failed", "raw_response": response_text[:1000]}
        except Exception as e:
            print(f"[ERROR] API call failed: {e}")
            return {"error": str(e)}


def main():
    """CLI for Ignition ontology analyzer."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze Ignition backup JSON and generate semantic ontology"
    )
    parser.add_argument('input', help='Path to Ignition backup JSON file')
    parser.add_argument('-o', '--output', help='Output JSON file for ontology')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    # Parse backup
    ignition_parser = IgnitionParser()
    backup = ignition_parser.parse_file(args.input)

    print(f"[INFO] Parsed: {len(backup.udt_definitions)} UDTs, {len(backup.udt_instances)} instances, {len(backup.windows)} views")

    # Analyze
    analyzer = IgnitionOntologyAnalyzer()
    ontology = analyzer.analyze_backup(backup, verbose=args.verbose)

    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(ontology, f, indent=2)
        print(f"[OK] Saved ontology to {args.output}")
    else:
        print("\n=== Ignition Ontology ===")
        print(json.dumps(ontology['analysis'], indent=2))


if __name__ == "__main__":
    main()
