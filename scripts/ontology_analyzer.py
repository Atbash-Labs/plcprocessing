#!/usr/bin/env python3
"""
LLM-based analyzer for PLC code using Anthropic's Claude API.
Generates semantic understanding of tags and logic.
"""

import os
import json
from typing import Dict, List, Optional, Any
from dataclasses import asdict
import anthropic
from pathlib import Path

from sc_parser import SCParser, SCFile, Tag


class OntologyAnalyzer:
    """Analyzes PLC code using Claude to generate semantic ontologies."""

    def __init__(self, api_key: Optional[str] = None, model: str = "claude-sonnet-4-5-20250929"):
        """Initialize the analyzer with Anthropic API."""
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment or .env file")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model

    def analyze_sc_file(self, sc_file: SCFile, verbose: bool = False) -> Dict[str, Any]:
        """Analyze a parsed SC file and generate ontology."""

        if verbose:
            print(f"[INFO] Analyzing {sc_file.name}...")

        # Build context for LLM
        context = self._build_analysis_context(sc_file)

        # Generate analysis
        analysis = self._query_llm(context, verbose)

        # Structure the response
        ontology = {
            'name': sc_file.name,
            'type': sc_file.type,
            'source_file': sc_file.file_path,
            'metadata': {
                'revision': sc_file.revision,
                'vendor': sc_file.vendor,
                'description': sc_file.description
            },
            'analysis': analysis
        }

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
                context_parts.append(f"... and {len(sc_file.local_tags) - 10} more local variables")
            context_parts.append("")

        # Logic rungs (sample key ones)
        if sc_file.routines:
            context_parts.append("## Logic Implementation")
            for routine in sc_file.routines:
                context_parts.append(f"### Routine: {routine['name']} ({routine['type']})")
                for rung in routine['rungs'][:15]:  # First 15 rungs
                    if rung.comment:
                        context_parts.append(f"\nRung {rung.number}: {rung.comment}")
                    else:
                        context_parts.append(f"\nRung {rung.number}:")
                    context_parts.append(f"```\n{rung.logic}\n```")

                if len(routine['rungs']) > 15:
                    context_parts.append(f"\n... and {len(routine['rungs']) - 15} more rungs")
                context_parts.append("")

        return "\n".join(context_parts)

    def _query_llm(self, context: str, verbose: bool = False) -> Dict[str, Any]:
        """Query Claude API for analysis."""

        system_prompt = """You are an expert PLC (Programmable Logic Controller) engineer specializing in analyzing industrial control logic. Your task is to analyze PLC code and generate semantic ontologies that explain what tags (variables) mean and how the PLC manipulates them.

For each PLC component, provide:
1. **Functional Purpose**: High-level description of what this component does
2. **Tag Semantics**: For each important tag, explain its semantic meaning (not just "a boolean" but "emergency stop signal that halts operation")
3. **Relationships**: How tags influence each other (e.g., "bInEstop inhibits bOutCommandForward")
4. **Control Patterns**: Identify common patterns (timers, interlocks, state machines, safety logic, etc.)
5. **Data Flow**: Key paths showing how inputs lead to outputs
6. **Safety-Critical Elements**: Any tags or logic related to safety

Focus on semantic meaning and operational intent, not just syntax."""

        user_prompt = f"""Analyze this PLC component and generate a semantic ontology:

{context}

Provide your analysis as a structured JSON object with these fields:
- "purpose": string describing the functional purpose
- "tags": object mapping tag names to their semantic descriptions
- "relationships": array of {{from, to, relationship_type, description}} objects
- "control_patterns": array of identified patterns with descriptions
- "data_flows": array describing key input-to-output paths
- "safety_critical": array of safety-critical tags/logic

Be concise but informative. Focus on the "why" and "what" rather than just restating the syntax."""

        if verbose:
            print("[INFO] Querying Claude API...")

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=20000,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )

            response_text = message.content[0].text

            # Try to extract JSON from response
            # Claude might wrap it in markdown code blocks
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()

            analysis = json.loads(response_text)
            return analysis

        except json.JSONDecodeError as e:
            print(f"[WARNING] Failed to parse JSON response: {e}")
            print(f"[DEBUG] Raw response: {response_text[:500]}")
            # Return raw text if JSON parsing fails
            return {
                "purpose": "Analysis failed - JSON parsing error",
                "raw_response": response_text,
                "tags": {},
                "relationships": [],
                "control_patterns": [],
                "data_flows": [],
                "safety_critical": []
            }
        except Exception as e:
            print(f"[ERROR] API call failed: {e}")
            return {
                "purpose": f"Analysis failed - {str(e)}",
                "tags": {},
                "relationships": [],
                "control_patterns": [],
                "data_flows": [],
                "safety_critical": []
            }

    def analyze_directory(self, directory: str, output_file: Optional[str] = None,
                         pattern: str = "*.aoi.sc", verbose: bool = False) -> List[Dict[str, Any]]:
        """Analyze all SC files in a directory."""

        dir_path = Path(directory)
        sc_files = list(dir_path.rglob(pattern))

        if not sc_files:
            print(f"[WARNING] No files matching '{pattern}' found in {directory}")
            return []

        print(f"[INFO] Found {len(sc_files)} files to analyze")

        parser = SCParser()
        ontologies = []

        for i, sc_path in enumerate(sc_files, 1):
            print(f"\n[{i}/{len(sc_files)}] Processing {sc_path.name}...")

            try:
                # Parse SC file
                sc_file = parser.parse_file(str(sc_path))

                # Analyze with LLM
                ontology = self.analyze_sc_file(sc_file, verbose)
                ontologies.append(ontology)

                print(f"[OK] Completed {sc_path.name}")

            except Exception as e:
                print(f"[ERROR] Failed to process {sc_path.name}: {e}")
                continue

        # Save results if output file specified
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(ontologies, f, indent=2)
            print(f"\n[OK] Saved ontologies to {output_file}")

        return ontologies


def main():
    """CLI for ontology analyzer."""
    import sys
    import argparse
    from dotenv import load_dotenv

    # Load .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Analyze PLC .sc files and generate semantic ontologies using Claude"
    )
    parser.add_argument('input', help='Path to .sc file or directory')
    parser.add_argument('-o', '--output', help='Output JSON file for ontology')
    parser.add_argument('-p', '--pattern', default='*.aoi.sc',
                       help='File pattern for directory mode (default: *.aoi.sc)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('--model', default='claude-sonnet-4-5-20250929',
                       help='Claude model to use')

    args = parser.parse_args()

    # Initialize analyzer
    try:
        analyzer = OntologyAnalyzer(model=args.model)
    except ValueError as e:
        print(f"[ERROR] {e}")
        print("[INFO] Please set ANTHROPIC_API_KEY in .env file or environment")
        sys.exit(1)

    input_path = Path(args.input)

    # Process directory or single file
    if input_path.is_dir():
        ontologies = analyzer.analyze_directory(
            str(input_path),
            output_file=args.output,
            pattern=args.pattern,
            verbose=args.verbose
        )
        print(f"\n[OK] Analyzed {len(ontologies)} files")

    elif input_path.is_file():
        # Parse and analyze single file
        sc_parser = SCParser()
        sc_file = sc_parser.parse_file(str(input_path))
        ontology = analyzer.analyze_sc_file(sc_file, verbose=args.verbose)

        # Print summary
        print(f"\n=== Ontology: {ontology['name']} ===")
        print(f"Purpose: {ontology['analysis'].get('purpose', 'N/A')}")

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(ontology, f, indent=2)
            print(f"\n[OK] Saved ontology to {args.output}")
        else:
            print("\n=== Full Analysis ===")
            print(json.dumps(ontology['analysis'], indent=2))

    else:
        print(f"[ERROR] Input path not found: {args.input}")
        sys.exit(1)


if __name__ == "__main__":
    main()
