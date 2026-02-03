#!/usr/bin/env python3
"""
Ingest script for Axilon Workbench backups.
Parses project.json format and stores in Neo4j using the same ontology
as standard Ignition backups.
"""

import argparse
import sys
from pathlib import Path

from workbench_parser import WorkbenchParser
from ignition_ontology import IgnitionOntologyAnalyzer
from claude_client import ClaudeClient


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Axilon Workbench backup into Neo4j ontology"
    )
    parser.add_argument(
        "input", help="Path to project.json file or folder containing it"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        default=True,
        help="Skip AI analysis (default: True, use incremental_analyzer.py later)",
    )
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-5-20250929",
        help="Claude model for AI analysis",
    )

    args = parser.parse_args()

    # Resolve input path
    input_path = Path(args.input)
    if input_path.is_dir():
        project_json = input_path / "project.json"
    else:
        project_json = input_path

    if not project_json.exists():
        print(f"[ERROR] project.json not found at: {project_json}", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(f"[INFO] Parsing workbench backup: {project_json}")

    # Parse workbench format
    wb_parser = WorkbenchParser()
    try:
        backup = wb_parser.parse_file(str(project_json))
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(
            f"[INFO] Parsed: {len(backup.windows)} views, "
            f"{len(backup.named_queries)} queries, "
            f"{len(backup.scripts)} scripts, "
            f"{len(backup.tags)} tags"
        )
        print(f"[INFO] Projects: {list(backup.projects.keys())}")

    # Initialize analyzer and store in Neo4j
    client = ClaudeClient(model=args.model, enable_tools=True)
    analyzer = IgnitionOntologyAnalyzer(client=client)

    try:
        if args.verbose:
            print(f"[INFO] Storing in Neo4j...")

        ontology = analyzer.analyze_backup(
            backup, verbose=args.verbose, skip_ai=args.skip_ai
        )

        if args.verbose:
            print(f"[OK] Workbench ingestion complete")
            print(f"[INFO] Summary:")
            print(f"  - Views: {ontology['summary'].get('windows', 0)}")
            print(f"  - Named Queries: {ontology['summary'].get('named_queries', 0)}")
            print(f"  - Tags: {ontology['summary'].get('tags', 0)}")
            print(
                f"  - UDT Definitions: {ontology['summary'].get('udt_definitions', 0)}"
            )
            print(f"  - UDT Instances: {ontology['summary'].get('udt_instances', 0)}")

    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
