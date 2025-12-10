#!/usr/bin/env python3
"""
Migration script to import existing JSON ontologies into Neo4j.
"""

import argparse
from pathlib import Path

from neo4j_ontology import (
    OntologyGraph, 
    get_ontology_graph, 
    import_json_ontology,
    Neo4jConfig
)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate existing JSON ontologies to Neo4j"
    )
    parser.add_argument('files', nargs='*', help='JSON files to import')
    parser.add_argument('--all', action='store_true',
                       help='Import all JSON files from ontologies/ directory')
    parser.add_argument('--clear', action='store_true',
                       help='Clear existing data before import')
    parser.add_argument('--uri', default='bolt://localhost:7687',
                       help='Neo4j URI')
    parser.add_argument('--user', default='neo4j',
                       help='Neo4j username')
    parser.add_argument('--password', default='leortest1!!!',
                       help='Neo4j password')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    config = Neo4jConfig(uri=args.uri, user=args.user, password=args.password)
    
    # Determine files to import
    files_to_import = []
    
    if args.all:
        ontologies_dir = Path(__file__).parent.parent / 'ontologies'
        files_to_import = list(ontologies_dir.glob('*.json'))
        if args.verbose:
            print(f"[INFO] Found {len(files_to_import)} JSON files in {ontologies_dir}")
    elif args.files:
        files_to_import = [Path(f) for f in args.files]
    else:
        print("[ERROR] No files specified. Use --all or provide file paths.")
        parser.print_help()
        return
    
    # Connect to Neo4j
    print("[INFO] Connecting to Neo4j...")
    with OntologyGraph(config) as graph:
        # Initialize schema
        graph.create_indexes()
        print("[OK] Schema initialized")
        
        # Clear if requested
        if args.clear:
            confirm = input("[WARNING] This will delete ALL existing data. Type 'yes' to confirm: ")
            if confirm.lower() == 'yes':
                graph.clear_all()
                print("[OK] Cleared existing data")
            else:
                print("[CANCELLED] Keeping existing data")
        
        # Import each file
        success_count = 0
        for file_path in files_to_import:
            if not file_path.exists():
                print(f"[SKIP] File not found: {file_path}")
                continue
            
            try:
                print(f"[INFO] Importing {file_path.name}...")
                import_json_ontology(str(file_path), graph)
                success_count += 1
            except Exception as e:
                print(f"[ERROR] Failed to import {file_path.name}: {e}")
        
        print(f"\n[OK] Successfully imported {success_count}/{len(files_to_import)} files")
        
        # Show summary
        with graph.session() as session:
            result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as type, count(n) as count
                ORDER BY count DESC
            """)
            print("\n[INFO] Database contents:")
            for record in result:
                print(f"  {record['type']}: {record['count']}")


if __name__ == "__main__":
    main()

