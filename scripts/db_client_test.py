#!/usr/bin/env python3
"""Tiny CLI used by the Electron UI to test a single database connection."""

import json
import sys

from dotenv import load_dotenv

load_dotenv()

from neo4j_ontology import get_ontology_graph
from db_client import DatabaseClient


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "Usage: db_client_test.py <connection_name>"}))
        sys.exit(1)

    connection_name = sys.argv[1]

    try:
        graph = get_ontology_graph()
        client = DatabaseClient(neo4j_graph=graph)
        result = client.test_connection(connection_name)
        print(json.dumps(result))
    except Exception as exc:
        print(json.dumps({"success": False, "error": str(exc)}))


if __name__ == "__main__":
    main()
