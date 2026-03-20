#!/usr/bin/env python3
"""
Case management API for Electron UI.
Provides JSON-based CRUD and report generation for investigation cases.
"""

import argparse
import json
import sys
from datetime import date, datetime
from typing import Any

from neo4j_ontology import get_ontology_graph


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that tolerates Neo4j and datetime-like values."""

    def default(self, obj: Any):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if hasattr(obj, "isoformat"):
            try:
                return obj.isoformat()
            except Exception:
                pass
        if hasattr(obj, "to_native"):
            try:
                return str(obj.to_native())
            except Exception:
                pass
        return str(obj)


def output_json(data: Any) -> None:
    print(json.dumps(data, cls=DateTimeEncoder))


def read_stdin_json() -> Any:
    payload = sys.stdin.read().strip()
    if not payload:
        return {}
    return json.loads(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Investigation case API")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List investigation cases")
    p_list.add_argument("--limit", type=int, default=100)
    p_list.add_argument("--status")

    p_get = sub.add_parser("get", help="Get one investigation case")
    p_get.add_argument("--case-id", required=True)

    sub.add_parser("create-from-event", help="Create a case from stdin JSON event payload")

    p_update = sub.add_parser("update", help="Update case fields from stdin JSON")
    p_update.add_argument("--case-id", required=True)

    p_delete = sub.add_parser("delete", help="Delete an investigation case")
    p_delete.add_argument("--case-id", required=True)

    p_draft = sub.add_parser("generate-draft", help="Generate an AI draft for a case")
    p_draft.add_argument("--case-id", required=True)

    p_approve = sub.add_parser("approve-draft", help="Approve a draft for a case")
    p_approve.add_argument("--case-id", required=True)

    p_reject = sub.add_parser("reject-draft", help="Reject a draft for a case")
    p_reject.add_argument("--case-id", required=True)

    p_report = sub.add_parser("generate-report", help="Generate a markdown case report")
    p_report.add_argument("--case-id", required=True)

    args = parser.parse_args()
    graph = get_ontology_graph()

    try:
        if args.command == "list":
            cases = graph.list_investigation_cases(limit=args.limit, status=args.status)
            output_json({"success": True, "cases": cases})
            return 0

        if args.command == "get":
            case = graph.get_investigation_case(args.case_id)
            if not case:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, "case": case})
            return 0

        if args.command == "create-from-event":
            event_payload = read_stdin_json()
            case = graph.create_investigation_case_from_event(event_payload)
            output_json({"success": True, "case": case})
            return 0

        if args.command == "update":
            patch = read_stdin_json()
            case = graph.update_investigation_case(args.case_id, patch)
            if not case:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, "case": case})
            return 0

        if args.command == "delete":
            deleted = graph.delete_investigation_case(args.case_id)
            if not deleted:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, "case_id": args.case_id})
            return 0

        if args.command == "generate-draft":
            case = graph.generate_investigation_case_draft(args.case_id)
            if not case:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, "case": case})
            return 0

        if args.command == "approve-draft":
            case = graph.approve_investigation_case_draft(args.case_id)
            if not case:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, "case": case})
            return 0

        if args.command == "reject-draft":
            case = graph.reject_investigation_case_draft(args.case_id)
            if not case:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, "case": case})
            return 0

        if args.command == "generate-report":
            report = graph.generate_investigation_case_report(args.case_id)
            if not report:
                output_json({"success": False, "error": f"Case not found: {args.case_id}"})
                return 1
            output_json({"success": True, **report})
            return 0

        output_json({"success": False, "error": f"Unsupported command: {args.command}"})
        return 1
    finally:
        graph.close()


if __name__ == "__main__":
    raise SystemExit(main())
