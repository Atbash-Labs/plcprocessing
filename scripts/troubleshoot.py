#!/usr/bin/env python3
"""
Interactive troubleshooting assistant for PLC/SCADA systems.
Uses Claude with Neo4j tools to answer operator questions about faults,
symptoms, and diagnostics based on the enriched ontology.

Supports conversation history for multi-turn dialogues.
"""

import sys
import json
from typing import Optional, List, Dict
from dotenv import load_dotenv

from claude_client import ClaudeClient


SYSTEM_PROMPT = """You are an expert industrial automation troubleshooting assistant. You help operators and technicians diagnose problems with PLC/SCADA systems.

You have access to a Neo4j graph database containing:
- **AOIs** (Add-On Instructions): PLC control logic components
- **UDTs** (User-Defined Types): SCADA data structures
- **Equipment**: Specific equipment instances (motors, valves, etc.) linked to UDTs
- **Views**: HMI screens operators interact with
- **ViewComponents**: Individual UI elements (buttons, labels, LEDs, inputs) within views
- **ScadaTags**: Standalone SCADA tags (query, memory, OPC, expression)
- **Scripts**: Project library scripts with Python code
- **NamedQueries**: Pre-defined SQL queries for database operations
- **Tags**: Individual data points with their purposes
- **FaultSymptoms**: Known fault conditions with causes and resolution steps
- **OperatorPhrases**: Common ways operators describe problems
- **ControlPatterns**: How components behave (interlocks, modes, etc.)
- **SafetyElements**: Safety-critical aspects of each component
- **MAPS_TO_SCADA relationships**: How PLC components connect to SCADA
- **BINDS_TO relationships**: How ViewComponents bind to UDTs/ScadaTags
  Properties: binding_type (tag/expression/query/property), target_text (original binding target), bidirectional (bool), property (which UI prop), tag_path

IMPORTANT - ViewComponent binding data:
- ViewComponent nodes may have an **unresolved_bindings** property (JSON array). These are bindings that could NOT be resolved to a known UDT, ScadaTag, or NamedQuery. Each entry has: property, type, target, bidirectional. Property-type bindings (type="property") show component-to-component data flow within a view. Expression bindings contain formula text. These are critical clues for UI troubleshooting.
- ViewComponent nodes may have an **event_scripts** property (JSON array). These are Python event handlers (onClick, onChange, etc.) that run when operators interact with the component.
- Always check unresolved_bindings and event_scripts when investigating UI/HMI issues.

MANDATORY TOOL USE:
You MUST query the database using run_query or get_node to look up actual data BEFORE answering. Do NOT answer based solely on get_schema results or general knowledge. Every response should be grounded in specific data retrieved from the graph.

CRITICAL - NO ASSUMPTIONS, VERIFY EVERYTHING:
This is the most important rule. NEVER assume that a pattern found on one component applies to similar components. Industrial systems are full of inconsistencies -- one motor may be configured differently from the next, one heat exchanger may have correct bindings while its sibling does not.

Before making any claim about a component's bindings, tag paths, or configuration, your query results MUST contain data for that specific component. You can query multiple components in a single query (e.g., WHERE c.path CONTAINS 'HX' to get all heat exchangers at once), but you must actually read and report the results for each component you make claims about. If a component doesn't appear in your query results, you cannot make claims about it.

Rules:
- Do NOT say "the same issue applies to X" unless your query results explicitly show X's data and confirm it.
- If your results cover HX1 but not HX2/HX3, say so and run a broader query before concluding.
- NEVER fabricate tag names, binding targets, or values. Only report what the database returned.
- When comparing siblings, use a broad query that returns all of them, then report what you actually see for each one -- differences are common and important.

Useful queries:
- Find components related to a problem: MATCH (c:ViewComponent) WHERE toLower(c.name) CONTAINS toLower('keyword') RETURN c.path, c.type, c.unresolved_bindings, c.event_scripts LIMIT 10
- Find equipment: MATCH (e:Equipment) WHERE toLower(e.name) CONTAINS toLower('keyword') RETURN e.name, e.type, e.purpose LIMIT 10
- Check ALL bindings on a component (ALWAYS do this for UI issues): MATCH (c:ViewComponent)-[r:BINDS_TO]->(t) WHERE c.path CONTAINS 'component_name' RETURN c.path, r.binding_type, r.target_text, r.property, r.bidirectional, labels(t), t.name
- Check unresolved bindings on a component: MATCH (c:ViewComponent) WHERE c.path CONTAINS 'component_name' AND c.unresolved_bindings IS NOT NULL RETURN c.path, c.unresolved_bindings
- Find fault symptoms: MATCH (f:FaultSymptom) WHERE toLower(f.description) CONTAINS toLower('keyword') RETURN f
- Trace from view to equipment: MATCH (v:View)-[:HAS_COMPONENT]->(c:ViewComponent)-[:BINDS_TO]->(u:UDT) WHERE v.name CONTAINS 'keyword' RETURN v.name, c.path, u.name
- Compare bindings across sibling components: MATCH (c:ViewComponent)-[r:BINDS_TO]->(t) WHERE c.path CONTAINS 'HX' RETURN c.path, r.target_text, r.property ORDER BY c.path

TROUBLESHOOTING APPROACH:
1. Identify which component(s) the operator is asking about
2. Use run_query to search for matching nodes (Equipment, ViewComponent, AOI, etc.)
3. For EACH component involved, query its specific BINDS_TO relationships AND unresolved_bindings
4. Query for FaultSymptoms and OperatorPhrases related to the described problem
5. Check ControlPatterns to understand normal behavior
6. Find SafetyElements that might be involved
7. Trace tag relationships to find root causes
8. When the user asks about additional components, query each one individually -- never extrapolate

RESPONSE FORMAT:
- Start with a brief summary of what you found in the database
- Clearly distinguish between verified findings (backed by query results) and uncertainties
- List possible causes in order of likelihood
- Provide specific things to check (PLC tags, SCADA screens, physical equipment)
- Suggest resolution steps
- Mention any safety considerations

Be practical and actionable. Operators need clear guidance, not theory."""


def run_interactive():
    """Run interactive troubleshooting session."""
    print("\n" + "=" * 60)
    print("  PLC/SCADA Troubleshooting Assistant")
    print("=" * 60)
    print("\nDescribe your problem and I'll help diagnose it.")
    print("Examples:")
    print("  - 'The motor won't start'")
    print("  - 'Valve is stuck in manual mode'")
    print("  - 'Getting a timeout error on conveyor 3'")
    print("  - 'What does the Motor_Reversing AOI do?'")
    print("\nType 'quit' or 'exit' to end the session.")
    print("Type 'clear' to start a new conversation.")
    print("-" * 60 + "\n")

    client = ClaudeClient(enable_tools=True)

    try:
        while True:
            try:
                question = input("\n🔧 You: ").strip()
            except EOFError:
                break

            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                print("\nGoodbye!")
                break
            if question.lower() == "clear":
                print("\n[Conversation cleared]\n")
                continue

            print("\n🤖 Assistant: ", end="", flush=True)

            # Query Claude with tools
            result = client.query(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=question,
                max_tokens=16000,
                use_tools=True,
                verbose=False,
                require_data_query=True,
            )

            # Handle encoding for Windows terminal
            response = result.get("text", "I couldn't process that question.")
            try:
                print(response)
            except UnicodeEncodeError:
                # Fallback for Windows terminals that can't handle unicode
                safe_response = response.encode("ascii", "replace").decode("ascii")
                print(safe_response)

            # Show tool usage summary
            tool_calls = result.get("tool_calls", [])
            if tool_calls:
                print(f"\n  [Queried {len(tool_calls)} data sources]")

    finally:
        client.close()


def ask_single(
    question: str, history: List[Dict] = None, verbose: bool = False, context: str = ""
) -> Dict:
    """
    Ask a troubleshooting question with optional conversation history.

    Args:
        question: The new user question
        history: Previous conversation as list of {role, content} dicts
        verbose: Enable debug output

    Returns:
        Dict with 'response' (text) and 'history' (updated conversation)
    """
    client = ClaudeClient(enable_tools=True)

    try:
        effective_question = question
        if context:
            effective_question = (
                "Authoritative case context for this investigation:\n"
                f"{context}\n\n"
                "Use this context together with tool calls against the ontology and connected systems.\n\n"
                f"Investigator question:\n{question}"
            )

        # Build messages from history + new question
        if history:
            messages = list(history)
            messages_for_model = list(history)
            messages_for_model.append({"role": "user", "content": effective_question})
        else:
            messages = []
            messages_for_model = [{"role": "user", "content": effective_question}]

        result = client.query(
            system_prompt=SYSTEM_PROMPT,
            messages=messages_for_model,
            max_tokens=4000,
            use_tools=True,
            verbose=verbose,
            require_data_query=True,
        )

        response = result.get("text", "I couldn't process that question.")

        if verbose:
            tool_calls = result.get("tool_calls", [])
            if tool_calls:
                print(f"\n[DEBUG] Made {len(tool_calls)} tool calls", file=sys.stderr)

        # Update history with the new exchange
        updated_history = list(history or [])
        updated_history.append({"role": "user", "content": question})
        updated_history.append({"role": "assistant", "content": response})

        return {"response": response, "history": updated_history}

    finally:
        client.close()


def ask_with_history_json(history_json: str, verbose: bool = False) -> str:
    """
    Process a request with conversation history from JSON.

    Expected JSON format:
    {
        "question": "new question",
        "history": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
    }

    Returns JSON with response and updated history.
    """
    try:
        data = json.loads(history_json)
        question = data.get("question", "")
        history = data.get("history", [])
        context = data.get("context", "")

        result = ask_single(question, history=history, verbose=verbose, context=context)

        return json.dumps(result, ensure_ascii=False)
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}", "history": []})
    except Exception as e:
        return json.dumps({"error": str(e), "history": []})


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Interactive troubleshooting assistant for PLC/SCADA systems"
    )
    parser.add_argument(
        "question", nargs="?", help="Single question to ask (omit for interactive mode)"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show debug output including tool calls",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Read JSON with question and history from stdin",
    )

    args = parser.parse_args()

    load_dotenv()

    # Fix Windows encoding
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if args.history:
        # Read JSON from stdin with conversation history
        history_json = sys.stdin.read()
        result = ask_with_history_json(history_json, verbose=args.verbose)
        print(result)
    elif args.question:
        # Single question mode (backwards compatible)
        result = ask_single(args.question, verbose=args.verbose)
        print(result["response"])
    else:
        # Interactive mode
        run_interactive()


if __name__ == "__main__":
    main()
