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
- **Tags**: Individual data points with their purposes
- **FaultSymptoms**: Known fault conditions with causes and resolution steps
- **OperatorPhrases**: Common ways operators describe problems
- **ControlPatterns**: How components behave (interlocks, modes, etc.)
- **SafetyElements**: Safety-critical aspects of each component
- **MAPS_TO_SCADA relationships**: How PLC components connect to SCADA

Use the available tools to investigate:
1. **get_schema**: See what data exists in the database
2. **run_query**: Execute Cypher queries to find relevant information
3. **get_node**: Get details about specific components

TROUBLESHOOTING APPROACH:
1. Identify which component(s) the operator is asking about
2. Query for FaultSymptoms related to the described problem
3. Look up OperatorPhrases that match their description
4. Check ControlPatterns to understand normal behavior
5. Find SafetyElements that might be involved
6. Trace tag relationships to find root causes

RESPONSE FORMAT:
- Start with a brief summary of what you found
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
            if question.lower() in ('quit', 'exit', 'q'):
                print("\nGoodbye!")
                break
            if question.lower() == 'clear':
                print("\n[Conversation cleared]\n")
                continue

            print("\n🤖 Assistant: ", end="", flush=True)
            
            # Query Claude with tools
            result = client.query(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=question,
                max_tokens=4000,
                use_tools=True,
                verbose=False
            )
            
            # Handle encoding for Windows terminal
            response = result.get("text", "I couldn't process that question.")
            try:
                print(response)
            except UnicodeEncodeError:
                # Fallback for Windows terminals that can't handle unicode
                safe_response = response.encode('ascii', 'replace').decode('ascii')
                print(safe_response)
            
            # Show tool usage summary
            tool_calls = result.get("tool_calls", [])
            if tool_calls:
                print(f"\n  [Queried {len(tool_calls)} data sources]")
    
    finally:
        client.close()


def ask_single(question: str, history: List[Dict] = None, verbose: bool = False) -> Dict:
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
        # Build messages from history + new question
        if history:
            messages = list(history)
            messages.append({"role": "user", "content": question})
        else:
            messages = [{"role": "user", "content": question}]
        
        result = client.query(
            system_prompt=SYSTEM_PROMPT,
            messages=messages,
            max_tokens=4000,
            use_tools=True,
            verbose=verbose
        )
        
        response = result.get("text", "I couldn't process that question.")
        
        if verbose:
            tool_calls = result.get("tool_calls", [])
            if tool_calls:
                print(f"\n[DEBUG] Made {len(tool_calls)} tool calls", file=sys.stderr)
        
        # Update history with the new exchange
        updated_history = messages + [{"role": "assistant", "content": response}]
        
        return {
            "response": response,
            "history": updated_history
        }
    
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
        
        result = ask_single(question, history=history, verbose=verbose)
        
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
    parser.add_argument('question', nargs='?', 
                       help='Single question to ask (omit for interactive mode)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Show debug output including tool calls')
    parser.add_argument('--history', action='store_true',
                       help='Read JSON with question and history from stdin')
    
    args = parser.parse_args()
    
    load_dotenv()
    
    # Fix Windows encoding
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    
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

