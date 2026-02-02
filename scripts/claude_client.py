#!/usr/bin/env python3
"""
Unified Claude API client with Neo4j tool support.
Provides generic graph exploration tools that allow Claude to query
the ontology database with maximum flexibility.
"""

import os
import sys
import json
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
import anthropic
from dotenv import load_dotenv

from neo4j_ontology import OntologyGraph, get_ontology_graph
from mes_ontology import (
    MES_TOOL_DEFINITIONS,
    MESTools,
    extend_ontology,
    MES_SYSTEM_PROMPT_EXTENSION,
)


# Load environment variables
load_dotenv()


@dataclass
class ToolResult:
    """Result from a tool call."""

    tool_use_id: str
    content: str
    is_error: bool = False


class OntologyTools:
    """
    Neo4j-backed tools for generic graph exploration.

    Provides flexible tools that let Claude discover and query
    the graph structure directly:

    - get_schema: Discover node labels, relationship types, and properties
    - run_query: Execute Cypher queries to explore data
    - get_node: Get a specific node by label and name

    Also includes MES/RCA tools for ISA-95 Level 3-4 integration:
    - get_batch_context: Full batch RCA context
    - get_equipment_rca: Equipment RCA with PLC/SCADA chain
    - get_ccp_context: Critical Control Point monitoring context
    - search_by_symptom: Search by operator observations
    - trace_tag_impact: Trace PLC tag to business impact
    - get_process_ccps: List CCPs for a process
    - get_open_deviations: Open deviations with context
    """

    # Tool definitions for Claude (base + MES tools)
    TOOL_DEFINITIONS = [
        {
            "name": "get_schema",
            "description": "Get the database schema including all node labels, relationship types, and their properties. Use this first to understand what data exists and how to query it.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "run_query",
            "description": "Execute a Cypher query against the Neo4j graph database. Use this to explore data, find patterns, and retrieve information. Results are limited to 50 rows. For string matching use toLower() and CONTAINS.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Cypher query to execute. Example: MATCH (a:AOI) RETURN a.name, a.purpose LIMIT 10",
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional query parameters as key-value pairs",
                    },
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_node",
            "description": "Get a specific node and all its properties by label and name. Also returns immediate relationships.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "label": {
                        "type": "string",
                        "description": "Node label (e.g., AOI, UDT, Tag, FaultSymptom)",
                    },
                    "name": {
                        "type": "string",
                        "description": "The name property of the node to retrieve",
                    },
                },
                "required": ["label", "name"],
            },
        },
        {
            "name": "create_mapping",
            "description": "Create a MAPS_TO_SCADA relationship between a PLC component (AOI) and a SCADA component (UDT or Equipment). Use exact node names from the database. Call this after querying to find matching pairs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "aoi_name": {
                        "type": "string",
                        "description": "Exact name of the AOI node (PLC component)",
                    },
                    "scada_name": {
                        "type": "string",
                        "description": "Exact name of the UDT or Equipment node (SCADA component)",
                    },
                    "mapping_type": {
                        "type": "string",
                        "description": "Type of mapping (e.g., 'control_interface', 'status_display', 'data_binding')",
                    },
                    "description": {
                        "type": "string",
                        "description": "Brief description of how these components relate",
                    },
                },
                "required": ["aoi_name", "scada_name", "mapping_type", "description"],
            },
        },
    ] + MES_TOOL_DEFINITIONS  # Add MES/RCA tools

    def __init__(self, graph: OntologyGraph):
        """Initialize with Neo4j graph connection."""
        self.graph = graph

        # Extend graph with MES methods
        extend_ontology(graph)

        # Initialize MES tools
        self._mes_tools = MESTools(graph)

        # Map tool names to methods (base tools)
        self._tools: Dict[str, Callable] = {
            "get_schema": self._get_schema,
            "run_query": self._run_query,
            "get_node": self._get_node,
            "create_mapping": self._create_mapping,
        }

    def execute(self, tool_name: str, tool_input: Dict) -> str:
        """Execute a tool and return the result as a string."""
        # Check if it's a MES tool
        if tool_name in self._mes_tools._tools:
            return self._mes_tools.execute(tool_name, tool_input)

        # Check base tools
        if tool_name not in self._tools:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

        try:
            result = self._tools[tool_name](**tool_input)
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def _get_schema(self) -> Dict:
        """Get the database schema - labels, relationships, and properties."""
        with self.graph.session() as session:
            # Get node labels and counts
            labels_result = session.run(
                """
                CALL db.labels() YIELD label
                CALL {
                    WITH label
                    MATCH (n) WHERE label IN labels(n)
                    RETURN count(n) as count
                }
                RETURN label, count
                ORDER BY count DESC
            """
            )
            labels = [{"label": r["label"], "count": r["count"]} for r in labels_result]

            # Get relationship types and counts
            rels_result = session.run(
                """
                CALL db.relationshipTypes() YIELD relationshipType
                CALL {
                    WITH relationshipType
                    MATCH ()-[r]->() WHERE type(r) = relationshipType
                    RETURN count(r) as count
                }
                RETURN relationshipType, count
                ORDER BY count DESC
            """
            )
            relationships = [
                {"type": r["relationshipType"], "count": r["count"]}
                for r in rels_result
            ]

            # Get sample properties for each major label
            properties = {}
            for label_info in labels[:10]:  # Top 10 labels
                label = label_info["label"]
                props_result = session.run(
                    f"""
                    MATCH (n:{label})
                    WITH n LIMIT 1
                    RETURN keys(n) as props
                """
                )
                record = props_result.single()
                if record:
                    properties[label] = record["props"]

            # Get relationship patterns (what connects to what)
            patterns_result = session.run(
                """
                MATCH (a)-[r]->(b)
                WITH labels(a)[0] as from_label, type(r) as rel_type, labels(b)[0] as to_label
                RETURN DISTINCT from_label, rel_type, to_label
                ORDER BY from_label, rel_type
                LIMIT 50
            """
            )
            patterns = [
                f"(:{r['from_label']})-[:{r['rel_type']}]->(:{r['to_label']})"
                for r in patterns_result
            ]

        return {
            "node_labels": labels,
            "relationship_types": relationships,
            "properties_by_label": properties,
            "relationship_patterns": patterns,
            "tips": [
                "Use MATCH (n:Label) to query nodes of a type",
                "Use MATCH (a)-[:REL_TYPE]->(b) to follow relationships",
                "Use WHERE toLower(n.property) CONTAINS toLower($term) for text search",
                "Use RETURN n.prop1, n.prop2 to select specific properties",
                "PLC/SCADA labels: AOI, Tag, UDT, FaultSymptom, Equipment, View",
                "MES/ERP labels: Material, Batch, ProductionOrder, Operation, CriticalControlPoint, ProcessDeviation",
                "Key cross-layer relationships: Equipment-[:CONTROLLED_BY]->AOI, CriticalControlPoint-[:MONITORED_BY]->Equipment",
            ],
        }

    def _run_query(self, query: str, params: Optional[Dict] = None) -> Dict:
        """Execute a Cypher query and return results."""
        # Safety check - prevent destructive operations
        query_upper = query.upper()
        forbidden = ["DELETE", "REMOVE", "SET ", "CREATE", "MERGE", "DROP"]
        for word in forbidden:
            if word in query_upper:
                return {
                    "error": f"Destructive operations ({word}) not allowed in queries"
                }

        # Add LIMIT if not present
        if "LIMIT" not in query_upper:
            query = query.rstrip().rstrip(";") + " LIMIT 50"

        with self.graph.session() as session:
            try:
                result = session.run(query, params or {})
                records = []
                for r in result:
                    # Convert record to dict, handling Neo4j types
                    record_dict = {}
                    for key in r.keys():
                        value = r[key]
                        # Handle Neo4j Node objects
                        if hasattr(value, "items"):
                            record_dict[key] = dict(value)
                        elif isinstance(value, list):
                            record_dict[key] = [
                                dict(v) if hasattr(v, "items") else v for v in value
                            ]
                        else:
                            record_dict[key] = value
                    records.append(record_dict)

                return {
                    "count": len(records),
                    "results": records,
                }
            except Exception as e:
                return {"error": f"Query failed: {str(e)}"}

    def _get_node(self, label: str, name: str) -> Dict:
        """Get a specific node and its relationships."""
        with self.graph.session() as session:
            # Get the node
            node_result = session.run(
                f"""
                MATCH (n:{label})
                WHERE n.name = $name OR n.symptom = $name OR n.phrase = $name
                RETURN n, labels(n) as labels
                LIMIT 1
            """,
                {"name": name},
            )
            record = node_result.single()
            if not record:
                return {"error": f"Node {label}:{name} not found"}

            node = dict(record["n"])
            node_labels = record["labels"]

            # Get outgoing relationships
            out_result = session.run(
                f"""
                MATCH (n:{label})-[r]->(m)
                WHERE n.name = $name OR n.symptom = $name OR n.phrase = $name
                RETURN type(r) as rel_type, labels(m)[0] as target_label, 
                       coalesce(m.name, m.symptom, m.phrase, 'unnamed') as target_name,
                       properties(r) as rel_props
                LIMIT 20
            """,
                {"name": name},
            )
            outgoing = [
                {
                    "relationship": r["rel_type"],
                    "target_label": r["target_label"],
                    "target_name": r["target_name"],
                    "properties": dict(r["rel_props"]) if r["rel_props"] else {},
                }
                for r in out_result
            ]

            # Get incoming relationships
            in_result = session.run(
                f"""
                MATCH (m)-[r]->(n:{label})
                WHERE n.name = $name OR n.symptom = $name OR n.phrase = $name
                RETURN type(r) as rel_type, labels(m)[0] as source_label,
                       coalesce(m.name, m.symptom, m.phrase, 'unnamed') as source_name,
                       properties(r) as rel_props
                LIMIT 20
            """,
                {"name": name},
            )
            incoming = [
                {
                    "relationship": r["rel_type"],
                    "source_label": r["source_label"],
                    "source_name": r["source_name"],
                    "properties": dict(r["rel_props"]) if r["rel_props"] else {},
                }
                for r in in_result
            ]

        return {
            "labels": node_labels,
            "properties": node,
            "outgoing_relationships": outgoing,
            "incoming_relationships": incoming,
        }

    def _create_mapping(
        self, aoi_name: str, scada_name: str, mapping_type: str, description: str
    ) -> Dict:
        """Create a MAPS_TO_SCADA relationship between AOI and UDT/Equipment."""
        with self.graph.session() as session:
            # Try to find and link the nodes
            result = session.run(
                """
                MATCH (aoi:AOI {name: $aoi_name})
                OPTIONAL MATCH (udt:UDT {name: $scada_name})
                OPTIONAL MATCH (equip:Equipment {name: $scada_name})
                WITH aoi, COALESCE(udt, equip) as scada
                WHERE aoi IS NOT NULL AND scada IS NOT NULL
                MERGE (aoi)-[r:MAPS_TO_SCADA]->(scada)
                SET r.mapping_type = $mapping_type,
                    r.description = $description
                RETURN aoi.name as aoi, 
                       COALESCE(scada.name, 'unknown') as scada,
                       labels(scada)[0] as scada_type
                """,
                {
                    "aoi_name": aoi_name,
                    "scada_name": scada_name,
                    "mapping_type": mapping_type,
                    "description": description,
                },
            )
            record = result.single()

            if record:
                return {
                    "success": True,
                    "created": f"{record['aoi']} -[MAPS_TO_SCADA]-> {record['scada']} ({record['scada_type']})",
                    "mapping_type": mapping_type,
                }
            else:
                # Find what exists to help Claude debug
                aoi_check = session.run(
                    "MATCH (a:AOI {name: $name}) RETURN a.name as name",
                    {"name": aoi_name},
                ).single()
                scada_check = session.run(
                    """
                    OPTIONAL MATCH (u:UDT {name: $name})
                    OPTIONAL MATCH (e:Equipment {name: $name})
                    RETURN COALESCE(u.name, e.name) as name
                    """,
                    {"name": scada_name},
                ).single()

                return {
                    "success": False,
                    "error": "Could not find one or both nodes",
                    "aoi_found": aoi_check["name"] if aoi_check else None,
                    "scada_found": scada_check["name"] if scada_check else None,
                    "hint": "Query for exact node names using get_schema or run_query first",
                }


class ClaudeClient:
    """
    Unified Claude API client with Neo4j tool support.

    Provides generic graph exploration tools that give Claude
    maximum flexibility to query and understand the ontology.

    Includes both PLC/SCADA tools and MES/ERP tools:
    - Base tools: get_schema, run_query, get_node, create_mapping
    - MES tools: get_batch_context, get_equipment_rca, get_ccp_context,
                 search_by_symptom, trace_tag_impact, get_process_ccps,
                 get_open_deviations
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5-20250929",
        graph: Optional[OntologyGraph] = None,
        enable_tools: bool = True,
    ):
        """
        Initialize the Claude client.

        Args:
            api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
            model: Claude model to use
            graph: Optional Neo4j graph connection (created if not provided)
            enable_tools: Whether to enable Neo4j tools for Claude
        """
        load_dotenv()
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found")

        # Set timeout to 5 minutes (streaming responses can take a while)
        self.client = anthropic.Anthropic(
            api_key=self.api_key,
            timeout=300.0,  # 5 minute timeout
        )
        self.model = model

        # Neo4j connection and tools
        self._graph = graph
        self._owns_graph = False
        self._tools: Optional[OntologyTools] = None
        self._enable_tools = enable_tools

        if enable_tools:
            self._init_tools()

    def _get_graph(self) -> OntologyGraph:
        """Get or create Neo4j connection."""
        if self._graph is None:
            self._graph = get_ontology_graph()
            self._owns_graph = True
        return self._graph

    def _init_tools(self) -> None:
        """Initialize Neo4j tools (includes MES tools)."""
        self._tools = OntologyTools(self._get_graph())

    @staticmethod
    def get_mes_system_prompt() -> str:
        """Get the MES system prompt extension for RCA workflows."""
        return MES_SYSTEM_PROMPT_EXTENSION

    def _stream_response(
        self,
        system_prompt: str,
        messages: List[Dict],
        max_tokens: int,
        tools: Optional[List[Dict]],
    ):
        """Stream response from Claude, printing text as it arrives."""
        print("[STREAM] ", end="", file=sys.stderr, flush=True)

        try:
            # Build kwargs - only include tools if provided
            kwargs = {
                "model": self.model,
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": messages,
            }
            if tools:
                kwargs["tools"] = tools

            with self.client.messages.stream(**kwargs) as stream:
                # Track if we got any text (tool_use responses might not have text)
                got_text = False
                try:
                    for text in stream.text_stream:
                        got_text = True
                        # Print each chunk as it arrives
                        print(text, end="", file=sys.stderr, flush=True)
                except Exception as e:
                    # text_stream can fail if Claude is doing tool_use
                    print(
                        f"\n[STREAM END: {type(e).__name__}]",
                        file=sys.stderr,
                        flush=True,
                    )

                # Get the final message
                response = stream.get_final_message()

                if not got_text and response.stop_reason == "tool_use":
                    print("[TOOL CALL]", file=sys.stderr, flush=True)

            return response

        except anthropic.APITimeoutError as e:
            print(
                f"\n[TIMEOUT] API request timed out: {e}", file=sys.stderr, flush=True
            )
            raise
        except anthropic.APIConnectionError as e:
            print(f"\n[CONNECTION ERROR] {e}", file=sys.stderr, flush=True)
            raise
        except Exception as e:
            print(
                f"\n[STREAM ERROR] {type(e).__name__}: {e}", file=sys.stderr, flush=True
            )
            raise

    def close(self) -> None:
        """Close Neo4j connection if we own it."""
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(
        self,
        system_prompt: str,
        user_prompt: str = None,
        messages: List[Dict] = None,
        max_tokens: int = 16000,
        max_continuations: int = 3,
        max_tool_rounds: int = 50,  # High limit - Claude self-regulates
        use_tools: bool = True,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Send a query to Claude with optional tool support.

        Args:
            system_prompt: System instructions for Claude
            user_prompt: Single user query (for simple calls)
            messages: Full conversation history (list of {role, content} dicts)
                     If provided, user_prompt is ignored
            max_tokens: Max tokens per response
            max_continuations: Max continuation attempts for truncated responses
            max_tool_rounds: Max rounds of tool calls before final response
            use_tools: Whether to allow tool calls (if enabled at init)
            verbose: Print debug information

        Returns:
            Dict with 'text' (final text response), 'tool_calls' (list of tool calls made),
            'usage' (token usage)
        """
        # Use provided messages or create from user_prompt
        if messages:
            messages = list(messages)  # Copy to avoid modifying original
        elif user_prompt:
            messages = [{"role": "user", "content": user_prompt}]
        else:
            raise ValueError("Either user_prompt or messages must be provided")

        # Determine if we should use tools
        tools = None
        if use_tools and self._enable_tools and self._tools:
            tools = OntologyTools.TOOL_DEFINITIONS

        tool_calls_made = []
        total_input_tokens = 0
        total_output_tokens = 0

        for tool_round in range(max_tool_rounds + 1):
            if verbose:
                print(
                    f"[DEBUG] Round {tool_round + 1}, messages: {len(messages)}",
                    file=sys.stderr,
                    flush=True,
                )

            # Make API call with streaming for visibility
            api_start = time.time()
            if verbose:
                # Use streaming to show response as it's generated
                response = self._stream_response(
                    system_prompt, messages, max_tokens, tools
                )
            else:
                # Build kwargs - only include tools if provided
                kwargs = {
                    "model": self.model,
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                    "messages": messages,
                }
                if tools:
                    kwargs["tools"] = tools
                response = self.client.messages.create(**kwargs)
            api_elapsed = time.time() - api_start

            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            if verbose:
                print(
                    f"\n[DEBUG] API call took {api_elapsed:.1f}s, stop_reason: {response.stop_reason}",
                    file=sys.stderr,
                    flush=True,
                )
                print(
                    f"[DEBUG] Content blocks: {len(response.content)}, tokens: {response.usage.input_tokens}+{response.usage.output_tokens}",
                    file=sys.stderr,
                    flush=True,
                )

            # Check if we need to handle tool use
            if response.stop_reason == "tool_use":
                # Extract tool uses and text from response
                assistant_content = []
                tool_uses = []

                for block in response.content:
                    if block.type == "tool_use":
                        tool_uses.append(block)
                        assistant_content.append(
                            {
                                "type": "tool_use",
                                "id": block.id,
                                "name": block.name,
                                "input": block.input,
                            }
                        )
                    elif block.type == "text":
                        assistant_content.append({"type": "text", "text": block.text})

                # Add assistant message
                messages.append({"role": "assistant", "content": assistant_content})

                # Execute tools and collect results
                tool_results = []
                for tool_use in tool_uses:
                    if verbose:
                        tool_input_str = json.dumps(tool_use.input, default=str)
                        if len(tool_input_str) > 100:
                            tool_input_str = tool_input_str[:100] + "..."
                        print(
                            f"[TOOL] {tool_use.name}: {tool_input_str}",
                            file=sys.stderr,
                            flush=True,
                        )

                    tool_start = time.time()
                    result = self._tools.execute(tool_use.name, tool_use.input)
                    tool_elapsed = time.time() - tool_start

                    tool_calls_made.append(
                        {
                            "name": tool_use.name,
                            "input": tool_use.input,
                            "result": (
                                result[:500] + "..." if len(result) > 500 else result
                            ),
                        }
                    )

                    if verbose:
                        print(
                            f"[TOOL] Result: {len(result)} chars ({tool_elapsed:.1f}s)",
                            file=sys.stderr,
                            flush=True,
                        )

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use.id,
                            "content": result,
                        }
                    )

                # Add tool results
                messages.append({"role": "user", "content": tool_results})

                if verbose:
                    print(
                        f"[DEBUG] Continuing to next round...",
                        file=sys.stderr,
                        flush=True,
                    )
                # Continue to next round
                continue

            # No tool use - extract final text
            if verbose:
                print(
                    f"[DEBUG] Extracting final response, content blocks: {[b.type for b in response.content]}",
                    file=sys.stderr,
                    flush=True,
                )
            full_response = ""
            for block in response.content:
                if hasattr(block, "text"):
                    full_response += block.text

            if verbose:
                print(
                    f"[DEBUG] Final response length: {len(full_response)} chars",
                    file=sys.stderr,
                    flush=True,
                )

            # Handle continuation if needed
            if response.stop_reason == "max_tokens":
                # Build continuation messages once, then update assistant content each iteration
                cont_messages = messages.copy()
                cont_messages.append({"role": "assistant", "content": full_response})
                cont_messages.append(
                    {
                        "role": "user",
                        "content": "Continue from where you left off. Do not repeat any content. If outputting JSON, continue the JSON structure exactly.",
                    }
                )

                for cont in range(max_continuations):
                    if verbose:
                        print(
                            f"[INFO] Continuing response ({cont + 1}/{max_continuations})...",
                            file=sys.stderr,
                            flush=True,
                        )

                    cont_response = self.client.messages.create(
                        model=self.model,
                        max_tokens=max_tokens,
                        system=system_prompt,
                        messages=cont_messages,
                    )

                    total_input_tokens += cont_response.usage.input_tokens
                    total_output_tokens += cont_response.usage.output_tokens

                    cont_text = ""
                    for block in cont_response.content:
                        if hasattr(block, "text"):
                            cont_text += block.text

                    full_response += cont_text

                    if cont_response.stop_reason != "max_tokens":
                        break

                    # Update assistant content for next continuation
                    cont_messages[-2]["content"] = full_response

            return {
                "text": full_response,
                "tool_calls": tool_calls_made,
                "usage": {
                    "input_tokens": total_input_tokens,
                    "output_tokens": total_output_tokens,
                },
            }

        # Should not reach here, but return what we have
        return {
            "text": "",
            "tool_calls": tool_calls_made,
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
            },
            "error": "Max tool rounds exceeded",
        }

    def query_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 16000,
        max_continuations: int = 3,
        max_tool_rounds: int = 50,  # High limit - Claude self-regulates
        use_tools: bool = True,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Query Claude expecting a JSON response.
        Automatically extracts and parses JSON from the response.

        Returns:
            Dict with 'data' (parsed JSON), 'tool_calls', 'usage', and optionally 'error'
        """
        result = self.query(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            max_continuations=max_continuations,
            max_tool_rounds=max_tool_rounds,
            use_tools=use_tools,
            verbose=verbose,
        )

        # Extract JSON from response
        text = result["text"].strip()

        if not text:
            return {
                "data": None,
                "error": "Empty response from Claude",
                "tool_calls": result["tool_calls"],
                "usage": result["usage"],
            }

        # Remove markdown code blocks if present
        if "```json" in text:
            json_start = text.find("```json") + 7
            json_end = text.rfind("```")
            if json_end > json_start:
                text = text[json_start:json_end].strip()
        elif "```" in text:
            json_start = text.find("```") + 3
            json_end = text.rfind("```")
            if json_end > json_start:
                text = text[json_start:json_end].strip()

        # Try to parse JSON
        try:
            data = json.loads(text)
            return {
                "data": data,
                "tool_calls": result["tool_calls"],
                "usage": result["usage"],
            }
        except json.JSONDecodeError as e:
            # Try to fix common issues
            fixed = self._attempt_json_fix(text)
            if fixed:
                return {
                    "data": fixed,
                    "tool_calls": result["tool_calls"],
                    "usage": result["usage"],
                }

            return {
                "data": None,
                "error": f"JSON parse error: {e}",
                "raw_text": text[:2000],
                "tool_calls": result["tool_calls"],
                "usage": result["usage"],
            }

    def _attempt_json_fix(self, json_str: str) -> Optional[Dict]:
        """Attempt to fix truncated or malformed JSON."""
        open_braces = json_str.count("{")
        close_braces = json_str.count("}")
        open_brackets = json_str.count("[")
        close_brackets = json_str.count("]")

        fixed = json_str.rstrip()

        # Remove trailing comma
        if fixed.endswith(","):
            fixed = fixed[:-1]

        # Close unclosed strings
        if fixed.count('"') % 2 != 0:
            last_quote = fixed.rfind('"')
            if last_quote > 0:
                search_back = fixed[:last_quote].rfind('"')
                if search_back > 0:
                    fixed = fixed[:search_back] + '""'

        # Add missing closing brackets/braces
        missing_brackets = open_brackets - close_brackets
        missing_braces = open_braces - close_braces

        if missing_brackets > 0 or missing_braces > 0:
            fixed += "]" * missing_brackets
            fixed += "}" * missing_braces

        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            return None

    @property
    def graph(self) -> OntologyGraph:
        """Access the Neo4j graph connection."""
        return self._get_graph()


# Convenience function
def get_claude_client(
    model: str = "claude-sonnet-4-5-20250929", enable_tools: bool = True
) -> ClaudeClient:
    """Get a configured Claude client with Neo4j tools."""
    return ClaudeClient(model=model, enable_tools=enable_tools)


# Relationship proposal system prompt
RELATIONSHIP_PROPOSAL_PROMPT = """You are an expert at understanding industrial automation ontology relationships.

Given a natural language description of a relationship, you will:
1. Use tools to explore the existing ontology and find the referenced nodes
2. Determine what graph changes are needed to represent the relationship
3. Return a structured JSON response with proposed changes

Available node types in the ontology:
- PLC Layer: AOI, Tag
- SCADA Layer: UDT, Equipment, View, Script, NamedQuery, Project, ViewComponent
- MES Layer: Material, Batch, ProductionOrder, Operation, CCP (Critical Control Point)
- Troubleshooting: FaultSymptom, FaultCause, OperatorPhrase

Common relationship types:
- CONTROLLED_BY: Equipment controlled by AOI
- INSTANCE_OF: Equipment is instance of UDT
- MAPS_TO_SCADA: PLC component maps to SCADA component
- DISPLAYS: View displays UDT/Equipment
- HAS_TAG: AOI has tag
- HAS_SYMPTOM: AOI has fault symptom
- MONITORED_BY: CCP monitored by equipment
- BINDS_TO: ViewComponent binds to UDT/Equipment

IMPORTANT: First use tools to verify that the referenced nodes exist. If they don't exist, include a create_node action.

Return your response as JSON with this structure:
{
  "proposed_changes": [
    {"action": "create_edge", "source": "SourceName", "source_type": "NodeType", "target": "TargetName", "target_type": "NodeType", "type": "RELATIONSHIP_TYPE", "confidence": 0.0-1.0},
    {"action": "create_node", "name": "NodeName", "node_type": "NodeType", "properties": {}, "confidence": 0.0-1.0}
  ],
  "explanation": "Brief explanation of why these changes are proposed"
}
"""


def propose_relationship(
    client: "ClaudeClient", description: str, verbose: bool = False
) -> Dict:
    """
    Use AI to propose graph changes based on natural language description.

    Args:
        client: Claude client instance
        description: Natural language description of the relationship
        verbose: Print debug info to stderr

    Returns:
        Dict with proposed_changes and explanation
    """
    user_prompt = f"""Analyze this relationship description and propose graph changes:

"{description}"

First, use tools to find if the referenced nodes exist in the database. Then return the JSON with proposed changes."""

    result = client.query_json(
        system_prompt=RELATIONSHIP_PROPOSAL_PROMPT,
        user_prompt=user_prompt,
        use_tools=True,
        verbose=verbose,
    )

    if result.get("error"):
        return {
            "success": False,
            "error": result["error"],
            "proposed_changes": [],
            "explanation": "",
        }

    data = result.get("data", {})

    return {
        "success": True,
        "proposed_changes": data.get("proposed_changes", []),
        "explanation": data.get("explanation", ""),
        "tool_calls": result.get("tool_calls", []),
    }


# CLI for testing
def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Test Claude client with Neo4j tools")
    parser.add_argument("prompt", nargs="?", help="User prompt to send")
    default_system = (
        "You are an expert PLC/SCADA/MES engineer specializing in pharmaceutical manufacturing. "
        "Use the available tools to explore the ontology database and answer questions. "
        "The database contains PLC control logic (AOIs, Tags), SCADA components (UDTs, Views), "
        "and MES/ERP data (Materials, Batches, Production Orders, Critical Control Points). "
        "When troubleshooting, trace from operator symptoms through equipment to PLC tags."
    )
    parser.add_argument(
        "--system",
        "-s",
        default=default_system,
        help="System prompt",
    )
    parser.add_argument("--no-tools", action="store_true", help="Disable Neo4j tools")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--test-tools", action="store_true", help="Test tool execution directly"
    )
    parser.add_argument("--schema", action="store_true", help="Show database schema")
    parser.add_argument(
        "--propose-relationship",
        action="store_true",
        help="Propose relationship changes from JSON on stdin",
    )
    parser.add_argument(
        "--explain-nodes",
        nargs="*",
        help="Explain the relationships between specified nodes",
    )

    args = parser.parse_args()

    # Handle encoding for Windows
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    with ClaudeClient(enable_tools=not args.no_tools) as client:
        if args.propose_relationship:
            # Read JSON from stdin with description
            try:
                input_data = json.load(sys.stdin)
                description = input_data.get("description", "")

                if not description:
                    print(
                        json.dumps(
                            {"success": False, "error": "No description provided"}
                        )
                    )
                    sys.exit(1)

                result = propose_relationship(client, description, verbose=args.verbose)
                print(json.dumps(result))

            except json.JSONDecodeError as e:
                print(
                    json.dumps({"success": False, "error": f"Invalid JSON input: {e}"})
                )
                sys.exit(1)
            except Exception as e:
                print(json.dumps({"success": False, "error": str(e)}))
                sys.exit(1)

        elif args.explain_nodes:
            # Explain relationships between nodes
            node_names = args.explain_nodes
            if not node_names:
                print(json.dumps({"success": False, "error": "No node names provided"}))
                sys.exit(1)

            prompt = f"""Explain the relationships between these nodes in the ontology graph: {', '.join(node_names)}

Use tools to find these nodes and their connections. Describe:
1. What each node represents
2. How they are connected
3. The purpose of these relationships in the automation context

Return a JSON response:
{{
  "nodes": [{{"name": "...", "type": "...", "purpose": "..."}}],
  "relationships": [{{"from": "...", "to": "...", "type": "...", "description": "..."}}],
  "summary": "Brief summary of how these components work together"
}}"""

            result = client.query_json(
                system_prompt=default_system,
                user_prompt=prompt,
                use_tools=True,
                verbose=args.verbose,
            )

            if result.get("error"):
                print(json.dumps({"success": False, "error": result["error"]}))
            else:
                print(json.dumps({"success": True, **result.get("data", {})}))

        elif args.test_tools or args.schema:
            # Test tools directly
            print("\n=== Database Schema ===\n")
            tools = client._tools
            result = json.loads(tools.execute("get_schema", {}))

            print("Node Labels:")
            for label in result["node_labels"]:
                print(f"  {label['label']}: {label['count']} nodes")

            print("\nRelationship Types:")
            for rel in result["relationship_types"][:15]:
                print(f"  {rel['type']}: {rel['count']}")

            print("\nRelationship Patterns:")
            for pattern in result["relationship_patterns"][:15]:
                print(f"  {pattern}")

            if args.test_tools:
                print("\n=== Test Query ===\n")
                query_result = json.loads(
                    tools.execute(
                        "run_query",
                        {"query": "MATCH (a:AOI) RETURN a.name, a.purpose LIMIT 5"},
                    )
                )
                print(f"Found {query_result['count']} results:")
                for r in query_result["results"]:
                    print(f"  {r.get('a.name')}: {str(r.get('a.purpose', ''))[:60]}...")

        elif args.prompt:
            result = client.query(
                system_prompt=args.system,
                user_prompt=args.prompt,
                use_tools=not args.no_tools,
                verbose=args.verbose,
            )

            print("\n=== Response ===\n")
            print(result["text"])

            if result["tool_calls"]:
                print(f"\n=== Tool Calls ({len(result['tool_calls'])}) ===")
                for tc in result["tool_calls"]:
                    print(f"  - {tc['name']}")

            print(f"\n=== Usage ===")
            print(f"  Input tokens: {result['usage']['input_tokens']}")
            print(f"  Output tokens: {result['usage']['output_tokens']}")

        else:
            parser.print_help()


if __name__ == "__main__":
    main()
