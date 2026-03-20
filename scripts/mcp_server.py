#!/usr/bin/env python3
"""
MCP server that exposes all OntologyTools from claude_client.py.

Bridges the existing tool layer (get_schema, run_query, get_node, create_mapping,
get_current_time, MES/RCA tools, live Ignition tools, and DB tools) into the
Model Context Protocol so any MCP-aware client (Cursor, Claude Desktop, etc.)
can use them.

Usage:
    python scripts/mcp_server.py

Configure in Cursor (settings.json) or Claude Desktop (claude_desktop_config.json):
    {
      "mcpServers": {
        "plc-ontology": {
          "command": "python",
          "args": ["c:/path/to/plcprocessing/scripts/mcp_server.py"]
        }
      }
    }

Environment variables (from .env or shell):
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD  – Neo4j connection
    IGNITION_API_URL, IGNITION_API_TOKEN    – optional live Ignition API
"""

import asyncio
import json
import os
import sys
from typing import Any

# Ensure the scripts directory is on the path so local imports work.
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv

load_dotenv()

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

from claude_client import OntologyTools
from neo4j_ontology import get_ontology_graph
from ignition_api_client import IgnitionApiClient

# ---------------------------------------------------------------------------
# Lazy singleton for OntologyTools (Neo4j connection is expensive to open)
# ---------------------------------------------------------------------------

_ontology_tools: OntologyTools | None = None


def _get_tools() -> OntologyTools:
    global _ontology_tools
    if _ontology_tools is None:
        graph = get_ontology_graph()

        api_client: IgnitionApiClient | None = None
        api_url = os.getenv("IGNITION_API_URL")
        if api_url:
            api_client = IgnitionApiClient(
                base_url=api_url,
                api_token=os.getenv("IGNITION_API_TOKEN"),
            )

        _ontology_tools = OntologyTools(graph, api_client=api_client)
    return _ontology_tools


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

app = Server("plc-ontology")


@app.list_tools()
async def list_tools() -> list[types.Tool]:
    """Return all tool definitions from OntologyTools."""
    tools = _get_tools()
    mcp_tools = []
    for defn in tools.get_all_tool_definitions():
        mcp_tools.append(
            types.Tool(
                name=defn["name"],
                description=defn["description"],
                inputSchema=defn["input_schema"],
            )
        )
    return mcp_tools


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    """Delegate tool execution to OntologyTools.execute()."""
    tools = _get_tools()
    # execute() is synchronous – run it in a thread to avoid blocking the event loop
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, tools.execute, name, arguments or {})

    # result is already a JSON string; surface it as text content
    return [types.TextContent(type="text", text=result)]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def _serve() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(_serve())
