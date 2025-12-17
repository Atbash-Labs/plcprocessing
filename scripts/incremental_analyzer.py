#!/usr/bin/env python3
"""
Incremental semantic analyzer for Ignition SCADA configurations.

Processes items in batches, allowing for resumable analysis sessions.
Tracks semantic_status on each item to know what has been analyzed.
"""

import json
import sys
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

from ignition_parser import IgnitionParser, IgnitionBackup, UDTDefinition
from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient


@dataclass
class AnalysisSession:
    """Tracks progress of an analysis session."""
    
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    errors: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class IncrementalAnalyzer:
    """
    Analyzes Ignition configurations incrementally, batch by batch.
    
    Uses semantic_status field on Neo4j nodes to track progress:
    - pending: Not yet analyzed
    - in_progress: Currently being analyzed
    - complete: Has semantic description
    - review: Needs human review
    """
    
    # Order of analysis (dependencies flow down)
    # AOI first (PLC), then Ignition entities, ScadaTags, ViewComponent last
    ANALYSIS_ORDER = ["AOI", "UDT", "Equipment", "ScadaTag", "View", "ViewComponent"]
    
    def __init__(
        self,
        backup: IgnitionBackup,
        graph: Optional[OntologyGraph] = None,
        client: Optional[ClaudeClient] = None,
        batch_size: int = 10,
    ):
        """Initialize the incremental analyzer.
        
        Args:
            backup: Parsed Ignition backup (for raw data context)
            graph: Neo4j graph connection (created if not provided)
            client: Claude client (created if not provided)
            batch_size: Number of items to process per batch
        """
        load_dotenv()
        
        self.backup = backup
        self.batch_size = batch_size
        
        # Build lookup maps from backup for quick access
        self._udt_defs: Dict[str, UDTDefinition] = {
            udt.name: udt for udt in backup.udt_definitions
        }
        self._windows = {w.name: w for w in backup.windows}
        self._instances = {inst.name: inst for inst in backup.udt_instances}
        self._tags = {tag.name: tag for tag in backup.tags}
        
        # Connections
        self._graph = graph
        self._owns_graph = False
        self._client = client
        self._owns_client = False
        
        if self._graph is None:
            self._graph = get_ontology_graph()
            self._owns_graph = True
        
        if self._client is None:
            self._client = ClaudeClient(graph=self._graph, enable_tools=True)
            self._owns_client = True
    
    def close(self):
        """Clean up resources."""
        if self._owns_client and self._client:
            self._client.close()
            self._client = None
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @property
    def graph(self) -> OntologyGraph:
        return self._graph
    
    def get_status(self) -> Dict[str, Dict[str, int]]:
        """Get current analysis status for all item types."""
        return self._graph.get_semantic_status_counts()
    
    def recover_stuck_items(self, verbose: bool = False) -> int:
        """Reset any items stuck in 'in_progress' back to 'pending'.
        
        This handles cases where a previous run was interrupted.
        
        Returns:
            Number of items recovered
        """
        total_recovered = 0
        
        for item_type in self.ANALYSIS_ORDER:
            with self._graph.session() as session:
                if item_type == "ViewComponent":
                    result = session.run(
                        """
                        MATCH (n:ViewComponent)
                        WHERE n.semantic_status = 'in_progress'
                        SET n.semantic_status = 'pending'
                        RETURN count(n) as count
                        """
                    )
                else:
                    result = session.run(
                        f"""
                        MATCH (n:{item_type})
                        WHERE n.semantic_status = 'in_progress'
                        SET n.semantic_status = 'pending'
                        RETURN count(n) as count
                        """
                    )
                count = result.single()["count"]
                if count > 0:
                    if verbose:
                        print(f"[INFO] Recovered {count} stuck {item_type} items")
                    total_recovered += count
        
        return total_recovered
    
    def print_status(self):
        """Print a formatted status report."""
        status = self.get_status()
        print("\n=== Semantic Analysis Status ===\n")
        
        total_pending = 0
        total_complete = 0
        
        for item_type in self.ANALYSIS_ORDER:
            counts = status.get(item_type, {})
            pending = counts.get("pending", 0) + counts.get(None, 0)
            complete = counts.get("complete", 0)
            in_progress = counts.get("in_progress", 0)
            review = counts.get("review", 0)
            total = pending + complete + in_progress + review
            
            total_pending += pending
            total_complete += complete
            
            if total > 0:
                pct = (complete / total * 100) if total > 0 else 0
                print(f"  {item_type:15} {complete:3}/{total:<3} complete ({pct:.0f}%)")
                if in_progress > 0:
                    print(f"                  ({in_progress} in progress)")
                if review > 0:
                    print(f"                  ({review} needs review)")
        
        print()
        if total_pending == 0 and total_complete > 0:
            print("  ✓ All items have been analyzed!")
        elif total_pending > 0:
            print(f"  → {total_pending} items remaining to analyze")
        print()
    
    def analyze_next_batch(
        self,
        item_type: Optional[str] = None,
        verbose: bool = False,
    ) -> AnalysisSession:
        """Analyze the next batch of pending items.
        
        Args:
            item_type: Specific type to analyze, or None to auto-select
            verbose: Print detailed progress
            
        Returns:
            AnalysisSession with results
        """
        session = AnalysisSession()
        
        # Determine what to analyze
        if item_type is None:
            item_type = self._get_next_item_type()
            if item_type is None:
                if verbose:
                    print("[INFO] No pending items to analyze")
                return session
        
        # Get pending items
        pending = self._graph.get_pending_items(item_type, self.batch_size)
        if not pending:
            if verbose:
                print(f"[INFO] No pending {item_type} items")
            return session
        
        if verbose:
            print(f"[INFO] Analyzing {len(pending)} {item_type} items...")
        
        # Mark items as in_progress
        item_names = []
        for item in pending:
            name = item.get("path") if item_type == "ViewComponent" else item.get("name")
            item_names.append(name)
            self._graph.set_semantic_status(item_type, name, "in_progress")
        
        # Build context and analyze
        try:
            results = self._analyze_batch(item_type, pending, verbose)
            
            # Update items with results
            for name, result in results.items():
                session.items_processed += 1
                if result.get("error"):
                    session.items_failed += 1
                    session.errors.append({"name": name, "error": result["error"]})
                    self._graph.set_semantic_status(item_type, name, "review")
                else:
                    session.items_succeeded += 1
                    purpose = result.get("purpose", "")
                    self._graph.set_semantic_status(
                        item_type, name, "complete", purpose
                    )
                    if verbose:
                        print(f"  [OK] {name}: {purpose[:60]}...")
        
        except Exception as e:
            # Reset all items to pending on failure
            for name in item_names:
                self._graph.set_semantic_status(item_type, name, "pending")
            raise
        
        return session
    
    def run_session(
        self,
        max_items: int = 50,
        verbose: bool = False,
    ) -> AnalysisSession:
        """Run an analysis session, processing up to max_items.
        
        Processes items in dependency order (UDT → Equipment → View).
        
        Args:
            max_items: Maximum total items to process
            verbose: Print detailed progress
            
        Returns:
            Combined AnalysisSession with all results
        """
        total_session = AnalysisSession()
        items_remaining = max_items
        
        while items_remaining > 0:
            # Determine next type to analyze
            item_type = self._get_next_item_type()
            if item_type is None:
                if verbose:
                    print("[INFO] All items have been analyzed!")
                break
            
            # Analyze a batch
            batch_size = min(self.batch_size, items_remaining)
            original_batch_size = self.batch_size
            self.batch_size = batch_size
            
            try:
                batch_session = self.analyze_next_batch(item_type, verbose)
            finally:
                self.batch_size = original_batch_size
            
            # Accumulate results
            total_session.items_processed += batch_session.items_processed
            total_session.items_succeeded += batch_session.items_succeeded
            total_session.items_failed += batch_session.items_failed
            total_session.errors.extend(batch_session.errors)
            
            items_remaining -= batch_session.items_processed
            
            # If no items were processed, break to avoid infinite loop
            if batch_session.items_processed == 0:
                break
        
        return total_session
    
    def _get_next_item_type(self) -> Optional[str]:
        """Determine the next item type to analyze based on dependency order."""
        status = self.get_status()
        
        for item_type in self.ANALYSIS_ORDER:
            counts = status.get(item_type, {})
            pending = counts.get("pending", 0)
            # Also count items with no status as pending
            if None in counts:
                pending += counts[None]
            if pending > 0:
                return item_type
        
        return None
    
    def _analyze_batch(
        self,
        item_type: str,
        items: List[Dict],
        verbose: bool = False,
    ) -> Dict[str, Dict]:
        """Send a batch of items to Claude for analysis.
        
        Args:
            item_type: Type of items being analyzed
            items: List of item data from get_pending_items
            verbose: Print debug info
            
        Returns:
            Dict mapping item names to their analysis results
        """
        # Build context for the batch
        context = self._build_batch_context(item_type, items)
        
        # Build the prompt
        system_prompt = self._get_system_prompt(item_type)
        user_prompt = self._get_user_prompt(item_type, items, context)
        
        if verbose:
            print(f"[DEBUG] Sending {len(items)} items to Claude...")
        
        # Query Claude
        result = self._client.query_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=8000,
            use_tools=True,
            verbose=verbose,
        )
        
        if verbose:
            if result.get("tool_calls"):
                print(f"[DEBUG] Claude made {len(result['tool_calls'])} tool calls")
        
        # Parse response
        if result.get("error"):
            # Return error for all items
            return {
                self._get_item_name(item_type, item): {"error": result["error"]}
                for item in items
            }
        
        data = result.get("data", {})
        
        # Map results back to item names
        results = {}
        analyses = data.get("analyses", {})
        
        for item in items:
            name = self._get_item_name(item_type, item)
            if name in analyses:
                results[name] = {"purpose": analyses[name]}
            else:
                # Check for alternative key formats
                found = False
                for key, value in analyses.items():
                    if key.lower() == name.lower():
                        results[name] = {"purpose": value}
                        found = True
                        break
                if not found:
                    results[name] = {"error": "No analysis returned by Claude"}
        
        return results
    
    def _get_item_name(self, item_type: str, item: Dict) -> str:
        """Get the identifying name for an item."""
        if item_type == "ViewComponent":
            return item.get("path", "")
        return item.get("name", "")
    
    def _build_batch_context(
        self,
        item_type: str,
        items: List[Dict],
    ) -> Dict[str, Any]:
        """Build context information for a batch of items."""
        context = {
            "already_analyzed": [],
            "raw_definitions": {},
        }
        
        # Get some already-analyzed items for consistency reference
        with self._graph.session() as session:
            if item_type == "ViewComponent":
                result = session.run(
                    """
                    MATCH (n:ViewComponent)
                    WHERE n.semantic_status = 'complete' AND n.purpose IS NOT NULL
                    RETURN n.path as name, n.purpose as purpose
                    LIMIT 3
                    """
                )
            else:
                result = session.run(
                    f"""
                    MATCH (n:{item_type})
                    WHERE n.semantic_status = 'complete' AND n.purpose IS NOT NULL
                    RETURN n.name as name, n.purpose as purpose
                    LIMIT 3
                    """
                )
            context["already_analyzed"] = [dict(r) for r in result]
        
        # Add raw definitions from backup or Neo4j
        if item_type == "AOI":
            # For AOIs, get context from Neo4j (since we don't have a backup file)
            for item in items:
                name = item.get("name")
                aoi_context = self._graph.get_item_with_context("AOI", name)
                if aoi_context:
                    context["raw_definitions"][name] = {
                        "type": aoi_context["item"].get("type", ""),
                        "description": aoi_context["item"].get("description", ""),
                        "tags": aoi_context["context"].get("tags", []),
                        "patterns": aoi_context["context"].get("patterns", []),
                        "scada_mappings": aoi_context["context"].get("scada_mappings", []),
                    }
        
        elif item_type == "UDT":
            for item in items:
                name = item.get("name")
                if name in self._udt_defs:
                    udt = self._udt_defs[name]
                    context["raw_definitions"][name] = {
                        "members": [
                            {
                                "name": m.name,
                                "data_type": m.data_type,
                                "tag_type": m.tag_type,
                            }
                            for m in udt.members
                        ],
                        "parameters": {
                            k: {"data_type": v.data_type}
                            for k, v in udt.parameters.items()
                        },
                        "parent": udt.parent_name,
                    }
        
        elif item_type == "View":
            for item in items:
                name = item.get("name")
                if name in self._windows:
                    window = self._windows[name]
                    context["raw_definitions"][name] = {
                        "path": window.path,
                        "components_count": len(window.components),
                        "component_types": list(set(
                            c.component_type for c in window.components
                        ))[:10],
                    }
        
        elif item_type == "Equipment":
            for item in items:
                name = item.get("name")
                if name in self._instances:
                    inst = self._instances[name]
                    context["raw_definitions"][name] = {
                        "type_id": inst.type_id,
                        "parameters": inst.parameters,
                    }
        
        elif item_type == "ViewComponent":
            for item in items:
                path = item.get("path", "")
                context["raw_definitions"][path] = {
                    "view": item.get("view", ""),
                    "name": item.get("name", ""),
                    "type": item.get("type", ""),
                    "inferred_purpose": item.get("inferred_purpose", ""),
                    "props": item.get("props", ""),
                }
        
        elif item_type == "ScadaTag":
            for item in items:
                name = item.get("name")
                if name in self._tags:
                    tag = self._tags[name]
                    context["raw_definitions"][name] = {
                        "tag_type": tag.tag_type,
                        "data_type": tag.data_type,
                        "folder_name": tag.folder_name,
                        "query": tag.query[:500] if tag.query else None,
                        "datasource": tag.datasource,
                        "opc_item_path": tag.opc_item_path,
                        "expression": tag.expression,
                        "initial_value": tag.initial_value,
                    }
        
        return context
    
    def _get_system_prompt(self, item_type: str) -> str:
        """Get the system prompt for analyzing a specific item type."""
        base = """You are an expert in industrial automation and SCADA systems, specializing in Ignition by Inductive Automation.

Your task is to generate concise semantic descriptions for SCADA configuration items. Focus on:
- Industrial/operational purpose (what does this control/monitor?)
- Role in the system (is this for an operator? for data logging?)
- Key functionality (what are the main features?)

Keep descriptions to 1-2 sentences. Be specific about industrial function, not just technical structure.

You have access to tools to query the existing ontology database for additional context.
"""
        
        type_specific = {
            "AOI": """
You are analyzing Add-On Instructions (AOIs), which are reusable PLC logic components.
AOIs encapsulate control logic for equipment types like motors, valves, and sensors.
Look at the input/output tags and any existing patterns to understand what the AOI controls.
Focus on the industrial function - what equipment does this control and how?
""",
            "UDT": """
You are analyzing User Defined Types (UDTs), which are templates for equipment or data structures.
UDTs typically represent types of equipment (motors, valves, sensors) or data patterns.
Look at the member tags to understand what the UDT controls/monitors.
""",
            "View": """
You are analyzing Views (HMI screens) that operators use to monitor and control equipment.
Consider what type of operator interaction this view supports.
""",
            "Equipment": """
You are analyzing Equipment instances, which are specific pieces of equipment in the plant.
Equipment instances are based on UDT templates.
""",
            "ViewComponent": """
You are analyzing UI components within HMI views.
Each component has a type (button, label, LED, input field, etc.) and may be bound to equipment data.
Describe what this specific component does for the operator - what action it triggers or what information it shows.
Consider the component's type, properties, and any bindings to understand its purpose.
""",
            "ScadaTag": """
You are analyzing standalone SCADA tags in Ignition. These are not part of UDTs but exist independently.
Tag types include:
- Query tags: Fetch data from databases using SQL queries
- Memory tags: Store values in memory (counters, flags, setpoints)
- OPC tags: Read/write from PLC or other OPC servers
- Expression tags: Compute values from other tags or expressions

Describe what data this tag provides, what it's used for, and how it fits into the system.
For query tags, explain what data the query retrieves and how it's used.
For OPC tags, explain what equipment/signal this monitors or controls.
""",
        }
        
        return base + type_specific.get(item_type, "")
    
    def _get_user_prompt(
        self,
        item_type: str,
        items: List[Dict],
        context: Dict,
    ) -> str:
        """Build the user prompt for a batch analysis."""
        parts = []
        
        parts.append(f"Analyze these {item_type} items and provide semantic descriptions.\n")
        
        # Add reference examples if available
        if context.get("already_analyzed"):
            parts.append("## Previously Analyzed (for style reference):\n")
            for ref in context["already_analyzed"]:
                parts.append(f"- {ref['name']}: {ref['purpose']}")
            parts.append("")
        
        # Add items to analyze
        parts.append(f"## Items to Analyze:\n")
        for item in items:
            name = self._get_item_name(item_type, item)
            parts.append(f"### {name}")
            
            # Add raw definition if available
            if name in context.get("raw_definitions", {}):
                raw = context["raw_definitions"][name]
                parts.append(f"Definition: {json.dumps(raw, indent=2)}")
            
            # Add any graph context
            graph_context = self._graph.get_item_with_context(item_type, name)
            if graph_context and graph_context.get("context"):
                ctx = graph_context["context"]
                if ctx.get("views"):
                    parts.append(f"Referenced by views: {ctx['views']}")
                if ctx.get("aois"):
                    parts.append(f"Mapped to PLC AOIs: {ctx['aois']}")
                if ctx.get("udts"):
                    parts.append(f"Displays UDTs: {ctx['udts']}")
            
            parts.append("")
        
        parts.append("""
## Required Response Format

Respond with a JSON object containing an "analyses" field that maps each item name to its semantic description:

```json
{
  "analyses": {
    "ItemName1": "Semantic description for item 1",
    "ItemName2": "Semantic description for item 2"
  }
}
```

Be concise but informative. Focus on industrial/operational meaning.
""")
        
        return "\n".join(parts)


def main():
    """CLI for incremental semantic analysis."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Incrementally analyze Ignition configurations"
    )
    parser.add_argument(
        "command",
        choices=["status", "analyze", "reset", "recover"],
        help="Command to execute (recover: reset stuck in_progress items)",
    )
    parser.add_argument(
        "--input", "-i",
        help="Path to Ignition backup JSON file",
    )
    parser.add_argument(
        "--type", "-t",
        choices=["AOI", "UDT", "View", "Equipment", "ViewComponent", "ScadaTag"],
        help="Specific item type to analyze/reset",
    )
    parser.add_argument(
        "--batch", "-b",
        type=int,
        default=10,
        help="Batch size for analysis (default: 10)",
    )
    parser.add_argument(
        "--max", "-m",
        type=int,
        default=50,
        help="Maximum items to analyze in this session (default: 50)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    
    args = parser.parse_args()
    
    # Handle encoding for Windows
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    
    if args.command == "status":
        # Status doesn't need the backup file
        graph = get_ontology_graph()
        try:
            status = graph.get_semantic_status_counts()
            print("\n=== Semantic Analysis Status ===\n")
            
            has_stuck = False
            for item_type in IncrementalAnalyzer.ANALYSIS_ORDER:
                counts = status.get(item_type, {})
                pending = counts.get("pending", 0)
                complete = counts.get("complete", 0)
                in_progress = counts.get("in_progress", 0)
                review = counts.get("review", 0)
                total = pending + complete + in_progress + review
                
                if total > 0:
                    pct = (complete / total * 100) if total > 0 else 0
                    line = f"  {item_type:15} {complete:3}/{total:<3} complete ({pct:.0f}%)"
                    if in_progress > 0:
                        line += f"  ⚠️  {in_progress} stuck in_progress"
                        has_stuck = True
                    print(line)
            
            print()
            if has_stuck:
                print("  ⚠️  Some items are stuck in 'in_progress' (interrupted run).")
                print("     Run 'recover' command to reset them to 'pending'.\n")
        finally:
            graph.close()
        return
    
    if args.command == "recover":
        # Recover stuck in_progress items
        graph = get_ontology_graph()
        try:
            # We need a minimal analyzer just for the recover method
            # Create a dummy backup for this
            from ignition_parser import IgnitionBackup
            dummy_backup = IgnitionBackup(file_path="", version="")
            
            with IncrementalAnalyzer(dummy_backup, graph=graph) as analyzer:
                recovered = analyzer.recover_stuck_items(verbose=True)
                if recovered > 0:
                    print(f"\n[OK] Recovered {recovered} stuck items (reset to pending)\n")
                else:
                    print("\n[INFO] No stuck items found\n")
        finally:
            graph.close()
        return
    
    if args.command == "reset":
        graph = get_ontology_graph()
        try:
            if args.type:
                types_to_reset = [args.type]
            else:
                types_to_reset = ["UDT", "View", "Equipment", "ViewComponent"]
            
            for item_type in types_to_reset:
                with graph.session() as session:
                    result = session.run(
                        f"""
                        MATCH (n:{item_type})
                        SET n.semantic_status = 'pending',
                            n.purpose = null,
                            n.analyzed_at = null
                        RETURN count(n) as count
                        """
                    )
                    count = result.single()["count"]
                    print(f"[OK] Reset {count} {item_type} items to pending")
        finally:
            graph.close()
        return
    
    # Analyze command requires input file
    if args.command == "analyze":
        if not args.input:
            print("[ERROR] --input required for analyze command")
            return
        
        # Parse backup
        ignition_parser = IgnitionParser()
        backup = ignition_parser.parse_file(args.input)
        
        print(f"[INFO] Loaded: {len(backup.udt_definitions)} UDTs, "
              f"{len(backup.udt_instances)} instances, {len(backup.windows)} views")
        
        with IncrementalAnalyzer(backup, batch_size=args.batch) as analyzer:
            # Auto-recover any stuck items from interrupted runs
            recovered = analyzer.recover_stuck_items(verbose=args.verbose)
            if recovered > 0:
                print(f"[INFO] Auto-recovered {recovered} stuck items from previous interrupted run")
            
            # Show current status
            analyzer.print_status()
            
            # Run analysis session
            if args.type:
                session = analyzer.analyze_next_batch(args.type, verbose=args.verbose)
            else:
                session = analyzer.run_session(
                    max_items=args.max,
                    verbose=args.verbose,
                )
            
            # Report results
            print(f"\n=== Session Complete ===\n")
            print(f"  Processed: {session.items_processed}")
            print(f"  Succeeded: {session.items_succeeded}")
            print(f"  Failed: {session.items_failed}")
            
            if session.errors:
                print(f"\n  Errors:")
                for err in session.errors[:5]:
                    print(f"    - {err['name']}: {err['error'][:60]}...")
            
            # Show updated status
            analyzer.print_status()


if __name__ == "__main__":
    main()

