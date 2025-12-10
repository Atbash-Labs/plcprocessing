#!/usr/bin/env python3
"""
Troubleshooting-focused ontology generator.
Enriches existing ontologies with fault trees, operator language mappings,
and diagnostic guidance for AI-assisted troubleshooting.
Stores results in Neo4j graph database.

Uses tool calls to query existing ontology and troubleshooting data,
enabling Claude to build on existing knowledge.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient, get_claude_client


class TroubleshootingOntologyGenerator:
    """
    Generates troubleshooting-focused ontology layers:
    1. Fault trees (symptom -> causes -> checks)
    2. Operator language dictionary (natural language -> SCADA -> PLC)
    3. Intent annotations (why things exist)
    4. Expected states by context
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5-20250929",
        graph: Optional[OntologyGraph] = None,
        client: Optional[ClaudeClient] = None
    ):
        load_dotenv()
        
        # Use provided client or create one
        if client:
            self._client = client
            self._owns_client = False
        else:
            self._client = ClaudeClient(
                api_key=api_key,
                model=model,
                graph=graph,
                enable_tools=True
            )
            self._owns_client = True

    @property
    def graph(self) -> OntologyGraph:
        """Access the Neo4j graph."""
        return self._client.graph

    def close(self):
        """Close resources if we own them."""
        if self._owns_client and self._client:
            self._client.close()
            self._client = None

    def enrich_all_aois(self, verbose: bool = False) -> int:
        """
        Enrich all AOIs in Neo4j with troubleshooting data.
        Returns count of enriched AOIs.
        """
        aois = self.graph.get_all_aois()
        
        if verbose:
            print(f"[INFO] Found {len(aois)} AOIs to enrich")
        
        enriched_count = 0
        for aoi in aois:
            try:
                self.enrich_aoi(aoi['name'], verbose=verbose)
                enriched_count += 1
            except Exception as e:
                print(f"[ERROR] Failed to enrich {aoi['name']}: {e}")
        
        # Generate operator dictionary
        self._generate_operator_dictionary(verbose)
        
        return enriched_count

    def enrich_aoi(self, aoi_name: str, verbose: bool = False) -> Dict:
        """Enrich a single AOI with troubleshooting data."""
        
        # Get existing AOI data
        aoi = self.graph.get_aoi(aoi_name)
        if not aoi:
            raise ValueError(f"AOI '{aoi_name}' not found in Neo4j")
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"[INFO] Generating troubleshooting data for {aoi_name}...")
        
        # Build context for LLM
        context = self._build_aoi_context(aoi)
        
        # Generate troubleshooting additions with tool support
        troubleshooting = self._query_troubleshooting_llm(context, aoi_name, verbose)
        
        if 'error' not in troubleshooting:
            # Store in Neo4j
            self.graph.add_troubleshooting(aoi_name, troubleshooting)
            
            if verbose:
                print(f"[OK] Stored troubleshooting data for {aoi_name}")
        else:
            if verbose:
                print(f"[ERROR] Troubleshooting generation failed: {troubleshooting.get('error')}")
        
        return troubleshooting

    def _build_aoi_context(self, aoi: Dict) -> str:
        """Build context string for troubleshooting analysis."""

        parts = []
        name = aoi.get('name', 'Unknown')
        analysis = aoi.get('analysis', {})

        parts.append(f"# AOI: {name}")
        parts.append(f"Purpose: {analysis.get('purpose', 'Unknown')}")
        parts.append("")

        # Tags
        tags = analysis.get('tags', {})
        if tags:
            parts.append("## Tags and Meanings:")
            for tag_name, tag_desc in tags.items():
                parts.append(f"- {tag_name}: {tag_desc}")
            parts.append("")

        # Relationships
        relationships = analysis.get('relationships', [])
        if relationships:
            parts.append("## Relationships:")
            for rel in relationships[:10]:
                parts.append(f"- {rel.get('from', '?')} -> {rel.get('to', '?')}: {rel.get('description', '')}")
            parts.append("")

        # Control patterns
        patterns = analysis.get('control_patterns', [])
        if patterns:
            parts.append("## Control Patterns:")
            for pattern in patterns:
                if isinstance(pattern, dict):
                    parts.append(f"- {pattern.get('pattern', pattern.get('name', str(pattern)))}")
                else:
                    parts.append(f"- {pattern}")
            parts.append("")

        # Safety critical
        safety = analysis.get('safety_critical', [])
        if safety:
            parts.append("## Safety-Critical Elements:")
            for item in safety:
                if isinstance(item, dict):
                    parts.append(f"- {item.get('element', item)}: {item.get('reason', '')}")
                else:
                    parts.append(f"- {item}")
            parts.append("")

        return "\n".join(parts)

    def _query_troubleshooting_llm(self, context: str, aoi_name: str, verbose: bool) -> Dict:
        """Query LLM to generate troubleshooting data with tool support."""

        if verbose:
            print(f"[DEBUG] Context length: {len(context)} chars")

        system_prompt = """You are an expert industrial automation troubleshooter.
Your task is to analyze PLC components and generate troubleshooting guidance that helps:
1. Operators understand what's wrong in plain language
2. Technicians diagnose root causes systematically
3. AI systems translate between operator observations, SCADA displays, and PLC states

You have access to tools to query the existing ontology database:
- get_schema: Discover what node types exist (FaultSymptom, CommonPhrase, etc.)
- run_query: Execute Cypher queries to find existing troubleshooting data
- get_node: Get details of specific components

USE THESE TOOLS to explore existing troubleshooting patterns before generating new ones. This helps you:
- Build on existing troubleshooting patterns
- Maintain consistent symptom descriptions
- Link to related component issues
- Use established operator language

Focus on PRACTICAL troubleshooting - what an operator sees, what it means, and what to check."""

        user_prompt = f"""Analyze this PLC component and generate troubleshooting guidance.

FIRST, use the available tools to explore existing troubleshooting data:
1. Use get_schema to see what troubleshooting-related nodes exist
2. Query for existing FaultSymptom nodes to see symptom patterns
3. Check CommonPhrase nodes for operator language dictionary

THEN, generate a JSON object with these sections:

1. "fault_tree": Array of 2-3 most common fault conditions, each with:
   - "symptom": What the operator/technician observes
   - "plc_indicators": Array of PLC tag states (2-3 key ones)
   - "scada_indicators": What would show on HMI/SCADA (2-3 key ones)
   - "possible_causes": Array of 3-4 {{cause, likelihood, check}} objects
   - "resolution_steps": 3-5 ordered steps to diagnose and fix

2. "intents": Object mapping 2-4 key features/interlocks to WHY they exist:
   - Key name -> {{what, why, consequence_if_missing, failure_symptom}}

3. "operator_phrases": Array of 3-5 phrases mapping natural language to technical meaning:
   - {{phrase, means, check_first, related_tags}}

4. "expected_states": Object with: startup, running, stopped, faulted descriptions

5. "diagnostic_tags": Array of 3-5 most important tags with:
   - {{tag, normal_value, meaning_if_abnormal}}

Output valid JSON only, no markdown, no explanations before or after.

## Component to Analyze:

{context}"""

        if verbose:
            print("[INFO] Querying Claude for troubleshooting data with tool support...")

        result = self._client.query_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=8000,
            use_tools=True,
            verbose=verbose
        )

        if verbose and result.get("tool_calls"):
            print(f"[INFO] Claude made {len(result['tool_calls'])} tool calls")

        if result.get("data"):
            return result["data"]
        else:
            return {
                "error": result.get("error", "Unknown error"),
                "raw": result.get("raw_text", "")[:2000]
            }

    def _generate_operator_dictionary(self, verbose: bool) -> None:
        """Generate a unified operator language dictionary from all AOIs."""
        
        if verbose:
            print("[INFO] Generating operator dictionary...")

        # Build common operator language patterns
        common_phrases = {
            "line_stopping": {
                "variations": ["line keeps stopping", "production stopped", "machine won't cycle"],
                "means": "Production sequence interrupted",
                "scada_check": ["Line status display", "Active alarms", "Equipment faceplates"],
                "plc_check": ["Sequence state", "Fault bits", "Interlock chain"],
                "follow_up_questions": [
                    "Does it stop at the same place each time?",
                    "Are there any alarms showing?",
                    "Did anything change recently?"
                ]
            },
            "equipment_stuck": {
                "variations": ["it's stuck", "won't move", "not extending", "not retracting"],
                "means": "Actuator not responding to commands",
                "scada_check": ["Equipment faceplate", "Position indicators", "Fault status"],
                "plc_check": ["Command outputs", "Feedback inputs", "Timeout status"],
                "follow_up_questions": [
                    "Which equipment specifically?",
                    "What position is it stuck in?",
                    "Can you hear it trying to move?"
                ]
            },
            "motor_issues": {
                "variations": ["motor won't run", "motor stopped", "motor keeps faulting"],
                "means": "Motor start failure or unexpected stop",
                "scada_check": ["Motor faceplate", "Status/fault indicators", "Interlock display"],
                "plc_check": ["Run command", "Run feedback", "Interlock bits", "Overload status"],
                "follow_up_questions": [
                    "Is the motor trying to start or completely dead?",
                    "Any overload or fault lights on the starter?",
                    "Was it working before?"
                ]
            },
            "sensor_issues": {
                "variations": ["sensor not working", "wrong count", "not detecting"],
                "means": "Sensor signal issue",
                "scada_check": ["Sensor status on screen", "Count displays"],
                "plc_check": ["Raw input state", "Debounced output", "Mode bits"],
                "follow_up_questions": [
                    "Which sensor?",
                    "Is the indicator light on the sensor itself?",
                    "Intermittent or completely dead?"
                ]
            },
            "alarm_issues": {
                "variations": ["alarm keeps coming back", "can't clear alarm", "alarm won't reset"],
                "means": "Recurring or persistent fault condition",
                "scada_check": ["Alarm history", "Current alarms", "Equipment status"],
                "plc_check": ["Fault bits", "Reset logic", "Condition that triggers alarm"],
                "follow_up_questions": [
                    "What's the exact alarm message?",
                    "Does it clear temporarily when reset?",
                    "How often does it come back?"
                ]
            }
        }

        for key, phrase_data in common_phrases.items():
            self.graph.create_common_phrase(key, phrase_data)
        
        if verbose:
            print(f"[OK] Created {len(common_phrases)} common phrases in operator dictionary")

    def find_by_symptom(self, symptom_text: str) -> List[Dict]:
        """Find AOIs and troubleshooting info by symptom description."""
        return self.graph.find_by_symptom(symptom_text)

    def find_by_operator_phrase(self, phrase: str) -> Dict:
        """Find matches for operator language."""
        return self.graph.find_by_operator_phrase(phrase)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate troubleshooting-focused ontology enrichments (stored in Neo4j)"
    )
    parser.add_argument('--enrich-all', action='store_true',
                       help='Enrich all AOIs in Neo4j with troubleshooting data')
    parser.add_argument('--enrich', metavar='AOI_NAME',
                       help='Enrich a specific AOI')
    parser.add_argument('--symptom', metavar='TEXT',
                       help='Search for AOIs by symptom text')
    parser.add_argument('--phrase', metavar='TEXT',
                       help='Search for operator phrase matches')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--model', default='claude-sonnet-4-5-20250929', help='Claude model')
    parser.add_argument('--no-tools', action='store_true',
                       help='Disable Neo4j tool calls')
    
    # Legacy JSON import support
    parser.add_argument('--import-json', metavar='FILE',
                       help='Import existing JSON ontology into Neo4j and enrich')

    args = parser.parse_args()

    client = ClaudeClient(model=args.model, enable_tools=not args.no_tools)
    generator = TroubleshootingOntologyGenerator(client=client)

    try:
        if args.import_json:
            # Import JSON then enrich
            from neo4j_ontology import import_json_ontology
            print(f"[INFO] Importing {args.import_json} to Neo4j...")
            import_json_ontology(args.import_json, generator.graph)
            print(f"[INFO] Enriching with troubleshooting data...")
            count = generator.enrich_all_aois(verbose=args.verbose)
            print(f"[OK] Enriched {count} AOIs")
        
        elif args.enrich_all:
            count = generator.enrich_all_aois(verbose=args.verbose)
            print(f"[OK] Enriched {count} AOIs with troubleshooting data")
        
        elif args.enrich:
            result = generator.enrich_aoi(args.enrich, verbose=args.verbose)
            if 'error' not in result:
                print(f"[OK] Enriched {args.enrich}")
            else:
                print(f"[ERROR] {result['error']}")
        
        elif args.symptom:
            results = generator.find_by_symptom(args.symptom)
            if results:
                print(f"\n[INFO] Found {len(results)} matches:\n")
                for r in results:
                    print(f"AOI: {r['aoi']}")
                    print(f"  Symptom: {r['symptom']}")
                    print(f"  Steps: {r.get('steps', [])[:3]}")
                    print()
            else:
                print("[INFO] No matches found")
        
        elif args.phrase:
            results = generator.find_by_operator_phrase(args.phrase)
            if results.get('common_phrases'):
                print("\n[Common Phrases]")
                for p in results['common_phrases']:
                    print(f"  {p['key']}: {p['means']}")
            if results.get('aoi_phrases'):
                print("\n[AOI-Specific Phrases]")
                for p in results['aoi_phrases']:
                    print(f"  [{p['aoi']}] {p['phrase']}: {p['means']}")
            if not results.get('common_phrases') and not results.get('aoi_phrases'):
                print("[INFO] No matches found")
        
        else:
            parser.print_help()
    
    finally:
        generator.close()


if __name__ == "__main__":
    main()
