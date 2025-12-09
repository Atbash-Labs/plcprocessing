#!/usr/bin/env python3
"""
Unified ontology merger that combines L5X (PLC) and Ignition (SCADA) ontologies.
Creates a comprehensive system ontology showing how PLCs and HMI work together.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
import anthropic
from dotenv import load_dotenv


class UnifiedOntologyMerger:
    """Merges PLC and SCADA ontologies into a unified system view."""

    def __init__(self, api_key: Optional[str] = None, model: str = "claude-sonnet-4-5-20250929"):
        """Initialize with Anthropic API."""
        load_dotenv()
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model

    def merge_ontologies(self, l5x_ontology_path: str, ignition_ontology_path: str,
                         output_path: Optional[str] = None, verbose: bool = False) -> Dict[str, Any]:
        """Merge L5X and Ignition ontologies into unified system ontology."""

        # Load ontologies
        with open(l5x_ontology_path, 'r') as f:
            l5x_ontology = json.load(f)

        with open(ignition_ontology_path, 'r') as f:
            ignition_ontology = json.load(f)

        if verbose:
            print(f"[INFO] Loaded L5X ontology: {len(l5x_ontology)} AOIs")
            print(f"[INFO] Loaded Ignition ontology")

        # Build context for merging
        context = self._build_merge_context(l5x_ontology, ignition_ontology)

        # Query LLM for unified analysis
        unified_analysis = self._query_merge_llm(context, verbose)

        # Structure the unified ontology
        unified = {
            'type': 'unified_system_ontology',
            'sources': {
                'plc': {
                    'file': l5x_ontology_path,
                    'type': 'L5X/Rockwell',
                    'components': len(l5x_ontology) if isinstance(l5x_ontology, list) else 1
                },
                'scada': {
                    'file': ignition_ontology_path,
                    'type': 'Ignition',
                    'udt_definitions': ignition_ontology.get('summary', {}).get('udt_definitions', 0),
                    'views': ignition_ontology.get('summary', {}).get('windows', 0)
                }
            },
            'unified_analysis': unified_analysis,
            'component_ontologies': {
                'plc': l5x_ontology,
                'scada': ignition_ontology
            }
        }

        # Save if output path provided
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(unified, f, indent=2)
            if verbose:
                print(f"[OK] Saved unified ontology to {output_path}")

        return unified

    def _build_merge_context(self, l5x_ontology: Any, ignition_ontology: Dict) -> str:
        """Build context for LLM merge analysis."""
        parts = []

        parts.append("# System Integration Analysis: PLC + SCADA")
        parts.append("")

        # L5X (PLC) summary
        parts.append("## PLC Layer (Rockwell L5X)")
        parts.append("")

        if isinstance(l5x_ontology, list):
            for aoi in l5x_ontology[:8]:  # Limit
                name = aoi.get('name', 'Unknown')
                analysis = aoi.get('analysis', {})
                purpose = analysis.get('purpose', 'No description')

                parts.append(f"### AOI: {name}")
                parts.append(f"Purpose: {purpose[:500]}")

                # Key tags
                tags = analysis.get('tags', {})
                if tags:
                    parts.append("Key tags:")
                    for tag_name, tag_desc in list(tags.items())[:10]:
                        parts.append(f"  - {tag_name}: {tag_desc[:100]}")

                # Relationships
                rels = analysis.get('relationships', [])
                if rels:
                    parts.append("Key relationships:")
                    for rel in rels[:5]:
                        parts.append(f"  - {rel.get('from')} -> {rel.get('to')}: {rel.get('relationship_type')}")

                parts.append("")
        else:
            parts.append(json.dumps(l5x_ontology.get('analysis', {}), indent=2)[:2000])

        # Ignition (SCADA) summary
        parts.append("\n## SCADA Layer (Ignition)")
        parts.append("")

        ign_analysis = ignition_ontology.get('analysis', {})

        parts.append(f"System Purpose: {ign_analysis.get('system_purpose', 'Unknown')}")
        parts.append("")

        # UDT semantics
        udt_sem = ign_analysis.get('udt_semantics', {})
        if udt_sem:
            parts.append("### SCADA UDT Templates")
            for udt_name, udt_purpose in udt_sem.items():
                parts.append(f"- {udt_name}: {udt_purpose[:200]}")
            parts.append("")

        # Equipment instances
        equip = ign_analysis.get('equipment_instances', [])
        if equip:
            parts.append("### Equipment Instances")
            for eq in equip:
                parts.append(f"- {eq.get('name')}: {eq.get('type')} - {eq.get('purpose', '')[:100]}")
            parts.append("")

        # Data flows
        flows = ign_analysis.get('data_flows', [])
        if flows:
            parts.append("### Data Flows")
            for flow in flows[:5]:
                parts.append(f"- {flow.get('flow_id')}: {flow.get('path', '')[:150]}")
            parts.append("")

        # Tag references
        tag_cats = ign_analysis.get('tag_categories', {})
        if tag_cats:
            parts.append("### Tag Categories")
            for cat, tags in list(tag_cats.items())[:6]:
                parts.append(f"- {cat}: {', '.join(tags[:5])}")
            parts.append("")

        return "\n".join(parts)

    def _query_merge_llm(self, context: str, verbose: bool = False, max_continuations: int = 3) -> Dict[str, Any]:
        """Query LLM to generate unified analysis with continuation support."""

        system_prompt = """You are an expert industrial automation architect who understands both PLC programming and SCADA/HMI systems. Your task is to analyze PLC and SCADA ontologies together and create a UNIFIED SYSTEM ONTOLOGY that shows:

1. How PLC logic components (AOIs) map to SCADA elements (UDTs, tags, views)
2. Complete data flow from field devices through PLCs to operator interfaces
3. The overall industrial process being controlled/monitored
4. Cross-system relationships and dependencies
5. Integration patterns between the control and visualization layers

Focus on creating a holistic view that an automation engineer could use to understand the ENTIRE system, not just individual components.

IMPORTANT: Output valid JSON only. No markdown code blocks."""

        user_prompt = f"""Analyze these PLC and SCADA ontologies together and create a UNIFIED SYSTEM ONTOLOGY:

{context}

Provide your analysis as JSON with these fields:
- "system_overview": High-level description of what this automation system does
- "plc_to_scada_mappings": Array of {{plc_component, scada_component, mapping_type, description}} showing how PLC AOIs connect to SCADA UDTs
- "end_to_end_flows": Array describing complete data paths from sensors through PLC logic to operator displays
- "equipment_hierarchy": Object describing the physical/logical equipment organization
- "control_responsibilities": Object mapping which layer (PLC vs SCADA) handles what functions
- "integration_points": Array of specific tag/data connections between systems
- "operational_modes": Description of how operators interact with the system through different modes
- "safety_architecture": How safety is distributed between PLC and SCADA layers
- "recommendations": Array of observations about the system architecture

Output ONLY valid JSON, no markdown formatting. Be comprehensive but concise."""

        if verbose:
            print("[INFO] Querying Claude for unified analysis...")

        try:
            messages = [{"role": "user", "content": user_prompt}]
            full_response = ""
            continuation_count = 0

            while continuation_count <= max_continuations:
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=16000,
                    system=system_prompt,
                    messages=messages
                )

                response_text = message.content[0].text
                full_response += response_text

                # Check if response was complete
                if message.stop_reason == "end_turn":
                    if verbose:
                        print(f"[INFO] Response complete after {continuation_count} continuation(s)")
                    break
                elif message.stop_reason == "max_tokens":
                    continuation_count += 1
                    if continuation_count > max_continuations:
                        print(f"[WARNING] Response truncated after {max_continuations} continuations")
                        break

                    if verbose:
                        print(f"[INFO] Continuing response (continuation {continuation_count}/{max_continuations})...")

                    # Add assistant response and request continuation
                    messages.append({"role": "assistant", "content": response_text})
                    messages.append({"role": "user", "content": "Continue the JSON from where you left off. Do not repeat any content."})
                else:
                    # Unknown stop reason
                    if verbose:
                        print(f"[INFO] Stop reason: {message.stop_reason}")
                    break

            # Clean up response - remove markdown if present
            response_text = full_response
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                if json_end > json_start:
                    response_text = response_text[json_start:json_end].strip()
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.find("```", json_start)
                if json_end > json_start:
                    response_text = response_text[json_start:json_end].strip()

            return json.loads(response_text)

        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON parse error: {e}")
            # Return full raw response for manual inspection
            return {"error": "JSON parsing failed", "raw": full_response}
        except Exception as e:
            print(f"[ERROR] API call failed: {e}")
            return {"error": str(e)}


def main():
    """CLI for unified ontology merger."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Merge L5X and Ignition ontologies into unified system ontology"
    )
    parser.add_argument('l5x_ontology', help='Path to L5X ontology JSON')
    parser.add_argument('ignition_ontology', help='Path to Ignition ontology JSON')
    parser.add_argument('-o', '--output', help='Output JSON file', default='ontologies/unified_ontology.json')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    merger = UnifiedOntologyMerger()
    unified = merger.merge_ontologies(
        args.l5x_ontology,
        args.ignition_ontology,
        args.output,
        verbose=args.verbose
    )

    print(f"\n[OK] Created unified ontology")
    print(f"System Overview: {unified['unified_analysis'].get('system_overview', 'N/A')[:200]}...")


if __name__ == "__main__":
    main()
