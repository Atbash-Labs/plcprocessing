#!/usr/bin/env python3
"""
SIF RCA Enrichment Generator - Uses Claude to add troubleshooting context to SIFs.

This module generates RCA enrichments for Safety Instrumented Functions by:
1. Traversing existing ontology relationships to build context
2. Using Claude to generate failure modes, demand scenarios, diagnostics
3. Storing enrichments ON existing SIF nodes
"""

import json
from typing import Dict, List, Optional

from neo4j_ontology import get_ontology_graph
from claude_client import ClaudeClient


class SIFEnrichmentGenerator:
    """Generates RCA enrichments for SIF nodes."""
    
    def __init__(self, graph=None, client=None):
        self._graph = graph
        self._client = client
        self._owns_graph = False
        self._owns_client = False
        
        if self._graph is None:
            self._graph = get_ontology_graph()
            self._owns_graph = True
        
        if self._client is None:
            self._client = ClaudeClient(enable_tools=False)
            self._owns_client = True
    
    @property
    def graph(self):
        return self._graph
    
    def close(self):
        if self._owns_client and self._client:
            self._client.close()
            self._client = None
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None
    
    def enrich_sif(self, sif_id: str, verbose: bool = False) -> Dict:
        """Enrich a SIF node with RCA context."""
        if verbose:
            print(f"[RCA] Enriching SIF: {sif_id}")
        
        # Build context from graph
        context = self._build_sif_context(sif_id)
        
        if "error" in context:
            return context
        
        if verbose:
            print(f"  Found: {context['sif']['name']} at {context['site']['name'] if context['site'] else 'unknown'}")
            print(f"  SIL Level: {context['sif'].get('sil_level', '?')}")
            print(f"  Demand Events: {len(context.get('demands', []))}")
        
        # Generate enrichment using Claude
        enrichment = self._generate_sif_enrichment(context, verbose)
        
        # Store enrichment ON the SIF node
        self._store_sif_enrichment(sif_id, enrichment)
        
        if verbose:
            print(f"  [OK] Stored RCA enrichment on {sif_id}")
        
        return enrichment
    
    def _build_sif_context(self, sif_id: str) -> Dict:
        """Build context from graph relationships."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (s:SIF {sif_id: $sif_id})
                
                OPTIONAL MATCH (s)-[:LOCATED_AT]->(site:Site)
                OPTIONAL MATCH (site)-[:PART_OF]->(bu:BusinessUnit)
                OPTIONAL MATCH (s)-[:LOGGED_BY]->(script:Script)
                OPTIONAL MATCH (d:DemandEvent)-[:DEMAND_ON]->(s)
                
                RETURN s as sif,
                       site,
                       bu,
                       collect(DISTINCT script.name) as scripts,
                       collect(DISTINCT {
                           demand_id: d.demand_id,
                           demand_type: d.demand_type,
                           outcome: d.outcome,
                           description: d.description
                       }) as demands
            """, {"sif_id": sif_id})
            
            record = result.single()
            if not record or not record["sif"]:
                return {"error": f"SIF {sif_id} not found"}
            
            return {
                "sif": dict(record["sif"]),
                "site": dict(record["site"]) if record["site"] else None,
                "business_unit": dict(record["bu"]) if record["bu"] else None,
                "scripts": record["scripts"],
                "demands": [d for d in record["demands"] if d.get("demand_id")],
            }
    
    def _generate_sif_enrichment(self, context: Dict, verbose: bool) -> Dict:
        """Use Claude to generate RCA enrichment."""
        prompt = self._build_sif_prompt(context)
        
        if verbose:
            print(f"  [LLM] Generating enrichment ({len(prompt)} chars context)")
        
        result = self._client.query_json(
            system_prompt=SIF_RCA_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=8000,  # Increased to avoid truncation
            verbose=verbose,
        )
        
        data = result.get("data")
        if data is None:
            if verbose:
                error = result.get("error", "Unknown error")
                print(f"  [WARN] JSON parsing failed: {error}")
            return {}
        return data
    
    def _build_sif_prompt(self, context: Dict) -> str:
        """Build prompt from SIF context."""
        parts = []
        
        sif = context.get("sif", {})
        parts.append(f"# Safety Instrumented Function: {sif.get('sif_id', 'Unknown')}")
        parts.append(f"Name: {sif.get('name', 'Unknown')}")
        parts.append(f"SIL Level: {sif.get('sil_level', 'Unknown')}")
        parts.append(f"Demand Mode: {sif.get('demand_mode', 'Unknown')}")
        parts.append(f"Proof Test Interval: {sif.get('proof_test_interval_months', 'Unknown')} months")
        parts.append("")
        
        site = context.get("site")
        if site:
            parts.append(f"## Location: {site.get('name', 'Unknown')}")
            parts.append(f"Physical Location: {site.get('location', 'Unknown')}")
            parts.append("")
        
        bu = context.get("business_unit")
        if bu:
            parts.append(f"## Business Unit: {bu.get('name', 'Unknown')}")
            parts.append(f"Region: {bu.get('region', 'Unknown')}")
            parts.append("")
        
        demands = context.get("demands", [])
        if demands:
            parts.append("## Historical Demand Events:")
            for d in demands:
                parts.append(f"- {d.get('demand_type', '?')}: {d.get('outcome', '?')} - {d.get('description', '')}")
            parts.append("")
        
        scripts = context.get("scripts", [])
        if scripts:
            parts.append("## Logging Scripts:")
            for s in scripts:
                parts.append(f"- {s}")
            parts.append("")
        
        return "\n".join(parts)
    
    def _store_sif_enrichment(self, sif_id: str, enrichment: Dict) -> None:
        """Store RCA enrichment ON the SIF node."""
        with self.graph.session() as session:
            session.run("""
                MATCH (s:SIF {sif_id: $sif_id})
                SET s.rca_enriched = true,
                    s.rca_enriched_at = datetime(),
                    s.rca_failure_modes = $failure_modes,
                    s.rca_demand_scenarios = $demand_scenarios,
                    s.rca_diagnostic_sequence = $diagnostic_sequence,
                    s.rca_spurious_trip_causes = $spurious_causes,
                    s.rca_proof_test_guidance = $proof_test_guidance
            """, {
                "sif_id": sif_id,
                "failure_modes": json.dumps(enrichment.get("failure_modes", [])),
                "demand_scenarios": json.dumps(enrichment.get("demand_scenarios", [])),
                "diagnostic_sequence": enrichment.get("diagnostic_sequence", []),
                "spurious_causes": enrichment.get("spurious_trip_causes", []),
                "proof_test_guidance": enrichment.get("proof_test_guidance", ""),
            })
    
    def enrich_all_sifs(self, verbose: bool = False) -> int:
        """Enrich all SIF nodes."""
        with self.graph.session() as session:
            result = session.run("MATCH (s:SIF) RETURN s.sif_id as sif_id")
            sif_ids = [r["sif_id"] for r in result]
        
        if verbose:
            print(f"[RCA] Found {len(sif_ids)} SIFs to enrich")
        
        count = 0
        for sif_id in sif_ids:
            try:
                self.enrich_sif(sif_id, verbose=verbose)
                count += 1
            except Exception as e:
                print(f"[ERROR] Failed to enrich {sif_id}: {e}")
        
        return count


SIF_RCA_SYSTEM_PROMPT = """You are an expert in Safety Instrumented Systems (SIS) and functional safety (IEC 61511/61508).

Given information about a Safety Instrumented Function (SIF) extracted from actual system configurations, generate RCA (Root Cause Analysis) enrichment.

You will see:
- SIF details (name, SIL level, demand mode, proof test interval)
- Location and business unit
- Historical demand events (if any)
- Associated logging scripts

Generate a JSON object with:

1. "failure_modes": Array of possible dangerous failure modes for this SIF type. Each with:
   - "mode": What can fail (sensor, logic solver, final element, etc.)
   - "failure_type": "Dangerous Detected", "Dangerous Undetected", "Safe", or "No Effect"
   - "detection_method": How this failure would be detected
   - "mitigation": How to prevent or detect early

2. "demand_scenarios": Array of realistic demand scenarios. Each with:
   - "scenario": What process condition triggers the demand
   - "expected_response": What the SIF should do
   - "failure_consequences": What happens if SIF fails to respond

3. "spurious_trip_causes": Array of common causes of spurious/false trips for this SIF type

4. "diagnostic_sequence": Array of steps to diagnose a SIF issue, in order

5. "proof_test_guidance": String with key points for proof testing this SIF type

Be specific to THIS SIF type (e.g., high pressure trip vs. gas detection vs. HIPPS have different failure modes)."""


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="SIF RCA Enrichment Generator")
    parser.add_argument('--enrich-sif', metavar='SIF_ID',
                       help='Enrich specific SIF')
    parser.add_argument('--enrich-all', action='store_true',
                       help='Enrich all SIFs')
    parser.add_argument('--list', action='store_true',
                       help='List all SIFs')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    generator = SIFEnrichmentGenerator()
    
    try:
        if args.list:
            with generator.graph.session() as session:
                result = session.run("""
                    MATCH (s:SIF)
                    OPTIONAL MATCH (s)-[:LOCATED_AT]->(site:Site)
                    RETURN s.sif_id as sif_id, s.name as name, 
                           s.sil_level as sil, site.name as site,
                           s.rca_enriched as enriched
                    ORDER BY s.sif_id
                """)
                print("SIFs:")
                for r in result:
                    enriched = "[enriched]" if r["enriched"] else ""
                    print(f"  {r['sif_id']}: {r['name']} (SIL {r['sil']}) @ {r['site']} {enriched}")
        
        elif args.enrich_sif:
            result = generator.enrich_sif(args.enrich_sif, args.verbose)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.enrich_all:
            count = generator.enrich_all_sifs(args.verbose)
            print(f"\n[OK] Enriched {count} SIFs")
        
        else:
            parser.print_help()
    
    finally:
        generator.close()


if __name__ == "__main__":
    main()
