#!/usr/bin/env python3
"""
RCA Enrichment Generator - Uses Claude to add troubleshooting context.

This module generates RCA enrichments by:
1. Traversing existing ontology relationships to build context
2. Using Claude to generate failure modes, operator language, diagnostics
3. Storing enrichments ON existing nodes (not parallel structures)

Follows Leor's pattern in troubleshooting_ontology.py.
"""

import json
from typing import Dict, List, Optional, Any

from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient
from mes_ontology import extend_ontology


class RCAEnrichmentGenerator:
    """
    Generates RCA enrichments for Equipment and CCP nodes.
    
    Follows Axilon philosophy:
    - Deterministic: Context built from actual graph relationships
    - Semantic: Claude enriches with "why" and "how"
    - Unified: Enrichments stored on existing nodes
    """
    
    def __init__(self, graph: OntologyGraph = None, client: ClaudeClient = None):
        """Initialize with graph and Claude client."""
        self._graph = graph
        self._client = client
        self._owns_graph = False
        self._owns_client = False
        
        if self._graph is None:
            self._graph = get_ontology_graph()
            self._owns_graph = True
        
        # Ensure graph has MES methods
        if not hasattr(self._graph, 'get_batch_rca_context'):
            extend_ontology(self._graph)
        
        if self._client is None:
            self._client = ClaudeClient(enable_tools=False)  # Don't need tools for enrichment
            self._owns_client = True
    
    @property
    def graph(self) -> OntologyGraph:
        return self._graph
    
    def close(self):
        """Clean up resources."""
        if self._owns_client and self._client:
            self._client.close()
            self._client = None
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None
    
    # =========================================================================
    # EQUIPMENT ENRICHMENT
    # =========================================================================
    
    def enrich_equipment(self, equipment_name: str, verbose: bool = False) -> Dict:
        """
        Enrich an Equipment node with RCA context.
        
        1. Gets equipment and traverses to connected AOIs, tags, alarms, CCPs
        2. Builds context from actual configurations
        3. Uses Claude to generate failure modes, operator language, diagnostics
        4. Stores enrichment ON the equipment node
        """
        if verbose:
            print(f"[RCA] Enriching equipment: {equipment_name}")
        
        # Step 1: Build context from graph (DETERMINISTIC)
        context = self._build_equipment_context(equipment_name)
        
        if "error" in context:
            return context
        
        if verbose:
            print(f"  Found: {len(context.get('tags', []))} tags, "
                  f"{len(context.get('ccps', []))} CCPs")
        
        # Step 2: Generate enrichment using Claude (SEMANTIC)
        enrichment = self._generate_equipment_enrichment(context, verbose)
        
        # Step 3: Store enrichment ON the equipment node
        self.graph.store_equipment_rca_enrichment(equipment_name, enrichment)
        
        if verbose:
            print(f"  [OK] Stored RCA enrichment on {equipment_name}")
        
        return enrichment
    
    def _build_equipment_context(self, equipment_name: str) -> Dict:
        """Build context from actual graph relationships."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (e:Equipment {name: $name})
                
                OPTIONAL MATCH (e)-[:LOCATED_IN]->(loc:FunctionalLocation)
                OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(aoi:AOI)
                OPTIONAL MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
                OPTIONAL MATCH (aoi)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
                OPTIONAL MATCH (sym)-[:CAUSED_BY]->(cause:FaultCause)
                OPTIONAL MATCH (c:CriticalControlPoint)-[:MONITORED_BY]->(e)
                OPTIONAL MATCH (op:Operation)-[:EXECUTED_ON]->(e)
                
                RETURN e as equipment,
                       loc as location,
                       aoi,
                       collect(DISTINCT {name: tag.name, description: tag.description}) as tags,
                       collect(DISTINCT {
                           symptom: sym.symptom,
                           plc_indicators: sym.plc_indicators,
                           causes: collect(DISTINCT cause.cause)
                       }) as fault_symptoms,
                       collect(DISTINCT c) as ccps,
                       collect(DISTINCT op.description) as operations
            """, {"name": equipment_name})
            
            record = result.single()
            if not record or not record["equipment"]:
                return {"error": f"Equipment {equipment_name} not found"}
            
            return {
                "equipment": dict(record["equipment"]),
                "location": dict(record["location"]) if record["location"] else None,
                "aoi": dict(record["aoi"]) if record["aoi"] else None,
                "tags": [t for t in record["tags"] if t.get("name")],
                "fault_symptoms": [s for s in record["fault_symptoms"] if s.get("symptom")],
                "ccps": [dict(c) for c in record["ccps"] if c],
                "operations": [o for o in record["operations"] if o],
            }
    
    def _generate_equipment_enrichment(self, context: Dict, verbose: bool) -> Dict:
        """Use Claude to generate RCA enrichment."""
        # Build prompt from context
        prompt = self._build_equipment_prompt(context)
        
        if verbose:
            print(f"  [LLM] Generating enrichment ({len(prompt)} chars context)")
        
        result = self._client.query_json(
            system_prompt=EQUIPMENT_RCA_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=4000,
            verbose=verbose,
        )
        
        return result.get("data", {})
    
    def _build_equipment_prompt(self, context: Dict) -> str:
        """Build prompt from equipment context."""
        parts = []
        
        equip = context.get("equipment", {})
        parts.append(f"# Equipment: {equip.get('name', 'Unknown')}")
        parts.append(f"Type: {equip.get('equipment_type', 'Unknown')}")
        parts.append(f"Validation Status: {equip.get('validation_status', 'Unknown')}")
        parts.append("")
        
        aoi = context.get("aoi")
        if aoi:
            parts.append(f"## PLC Controller: {aoi.get('name', 'Unknown')}")
            parts.append(f"Purpose: {aoi.get('purpose', 'Unknown')}")
            parts.append("")
        
        tags = context.get("tags", [])
        if tags:
            parts.append("## PLC Tags:")
            for tag in tags[:15]:
                parts.append(f"- {tag.get('name', '?')}: {tag.get('description', '')}")
            parts.append("")
        
        symptoms = context.get("fault_symptoms", [])
        if symptoms:
            parts.append("## Existing Fault Symptoms (from PLC analysis):")
            for sym in symptoms[:5]:
                if sym.get("symptom"):
                    parts.append(f"- {sym['symptom']}")
            parts.append("")
        
        ccps = context.get("ccps", [])
        if ccps:
            parts.append("## Critical Control Points:")
            for ccp in ccps:
                parts.append(f"- {ccp.get('ccp_id', '?')}: {ccp.get('parameter_name', '?')}")
                parts.append(f"  Target: {ccp.get('target', '?')}, "
                           f"Limits: {ccp.get('low_limit', '?')} - {ccp.get('high_limit', '?')}")
            parts.append("")
        
        return "\n".join(parts)
    
    # =========================================================================
    # CCP ENRICHMENT
    # =========================================================================
    
    def enrich_ccp(self, ccp_id: str, verbose: bool = False) -> Dict:
        """
        Enrich a CriticalControlPoint node with RCA context.
        """
        if verbose:
            print(f"[RCA] Enriching CCP: {ccp_id}")
        
        context = self._build_ccp_context(ccp_id)
        
        if "error" in context:
            return context
        
        enrichment = self._generate_ccp_enrichment(context, verbose)
        
        self.graph.store_ccp_rca_enrichment(ccp_id, enrichment)
        
        if verbose:
            print(f"  [OK] Stored RCA enrichment on {ccp_id}")
        
        return enrichment
    
    def _build_ccp_context(self, ccp_id: str) -> Dict:
        """Build context for CCP."""
        with self.graph.session() as session:
            result = session.run("""
                MATCH (c:CriticalControlPoint {ccp_id: $ccp_id})
                
                OPTIONAL MATCH (c)-[:MONITORED_BY]->(equip:Equipment)
                OPTIONAL MATCH (equip)-[:CONTROLLED_BY]->(aoi:AOI)
                OPTIONAL MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
                WHERE toLower(tag.name) CONTAINS toLower(c.parameter_name)
                   OR toLower(coalesce(tag.description, '')) CONTAINS toLower(c.parameter_name)
                
                RETURN c as ccp,
                       equip as equipment,
                       aoi,
                       collect(DISTINCT tag) as monitoring_tags
            """, {"ccp_id": ccp_id})
            
            record = result.single()
            if not record or not record["ccp"]:
                return {"error": f"CCP {ccp_id} not found"}
            
            return {
                "ccp": dict(record["ccp"]),
                "equipment": dict(record["equipment"]) if record["equipment"] else None,
                "aoi": dict(record["aoi"]) if record["aoi"] else None,
                "monitoring_tags": [dict(t) for t in record["monitoring_tags"] if t],
            }
    
    def _generate_ccp_enrichment(self, context: Dict, verbose: bool) -> Dict:
        """Use Claude to generate CCP enrichment."""
        prompt = self._build_ccp_prompt(context)
        
        if verbose:
            print(f"  [LLM] Generating CCP enrichment ({len(prompt)} chars)")
        
        result = self._client.query_json(
            system_prompt=CCP_RCA_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=4000,
            verbose=verbose,
        )
        
        return result.get("data", {})
    
    def _build_ccp_prompt(self, context: Dict) -> str:
        """Build prompt from CCP context."""
        parts = []
        
        ccp = context.get("ccp", {})
        parts.append(f"# Critical Control Point: {ccp.get('ccp_id', 'Unknown')}")
        parts.append(f"Parameter: {ccp.get('parameter_name', 'Unknown')}")
        parts.append(f"Target: {ccp.get('target', 'Unknown')}")
        parts.append(f"Low Limit: {ccp.get('low_limit', 'Unknown')}")
        parts.append(f"High Limit: {ccp.get('high_limit', 'Unknown')}")
        parts.append(f"Criticality: {ccp.get('criticality', 'Unknown')}")
        parts.append("")
        
        equip = context.get("equipment")
        if equip:
            parts.append(f"## Equipment: {equip.get('name', 'Unknown')}")
            parts.append(f"Type: {equip.get('equipment_type', 'Unknown')}")
            parts.append("")
        
        aoi = context.get("aoi")
        if aoi:
            parts.append(f"## PLC Controller: {aoi.get('name', 'Unknown')}")
            parts.append("")
        
        tags = context.get("monitoring_tags", [])
        if tags:
            parts.append("## Monitoring Tags:")
            for tag in tags:
                parts.append(f"- {tag.get('name', '?')}: {tag.get('description', '')}")
            parts.append("")
        
        return "\n".join(parts)
    
    # =========================================================================
    # BATCH ENRICHMENT
    # =========================================================================
    
    def enrich_all_equipment(self, verbose: bool = False) -> int:
        """Enrich all equipment nodes."""
        with self.graph.session() as session:
            result = session.run("MATCH (e:Equipment) RETURN e.name as name")
            names = [r["name"] for r in result]
        
        if verbose:
            print(f"[RCA] Found {len(names)} equipment to enrich")
        
        count = 0
        for name in names:
            try:
                self.enrich_equipment(name, verbose=verbose)
                count += 1
            except Exception as e:
                print(f"[ERROR] Failed to enrich {name}: {e}")
        
        return count
    
    def enrich_all_ccps(self, verbose: bool = False) -> int:
        """Enrich all CCP nodes."""
        with self.graph.session() as session:
            result = session.run("MATCH (c:CriticalControlPoint) RETURN c.ccp_id as ccp_id")
            ccp_ids = [r["ccp_id"] for r in result]
        
        if verbose:
            print(f"[RCA] Found {len(ccp_ids)} CCPs to enrich")
        
        count = 0
        for ccp_id in ccp_ids:
            try:
                self.enrich_ccp(ccp_id, verbose=verbose)
                count += 1
            except Exception as e:
                print(f"[ERROR] Failed to enrich {ccp_id}: {e}")
        
        return count


# =============================================================================
# SYSTEM PROMPTS
# =============================================================================

EQUIPMENT_RCA_SYSTEM_PROMPT = """You are an expert industrial automation troubleshooter specializing in pharmaceutical manufacturing.

Given information about equipment extracted from actual system configurations, generate RCA (Root Cause Analysis) enrichment.

You will see:
- Equipment details and type
- Actual PLC tags configured for this equipment
- Existing fault symptoms from PLC code analysis
- Critical Control Points that depend on this equipment

Generate a JSON object with:

1. "failure_modes": Array of likely failure modes for THIS SPECIFIC equipment. Each with:
   - "mode": What can fail
   - "likelihood": "high", "medium", or "low"
   - "indicators": Which of the ACTUAL TAGS would indicate this failure

2. "operator_observations": Array of phrases an operator might use to describe problems with THIS equipment (natural language)

3. "diagnostic_sequence": Array of diagnostic steps, referencing ACTUAL TAG names from the context

4. "resolution_guidance": Array of fix steps

5. "gmp_impact": If CCPs are involved, describe the GMP/regulatory impact

Be specific to THIS equipment and its actual configuration. Reference actual tag names."""

CCP_RCA_SYSTEM_PROMPT = """You are an expert in pharmaceutical GMP (Good Manufacturing Practice) and process control.

Given information about a Critical Control Point (CCP) extracted from actual system configurations, generate RCA enrichment.

You will see:
- CCP details (parameter, target, limits, criticality)
- Equipment that monitors this CCP
- PLC tags that monitor this parameter

Generate a JSON object with:

1. "violation_scenarios": Array of ways this CCP could be violated. Each with:
   - "scenario": What happens
   - "causes": Root causes (array)
   - "immediate_actions": What to do immediately
   - "plc_indicators": Which ACTUAL TAGS would show this

2. "operator_language": Array of phrases operators use when this CCP has issues

3. "diagnostic_steps": Steps to diagnose, referencing ACTUAL tag names

4. "regulatory_impact": What this violation means for GMP compliance, batch disposition

Be specific to THIS CCP and its actual monitoring configuration."""


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="RCA Enrichment Generator")
    parser.add_argument('--enrich-equipment', metavar='NAME',
                       help='Enrich specific equipment')
    parser.add_argument('--enrich-ccp', metavar='CCP_ID',
                       help='Enrich specific CCP')
    parser.add_argument('--enrich-all-equipment', action='store_true',
                       help='Enrich all equipment')
    parser.add_argument('--enrich-all-ccps', action='store_true',
                       help='Enrich all CCPs')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    generator = RCAEnrichmentGenerator()
    
    try:
        if args.enrich_equipment:
            result = generator.enrich_equipment(args.enrich_equipment, args.verbose)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.enrich_ccp:
            result = generator.enrich_ccp(args.enrich_ccp, args.verbose)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.enrich_all_equipment:
            count = generator.enrich_all_equipment(args.verbose)
            print(f"[OK] Enriched {count} equipment nodes")
        
        elif args.enrich_all_ccps:
            count = generator.enrich_all_ccps(args.verbose)
            print(f"[OK] Enriched {count} CCP nodes")
        
        else:
            parser.print_help()
    
    finally:
        generator.close()


if __name__ == "__main__":
    main()
