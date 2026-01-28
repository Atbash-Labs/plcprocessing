#!/usr/bin/env python3
"""
MES/ERP Ontology Extension for Axilon.

This module EXTENDS Leor's existing neo4j_ontology.py and claude_client.py
to add ISA-95 Level 3-4 (MES/ERP) capabilities.

Integration approach:
- Adds methods to OntologyGraph via mixin pattern
- Adds new tool definitions to OntologyTools
- Provides RCA enrichment that stores on existing nodes

Usage:
    from mes_ontology import extend_ontology, MES_TOOL_DEFINITIONS
    
    # Extend existing graph with MES methods
    graph = get_ontology_graph()
    extend_ontology(graph)
    
    # Now graph has new methods:
    # graph.create_material(), graph.create_batch(), etc.
    # graph.get_batch_rca_context(), graph.trace_ccp_to_tags(), etc.
"""

import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


# =============================================================================
# MES NODE TYPES (ISA-95 Levels 3-4)
# =============================================================================

class MaterialType(str, Enum):
    """SAP material types."""
    RAW = "ROH"      # Raw material
    SEMI = "HALB"    # Semi-finished
    FINISHED = "FERT"  # Finished product


class OrderStatus(str, Enum):
    """SAP production order status."""
    CREATED = "CRTD"
    RELEASED = "REL"
    CONFIRMED = "CNF"
    DELIVERED = "DLV"
    TECH_COMPLETE = "TECO"
    CLOSED = "CLSD"


# =============================================================================
# TOOL DEFINITIONS FOR CLAUDE
# =============================================================================

MES_TOOL_DEFINITIONS = [
    {
        "name": "get_batch_context",
        "description": "Get complete context for a batch including materials, operations, equipment, quality results, and any deviations. Use this when investigating batch issues or tracing quality problems.",
        "input_schema": {
            "type": "object",
            "properties": {
                "batch_id": {
                    "type": "string",
                    "description": "The batch number (e.g., 'HCC2601001')"
                }
            },
            "required": ["batch_id"]
        }
    },
    {
        "name": "get_equipment_rca",
        "description": "Get RCA (root cause analysis) context for equipment including connected PLC tags, fault symptoms, CCPs that depend on it, and diagnostic guidance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "equipment_name": {
                    "type": "string",
                    "description": "Equipment name (e.g., 'BR-500-001')"
                }
            },
            "required": ["equipment_name"]
        }
    },
    {
        "name": "get_ccp_context",
        "description": "Get context for a Critical Control Point including process location, monitoring equipment, PLC tags, and violation scenarios.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ccp_id": {
                    "type": "string",
                    "description": "CCP identifier (e.g., 'CCP-BR-TEMP')"
                }
            },
            "required": ["ccp_id"]
        }
    },
    {
        "name": "search_by_symptom",
        "description": "Search for troubleshooting context by symptom description. Searches fault symptoms, operator observations, and CCP violation language.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symptom": {
                    "type": "string",
                    "description": "What the operator observes (e.g., 'temperature drifting', 'batch not growing')"
                }
            },
            "required": ["symptom"]
        }
    },
    {
        "name": "trace_tag_impact",
        "description": "Trace from a PLC tag up to business impact - what batches, CCPs, and products are affected if this tag has issues.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tag_name": {
                    "type": "string",
                    "description": "PLC tag name"
                }
            },
            "required": ["tag_name"]
        }
    },
    {
        "name": "get_process_ccps",
        "description": "Get all Critical Control Points for a process with their monitoring equipment and limits.",
        "input_schema": {
            "type": "object",
            "properties": {
                "process_name": {
                    "type": "string",
                    "description": "Process name (e.g., 'Axilumab mAb Production')"
                }
            },
            "required": ["process_name"]
        }
    },
    {
        "name": "get_open_deviations",
        "description": "Get all open/investigating deviations with their equipment and CCP context.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
]


# =============================================================================
# ONTOLOGY GRAPH EXTENSION
# =============================================================================

def extend_ontology(graph):
    """
    Extend an OntologyGraph instance with MES/RCA methods.
    
    This adds methods directly to the instance without modifying the class,
    allowing clean integration with Leor's existing code.
    
    Usage:
        graph = get_ontology_graph()
        extend_ontology(graph)
        # Now graph.create_material(), graph.get_batch_rca_context(), etc. work
    """
    import types
    
    # Bind all the extension methods to the graph instance
    graph.create_mes_schema = types.MethodType(create_mes_schema, graph)
    graph.create_material = types.MethodType(create_material, graph)
    graph.create_batch = types.MethodType(create_batch, graph)
    graph.create_production_order = types.MethodType(create_production_order, graph)
    graph.create_operation = types.MethodType(create_operation, graph)
    graph.create_process = types.MethodType(create_process, graph)
    graph.create_ccp = types.MethodType(create_ccp, graph)
    graph.create_functional_location = types.MethodType(create_functional_location, graph)
    graph.create_vendor = types.MethodType(create_vendor, graph)
    graph.link_equipment_to_aoi = types.MethodType(link_equipment_to_aoi, graph)
    
    # RCA query methods
    graph.get_batch_rca_context = types.MethodType(get_batch_rca_context, graph)
    graph.get_equipment_rca_context = types.MethodType(get_equipment_rca_context, graph)
    graph.get_ccp_rca_context = types.MethodType(get_ccp_rca_context, graph)
    graph.search_by_symptom_extended = types.MethodType(search_by_symptom_extended, graph)
    graph.trace_tag_to_business_impact = types.MethodType(trace_tag_to_business_impact, graph)
    graph.get_process_ccps = types.MethodType(get_process_ccps, graph)
    graph.get_open_deviations = types.MethodType(get_open_deviations, graph)
    
    # RCA enrichment methods
    graph.store_equipment_rca_enrichment = types.MethodType(store_equipment_rca_enrichment, graph)
    graph.store_ccp_rca_enrichment = types.MethodType(store_ccp_rca_enrichment, graph)
    graph.store_aoi_pharma_enrichment = types.MethodType(store_aoi_pharma_enrichment, graph)
    
    return graph


# =============================================================================
# SCHEMA CREATION
# =============================================================================

def create_mes_schema(self) -> None:
    """Create MES/ERP schema (indexes and constraints)."""
    with self.session() as session:
        constraints = [
            "CREATE CONSTRAINT material_matnr IF NOT EXISTS FOR (m:Material) REQUIRE m.matnr IS UNIQUE",
            "CREATE CONSTRAINT batch_charg IF NOT EXISTS FOR (b:Batch) REQUIRE b.charg IS UNIQUE",
            "CREATE CONSTRAINT order_aufnr IF NOT EXISTS FOR (po:ProductionOrder) REQUIRE po.aufnr IS UNIQUE",
            "CREATE CONSTRAINT ccp_id IF NOT EXISTS FOR (c:CriticalControlPoint) REQUIRE c.ccp_id IS UNIQUE",
            "CREATE CONSTRAINT process_id IF NOT EXISTS FOR (p:Process) REQUIRE p.process_id IS UNIQUE",
            "CREATE CONSTRAINT vendor_lifnr IF NOT EXISTS FOR (v:Vendor) REQUIRE v.lifnr IS UNIQUE",
            "CREATE CONSTRAINT func_loc IF NOT EXISTS FOR (f:FunctionalLocation) REQUIRE f.tplnr IS UNIQUE",
            "CREATE CONSTRAINT deviation_id IF NOT EXISTS FOR (d:ProcessDeviation) REQUIRE d.deviation_id IS UNIQUE",
        ]
        
        indexes = [
            "CREATE INDEX material_type IF NOT EXISTS FOR (m:Material) ON (m.material_type)",
            "CREATE INDEX batch_status IF NOT EXISTS FOR (b:Batch) ON (b.status)",
            "CREATE INDEX order_status IF NOT EXISTS FOR (po:ProductionOrder) ON (po.status)",
            "CREATE INDEX ccp_criticality IF NOT EXISTS FOR (c:CriticalControlPoint) ON (c.criticality)",
            "CREATE INDEX equipment_rca IF NOT EXISTS FOR (e:Equipment) ON (e.rca_enriched)",
            "CREATE INDEX ccp_rca IF NOT EXISTS FOR (c:CriticalControlPoint) ON (c.rca_enriched)",
            "CREATE INDEX deviation_status IF NOT EXISTS FOR (d:ProcessDeviation) ON (d.rca_status)",
        ]
        
        for stmt in constraints + indexes:
            try:
                session.run(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"[WARNING] {e}")


# =============================================================================
# NODE CREATION METHODS
# =============================================================================

def create_material(self, matnr: str, description: str, material_type: str,
                   base_unit: str = "EA", **properties) -> None:
    """Create a Material node."""
    with self.session() as session:
        session.run("""
            MERGE (m:Material {matnr: $matnr})
            SET m.description = $description,
                m.material_type = $material_type,
                m.base_unit = $base_unit,
                m += $properties
        """, {
            "matnr": matnr,
            "description": description,
            "material_type": material_type,
            "base_unit": base_unit,
            "properties": properties,
        })


def create_batch(self, charg: str, matnr: str, quantity: float,
                status: str = "ACTIVE", **properties) -> None:
    """Create a Batch node linked to its Material."""
    with self.session() as session:
        session.run("""
            MATCH (m:Material {matnr: $matnr})
            MERGE (b:Batch {charg: $charg})
            SET b.quantity = $quantity,
                b.status = $status,
                b += $properties
            MERGE (b)-[:BATCH_OF]->(m)
        """, {
            "charg": charg,
            "matnr": matnr,
            "quantity": quantity,
            "status": status,
            "properties": properties,
        })


def create_production_order(self, aufnr: str, matnr: str, batch: str,
                           target_quantity: float, status: str = "CRTD",
                           **properties) -> None:
    """Create a ProductionOrder node."""
    with self.session() as session:
        session.run("""
            MATCH (m:Material {matnr: $matnr})
            MERGE (po:ProductionOrder {aufnr: $aufnr})
            SET po.batch = $batch,
                po.target_quantity = $target_quantity,
                po.status = $status,
                po += $properties
            MERGE (po)-[:PRODUCES]->(m)
        """, {
            "aufnr": aufnr,
            "matnr": matnr,
            "batch": batch,
            "target_quantity": target_quantity,
            "status": status,
            "properties": properties,
        })


def create_operation(self, aufnr: str, vornr: str, description: str,
                    equipment_name: str = None, **properties) -> None:
    """Create an Operation node linked to its ProductionOrder and optionally Equipment."""
    with self.session() as session:
        # Create operation and link to order
        session.run("""
            MATCH (po:ProductionOrder {aufnr: $aufnr})
            MERGE (op:Operation {aufnr: $aufnr, vornr: $vornr})
            SET op.description = $description,
                op += $properties
            MERGE (po)-[:HAS_OPERATION]->(op)
        """, {
            "aufnr": aufnr,
            "vornr": vornr,
            "description": description,
            "properties": properties,
        })
        
        # Link to equipment if specified
        if equipment_name:
            session.run("""
                MATCH (op:Operation {aufnr: $aufnr, vornr: $vornr})
                MATCH (e:Equipment {name: $equipment_name})
                MERGE (op)-[:EXECUTED_ON]->(e)
            """, {
                "aufnr": aufnr,
                "vornr": vornr,
                "equipment_name": equipment_name,
            })


def create_process(self, process_id: str, name: str, description: str = None,
                  **properties) -> None:
    """Create a Process node."""
    with self.session() as session:
        session.run("""
            MERGE (p:Process {process_id: $process_id})
            SET p.name = $name,
                p.description = $description,
                p += $properties
        """, {
            "process_id": process_id,
            "name": name,
            "description": description,
            "properties": properties,
        })


def create_ccp(self, ccp_id: str, parameter_name: str, target: float,
              low_limit: float, high_limit: float, criticality: str = "Critical",
              equipment_name: str = None, **properties) -> None:
    """Create a CriticalControlPoint node, optionally linked to equipment."""
    with self.session() as session:
        session.run("""
            MERGE (c:CriticalControlPoint {ccp_id: $ccp_id})
            SET c.parameter_name = $parameter_name,
                c.target = $target,
                c.low_limit = $low_limit,
                c.high_limit = $high_limit,
                c.criticality = $criticality,
                c += $properties
        """, {
            "ccp_id": ccp_id,
            "parameter_name": parameter_name,
            "target": target,
            "low_limit": low_limit,
            "high_limit": high_limit,
            "criticality": criticality,
            "properties": properties,
        })
        
        # Link to equipment if specified
        if equipment_name:
            session.run("""
                MATCH (c:CriticalControlPoint {ccp_id: $ccp_id})
                MATCH (e:Equipment {name: $equipment_name})
                MERGE (c)-[:MONITORED_BY]->(e)
            """, {"ccp_id": ccp_id, "equipment_name": equipment_name})


def create_functional_location(self, tplnr: str, description: str,
                               classification: str = None, **properties) -> None:
    """Create a FunctionalLocation node."""
    with self.session() as session:
        session.run("""
            MERGE (f:FunctionalLocation {tplnr: $tplnr})
            SET f.description = $description,
                f.classification = $classification,
                f += $properties
        """, {
            "tplnr": tplnr,
            "description": description,
            "classification": classification,
            "properties": properties,
        })


def create_vendor(self, lifnr: str, name: str, **properties) -> None:
    """Create a Vendor node."""
    with self.session() as session:
        session.run("""
            MERGE (v:Vendor {lifnr: $lifnr})
            SET v.name = $name,
                v += $properties
        """, {
            "lifnr": lifnr,
            "name": name,
            "properties": properties,
        })


def link_equipment_to_aoi(self, equipment_name: str, aoi_name: str) -> bool:
    """
    Create CONTROLLED_BY relationship between Equipment and AOI.
    This is the critical integration point between MES and PLC layers.
    """
    with self.session() as session:
        result = session.run("""
            MATCH (e:Equipment {name: $equipment_name})
            MATCH (a:AOI {name: $aoi_name})
            MERGE (e)-[r:CONTROLLED_BY]->(a)
            RETURN e.name as equipment, a.name as aoi
        """, {
            "equipment_name": equipment_name,
            "aoi_name": aoi_name,
        })
        return result.single() is not None


# =============================================================================
# RCA QUERY METHODS
# =============================================================================

def get_batch_rca_context(self, batch_id: str) -> Dict:
    """
    Get complete RCA context for a batch.
    
    Traverses: Batch → ProductionOrder → Operation → Equipment → AOI → Tag
    """
    with self.session() as session:
        result = session.run("""
            MATCH (b:Batch {charg: $batch_id})
            
            OPTIONAL MATCH (b)-[:BATCH_OF]->(mat:Material)
            OPTIONAL MATCH (b)-[:HAS_QUALITY_RESULT]->(qr:QualityResult)
            
            OPTIONAL MATCH (po:ProductionOrder {batch: $batch_id})
            OPTIONAL MATCH (po)-[:HAS_OPERATION]->(op:Operation)
            OPTIONAL MATCH (op)-[:EXECUTED_ON]->(equip:Equipment)
            OPTIONAL MATCH (equip)-[:CONTROLLED_BY]->(aoi:AOI)
            OPTIONAL MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
            OPTIONAL MATCH (aoi)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
            
            OPTIONAL MATCH (c:CriticalControlPoint)-[:MONITORED_BY]->(equip)
            
            OPTIONAL MATCH (dev:ProcessDeviation)-[:AFFECTS_BATCH]->(b)
            
            RETURN b as batch,
                   mat as material,
                   collect(DISTINCT qr) as quality_results,
                   po as production_order,
                   collect(DISTINCT {
                       operation: op.description,
                       equipment: equip.name,
                       equipment_type: equip.equipment_type,
                       aoi: aoi.name,
                       rca_enrichment: equip.rca_failure_modes
                   }) as operations,
                   collect(DISTINCT tag.name) as plc_tags,
                   collect(DISTINCT {symptom: sym.symptom, causes: sym.causes}) as fault_symptoms,
                   collect(DISTINCT c.ccp_id) as ccps,
                   collect(DISTINCT dev) as deviations
        """, {"batch_id": batch_id})
        
        record = result.single()
        if not record or not record["batch"]:
            return {"error": f"Batch {batch_id} not found"}
        
        return {
            "batch": dict(record["batch"]),
            "material": dict(record["material"]) if record["material"] else None,
            "quality_results": [dict(qr) for qr in record["quality_results"] if qr],
            "production_order": dict(record["production_order"]) if record["production_order"] else None,
            "operations": [op for op in record["operations"] if op.get("operation")],
            "plc_tags": [t for t in record["plc_tags"] if t],
            "fault_symptoms": [s for s in record["fault_symptoms"] if s.get("symptom")],
            "ccps": [c for c in record["ccps"] if c],
            "deviations": [dict(d) for d in record["deviations"] if d],
        }


def get_equipment_rca_context(self, equipment_name: str) -> Dict:
    """
    Get RCA context for equipment.
    
    Includes: PLC control, fault symptoms, CCPs, RCA enrichment.
    """
    with self.session() as session:
        result = session.run("""
            MATCH (e:Equipment {name: $name})
            
            OPTIONAL MATCH (e)-[:LOCATED_IN]->(loc:FunctionalLocation)
            OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(aoi:AOI)
            OPTIONAL MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
            OPTIONAL MATCH (aoi)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
            OPTIONAL MATCH (sym)-[:CAUSED_BY]->(cause:FaultCause)
            
            OPTIONAL MATCH (e)-[:INSTANCE_OF]->(udt:UDT)
            OPTIONAL MATCH (view:View)-[:DISPLAYS]->(udt)
            
            OPTIONAL MATCH (c:CriticalControlPoint)-[:MONITORED_BY]->(e)
            
            OPTIONAL MATCH (op:Operation)-[:EXECUTED_ON]->(e)
            OPTIONAL MATCH (po:ProductionOrder)-[:HAS_OPERATION]->(op)
            WHERE po.status IN ['REL', 'CRTD', 'TECO']
            
            RETURN e as equipment,
                   loc as location,
                   aoi,
                   collect(DISTINCT {name: tag.name, description: tag.description}) as tags,
                   collect(DISTINCT {
                       symptom: sym.symptom,
                       plc_indicators: sym.plc_indicators,
                       scada_indicators: sym.scada_indicators,
                       resolution_steps: sym.resolution_steps,
                       causes: collect(DISTINCT cause.cause)
                   }) as fault_symptoms,
                   udt,
                   collect(DISTINCT view.name) as scada_views,
                   collect(DISTINCT c) as ccps,
                   collect(DISTINCT po.aufnr) as recent_orders
        """, {"name": equipment_name})
        
        record = result.single()
        if not record or not record["equipment"]:
            return {"error": f"Equipment {equipment_name} not found"}
        
        equip = dict(record["equipment"])
        
        return {
            "equipment": {
                **equip,
                "rca_enrichment": {
                    "failure_modes": json.loads(equip.get("rca_failure_modes", "[]")) if equip.get("rca_failure_modes") else [],
                    "operator_observations": equip.get("rca_operator_observations", []),
                    "diagnostic_sequence": equip.get("rca_diagnostic_sequence", []),
                } if equip.get("rca_enriched") else None
            },
            "location": dict(record["location"]) if record["location"] else None,
            "plc_control": {
                "aoi": dict(record["aoi"]) if record["aoi"] else None,
                "tags": [t for t in record["tags"] if t.get("name")],
                "fault_symptoms": [s for s in record["fault_symptoms"] if s.get("symptom")],
            },
            "scada": {
                "udt": dict(record["udt"]) if record["udt"] else None,
                "views": record["scada_views"],
            },
            "ccps": [dict(c) for c in record["ccps"] if c],
            "recent_orders": record["recent_orders"],
        }


def get_ccp_rca_context(self, ccp_id: str) -> Dict:
    """Get RCA context for a Critical Control Point."""
    with self.session() as session:
        result = session.run("""
            MATCH (c:CriticalControlPoint {ccp_id: $ccp_id})
            
            OPTIONAL MATCH (c)-[:MONITORED_BY]->(equip:Equipment)
            OPTIONAL MATCH (equip)-[:CONTROLLED_BY]->(aoi:AOI)
            OPTIONAL MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
            WHERE toLower(tag.name) CONTAINS toLower(c.parameter_name)
               OR toLower(coalesce(tag.description, '')) CONTAINS toLower(c.parameter_name)
            
            OPTIONAL MATCH (aoi)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
            
            RETURN c as ccp,
                   equip as equipment,
                   aoi,
                   collect(DISTINCT tag) as monitoring_tags,
                   collect(DISTINCT sym) as related_symptoms
        """, {"ccp_id": ccp_id})
        
        record = result.single()
        if not record or not record["ccp"]:
            return {"error": f"CCP {ccp_id} not found"}
        
        ccp = dict(record["ccp"])
        
        return {
            "ccp": {
                **ccp,
                "rca_enrichment": {
                    "violation_scenarios": json.loads(ccp.get("rca_violation_scenarios", "[]")) if ccp.get("rca_violation_scenarios") else [],
                    "operator_language": ccp.get("rca_operator_language", []),
                    "diagnostic_steps": ccp.get("rca_diagnostic_steps", []),
                } if ccp.get("rca_enriched") else None
            },
            "monitoring_chain": {
                "equipment": dict(record["equipment"]) if record["equipment"] else None,
                "plc_controller": dict(record["aoi"]) if record["aoi"] else None,
                "monitoring_tags": [dict(t) for t in record["monitoring_tags"] if t],
                "related_symptoms": [dict(s) for s in record["related_symptoms"] if s],
            },
        }


def search_by_symptom_extended(self, symptom_text: str) -> Dict:
    """
    Search for RCA context by symptom description.
    
    Searches: FaultSymptoms, Equipment RCA enrichments, CCP RCA enrichments, CommonPhrases.
    """
    symptom_lower = symptom_text.lower()
    
    with self.session() as session:
        # Search FaultSymptoms
        result = session.run("""
            MATCH (aoi:AOI)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
            WHERE toLower(sym.symptom) CONTAINS $search
            OPTIONAL MATCH (sym)-[:CAUSED_BY]->(cause:FaultCause)
            OPTIONAL MATCH (equip:Equipment)-[:CONTROLLED_BY]->(aoi)
            
            RETURN aoi.name as aoi,
                   sym.symptom as symptom,
                   sym.plc_indicators as plc_indicators,
                   sym.scada_indicators as scada_indicators,
                   sym.resolution_steps as resolution_steps,
                   collect(DISTINCT cause.cause) as causes,
                   collect(DISTINCT equip.name) as equipment
        """, {"search": symptom_lower})
        fault_symptoms = [dict(r) for r in result]
        
        # Search Equipment RCA enrichments
        result = session.run("""
            MATCH (e:Equipment)
            WHERE e.rca_enriched = true
              AND any(obs IN coalesce(e.rca_operator_observations, []) 
                      WHERE toLower(obs) CONTAINS $search)
            RETURN e.name as equipment,
                   e.rca_operator_observations as observations,
                   e.rca_failure_modes as failure_modes,
                   e.rca_diagnostic_sequence as diagnostics
        """, {"search": symptom_lower})
        equipment_matches = [dict(r) for r in result]
        
        # Search CCP RCA enrichments
        result = session.run("""
            MATCH (c:CriticalControlPoint)
            WHERE c.rca_enriched = true
              AND any(lang IN coalesce(c.rca_operator_language, [])
                      WHERE toLower(lang) CONTAINS $search)
            RETURN c.ccp_id as ccp_id,
                   c.parameter_name as parameter,
                   c.rca_operator_language as operator_language,
                   c.rca_violation_scenarios as scenarios
        """, {"search": symptom_lower})
        ccp_matches = [dict(r) for r in result]
        
        # Search CommonPhrases
        result = session.run("""
            MATCH (p:CommonPhrase)
            WHERE any(v IN p.variations WHERE toLower(v) CONTAINS $search)
               OR toLower(p.means) CONTAINS $search
            RETURN p.key as phrase_key,
                   p.variations as variations,
                   p.means as interpretation,
                   p.scada_check as scada_checks,
                   p.plc_check as plc_checks,
                   p.follow_up_questions as follow_up_questions
        """, {"search": symptom_lower})
        common_phrases = [dict(r) for r in result]
        
        return {
            "fault_symptoms": fault_symptoms,
            "equipment_matches": equipment_matches,
            "ccp_matches": ccp_matches,
            "common_phrases": common_phrases,
        }


def trace_tag_to_business_impact(self, tag_name: str) -> Dict:
    """Trace from a PLC tag up to business impact."""
    with self.session() as session:
        result = session.run("""
            MATCH (tag:Tag {name: $tag_name})
            OPTIONAL MATCH (aoi:AOI)-[:HAS_TAG]->(tag)
            OPTIONAL MATCH (equip:Equipment)-[:CONTROLLED_BY]->(aoi)
            OPTIONAL MATCH (c:CriticalControlPoint)-[:MONITORED_BY]->(equip)
            OPTIONAL MATCH (op:Operation)-[:EXECUTED_ON]->(equip)
            OPTIONAL MATCH (po:ProductionOrder)-[:HAS_OPERATION]->(op)
            OPTIONAL MATCH (po)-[:PRODUCES]->(mat:Material)
            
            RETURN tag,
                   aoi.name as aoi,
                   equip.name as equipment,
                   collect(DISTINCT c.ccp_id) as affected_ccps,
                   collect(DISTINCT po.aufnr) as affected_orders,
                   collect(DISTINCT mat.description) as affected_materials
        """, {"tag_name": tag_name})
        
        record = result.single()
        if not record or not record["tag"]:
            return {"error": f"Tag {tag_name} not found"}
        
        return {
            "tag": dict(record["tag"]),
            "impact_chain": {
                "plc_controller": record["aoi"],
                "equipment": record["equipment"],
                "affected_ccps": record["affected_ccps"],
                "affected_orders": record["affected_orders"],
                "affected_materials": record["affected_materials"],
            }
        }


def get_process_ccps(self, process_name: str) -> List[Dict]:
    """Get all CCPs for a process."""
    with self.session() as session:
        result = session.run("""
            MATCH (c:CriticalControlPoint)
            OPTIONAL MATCH (c)-[:MONITORED_BY]->(equip:Equipment)
            RETURN c.ccp_id as ccp_id,
                   c.parameter_name as parameter,
                   c.target as target,
                   c.low_limit as low_limit,
                   c.high_limit as high_limit,
                   c.criticality as criticality,
                   equip.name as equipment
            ORDER BY c.ccp_id
        """)
        return [dict(r) for r in result]


def get_open_deviations(self) -> List[Dict]:
    """Get all open/investigating deviations with context."""
    with self.session() as session:
        result = session.run("""
            MATCH (d:ProcessDeviation)
            WHERE d.rca_status IN ['Open', 'Investigating']
            
            OPTIONAL MATCH (d)-[:AFFECTS_BATCH]->(b:Batch)
            OPTIONAL MATCH (d)-[:OCCURRED_ON]->(e:Equipment)
            OPTIONAL MATCH (d)-[:VIOLATES]->(c:CriticalControlPoint)
            
            RETURN d as deviation,
                   b.charg as batch,
                   e.name as equipment,
                   c.ccp_id as ccp_violated
            ORDER BY 
                CASE d.impact WHEN 'Critical' THEN 1 WHEN 'Major' THEN 2 ELSE 3 END
        """)
        
        return [
            {
                "deviation": dict(r["deviation"]),
                "batch": r["batch"],
                "equipment": r["equipment"],
                "ccp_violated": r["ccp_violated"],
            }
            for r in result
        ]


# =============================================================================
# RCA ENRICHMENT STORAGE
# =============================================================================

def store_equipment_rca_enrichment(self, equipment_name: str, enrichment: Dict) -> None:
    """Store RCA enrichment ON the equipment node."""
    with self.session() as session:
        session.run("""
            MATCH (e:Equipment {name: $name})
            SET e.rca_enriched = true,
                e.rca_enriched_at = datetime(),
                e.rca_failure_modes = $failure_modes,
                e.rca_operator_observations = $operator_observations,
                e.rca_diagnostic_sequence = $diagnostic_sequence,
                e.rca_gmp_impact = $gmp_impact
        """, {
            "name": equipment_name,
            "failure_modes": json.dumps(enrichment.get("failure_modes", [])),
            "operator_observations": enrichment.get("operator_observations", []),
            "diagnostic_sequence": enrichment.get("diagnostic_sequence", []),
            "gmp_impact": enrichment.get("gmp_impact"),
        })


def store_ccp_rca_enrichment(self, ccp_id: str, enrichment: Dict) -> None:
    """Store RCA enrichment ON the CCP node."""
    with self.session() as session:
        session.run("""
            MATCH (c:CriticalControlPoint {ccp_id: $ccp_id})
            SET c.rca_enriched = true,
                c.rca_enriched_at = datetime(),
                c.rca_violation_scenarios = $violation_scenarios,
                c.rca_operator_language = $operator_language,
                c.rca_diagnostic_steps = $diagnostic_steps
        """, {
            "ccp_id": ccp_id,
            "violation_scenarios": json.dumps(enrichment.get("violation_scenarios", [])),
            "operator_language": enrichment.get("operator_language", []),
            "diagnostic_steps": enrichment.get("diagnostic_steps", []),
        })


def store_aoi_pharma_enrichment(self, aoi_name: str, enrichment: Dict) -> None:
    """Store pharma context ON the AOI node."""
    with self.session() as session:
        session.run("""
            MATCH (a:AOI {name: $name})
            SET a.pharma_context_enriched = true,
                a.pharma_enriched_at = datetime(),
                a.pharma_gmp_impact = $gmp_impact,
                a.pharma_batch_impact = $batch_impact
        """, {
            "name": aoi_name,
            "gmp_impact": enrichment.get("gmp_impact"),
            "batch_impact": enrichment.get("batch_impact"),
        })


# =============================================================================
# TOOL EXECUTOR EXTENSION
# =============================================================================

class MESTools:
    """
    MES/RCA tools for Claude.
    
    Extends OntologyTools with MES-specific queries.
    """
    
    def __init__(self, graph):
        """Initialize with extended graph."""
        self.graph = graph
        # Ensure graph is extended
        if not hasattr(graph, 'get_batch_rca_context'):
            extend_ontology(graph)
        
        self._tools = {
            "get_batch_context": self._get_batch_context,
            "get_equipment_rca": self._get_equipment_rca,
            "get_ccp_context": self._get_ccp_context,
            "search_by_symptom": self._search_by_symptom,
            "trace_tag_impact": self._trace_tag_impact,
            "get_process_ccps": self._get_process_ccps,
            "get_open_deviations": self._get_open_deviations,
        }
    
    def execute(self, tool_name: str, tool_input: Dict) -> str:
        """Execute a tool and return JSON result."""
        if tool_name not in self._tools:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        
        try:
            result = self._tools[tool_name](**tool_input)
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})
    
    def _get_batch_context(self, batch_id: str) -> Dict:
        return self.graph.get_batch_rca_context(batch_id)
    
    def _get_equipment_rca(self, equipment_name: str) -> Dict:
        return self.graph.get_equipment_rca_context(equipment_name)
    
    def _get_ccp_context(self, ccp_id: str) -> Dict:
        return self.graph.get_ccp_rca_context(ccp_id)
    
    def _search_by_symptom(self, symptom: str) -> Dict:
        return self.graph.search_by_symptom_extended(symptom)
    
    def _trace_tag_impact(self, tag_name: str) -> Dict:
        return self.graph.trace_tag_to_business_impact(tag_name)
    
    def _get_process_ccps(self, process_name: str) -> List[Dict]:
        return self.graph.get_process_ccps(process_name)
    
    def _get_open_deviations(self) -> List[Dict]:
        return self.graph.get_open_deviations()


# =============================================================================
# SYSTEM PROMPT EXTENSION
# =============================================================================

MES_SYSTEM_PROMPT_EXTENSION = """
## MES/ERP Layer (ISA-95 Levels 3-4)

You now have access to the MES/ERP layer of the ontology, which connects to the PLC/SCADA layers you already know.

### New Node Types:
- **Material**: Raw materials, intermediates, finished products (SAP material master)
- **Batch**: Production batches with quality status
- **ProductionOrder**: Manufacturing orders with target quantities and status
- **Operation**: Steps within a production order, linked to Equipment
- **CriticalControlPoint (CCP)**: GMP-critical parameters with limits (e.g., temperature, pH)
- **ProcessDeviation**: Quality deviations linked to batches, equipment, and CCPs

### Key Relationships:
- `Equipment -[CONTROLLED_BY]-> AOI`: Links MES equipment to PLC control
- `CriticalControlPoint -[MONITORED_BY]-> Equipment`: Links CCPs to monitoring equipment
- `Operation -[EXECUTED_ON]-> Equipment`: Links production operations to equipment
- `ProcessDeviation -[AFFECTS_BATCH]-> Batch`: Links deviations to affected batches
- `ProcessDeviation -[VIOLATES]-> CriticalControlPoint`: Links deviations to CCPs

### New Tools:
- **get_batch_context**: Full context for batch investigation (materials, operations, equipment, tags, deviations)
- **get_equipment_rca**: RCA context for equipment (PLC control, fault symptoms, CCPs, diagnostics)
- **get_ccp_context**: CCP details with monitoring chain to PLC tags
- **search_by_symptom**: Search by operator description across all layers
- **trace_tag_impact**: Trace from PLC tag up to business impact (batches, CCPs, products)
- **get_process_ccps**: List all CCPs with their monitoring equipment
- **get_open_deviations**: Get open deviations with equipment/CCP context

### RCA Workflow:
When investigating issues:
1. Start with `search_by_symptom` using the operator's description
2. Get context with `get_batch_context` or `get_equipment_rca`
3. Trace to specific tags with `trace_tag_impact`
4. Check CCP status with `get_ccp_context`

### RCA Enrichments:
Equipment and CCP nodes may have RCA enrichments (properties added by LLM analysis):
- `equipment.rca_failure_modes`: Likely failure modes for this equipment
- `equipment.rca_diagnostic_sequence`: Steps to diagnose issues
- `ccp.rca_violation_scenarios`: Ways this CCP could be violated
- `ccp.rca_diagnostic_steps`: Steps to investigate violations

Use these enrichments when providing troubleshooting guidance.
"""


# =============================================================================
# INTEGRATION HELPER
# =============================================================================

def integrate_with_claude_client(client):
    """
    Integrate MES tools with an existing ClaudeClient instance.
    
    Usage:
        client = ClaudeClient(enable_tools=True)
        integrate_with_claude_client(client)
        # Now client has MES tools available
    """
    # Extend the graph
    extend_ontology(client._get_graph())
    
    # Create MES tools
    mes_tools = MESTools(client._get_graph())
    
    # Add MES tools to existing tools
    original_tools = client._tools
    
    # Store reference
    client._mes_tools = mes_tools
    
    # Extend tool definitions
    if hasattr(client._tools, 'TOOL_DEFINITIONS'):
        # Add MES tool definitions
        pass  # The TOOL_DEFINITIONS are class-level
    
    # Wrap execute method to handle MES tools
    original_execute = original_tools.execute
    def extended_execute(tool_name: str, tool_input: Dict) -> str:
        if tool_name in mes_tools._tools:
            return mes_tools.execute(tool_name, tool_input)
        return original_execute(tool_name, tool_input)
    
    original_tools.execute = extended_execute
    
    return client


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="MES Ontology Extension")
    parser.add_argument('--create-schema', action='store_true',
                       help='Create MES schema in Neo4j')
    parser.add_argument('--test-queries', action='store_true',
                       help='Test MES queries')
    
    args = parser.parse_args()
    
    try:
        from neo4j_ontology import get_ontology_graph
        graph = get_ontology_graph()
        extend_ontology(graph)
        
        if args.create_schema:
            graph.create_mes_schema()
            print("[OK] Created MES schema")
        
        elif args.test_queries:
            # Test queries
            print("[TEST] get_batch_rca_context('HCC2601001'):")
            print(json.dumps(graph.get_batch_rca_context('HCC2601001'), indent=2, default=str))
            
        else:
            parser.print_help()
            
    except ImportError as e:
        print(f"[ERROR] Cannot import neo4j_ontology: {e}")
        print("This module is designed to integrate with Leor's existing codebase.")


if __name__ == "__main__":
    main()
