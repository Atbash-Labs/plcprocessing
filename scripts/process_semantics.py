#!/usr/bin/env python3
"""
Process-semantics layer for the PLC/SCADA ontology.

Defines canonical node types for physics, chemistry, and operating constraints
that can be induced from both PLC/SCADA structure and external documents
(P&IDs, SOPs, engineering diagrams).

Node types:
  ProcessMedium, UnitOperation, OperatingEnvelope,
  PhysicalPrinciple, ChemicalSpecies, Reaction

All write helpers follow the same MERGE-based pattern as the existing ontology
and attach provenance metadata to every asserted fact.
"""

import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime


# ============================================================================
# Provenance metadata contract
# ============================================================================

@dataclass
class EvidenceItem:
    """Single piece of evidence supporting a graph fact."""
    source_file: str = ""
    source_kind: str = ""          # "pid", "sop", "diagram", "plc", "scada"
    source_page: Optional[int] = None
    source_region: str = ""        # bounding-box or section id
    source_excerpt: str = ""       # verbatim snippet
    extraction_model: str = ""     # "gpt-5.4", "claude-sonnet", "deterministic"
    extraction_method: str = ""    # "vision", "text", "structured_parse"
    confidence: float = 1.0
    extracted_at: str = ""

    def __post_init__(self):
        if not self.extracted_at:
            self.extracted_at = datetime.utcnow().isoformat()


def evidence_to_json(items: List[EvidenceItem]) -> str:
    return json.dumps([asdict(e) for e in items])


def merge_evidence(existing_json: Optional[str], new_items: List[EvidenceItem]) -> str:
    """Append new evidence items to an existing JSON array (append-only)."""
    existing: list = []
    if existing_json:
        try:
            existing = json.loads(existing_json)
        except (json.JSONDecodeError, TypeError):
            existing = []
    existing.extend([asdict(e) for e in new_items])
    return json.dumps(existing)


# ============================================================================
# Canonical process-semantic schemas
# ============================================================================

PROCESS_NODE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "ProcessMedium": {
        "key_property": "name",
        "properties": {
            "name": "str",           # e.g. "Steam", "CIP-Caustic", "Product-A"
            "category": "str",       # "utility", "product", "waste", "solvent", "gas"
            "phase": "str",          # "liquid", "gas", "solid", "mixed"
            "description": "str",
            "purpose": "str",
        },
        "description": "A material or utility stream handled by plant equipment.",
    },
    "UnitOperation": {
        "key_property": "name",
        "properties": {
            "name": "str",           # e.g. "Pumping", "CIP", "Heating"
            "category": "str",       # "transfer", "thermal", "mixing", "separation", "cleaning", "reaction"
            "description": "str",
            "purpose": "str",
        },
        "description": "A canonical plant operation such as pumping, mixing, or filtration.",
    },
    "OperatingEnvelope": {
        "key_property": "name",
        "properties": {
            "name": "str",           # e.g. "BR-500-001/Temperature"
            "parameter": "str",      # "temperature", "pressure", "flow", "level", "pH"
            "unit": "str",           # "degC", "bar", "L/min"
            "low_limit": "float",
            "low_warning": "float",
            "normal_low": "float",
            "normal_high": "float",
            "high_warning": "float",
            "high_limit": "float",
            "trip_low": "float",
            "trip_high": "float",
            "description": "str",
        },
        "description": "Normal ranges, alarm bands, and trip windows for a measured parameter.",
    },
    "PhysicalPrinciple": {
        "key_property": "name",
        "properties": {
            "name": "str",           # e.g. "Temperature", "Pressure", "Flow"
            "category": "str",       # "thermal", "fluid", "electrical", "mechanical", "analytical"
            "unit_family": "str",    # "temperature", "pressure", "volumetric_flow", etc.
            "description": "str",
        },
        "description": "A measurable physical quantity relevant to process control.",
    },
    "ChemicalSpecies": {
        "key_property": "name",
        "properties": {
            "name": "str",           # e.g. "NaOH", "Ethanol", "Product-X"
            "cas_number": "str",
            "category": "str",       # "reactant", "product", "byproduct", "additive", "cleaning_agent"
            "molecular_formula": "str",
            "description": "str",
        },
        "description": "A specific chemical substance involved in plant processes.",
    },
    "Reaction": {
        "key_property": "name",
        "properties": {
            "name": "str",           # e.g. "Neutralization-CIP", "Fermentation-Stage1"
            "category": "str",       # "neutralization", "fermentation", "oxidation", "blending", etc.
            "description": "str",
            "conditions": "str",     # brief summary of required conditions
        },
        "description": "A chemical or physical transformation step in the process.",
    },
}

# Allowed relationship types for the process layer
PROCESS_RELATIONSHIPS: Dict[str, Dict[str, str]] = {
    "HANDLES_MEDIUM":        {"from": "Equipment",        "to": "ProcessMedium"},
    "PERFORMS_OPERATION":     {"from": "Equipment",        "to": "UnitOperation"},
    "HAS_OPERATING_ENVELOPE":{"from": "Equipment",        "to": "OperatingEnvelope"},
    "FEEDS":                {"from": "Equipment",        "to": "Equipment"},
    "MEASURES":              {"from": "ScadaTag",          "to": "PhysicalPrinciple"},
    "MONITORS_ENVELOPE":     {"from": "ScadaTag",          "to": "OperatingEnvelope"},
    "IMPLEMENTS_CONTROL_OF": {"from": "AOI",               "to": "UnitOperation"},
    "USES_PRINCIPLE":        {"from": "UnitOperation",     "to": "PhysicalPrinciple"},
    "INVOLVES_SPECIES":      {"from": "Reaction",          "to": "ChemicalSpecies"},
    "PROCESSES_SPECIES":     {"from": "UnitOperation",     "to": "ChemicalSpecies"},
    "HAS_REACTION":          {"from": "UnitOperation",     "to": "Reaction"},
    "MEDIUM_CONTAINS":       {"from": "ProcessMedium",     "to": "ChemicalSpecies"},
    "ENVELOPE_FOR_PRINCIPLE":{"from": "OperatingEnvelope", "to": "PhysicalPrinciple"},
    "VISUALIZES":            {"from": "ViewComponent",     "to": "Equipment"},
}


# ============================================================================
# Schema metadata for graph API / UI contract
# ============================================================================

PROCESS_LABEL_META: Dict[str, Dict[str, Any]] = {
    label: {
        "key_property": schema["key_property"],
        "display_property": "name",
        "searchable_properties": ["name", "description", "purpose"] if "purpose" in schema["properties"] else ["name", "description"],
        "editable_properties": list(schema["properties"].keys()),
        "group": "process",
        "description": schema["description"],
    }
    for label, schema in PROCESS_NODE_SCHEMAS.items()
}
