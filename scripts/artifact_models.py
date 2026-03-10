#!/usr/bin/env python3
"""
Normalized extraction models for GPT-5.4 artifact ingestion.

Defines the intermediate schema between raw GPT output and Neo4j writes.
All extraction results are normalized into these dataclasses before any
graph mutations happen.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

from process_semantics import EvidenceItem


# ============================================================================
# Extracted fact types
# ============================================================================

@dataclass
class ExtractedNodeUpdate:
    """An update to an existing ontology node extracted from a source."""
    node_label: str                 # e.g. "Equipment", "AOI", "ScadaTag"
    node_name: str                  # name used to MERGE/match
    properties: Dict[str, Any] = field(default_factory=dict)
    evidence: List[EvidenceItem] = field(default_factory=list)


@dataclass
class ExtractedRelationship:
    """A relationship extracted between two entities."""
    source_label: str
    source_name: str
    target_label: str
    target_name: str
    rel_type: str                   # e.g. "HANDLES_MEDIUM", "PERFORMS_OPERATION"
    properties: Dict[str, Any] = field(default_factory=dict)
    evidence: List[EvidenceItem] = field(default_factory=list)


@dataclass
class ExtractedProcessConcept:
    """A new process-semantic concept to be induced."""
    label: str                      # e.g. "ProcessMedium", "UnitOperation"
    name: str
    properties: Dict[str, Any] = field(default_factory=dict)
    evidence: List[EvidenceItem] = field(default_factory=list)


@dataclass
class ExtractionResult:
    """Complete normalized result from extracting one source artifact."""
    source_file: str
    source_kind: str                # "pid", "sop", "diagram"
    node_updates: List[ExtractedNodeUpdate] = field(default_factory=list)
    relationships: List[ExtractedRelationship] = field(default_factory=list)
    process_concepts: List[ExtractedProcessConcept] = field(default_factory=list)
    raw_mentions: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


# ============================================================================
# GPT extraction prompt contract
# ============================================================================

EXTRACTION_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "equipment_facts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "equipment_name": {"type": "string"},
                    "service": {"type": "string"},
                    "function": {"type": "string"},
                    "media_handled": {"type": "array", "items": {"type": "string"}},
                    "operations_performed": {"type": "array", "items": {"type": "string"}},
                    "operating_parameters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "parameter": {"type": "string"},
                                "unit": {"type": "string"},
                                "normal_low": {"type": "number"},
                                "normal_high": {"type": "number"},
                                "alarm_low": {"type": "number"},
                                "alarm_high": {"type": "number"},
                                "trip_low": {"type": "number"},
                                "trip_high": {"type": "number"},
                            },
                        },
                    },
                },
            },
        },
        "tag_facts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "tag_name": {"type": "string"},
                    "measures": {"type": "string"},
                    "process_context": {"type": "string"},
                    "unit": {"type": "string"},
                },
            },
        },
        "process_media": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category": {"type": "string"},
                    "phase": {"type": "string"},
                    "description": {"type": "string"},
                },
            },
        },
        "unit_operations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category": {"type": "string"},
                    "description": {"type": "string"},
                },
            },
        },
        "chemical_species": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category": {"type": "string"},
                    "cas_number": {"type": "string"},
                    "description": {"type": "string"},
                },
            },
        },
        "reactions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category": {"type": "string"},
                    "description": {"type": "string"},
                    "species_involved": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_type": {"type": "string"},
                    "source_name": {"type": "string"},
                    "relationship": {"type": "string"},
                    "target_type": {"type": "string"},
                    "target_name": {"type": "string"},
                },
            },
        },
    },
}
