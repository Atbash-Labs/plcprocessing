#!/usr/bin/env python3
"""
Entity linker for artifact extraction results.

Resolves extracted mentions from GPT-5.4 output to existing ontology nodes,
deduplicates process-semantic concepts, and produces a clean set of graph
mutations ready for Neo4j writes.
"""

import json
from typing import Dict, List, Optional, Any, Tuple

from neo4j_ontology import OntologyGraph
from process_semantics import (
    EvidenceItem,
    PROCESS_NODE_SCHEMAS,
    PROCESS_RELATIONSHIPS,
)
from artifact_models import (
    ExtractedNodeUpdate,
    ExtractedRelationship,
    ExtractedProcessConcept,
    ExtractionResult,
)


class ArtifactLinker:
    """
    Resolves extracted mentions to existing graph entities and normalizes
    process-semantic concepts before graph writes.

    Uses GPT to match extracted names to existing graph nodes when a GPT
    client is provided; falls back to substring matching otherwise.
    """

    def __init__(self, graph: OntologyGraph, gpt_client=None):
        self._graph = graph
        self._gpt = gpt_client
        self._entity_cache: Dict[str, Dict[str, str]] = {}
        self._entity_cache_raw: Dict[str, List[str]] = {}
        self._gpt_resolved: Dict[str, Dict[str, Optional[str]]] = {}
        self._gpt_visualizes: Dict[str, List[str]] = {}

    def load_entity_cache(self) -> Dict[str, List[str]]:
        """
        Load known entity names from the graph for linking hints.
        Returns dict mapping label -> list of names.
        """
        labels_to_query = [
            "Equipment", "AOI", "UDT", "ScadaTag", "View", "ViewComponent",
            "ProcessMedium", "UnitOperation", "OperatingEnvelope",
            "PhysicalPrinciple", "ChemicalSpecies", "Reaction",
            "Process", "Operation", "CriticalControlPoint",
        ]
        cache: Dict[str, List[str]] = {}
        with self._graph.session() as session:
            for label in labels_to_query:
                try:
                    if label == "ViewComponent":
                        result = session.run(
                            "MATCH (n:ViewComponent) RETURN n.path AS name LIMIT 1000"
                        )
                    else:
                        result = session.run(
                            f"MATCH (n:{label}) RETURN n.name AS name LIMIT 500"
                        )
                    names = [r["name"] for r in result if r["name"]]
                    if names:
                        cache[label] = names
                except Exception:
                    pass
        self._entity_cache = {
            label: {n.lower(): n for n in names}
            for label, names in cache.items()
        }
        self._entity_cache_raw = cache
        return cache

    def _collect_extracted_mentions(self, raw: Dict[str, Any]) -> Dict[str, List[str]]:
        """Gather all entity names from a raw GPT extraction keyed by label."""
        mentions: Dict[str, set] = {}

        for eq in raw.get("equipment_facts", []):
            name = eq.get("equipment_name", "")
            if name:
                mentions.setdefault("Equipment", set()).add(name)

        for tag in raw.get("tag_facts", []):
            name = tag.get("tag_name", "")
            if name:
                mentions.setdefault("ScadaTag", set()).add(name)

        for rel in raw.get("relationships", []):
            src_label = rel.get("source_type", "")
            src_name = rel.get("source_name", "")
            tgt_label = rel.get("target_type", "")
            tgt_name = rel.get("target_name", "")
            if src_label and src_name:
                mentions.setdefault(src_label, set()).add(src_name)
            if tgt_label and tgt_name:
                mentions.setdefault(tgt_label, set()).add(tgt_name)

        return {label: sorted(names) for label, names in mentions.items()}

    def run_gpt_entity_resolution(
        self, raw: Dict[str, Any], verbose: bool = False
    ) -> None:
        """
        Use GPT to resolve extracted mentions against existing graph entities.
        Populates self._gpt_resolved with the mappings.
        """
        if not self._gpt:
            return

        self._gpt_visualizes = {}
        extracted = self._collect_extracted_mentions(raw)
        labels_to_resolve = {
            label: names for label, names in extracted.items()
            if label in self._entity_cache_raw and self._entity_cache_raw[label]
        }

        has_vc = bool(self._entity_cache_raw.get("ViewComponent"))
        if has_vc and "Equipment" in extracted and "Equipment" not in labels_to_resolve:
            labels_to_resolve["Equipment"] = extracted["Equipment"]

        if not labels_to_resolve:
            return

        import sys
        if verbose:
            total = sum(len(v) for v in labels_to_resolve.values())
            print(
                f"[ArtifactLinker] Resolving {total} extracted mentions "
                f"against {sum(len(v) for v in self._entity_cache_raw.values())} existing entities via GPT...",
                file=sys.stderr, flush=True,
            )

        raw_result = self._gpt.resolve_entities(
            labels_to_resolve,
            self._entity_cache_raw,
            verbose=verbose,
        )

        vis = raw_result.pop("visualizes", {})
        if isinstance(vis, dict):
            for equip, vcs in vis.items():
                if isinstance(vcs, list):
                    self._gpt_visualizes[equip] = [v for v in vcs if isinstance(v, str)]
                elif isinstance(vcs, str):
                    self._gpt_visualizes[equip] = [vcs]

        self._gpt_resolved = raw_result

        if verbose:
            matched = sum(
                1 for mappings in self._gpt_resolved.values()
                for v in mappings.values() if v
            )
            vis_count = sum(len(v) for v in self._gpt_visualizes.values())
            print(
                f"[ArtifactLinker] GPT matched {matched} mentions to existing entities, "
                f"{vis_count} VISUALIZES links proposed",
                file=sys.stderr, flush=True,
            )
            for equip, vcs in self._gpt_visualizes.items():
                for vc in vcs:
                    print(
                        f"[ArtifactLinker]   VISUALIZES: {vc} -> Equipment:{equip}",
                        file=sys.stderr, flush=True,
                    )

    def resolve_name(self, label: str, raw_name: str) -> Tuple[str, bool]:
        """
        Resolve an extracted name to an existing graph entity.

        Resolution order:
        1. GPT-resolved mapping (if available)
        2. Exact case-insensitive match
        3. Return raw name as-is (not matched)
        """
        if not raw_name:
            return raw_name, False

        # Check GPT resolution first
        gpt_mappings = self._gpt_resolved.get(label, {})
        if raw_name in gpt_mappings:
            resolved = gpt_mappings[raw_name]
            if resolved:
                return resolved, True
            else:
                return raw_name.strip(), False

        # Exact case-insensitive match
        label_cache = self._entity_cache.get(label, {})
        lower = raw_name.lower().strip()
        if lower in label_cache:
            return label_cache[lower], True

        return raw_name.strip(), False

    def normalize_extraction(
        self,
        raw: Dict[str, Any],
        source_file: str,
        source_kind: str,
        extraction_model: str = "gpt-5.4",
        verbose: bool = False,
    ) -> ExtractionResult:
        """
        Convert raw GPT-5.4 JSON output into a normalized ExtractionResult.

        Performs:
        - entity linking against known graph names
        - process concept normalization
        - relationship validation against allowed vocabulary
        - evidence attachment
        """
        result = ExtractionResult(
            source_file=source_file,
            source_kind=source_kind,
        )

        self.run_gpt_entity_resolution(raw, verbose=verbose)

        base_evidence = EvidenceItem(
            source_file=source_file,
            source_kind=source_kind,
            extraction_model=extraction_model,
            extraction_method="vision" if source_kind in ("pid", "diagram") else "text",
        )

        self._process_equipment_facts(raw, result, base_evidence)
        self._process_tag_facts(raw, result, base_evidence)
        self._process_media(raw, result, base_evidence)
        self._process_operations(raw, result, base_evidence)
        self._process_species(raw, result, base_evidence)
        self._process_reactions(raw, result, base_evidence)
        self._process_relationships(raw, result, base_evidence)
        self._process_visualizes(result, base_evidence)

        return result

    # ------------------------------------------------------------------
    # Internal normalization helpers
    # ------------------------------------------------------------------

    def _make_evidence(self, base: EvidenceItem, **overrides) -> EvidenceItem:
        from dataclasses import asdict
        d = asdict(base)
        d.update(overrides)
        return EvidenceItem(**d)

    def _process_equipment_facts(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        for eq in raw.get("equipment_facts", []):
            name = eq.get("equipment_name", "")
            if not name:
                continue

            resolved, _ = self.resolve_name("Equipment", name)
            ev = self._make_evidence(base_ev, source_excerpt=f"Equipment: {resolved}")

            props: Dict[str, Any] = {}
            if eq.get("service"):
                props["service"] = eq["service"]
            if eq.get("function"):
                props["process_function"] = eq["function"]

            result.node_updates.append(ExtractedNodeUpdate(
                node_label="Equipment",
                node_name=resolved,
                properties=props,
                evidence=[ev],
            ))

            for medium in eq.get("media_handled", []):
                med_resolved, _ = self.resolve_name("ProcessMedium", medium)
                result.process_concepts.append(ExtractedProcessConcept(
                    label="ProcessMedium",
                    name=med_resolved,
                    properties={"category": "product"},
                    evidence=[ev],
                ))
                result.relationships.append(ExtractedRelationship(
                    source_label="Equipment", source_name=resolved,
                    target_label="ProcessMedium", target_name=med_resolved,
                    rel_type="HANDLES_MEDIUM",
                    evidence=[ev],
                ))

            for op in eq.get("operations_performed", []):
                op_resolved, _ = self.resolve_name("UnitOperation", op)
                result.process_concepts.append(ExtractedProcessConcept(
                    label="UnitOperation",
                    name=op_resolved,
                    properties={"category": "transfer"},
                    evidence=[ev],
                ))
                result.relationships.append(ExtractedRelationship(
                    source_label="Equipment", source_name=resolved,
                    target_label="UnitOperation", target_name=op_resolved,
                    rel_type="PERFORMS_OPERATION",
                    evidence=[ev],
                ))

            for param in eq.get("operating_parameters", []):
                env_name = f"{resolved}/{param.get('parameter', 'unknown')}"
                env_props = {
                    k: param[k] for k in [
                        "parameter", "unit",
                        "normal_low", "normal_high",
                        "alarm_low", "alarm_high",
                        "trip_low", "trip_high",
                    ] if param.get(k) is not None
                }
                # Map alarm to warning for schema consistency
                if "alarm_low" in env_props:
                    env_props["low_warning"] = env_props.pop("alarm_low")
                if "alarm_high" in env_props:
                    env_props["high_warning"] = env_props.pop("alarm_high")
                if "trip_low" in env_props:
                    env_props["trip_low"] = env_props["trip_low"]
                if "trip_high" in env_props:
                    env_props["trip_high"] = env_props["trip_high"]

                result.process_concepts.append(ExtractedProcessConcept(
                    label="OperatingEnvelope",
                    name=env_name,
                    properties=env_props,
                    evidence=[ev],
                ))
                result.relationships.append(ExtractedRelationship(
                    source_label="Equipment", source_name=resolved,
                    target_label="OperatingEnvelope", target_name=env_name,
                    rel_type="HAS_OPERATING_ENVELOPE",
                    evidence=[ev],
                ))

    def _process_tag_facts(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        for tag in raw.get("tag_facts", []):
            name = tag.get("tag_name", "")
            if not name:
                continue

            resolved, _ = self.resolve_name("ScadaTag", name)
            ev = self._make_evidence(base_ev, source_excerpt=f"Tag: {resolved}")

            props: Dict[str, Any] = {}
            if tag.get("process_context"):
                props["process_context"] = tag["process_context"]

            result.node_updates.append(ExtractedNodeUpdate(
                node_label="ScadaTag",
                node_name=resolved,
                properties=props,
                evidence=[ev],
            ))

            if tag.get("measures"):
                principle_name = tag["measures"]
                pp_resolved, _ = self.resolve_name("PhysicalPrinciple", principle_name)
                result.process_concepts.append(ExtractedProcessConcept(
                    label="PhysicalPrinciple",
                    name=pp_resolved,
                    properties={"category": "analytical"},
                    evidence=[ev],
                ))
                result.relationships.append(ExtractedRelationship(
                    source_label="ScadaTag", source_name=resolved,
                    target_label="PhysicalPrinciple", target_name=pp_resolved,
                    rel_type="MEASURES",
                    evidence=[ev],
                ))

    def _process_media(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        for medium in raw.get("process_media", []):
            name = medium.get("name", "")
            if not name:
                continue
            resolved, _ = self.resolve_name("ProcessMedium", name)
            ev = self._make_evidence(base_ev, source_excerpt=f"Medium: {resolved}")
            result.process_concepts.append(ExtractedProcessConcept(
                label="ProcessMedium",
                name=resolved,
                properties={
                    k: medium[k] for k in ["category", "phase", "description"]
                    if medium.get(k)
                },
                evidence=[ev],
            ))

    def _process_operations(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        for op in raw.get("unit_operations", []):
            name = op.get("name", "")
            if not name:
                continue
            resolved, _ = self.resolve_name("UnitOperation", name)
            ev = self._make_evidence(base_ev, source_excerpt=f"Operation: {resolved}")
            result.process_concepts.append(ExtractedProcessConcept(
                label="UnitOperation",
                name=resolved,
                properties={
                    k: op[k] for k in ["category", "description"]
                    if op.get(k)
                },
                evidence=[ev],
            ))

    def _process_species(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        for sp in raw.get("chemical_species", []):
            name = sp.get("name", "")
            if not name:
                continue
            resolved, _ = self.resolve_name("ChemicalSpecies", name)
            ev = self._make_evidence(base_ev, source_excerpt=f"Species: {resolved}")
            result.process_concepts.append(ExtractedProcessConcept(
                label="ChemicalSpecies",
                name=resolved,
                properties={
                    k: sp[k] for k in ["category", "cas_number", "description"]
                    if sp.get(k)
                },
                evidence=[ev],
            ))

    def _process_reactions(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        for rx in raw.get("reactions", []):
            name = rx.get("name", "")
            if not name:
                continue
            resolved, _ = self.resolve_name("Reaction", name)
            ev = self._make_evidence(base_ev, source_excerpt=f"Reaction: {resolved}")
            result.process_concepts.append(ExtractedProcessConcept(
                label="Reaction",
                name=resolved,
                properties={
                    k: rx[k] for k in ["category", "description"]
                    if rx.get(k)
                },
                evidence=[ev],
            ))
            for species in rx.get("species_involved", []):
                sp_resolved, _ = self.resolve_name("ChemicalSpecies", species)
                result.relationships.append(ExtractedRelationship(
                    source_label="Reaction", source_name=resolved,
                    target_label="ChemicalSpecies", target_name=sp_resolved,
                    rel_type="INVOLVES_SPECIES",
                    evidence=[ev],
                ))

    def _process_visualizes(
        self, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        """Add VISUALIZES relationships proposed by GPT entity resolution."""
        for equip_name, vc_names in self._gpt_visualizes.items():
            for vc_name in vc_names:
                ev = self._make_evidence(
                    base_ev,
                    source_excerpt=f"ViewComponent:{vc_name} -[VISUALIZES]-> Equipment:{equip_name}",
                )
                result.relationships.append(ExtractedRelationship(
                    source_label="ViewComponent", source_name=vc_name,
                    target_label="Equipment", target_name=equip_name,
                    rel_type="VISUALIZES",
                    evidence=[ev],
                ))

    def _process_relationships(
        self, raw: Dict, result: ExtractionResult, base_ev: EvidenceItem
    ) -> None:
        allowed = set(PROCESS_RELATIONSHIPS.keys())
        for rel in raw.get("relationships", []):
            rel_type = rel.get("relationship", "")
            if rel_type not in allowed:
                continue

            src_label = rel.get("source_type", "")
            src_name = rel.get("source_name", "")
            tgt_label = rel.get("target_type", "")
            tgt_name = rel.get("target_name", "")

            if not all([src_label, src_name, tgt_label, tgt_name]):
                continue

            src_resolved, _ = self.resolve_name(src_label, src_name)
            tgt_resolved, _ = self.resolve_name(tgt_label, tgt_name)

            ev = self._make_evidence(
                base_ev,
                source_excerpt=f"{src_label}:{src_resolved} -{rel_type}-> {tgt_label}:{tgt_resolved}",
            )
            result.relationships.append(ExtractedRelationship(
                source_label=src_label, source_name=src_resolved,
                target_label=tgt_label, target_name=tgt_resolved,
                rel_type=rel_type,
                evidence=[ev],
            ))
