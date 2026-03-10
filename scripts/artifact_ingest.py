#!/usr/bin/env python3
"""
Artifact ingestion pipeline for P&IDs, SOPs, and engineering diagrams.

Orchestrates:
1. Source parsing (image or text)
2. GPT-5.4 structured extraction
3. Entity linking and concept normalization
4. Provenance-aware Neo4j writes

Usage:
    from artifact_ingest import ArtifactIngester

    ingester = ArtifactIngester(graph)
    result = ingester.ingest_file("path/to/pid.png", source_kind="pid")
"""

import os
import sys
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import asdict

from neo4j_ontology import OntologyGraph, get_ontology_graph
from gpt54_client import GPT54Client
from artifact_linker import ArtifactLinker
from artifact_models import ExtractionResult
from process_semantics import (
    EvidenceItem,
    evidence_to_json,
    merge_evidence,
    PROCESS_NODE_SCHEMAS,
    PROCESS_RELATIONSHIPS,
)


class ArtifactIngester:
    """
    End-to-end ingestion pipeline for process engineering artifacts.

    Supports:
    - P&IDs (images): .png, .jpg, .jpeg, .bmp, .tiff, .webp
    - SOPs (text): .txt, .md, .pdf (text extraction only)
    - Engineering diagrams (images): same as P&IDs
    """

    IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp", ".gif"}
    TEXT_EXTENSIONS = {".txt", ".md", ".csv", ".tsv"}

    def __init__(
        self,
        graph: Optional[OntologyGraph] = None,
        gpt_client: Optional[GPT54Client] = None,
        verbose: bool = False,
        on_progress: Optional[Callable[[str], None]] = None,
    ):
        self._graph = graph or get_ontology_graph()
        self._gpt = gpt_client
        self._linker = None  # initialized lazily after GPT client is ready
        self._verbose = verbose
        self._on_progress = on_progress or (lambda msg: None)

    def _ensure_gpt(self) -> GPT54Client:
        if self._gpt is None:
            self._gpt = GPT54Client()
        if self._linker is None:
            self._linker = ArtifactLinker(self._graph, gpt_client=self._gpt)
        return self._gpt

    def _log(self, msg: str) -> None:
        if self._verbose:
            print(f"[ArtifactIngest] {msg}", file=sys.stderr, flush=True)
        self._on_progress(msg)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest_file(
        self,
        file_path: str,
        source_kind: str = "pid",
    ) -> Dict[str, Any]:
        """
        Ingest a single artifact file end-to-end.

        Args:
            file_path: Path to the source file.
            source_kind: "pid", "sop", or "diagram".

        Returns:
            Summary dict with counts and any errors.
        """
        path = Path(file_path)
        if not path.exists():
            return {"error": f"File not found: {file_path}"}

        self._log(f"Extracting facts from {path.name} ({source_kind})...")
        gpt = self._ensure_gpt()

        self._log(f"Loading entity cache for linking...")
        entity_hints = self._linker.load_entity_cache()
        ext = path.suffix.lower()

        if ext in self.IMAGE_EXTENSIONS:
            raw = gpt.extract_from_image(
                str(path),
                source_kind=source_kind,
                existing_entities=entity_hints,
                verbose=self._verbose,
            )
        elif ext in self.TEXT_EXTENSIONS:
            text = path.read_text(encoding="utf-8", errors="replace")
            raw = gpt.extract_from_text(
                text,
                source_file=str(path),
                source_kind=source_kind,
                existing_entities=entity_hints,
                verbose=self._verbose,
            )
        elif ext == ".pdf":
            text = self._extract_pdf_text(str(path))
            raw = gpt.extract_from_text(
                text,
                source_file=str(path),
                source_kind=source_kind,
                existing_entities=entity_hints,
                verbose=self._verbose,
            )
        else:
            return {"error": f"Unsupported file type: {ext}"}

        if "error" in raw:
            return {"error": raw["error"], "raw": raw.get("raw", "")}

        eq_count = len(raw.get("equipment_facts", []))
        tag_count = len(raw.get("tag_facts", []))
        media_count = len(raw.get("process_media", []))
        op_count = len(raw.get("unit_operations", []))
        species_count = len(raw.get("chemical_species", []))
        rx_count = len(raw.get("reactions", []))
        rel_count = len(raw.get("relationships", []))
        self._log(
            f"GPT extracted: {eq_count} equipment, {tag_count} tags, "
            f"{media_count} media, {op_count} operations, "
            f"{species_count} species, {rx_count} reactions, {rel_count} relationships"
        )

        self._log("Resolving extracted entity names against existing graph nodes...")
        extraction = self._linker.normalize_extraction(
            raw, source_file=str(path), source_kind=source_kind,
            verbose=self._verbose,
        )

        self._log("Writing facts to Neo4j...")
        summary = self._write_extraction(extraction)

        self._log(
            f"Done: {summary['nodes_updated']} updates, "
            f"{summary['concepts_created']} concepts, "
            f"{summary['relationships_created']} relationships"
        )
        return summary

    def ingest_batch(
        self,
        files: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        """
        Ingest multiple artifacts.

        Args:
            files: List of dicts with "path" and "source_kind" keys.

        Returns:
            Aggregate summary.
        """
        totals = {
            "files_processed": 0,
            "files_failed": 0,
            "nodes_updated": 0,
            "concepts_created": 0,
            "relationships_created": 0,
            "node_details": [],
            "concept_details": [],
            "relationship_details": [],
            "errors": [],
        }

        for i, f in enumerate(files, 1):
            self._log(f"Processing file {i}/{len(files)}: {f['path']}")
            result = self.ingest_file(f["path"], f.get("source_kind", "pid"))
            if "error" in result:
                totals["files_failed"] += 1
                totals["errors"].append({"file": f["path"], "error": result["error"]})
            else:
                totals["files_processed"] += 1
                totals["nodes_updated"] += result.get("nodes_updated", 0)
                totals["concepts_created"] += result.get("concepts_created", 0)
                totals["relationships_created"] += result.get("relationships_created", 0)
                totals["node_details"].extend(result.get("node_details", []))
                totals["concept_details"].extend(result.get("concept_details", []))
                totals["relationship_details"].extend(result.get("relationship_details", []))

        return totals

    # ------------------------------------------------------------------
    # Graph write helpers
    # ------------------------------------------------------------------

    def _write_extraction(self, extraction: ExtractionResult) -> Dict[str, Any]:
        """Write a normalized ExtractionResult to Neo4j with provenance."""
        summary = {
            "source_file": extraction.source_file,
            "source_kind": extraction.source_kind,
            "nodes_updated": 0,
            "concepts_created": 0,
            "relationships_created": 0,
            "node_details": [],
            "concept_details": [],
            "relationship_details": [],
            "errors": extraction.errors[:],
        }

        with self._graph.session() as session:
            for update in extraction.node_updates:
                try:
                    matched, existed = self._write_node_update(session, update)
                    props_str = ", ".join(f"{k}={v}" for k, v in update.properties.items()) if update.properties else ""
                    detail = f"{update.node_label}:{update.node_name}"
                    if props_str:
                        detail += f" ({props_str})"
                    if matched:
                        summary["nodes_updated"] += 1
                        summary["node_details"].append(detail)
                        action = "Updated" if existed else "Created"
                        self._log(f"  {action} {detail}")
                    else:
                        self._log(f"  Skipped {detail} (not found in graph)")
                except Exception as e:
                    summary["errors"].append(f"Node update {update.node_name}: {e}")

            seen_concepts = set()
            for concept in extraction.process_concepts:
                dedup_key = f"{concept.label}:{concept.name}"
                if dedup_key in seen_concepts:
                    continue
                seen_concepts.add(dedup_key)
                try:
                    self._write_process_concept(session, concept)
                    summary["concepts_created"] += 1
                    props_str = ", ".join(f"{k}={v}" for k, v in concept.properties.items()) if concept.properties else ""
                    detail = f"{concept.label}:{concept.name}"
                    if props_str:
                        detail += f" ({props_str})"
                    summary["concept_details"].append(detail)
                    self._log(f"  Created {detail}")
                except Exception as e:
                    summary["errors"].append(f"Concept {concept.name}: {e}")

            seen_rels = set()
            for rel in extraction.relationships:
                dedup_key = f"{rel.source_label}:{rel.source_name}-{rel.rel_type}->{rel.target_label}:{rel.target_name}"
                if dedup_key in seen_rels:
                    continue
                seen_rels.add(dedup_key)
                try:
                    detail = f"{rel.source_label}:{rel.source_name} -[{rel.rel_type}]-> {rel.target_label}:{rel.target_name}"
                    linked = self._write_relationship(session, rel)
                    if linked:
                        summary["relationships_created"] += 1
                        summary["relationship_details"].append(detail)
                        self._log(f"  Linked {detail}")
                    else:
                        self._log(f"  Skipped {detail} (endpoint not found)")
                except Exception as e:
                    summary["errors"].append(f"Rel {rel.rel_type}: {e}")

        return summary

    EXISTING_LABELS = {
        "AOI", "Tag", "UDT", "View", "ViewComponent",
        "ScadaTag", "Script", "NamedQuery", "Project",
        "FaultSymptom", "FaultCause", "OperatorPhrase",
        "ControlPattern", "DataFlow", "SafetyElement",
        "Material", "Batch", "ProductionOrder", "Operation",
        "CriticalControlPoint", "ProcessDeviation",
        "TiaProject", "PLCDevice", "HMIDevice", "HMIConnection",
        "HMIAlarm", "HMIAlarmClass", "HMIScript", "HMIScreen",
        "PLCTagTable", "PLCTag", "HMITagTable", "HMITextList",
    }

    def _write_node_update(self, session, update) -> bool:
        """Update an existing node with new properties and evidence.

        For backbone labels (Equipment, ScadaTag, AOI, etc.) uses MATCH so
        it only updates nodes that already exist -- never creates new ones.
        Returns True if a node was actually matched and updated.
        """
        ev_json = evidence_to_json(update.evidence)

        set_clauses = []
        params: Dict[str, Any] = {"name": update.node_name, "ev_json": ev_json}

        for k, v in update.properties.items():
            param_key = f"prop_{k}"
            set_clauses.append(f"n.{k} = ${param_key}")
            params[param_key] = v

        set_clause = ", ".join(set_clauses) if set_clauses else ""
        if set_clause:
            set_clause = f"SET {set_clause}, "
        else:
            set_clause = "SET "

        if update.node_label in self.EXISTING_LABELS:
            verb = "MATCH"
        else:
            verb = "MERGE"

        query = f"""
            {verb} (n:{update.node_label} {{name: $name}})
            {set_clause}
                n.evidence_items = CASE
                    WHEN n.evidence_items IS NULL THEN $ev_json
                    ELSE n.evidence_items + $ev_json
                END,
                n.last_evidence_at = datetime()
            RETURN n.name AS matched, n.created_at IS NOT NULL AS existed
        """
        result = session.run(query, params)
        record = result.single()
        if record is None:
            return False, False
        return True, bool(record.get("existed", True))

    def _write_process_concept(self, session, concept) -> None:
        """Create or merge a process-semantic node with provenance."""
        ev_json = evidence_to_json(concept.evidence)

        set_clauses = []
        params: Dict[str, Any] = {"name": concept.name, "ev_json": ev_json}

        for k, v in concept.properties.items():
            param_key = f"prop_{k}"
            set_clauses.append(f"n.{k} = COALESCE(n.{k}, ${param_key})")
            params[param_key] = v

        set_clause = ", ".join(set_clauses) if set_clauses else ""
        if set_clause:
            set_clause = f"SET {set_clause}, "
        else:
            set_clause = "SET "

        query = f"""
            MERGE (n:{concept.label} {{name: $name}})
            {set_clause}
                n.evidence_items = CASE
                    WHEN n.evidence_items IS NULL THEN $ev_json
                    ELSE n.evidence_items + $ev_json
                END,
                n.last_evidence_at = datetime()
        """
        session.run(query, params)

    _PATH_KEYED_LABELS = {"ViewComponent"}

    def _match_clause(self, alias: str, label: str, param_name: str) -> str:
        """Return a MATCH clause using `path` for ViewComponent, `name` otherwise."""
        if label in self._PATH_KEYED_LABELS:
            return f"MATCH ({alias}:{label} {{path: ${param_name}}})"
        return f"MATCH ({alias}:{label} {{name: ${param_name}}})"

    def _write_relationship(self, session, rel) -> bool:
        """Create a relationship with provenance metadata.

        Returns True if both endpoints existed and the relationship was written.
        """
        ev_json = evidence_to_json(rel.evidence)

        params: Dict[str, Any] = {
            "src_name": rel.source_name,
            "tgt_name": rel.target_name,
            "ev_json": ev_json,
        }

        for k, v in rel.properties.items():
            param_key = f"prop_{k}"
            params[param_key] = v

        src_match = self._match_clause("src", rel.source_label, "src_name")
        tgt_match = self._match_clause("tgt", rel.target_label, "tgt_name")

        query = f"""
            {src_match}
            {tgt_match}
            MERGE (src)-[r:{rel.rel_type}]->(tgt)
            SET r.evidence_items = CASE
                    WHEN r.evidence_items IS NULL THEN $ev_json
                    ELSE r.evidence_items + $ev_json
                END,
                r.last_evidence_at = datetime()
            RETURN type(r) AS rel_type
        """
        result = session.run(query, params)
        return result.single() is not None

    # ------------------------------------------------------------------
    # PDF text extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_pdf_text(pdf_path: str) -> str:
        """Extract text from a PDF file. Falls back gracefully."""
        try:
            import PyPDF2
            text_pages = []
            with open(pdf_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    text_pages.append(page.extract_text() or "")
            return "\n\n".join(text_pages)
        except ImportError:
            try:
                import subprocess
                result = subprocess.run(
                    ["pdftotext", pdf_path, "-"],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode == 0:
                    return result.stdout
            except Exception:
                pass
        return f"[Could not extract text from {pdf_path}]"


# ============================================================================
# CLI entry point
# ============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest P&IDs/SOPs into ontology")
    parser.add_argument("files", nargs="+", help="Files to ingest")
    parser.add_argument(
        "--source-kind", default="pid",
        choices=["pid", "sop", "diagram"],
        help="Source type (default: pid)",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    ingester = ArtifactIngester(verbose=args.verbose)

    files = [{"path": f, "source_kind": args.source_kind} for f in args.files]
    result = ingester.ingest_batch(files)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"\nIngestion complete:")
        print(f"  Files processed: {result['files_processed']}")
        print(f"  Files failed:    {result['files_failed']}")
        print(f"  Nodes updated:   {result['nodes_updated']}")
        print(f"  Concepts created:{result['concepts_created']}")
        print(f"  Rels created:    {result['relationships_created']}")
        if result["errors"]:
            print(f"\nErrors:")
            for err in result["errors"]:
                print(f"  - {err}")


if __name__ == "__main__":
    main()
