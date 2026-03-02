#!/usr/bin/env python3
"""
Long-running anomaly monitor worker.

Modes:
  - run: start continuous monitoring loop
  - status: get run status
  - list-events: list persisted anomaly events
  - get-event: fetch one anomaly event
  - ack-event: mark event as acknowledged
  - cleanup: delete old events by retention policy
  - replay-fixtures: run deterministic fixture validation
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional fallback for minimal environments
    def load_dotenv(*_args, **_kwargs):
        return False

from anomaly_rules import (
    compute_deviation_scores,
    dedup_key,
    is_quality_good,
    is_stale,
    parse_timestamp,
    safe_float,
)


load_dotenv()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(prefix: str, payload: Dict[str, Any]) -> None:
    """Emit machine-parseable messages for Electron main process."""
    print(f"[{prefix}] {json.dumps(payload, default=str)}", flush=True)


DEFAULT_SUBSYSTEM_PRIORITY = ["view", "equipment", "group", "global"]


def _preview_value(value: Any, max_len: int = 120) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    text = str(value)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _canonical_subsystem_type(kind: Any) -> str:
    value = str(kind or "").strip().lower()
    if value in {"view", "views"}:
        return "view"
    if value in {"equipment", "equip", "asset"}:
        return "equipment"
    if value in {"group", "groups", "folder", "path", "prefix", "tag_group"}:
        return "group"
    if value in {"global", "all", "system"}:
        return "global"
    return "group"


def _subsystem_ref(kind: Any, name: Any) -> Dict[str, str]:
    subsystem_type = _canonical_subsystem_type(kind)
    subsystem_name = str(name or "").strip()
    if not subsystem_name:
        subsystem_type = "global"
        subsystem_name = "all"
    return {
        "type": subsystem_type,
        "name": subsystem_name,
        "id": f"{subsystem_type}:{subsystem_name.lower()}",
    }


def infer_tag_group(tag_path: Optional[str], folder_name: Optional[str] = None) -> Optional[str]:
    folder = str(folder_name or "").strip().strip("/")
    if folder:
        head = folder.split("/", 1)[0].strip()
        if head:
            return head

    raw = str(tag_path or "").strip()
    if not raw:
        return None
    if raw.startswith("[") and "]" in raw:
        raw = raw.split("]", 1)[1]
    raw = raw.strip("/")
    if not raw:
        return None
    parts = [p.strip() for p in raw.split("/") if p.strip()]
    # Ignore flat tags and only infer a group when there is at least one folder segment.
    if len(parts) < 2:
        return None
    return parts[0]


def _last_segment_from_tag_path(tag_path: Optional[str]) -> str:
    raw = str(tag_path or "").strip()
    if not raw:
        return ""
    if raw.startswith("[") and "]" in raw:
        raw = raw.split("]", 1)[1]
    raw = raw.strip("/")
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split("/") if p.strip()]
    return parts[-1] if parts else raw


def _looks_like_live_tag_path(value: Optional[str]) -> bool:
    path = str(value or "").strip()
    if not path:
        return False
    # Typical Ignition path shape: [provider]Folder/Tag or Folder/Tag
    if path.startswith("[") and "]" in path:
        return True
    if "/" in path and not any(ch in path for ch in "{}()"):
        return True
    return False


def derive_subsystems_for_tag(
    tag_meta: Dict[str, Any],
    subsystem_mode: str = "auto",
    priority: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    mode = str(subsystem_mode or "auto").strip().lower()
    if mode in {"global", "off", "disabled"}:
        global_ref = _subsystem_ref("global", "all")
        return [global_ref], global_ref

    refs: List[Dict[str, str]] = []
    seen: Set[str] = set()

    def add_ref(kind: str, name: Optional[str]) -> None:
        if not name:
            return
        ref = _subsystem_ref(kind, name)
        if ref["id"] in seen:
            return
        seen.add(ref["id"])
        refs.append(ref)

    for view_name in tag_meta.get("views") or []:
        add_ref("view", str(view_name))
    for equipment_name in tag_meta.get("equipment") or []:
        add_ref("equipment", str(equipment_name))
    add_ref("group", infer_tag_group(tag_meta.get("path"), tag_meta.get("folder_name")))

    if not refs:
        refs = [_subsystem_ref("global", "all")]

    ordered_priority = [
        _canonical_subsystem_type(x) for x in (priority or DEFAULT_SUBSYSTEM_PRIORITY)
    ]
    primary = refs[0]
    for kind in ordered_priority:
        found = next((s for s in refs if s.get("type") == kind), None)
        if found:
            primary = found
            break

    return refs, primary


def merge_defaults(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = dict(config or {})
    thresholds = raw.get("thresholds", {}) if isinstance(raw.get("thresholds"), dict) else {}
    defaults = {
        "pollIntervalMs": 15000,
        "historyWindowMinutes": 360,
        "minHistoryPoints": 30,
        "maxMonitoredTags": 200,
        "maxCandidatesPerCycle": 25,
        "maxCandidatesPerSubsystem": 8,
        "maxLlmTriagesPerCycle": 5,
        "maxLlmTriagesPerSubsystem": 2,
        "dedupCooldownMinutes": 10,
        "retentionDays": 14,
        "cleanupEveryCycles": 40,
        "runMode": "live",
        "scope": {
            "project": None,
            "equipmentTags": [],
            "tagRegex": None,
            "subsystemMode": "auto",
            "subsystemPriority": list(DEFAULT_SUBSYSTEM_PRIORITY),
            "subsystemInclude": [],
            "includeUnlinkedTags": False,
        },
        "thresholds": {
            "z": 3.0,
            "mad": 3.5,
            "rate": 0.0,
            "stalenessSec": 120,
            "flatline_std_epsilon": 1e-6,
            "stuck_window_size": 20,
        },
    }
    cfg = dict(defaults)
    cfg["scope"] = dict(defaults["scope"])
    cfg["thresholds"] = dict(defaults["thresholds"])
    cfg.update({k: v for k, v in raw.items() if k in defaults and k != "thresholds"})
    cfg["thresholds"].update({k: v for k, v in thresholds.items() if v is not None})
    if isinstance(raw.get("scope"), dict):
        cfg["scope"].update(raw["scope"])
    scope_cfg = cfg["scope"]
    mode = str(scope_cfg.get("subsystemMode") or "auto").strip().lower()
    if mode not in {"auto", "global", "off", "disabled"}:
        mode = "auto"
    scope_cfg["subsystemMode"] = mode
    if not isinstance(scope_cfg.get("subsystemPriority"), list) or not scope_cfg.get("subsystemPriority"):
        scope_cfg["subsystemPriority"] = list(DEFAULT_SUBSYSTEM_PRIORITY)
    scope_cfg["subsystemPriority"] = [
        str(x).strip()
        for x in scope_cfg.get("subsystemPriority", [])
        if str(x).strip()
    ] or list(DEFAULT_SUBSYSTEM_PRIORITY)
    if not isinstance(scope_cfg.get("subsystemInclude"), list):
        scope_cfg["subsystemInclude"] = []
    scope_cfg["subsystemInclude"] = [
        str(x).strip().lower()
        for x in scope_cfg.get("subsystemInclude", [])
        if str(x).strip()
    ]
    scope_cfg["includeUnlinkedTags"] = bool(scope_cfg.get("includeUnlinkedTags", False))
    return cfg


class AnomalyMonitor:
    def __init__(self, config: Dict[str, Any], run_id: Optional[str] = None):
        self.config = merge_defaults(config)
        self.run_id = run_id or f"agent-run-{uuid.uuid4()}"
        from ignition_api_client import IgnitionApiClient
        from neo4j_ontology import get_ontology_graph

        self.graph = get_ontology_graph()

        self.api = IgnitionApiClient(
            base_url=self.config.get("ignitionApiUrl") or os.getenv("IGNITION_API_URL"),
            api_token=self.config.get("ignitionApiToken") or os.getenv("IGNITION_API_TOKEN"),
            timeout=15.0,
        )

        self.llm = None
        self._llm_enabled = bool(os.getenv("ANTHROPIC_API_KEY"))
        if self._llm_enabled:
            try:
                from claude_client import ClaudeClient

                self.llm = ClaudeClient(
                    enable_tools=False,
                    ignition_api_url=self.config.get("ignitionApiUrl"),
                    ignition_api_token=self.config.get("ignitionApiToken"),
                )
            except Exception as exc:
                self._llm_enabled = False
                emit("AGENT_ERROR", {
                    "runId": self.run_id,
                    "code": "llm_init_failed",
                    "message": str(exc),
                    "recoverable": True,
                    "timestamp": utc_now_iso(),
                })

        self._running = True
        self._cycle_count = 0
        self._prev_values: Dict[str, float] = {}

    # -----------------------------
    # Schema / run lifecycle
    # -----------------------------
    def init_schema(self) -> None:
        self.graph.init_agent_monitoring_schema()

    def upsert_run(self, status: str, reason: Optional[str] = None) -> None:
        with self.graph.session() as session:
            session.run(
                """
                MERGE (r:AgentRun {run_id: $run_id})
                SET r.status = $status,
                    r.updated_at = datetime(),
                    r.last_heartbeat_at = datetime(),
                    r.config_json = $config_json,
                    r.cycle_count = $cycle_count,
                    r.started_at = coalesce(r.started_at, datetime()),
                    r.stopped_at = CASE WHEN $status IN ['stopped', 'failed'] THEN datetime() ELSE r.stopped_at END,
                    r.stop_reason = CASE WHEN $reason IS NULL THEN r.stop_reason ELSE $reason END
                """,
                run_id=self.run_id,
                status=status,
                config_json=json.dumps(self.config, default=str),
                cycle_count=self._cycle_count,
                reason=reason,
            )

    def heartbeat(self, metrics: Dict[str, Any]) -> None:
        with self.graph.session() as session:
            session.run(
                """
                MATCH (r:AgentRun {run_id: $run_id})
                SET r.last_heartbeat_at = datetime(),
                    r.cycle_count = $cycle_count,
                    r.last_cycle_ms = $cycle_ms,
                    r.last_candidates = $candidates,
                    r.last_triaged = $triaged,
                    r.last_emitted = $emitted
                """,
                run_id=self.run_id,
                cycle_count=self._cycle_count,
                cycle_ms=metrics.get("cycleMs", 0),
                candidates=metrics.get("candidates", 0),
                triaged=metrics.get("triaged", 0),
                emitted=metrics.get("emitted", 0),
            )

    # -----------------------------
    # Tag and context collection
    # -----------------------------
    def get_monitored_tags(self) -> List[Dict[str, Any]]:
        max_tags = int(self.config.get("maxMonitoredTags", 200))
        scope = self.config.get("scope", {})
        tag_regex = scope.get("tagRegex")
        equipment_tags = {
            str(x).strip().lower()
            for x in (scope.get("equipmentTags") or [])
            if str(x).strip()
        }
        subsystem_mode = str(scope.get("subsystemMode") or "auto").strip().lower()
        subsystem_priority = scope.get("subsystemPriority") or list(DEFAULT_SUBSYSTEM_PRIORITY)
        subsystem_include = {
            str(x).strip().lower()
            for x in (scope.get("subsystemInclude") or [])
            if str(x).strip()
        }
        include_unlinked = bool(scope.get("includeUnlinkedTags", False))
        tag_map: Dict[str, Dict[str, Any]] = {}

        def upsert_tag(
            *,
            tag_path: str,
            tag_name: str,
            folder_name: str = "",
            views: Optional[List[str]] = None,
            equipment: Optional[List[str]] = None,
            source: str = "unknown",
        ) -> None:
            path = str(tag_path or "").strip()
            if not path:
                return
            entry = tag_map.setdefault(
                path,
                {
                    "path": path,
                    "name": str(tag_name or _last_segment_from_tag_path(path) or path),
                    "folder_name": str(folder_name or ""),
                    "views": [],
                    "equipment": [],
                    "source": source,
                    "bound_to_view": False,
                },
            )
            if source == "view_binding":
                entry["bound_to_view"] = True
                entry["source"] = source
            if folder_name and not entry.get("folder_name"):
                entry["folder_name"] = str(folder_name)
            if tag_name and (
                not entry.get("name")
                or entry.get("name") == entry.get("path")
                or entry.get("name") == _last_segment_from_tag_path(entry.get("path"))
            ):
                entry["name"] = str(tag_name)
            for view_name in views or []:
                v = str(view_name or "").strip()
                if v and v not in entry["views"]:
                    entry["views"].append(v)
            for eq_name in equipment or []:
                eq = str(eq_name or "").strip()
                if eq and eq not in entry["equipment"]:
                    entry["equipment"].append(eq)

        with self.graph.session() as session:
            bound_result = session.run(
                """
                MATCH (v:View)-[:HAS_COMPONENT]->(c:ViewComponent)-[r:BINDS_TO]->(n)
                WHERE r.tag_path IS NOT NULL
                  AND trim(r.tag_path) <> ''
                  AND toLower(coalesce(r.binding_type, 'tag')) = 'tag'
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(n)
                RETURN DISTINCT trim(r.tag_path) AS tag_path,
                                coalesce(n.name, '') AS tag_name,
                                collect(DISTINCT v.name) AS views,
                                collect(DISTINCT eq.name) AS equipment
                LIMIT $limit
                """,
                limit=max_tags * 4,
            )
            for r in bound_result:
                path = str(r["tag_path"] or "").strip()
                if not _looks_like_live_tag_path(path):
                    continue
                upsert_tag(
                    tag_path=path,
                    tag_name=str(r["tag_name"] or _last_segment_from_tag_path(path)),
                    folder_name=infer_tag_group(path) or "",
                    views=[x for x in (r["views"] or []) if x],
                    equipment=[x for x in (r["equipment"] or []) if x],
                    source="view_binding",
                )

            scada_result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE t.opc_item_path IS NOT NULL
                  AND trim(t.opc_item_path) <> ''
                OPTIONAL MATCH (c:ViewComponent)-[:BINDS_TO]->(t)
                OPTIONAL MATCH (v:View)-[:HAS_COMPONENT]->(c)
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(t)
                RETURN DISTINCT trim(t.opc_item_path) AS tag_path,
                                coalesce(t.name, t.opc_item_path) AS tag_name,
                                coalesce(t.folder_name, '') AS folder_name,
                                collect(DISTINCT v.name) AS views,
                                collect(DISTINCT eq.name) AS equipment
                LIMIT $limit
                """,
                limit=max_tags * 6,
            )
            for r in scada_result:
                path = str(r["tag_path"] or "").strip()
                if not _looks_like_live_tag_path(path):
                    continue
                upsert_tag(
                    tag_path=path,
                    tag_name=str(r["tag_name"] or _last_segment_from_tag_path(path)),
                    folder_name=str(r["folder_name"] or ""),
                    views=[x for x in (r["views"] or []) if x],
                    equipment=[x for x in (r["equipment"] or []) if x],
                    source="scada_tag",
                )

        tags = list(tag_map.values())

        if not include_unlinked:
            linked = [t for t in tags if (t.get("views") or t.get("equipment") or t.get("bound_to_view"))]
            if linked:
                tags = linked

        if tag_regex:
            import re
            try:
                pattern = re.compile(tag_regex, re.IGNORECASE)
                tags = [t for t in tags if pattern.search(t["path"]) or pattern.search(t["name"])]
            except re.error:
                emit("AGENT_ERROR", {
                    "runId": self.run_id,
                    "code": "invalid_tag_regex",
                    "message": f"Invalid regex: {tag_regex}",
                    "recoverable": True,
                    "timestamp": utc_now_iso(),
                })

        if equipment_tags:
            tags = [
                t for t in tags
                if t["name"].lower() in equipment_tags
                or t["path"].lower() in equipment_tags
                or any(str(eq).strip().lower() in equipment_tags for eq in t.get("equipment", []))
            ]

        tags.sort(
            key=lambda t: (
                0 if t.get("bound_to_view") else 1,
                0 if (t.get("views") or t.get("equipment")) else 1,
                str(t.get("path", "")),
            )
        )

        for tag in tags:
            subsystems, primary = derive_subsystems_for_tag(
                tag_meta=tag,
                subsystem_mode=subsystem_mode,
                priority=subsystem_priority,
            )
            tag["subsystems"] = subsystems
            tag["primary_subsystem"] = primary

        if subsystem_include:
            tags = [
                t
                for t in tags
                if any(
                    s.get("id", "").lower() in subsystem_include
                    or s.get("name", "").lower() in subsystem_include
                    for s in (t.get("subsystems") or [])
                )
            ]

        return tags[:max_tags]

    def _extract_history_values(self, history_data: Any, tag_path: str) -> List[float]:
        """Normalize multiple gateway response shapes to numeric values list."""
        values: List[float] = []
        if history_data is None:
            return values
        if isinstance(history_data, dict) and history_data.get("error"):
            return values

        rows: List[Any] = []
        if isinstance(history_data, list):
            rows = history_data
        elif isinstance(history_data, dict):
            for key in ("rows", "data", "results", "values", "history"):
                chunk = history_data.get(key)
                if isinstance(chunk, list):
                    rows = chunk
                    break
            if not rows and "tagHistory" in history_data and isinstance(history_data["tagHistory"], list):
                rows = history_data["tagHistory"]

        for row in rows:
            if isinstance(row, (int, float, str)):
                val = safe_float(row)
                if val is not None:
                    values.append(val)
                continue
            if not isinstance(row, dict):
                continue
            candidate = None
            if "value" in row:
                candidate = row.get("value")
            elif tag_path in row:
                candidate = row.get(tag_path)
            else:
                # Wide format often has timestamp + one tag column.
                for k, v in row.items():
                    if k.lower() in {"timestamp", "ts", "t", "time"}:
                        continue
                    candidate = v
                    break
            val = safe_float(candidate)
            if val is not None:
                values.append(val)
        return values

    def fetch_history_values(self, tag_path: str) -> tuple[List[float], Optional[str]]:
        minutes = int(self.config.get("historyWindowMinutes", 360))
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(minutes=minutes)
        data = self.api.query_tag_history(
            [tag_path],
            start_dt.isoformat(),
            end_dt.isoformat(),
            return_size=max(100, int(self.config.get("minHistoryPoints", 30)) * 4),
            aggregation_mode="Average",
            return_format="Wide",
        )
        if isinstance(data, dict) and data.get("error"):
            return [], str(data.get("error"))
        return self._extract_history_values(data, tag_path), None

    def get_context(self, tag_path: str) -> Dict[str, Any]:
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE t.name = $tag OR t.opc_item_path = $tag
                OPTIONAL MATCH (vc:ViewComponent)-[:BINDS_TO]->(t)
                OPTIONAL MATCH (v:View)-[:HAS_COMPONENT]->(vc)
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(t)
                OPTIONAL MATCH (eq)-[:HAS_SYMPTOM]->(s:FaultSymptom)
                OPTIONAL MATCH (s)-[:CAUSED_BY]->(fc:FaultCause)
                OPTIONAL MATCH (eq)-[:HAS_PATTERN]->(p:ControlPattern)
                OPTIONAL MATCH (eq)-[:SAFETY_CRITICAL]->(se:SafetyElement)
                RETURN t,
                       collect(DISTINCT v.name) AS views,
                       collect(DISTINCT eq.name) AS equipment,
                       collect(DISTINCT s.symptom) AS symptoms,
                       collect(DISTINCT fc.cause) AS causes,
                       collect(DISTINCT p.pattern_name) AS patterns,
                       collect(DISTINCT se.name) AS safety
                LIMIT 1
                """,
                tag=tag_path,
            )
            record = result.single()
            fallback_views: List[str] = []
            fallback_equipment: List[str] = []
            fallback_result = session.run(
                """
                MATCH (v:View)-[:HAS_COMPONENT]->(vc:ViewComponent)-[r:BINDS_TO]->(n)
                WHERE r.tag_path = $tag
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(n)
                RETURN collect(DISTINCT v.name) AS views,
                       collect(DISTINCT eq.name) AS equipment
                LIMIT 1
                """,
                tag=tag_path,
            ).single()
            if fallback_result:
                fallback_views = [x for x in (fallback_result["views"] or []) if x]
                fallback_equipment = [x for x in (fallback_result["equipment"] or []) if x]

            if not record:
                return {
                    "tag_path": tag_path,
                    "tag_name": _last_segment_from_tag_path(tag_path) or tag_path,
                    "views": fallback_views,
                    "equipment": fallback_equipment,
                    "group": infer_tag_group(tag_path),
                    "symptoms": [],
                    "causes": [],
                    "patterns": [],
                    "safety": [],
                }
            node = record["t"]
            return {
                "tag_path": tag_path,
                "tag_name": node.get("name") if node else (_last_segment_from_tag_path(tag_path) or tag_path),
                "views": sorted(set([x for x in record["views"] if x] + fallback_views)),
                "equipment": sorted(set([x for x in record["equipment"] if x] + fallback_equipment)),
                "group": infer_tag_group(tag_path, node.get("folder_name") if node else None),
                "symptoms": [x for x in record["symptoms"] if x],
                "causes": [x for x in record["causes"] if x],
                "patterns": [x for x in record["patterns"] if x],
                "safety": [x for x in record["safety"] if x],
            }

    # -----------------------------
    # Triage and persistence
    # -----------------------------
    def run_llm_triage(
        self,
        context: Dict[str, Any],
        deterministic: Dict[str, Any],
        live_sample: Dict[str, Any],
    ) -> Dict[str, Any]:
        fallback = {
            "summary": f"Deterministic anomaly on {context.get('tag_name', context['tag_path'])}",
            "category": deterministic.get("category", "deviation"),
            "severity": "medium",
            "confidence": 0.55,
            "probable_causes": ["Signal deviates from historical baseline."],
            "verification_checks": [
                f"Check live quality/timestamp for {context.get('tag_path')}",
                "Inspect upstream interlocks and communication health.",
            ],
            "safety_notes": context.get("safety", []),
            "rationale": "LLM triage unavailable; using deterministic fallback.",
            "related_entities": [
                {"label": "Equipment", "name": e} for e in context.get("equipment", [])[:3]
            ] + [{"label": "View", "name": v} for v in context.get("views", [])[:2]],
        }
        if not self.llm:
            return fallback

        system_prompt = (
            "You are an industrial anomaly triage assistant. "
            "Return ONLY valid JSON with keys: summary, category, severity, confidence, "
            "probable_causes, verification_checks, safety_notes, rationale, related_entities. "
            "Severity must be one of critical/high/medium/low. "
            "Category must be one of spike/drift/stuck/state-conflict/quality-issue/deviation. "
            "related_entities is a list of objects: {label,name}."
        )
        user_prompt = json.dumps(
            {
                "context": context,
                "deterministic": deterministic,
                "live_sample": live_sample,
            },
            default=str,
        )
        try:
            result = self.llm.query_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=900,
                use_tools=False,
            )
            data = result.get("data")
            if not isinstance(data, dict):
                return fallback
            merged = dict(fallback)
            merged.update({k: v for k, v in data.items() if v is not None})
            return merged
        except Exception as exc:
            emit("AGENT_ERROR", {
                "runId": self.run_id,
                "code": "llm_triage_failed",
                "message": str(exc),
                "recoverable": True,
                "timestamp": utc_now_iso(),
            })
            return fallback

    def _severity_from_scores(self, deterministic: Dict[str, Any], llm_out: Dict[str, Any]) -> str:
        sev = str(llm_out.get("severity", "")).lower()
        if sev in {"critical", "high", "medium", "low"}:
            return sev
        z = abs(float(deterministic.get("z_score", 0.0)))
        if z >= 8:
            return "critical"
        if z >= 5:
            return "high"
        if z >= 3:
            return "medium"
        return "low"

    def is_duplicate_recent(self, dedup_sig: str) -> bool:
        cooldown = max(1, int(self.config.get("dedupCooldownMinutes", 10)))
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent {dedup_key: $dedup_key})
                WHERE e.created_at IS NOT NULL
                  AND datetime(e.created_at) > datetime() - duration({minutes: $minutes})
                RETURN count(e) AS cnt
                """,
                dedup_key=dedup_sig,
                minutes=cooldown,
            )
            row = result.single()
            return bool(row and row["cnt"] > 0)

    def persist_event(
        self,
        context: Dict[str, Any],
        deterministic: Dict[str, Any],
        live_sample: Dict[str, Any],
        triage: Dict[str, Any],
        subsystem: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        category = triage.get("category") or deterministic.get("category", "deviation")
        subsystem_ref = subsystem or _subsystem_ref("global", "all")
        dedup_source = f"{context['tag_path']}::{subsystem_ref.get('id', 'global:all')}"
        dedup_sig = dedup_key(dedup_source, category, int(self.config.get("dedupCooldownMinutes", 10)))
        if self.is_duplicate_recent(dedup_sig):
            return None

        event_id = f"ae-{uuid.uuid4()}"
        severity = self._severity_from_scores(deterministic, triage)
        confidence = float(max(0.0, min(1.0, triage.get("confidence", 0.5))))
        event_data = {
            "event_id": event_id,
            "run_id": self.run_id,
            "event_schema_version": 1,
            "state": "open",
            "severity": severity,
            "confidence": confidence,
            "category": category,
            "summary": triage.get("summary", f"Anomaly on {context['tag_path']}"),
            "explanation": triage.get("rationale", ""),
            "recommended_checks_json": json.dumps(triage.get("verification_checks", []), default=str),
            "probable_causes_json": json.dumps(triage.get("probable_causes", []), default=str),
            "safety_notes_json": json.dumps(triage.get("safety_notes", []), default=str),
            "deterministic_reasons_json": json.dumps(deterministic.get("reasons", []), default=str),
            "z_score": float(deterministic.get("z_score", 0.0)),
            "mad_score": float(deterministic.get("mad_score", 0.0)),
            "delta_rate": float(deterministic.get("delta_rate", 0.0)),
            "window_volatility": float(deterministic.get("window_volatility", 0.0)),
            "source_tag": context["tag_path"],
            "tag_name": context.get("tag_name") or context["tag_path"],
            "subsystem_type": subsystem_ref.get("type"),
            "subsystem_name": subsystem_ref.get("name"),
            "subsystem_id": subsystem_ref.get("id"),
            "live_quality": live_sample.get("quality"),
            "live_timestamp": live_sample.get("timestamp"),
            "live_value": str(live_sample.get("value")),
            "dedup_key": dedup_sig,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }

        with self.graph.session() as session:
            session.run(
                """
                MATCH (r:AgentRun {run_id: $run_id})
                CREATE (e:AnomalyEvent $props)
                MERGE (r)-[:EMITTED]->(e)
                """,
                run_id=self.run_id,
                props=event_data,
            )

            session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $event_id})
                MATCH (t:ScadaTag)
                WHERE t.name = $tag OR t.opc_item_path = $tag
                MERGE (e)-[:OBSERVED_ON]->(t)
                """,
                event_id=event_id,
                tag=context["tag_path"],
            )

            for equipment_name in context.get("equipment", [])[:5]:
                session.run(
                    """
                    MATCH (e:AnomalyEvent {event_id: $event_id})
                    MATCH (eq:Equipment {name: $name})
                    MERGE (e)-[:AFFECTS]->(eq)
                    """,
                    event_id=event_id,
                    name=equipment_name,
                )

            if subsystem_ref.get("type") == "view":
                session.run(
                    """
                    MATCH (e:AnomalyEvent {event_id: $event_id})
                    MATCH (v:View {name: $name})
                    MERGE (e)-[:SCOPED_TO]->(v)
                    """,
                    event_id=event_id,
                    name=subsystem_ref.get("name"),
                )
            elif subsystem_ref.get("type") == "equipment":
                session.run(
                    """
                    MATCH (e:AnomalyEvent {event_id: $event_id})
                    MATCH (eq:Equipment {name: $name})
                    MERGE (e)-[:SCOPED_TO]->(eq)
                    """,
                    event_id=event_id,
                    name=subsystem_ref.get("name"),
                )

            related_inputs: List[Dict[str, str]] = []
            for item in triage.get("related_entities", []) or []:
                if isinstance(item, dict) and item.get("label") and item.get("name"):
                    related_inputs.append({"label": str(item["label"]), "name": str(item["name"])})
            for name in context.get("symptoms", [])[:3]:
                related_inputs.append({"label": "FaultSymptom", "name": name})
            for name in context.get("causes", [])[:3]:
                related_inputs.append({"label": "FaultCause", "name": name})

            for rel in related_inputs[:8]:
                label = rel["label"]
                if label not in {"FaultSymptom", "FaultCause", "ControlPattern", "SafetyElement", "Equipment", "ScadaTag", "View"}:
                    continue
                session.run(
                    f"""
                    MATCH (e:AnomalyEvent {{event_id: $event_id}})
                    MATCH (n:{label})
                    WHERE n.name = $name OR n.symptom = $name OR n.cause = $name
                    MERGE (e)-[:RELATED_TO]->(n)
                    """,
                    event_id=event_id,
                    name=rel["name"],
                )

        return event_data

    def _emit_persisted_event(self, persisted: Dict[str, Any]) -> None:
        """Emit normalized AGENT_EVENT payload for UI stream."""
        emit("AGENT_EVENT", {
            "runId": self.run_id,
            "eventId": persisted["event_id"],
            "severity": persisted["severity"],
            "summary": persisted["summary"],
            "category": persisted.get("category"),
            "entityRefs": {
                "tag": persisted.get("tag_name") or persisted.get("source_tag"),
                "sourceTag": persisted.get("source_tag"),
                "subsystemType": persisted.get("subsystem_type"),
                "subsystemName": persisted.get("subsystem_name"),
            },
            "createdAt": persisted.get("created_at"),
        })

    def emit_provider_failure_event(
        self,
        code: str,
        message: str,
        *,
        severity: str = "high",
        category: str = "quality-issue",
        source_tag: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        subsystem: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Persist and stream provider-health anomalies so failures appear in feed.

        Returns:
            True if a new event was persisted (false if deduped).
        """
        emit("AGENT_ERROR", {
            "runId": self.run_id,
            "code": code,
            "message": message,
            "recoverable": True,
            "timestamp": utc_now_iso(),
        })

        tag = source_tag or f"provider://{code}"
        detail_blob = json.dumps(details or {}, default=str)
        context = {
            "tag_path": tag,
            "tag_name": source_tag or "ProviderHealth",
            "equipment": [],
            "symptoms": [],
            "causes": [],
            "patterns": [],
            "safety": [],
        }
        deterministic = {
            "candidate": True,
            "reasons": [code],
            "category": category,
            "z_score": 0.0,
            "mad_score": 0.0,
            "delta_rate": 0.0,
            "window_volatility": 0.0,
            "history_points": 0,
        }
        triage = {
            "summary": message,
            "category": category,
            "severity": severity,
            "confidence": 0.9,
            "probable_causes": [message],
            "verification_checks": [
                "Check Ignition gateway connectivity and credentials.",
                "Validate tag provider availability and endpoint health.",
            ],
            "safety_notes": [],
            "rationale": f"Provider health event ({code}). Details: {detail_blob}",
            "related_entities": [],
        }
        persisted = self.persist_event(
            context=context,
            deterministic=deterministic,
            live_sample={
                "path": tag,
                "value": "",
                "quality": "Bad",
                "timestamp": utc_now_iso(),
                "data_type": "provider_health",
            },
            triage=triage,
            subsystem=subsystem,
        )
        if persisted:
            self._emit_persisted_event(persisted)
            return True
        return False

    # -----------------------------
    # Monitoring loop
    # -----------------------------
    def run_cycle(self) -> Dict[str, Any]:
        cycle_start = time.time()
        metrics = {"candidates": 0, "triaged": 0, "emitted": 0, "cycleMs": 0, "diagnostics": {}}
        thresholds = self.config.get("thresholds", {})
        min_history = int(self.config.get("minHistoryPoints", 30))
        max_candidates_total = max(1, int(self.config.get("maxCandidatesPerCycle", 25)))
        max_candidates_per_subsystem = max(1, int(self.config.get("maxCandidatesPerSubsystem", 8)))
        max_triage_total = max(0, int(self.config.get("maxLlmTriagesPerCycle", 5)))
        max_triage_per_subsystem = max(0, int(self.config.get("maxLlmTriagesPerSubsystem", 2)))

        if not self.api.is_configured:
            emitted = self.emit_provider_failure_event(
                "ignition_not_configured",
                "Ignition API URL/token not configured.",
                severity="critical",
                category="state-conflict",
            )
            if emitted:
                metrics["emitted"] += 1
            metrics["cycleMs"] = int((time.time() - cycle_start) * 1000)
            return metrics

        tags = self.get_monitored_tags()
        if not tags:
            emit("AGENT_ERROR", {
                "runId": self.run_id,
                "code": "no_tags_found",
                "message": "No ScadaTag nodes with readable tag paths found.",
                "recoverable": True,
                "timestamp": utc_now_iso(),
            })
            metrics["cycleMs"] = int((time.time() - cycle_start) * 1000)
            return metrics

        tag_paths = [t["path"] for t in tags]
        tag_lookup = {t["path"]: t for t in tags}
        linked_tag_count = sum(
            1 for t in tags if (t.get("views") or t.get("equipment"))
        )
        unlinked_tag_count = max(0, len(tags) - linked_tag_count)
        detected_subsystems = sorted(
            {
                (t.get("primary_subsystem") or _subsystem_ref("global", "all")).get("id", "global:all")
                for t in tags
            }
        )
        live_values = self.api.read_tags(tag_paths)
        tool_calls: List[Dict[str, Any]] = []
        tool_calls.append({
            "tool": "read_tags",
            "request": {
                "count": len(tag_paths),
                "samplePaths": tag_paths[:8],
            },
            "result": {
                "count": len(live_values),
                "errorCount": sum(1 for tv in live_values if tv.error),
                "qualityGoodCount": sum(1 for tv in live_values if is_quality_good(tv.quality)),
                "timestampMissingCount": sum(1 for tv in live_values if not tv.timestamp),
                "timestampInferredCount": sum(
                    1
                    for tv in live_values
                    if isinstance(tv.config, dict) and bool(tv.config.get("timestamp_inferred"))
                ),
                "sample": [
                    {
                        "path": tv.path,
                        "value": _preview_value(tv.value),
                        "quality": tv.quality,
                        "timestamp": tv.timestamp,
                        "timestampInferred": bool(tv.config.get("timestamp_inferred"))
                        if isinstance(tv.config, dict)
                        else False,
                        "configKeys": sorted(list(tv.config.keys()))[:8]
                        if isinstance(tv.config, dict)
                        else [],
                        "error": tv.error,
                    }
                    for tv in live_values[:5]
                ],
            },
        })
        candidates: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        live_error_count = 0
        live_error_samples: List[str] = []
        history_error_count = 0
        history_error_samples: List[str] = []
        valid_live_count = 0
        missing_timestamp_count = 0
        inferred_timestamp_count = 0
        quality_filtered_count = 0
        stale_filtered_count = 0
        insufficient_history_count = 0
        low_history_candidate_count = 0
        candidate_subsystem_counts: Dict[str, int] = {}
        live_error_linked = 0
        live_error_unlinked = 0
        history_error_linked = 0
        history_error_unlinked = 0
        quality_filtered_linked = 0
        quality_filtered_unlinked = 0
        stale_filtered_linked = 0
        stale_filtered_unlinked = 0
        evaluated_linked = 0
        evaluated_unlinked = 0
        candidate_linked = 0
        candidate_unlinked = 0
        near_shift_count = 0
        near_shift_linked = 0
        near_shift_unlinked = 0
        stale_samples: List[Dict[str, Any]] = []
        subsystem_shift_signals: Dict[str, Dict[str, Any]] = {}

        def _update_subsystem_signal(
            subsystem_ref: Dict[str, str], deterministic: Dict[str, Any], tag_path: str
        ) -> None:
            sub_id = subsystem_ref.get("id", "global:all")
            abs_z = abs(float(deterministic.get("z_score", 0.0)))
            z = float(deterministic.get("z_score", 0.0))
            bucket = subsystem_shift_signals.setdefault(
                sub_id,
                {
                    "subsystemId": sub_id,
                    "subsystemType": subsystem_ref.get("type", "global"),
                    "subsystemName": subsystem_ref.get("name", "all"),
                    "evaluated": 0,
                    "candidate": 0,
                    "nearShift": 0,
                    "sumAbsZ": 0.0,
                    "sumZ": 0.0,
                    "maxAbsZ": 0.0,
                    "sampleTag": tag_path,
                },
            )
            bucket["evaluated"] += 1
            bucket["sumAbsZ"] += abs_z
            bucket["sumZ"] += z
            if abs_z >= 1.5:
                bucket["nearShift"] += 1
            if abs_z > bucket["maxAbsZ"]:
                bucket["maxAbsZ"] = abs_z
                bucket["sampleTag"] = tag_path

        for tv in live_values:
            tag_meta = tag_lookup.get(tv.path, {"path": tv.path, "name": tv.path})
            subsystem = tag_meta.get("primary_subsystem") or _subsystem_ref("global", "all")
            is_linked = bool(tag_meta.get("views") or tag_meta.get("equipment"))

            if tv.error:
                live_error_count += 1
                if is_linked:
                    live_error_linked += 1
                else:
                    live_error_unlinked += 1
                if len(live_error_samples) < 5:
                    live_error_samples.append(f"{tv.path}: {tv.error}")
                continue
            valid_live_count += 1
            if not tv.timestamp:
                missing_timestamp_count += 1
            if isinstance(tv.config, dict) and bool(tv.config.get("timestamp_inferred")):
                inferred_timestamp_count += 1
            if not is_quality_good(tv.quality):
                # quality gate: only emit quality anomalies if this persists via triage.
                quality_filtered_count += 1
                if is_linked:
                    quality_filtered_linked += 1
                else:
                    quality_filtered_unlinked += 1
                continue
            parsed_ts = parse_timestamp(tv.timestamp)
            age_sec = (now - parsed_ts).total_seconds() if parsed_ts is not None else None
            stale_threshold_sec = int(thresholds.get("stalenessSec", 120))
            if is_stale(tv.timestamp, stale_threshold_sec, now=now):
                stale_filtered_count += 1
                if is_linked:
                    stale_filtered_linked += 1
                else:
                    stale_filtered_unlinked += 1
                if len(stale_samples) < 8:
                    stale_samples.append({
                        "path": tv.path,
                        "timestampRaw": tv.timestamp,
                        "timestampParsedUtc": parsed_ts.isoformat() if parsed_ts else None,
                        "ageSec": round(age_sec, 3) if age_sec is not None else None,
                        "thresholdSec": stale_threshold_sec,
                        "reason": "timestamp_parse_failed" if parsed_ts is None else "age_exceeds_threshold",
                    })
                continue

            history, history_error = self.fetch_history_values(tv.path)
            if len(tool_calls) < 18:
                tool_calls.append({
                    "tool": "query_tag_history",
                    "request": {
                        "tagPath": tv.path,
                        "historyWindowMinutes": int(self.config.get("historyWindowMinutes", 360)),
                    },
                    "result": {
                        "historyPoints": len(history),
                        "error": history_error,
                    },
                })
            if history_error:
                history_error_count += 1
                if is_linked:
                    history_error_linked += 1
                else:
                    history_error_unlinked += 1
                if len(history_error_samples) < 5:
                    history_error_samples.append(f"{tv.path}: {history_error}")
                continue
            if len(history) < min_history:
                insufficient_history_count += 1
                # Low-history fallback: still score dramatic shifts when at least a
                # small baseline exists, otherwise simulator users see no events.
                if len(history) >= 5:
                    prev_val = self._prev_values.get(tv.path)
                    deterministic = compute_deviation_scores(
                        current_value=tv.value,
                        history_values=history,
                        prev_value=prev_val,
                        thresholds=thresholds,
                    )
                    curr_num = safe_float(tv.value)
                    if curr_num is not None:
                        self._prev_values[tv.path] = curr_num

                    _update_subsystem_signal(subsystem, deterministic, tv.path)
                    if is_linked:
                        evaluated_linked += 1
                    else:
                        evaluated_unlinked += 1
                    if abs(float(deterministic.get("z_score", 0.0))) >= 1.5:
                        near_shift_count += 1
                        if is_linked:
                            near_shift_linked += 1
                        else:
                            near_shift_unlinked += 1

                    if deterministic.get("candidate"):
                        sub_bucket = subsystem_shift_signals.setdefault(
                            subsystem.get("id", "global:all"),
                            {
                                "subsystemId": subsystem.get("id", "global:all"),
                                "subsystemType": subsystem.get("type", "global"),
                                "subsystemName": subsystem.get("name", "all"),
                                "evaluated": 0,
                                "candidate": 0,
                                "nearShift": 0,
                                "sumAbsZ": 0.0,
                                "sumZ": 0.0,
                                "maxAbsZ": 0.0,
                                "sampleTag": tv.path,
                            },
                        )
                        sub_bucket["candidate"] += 1
                        if is_linked:
                            candidate_linked += 1
                        else:
                            candidate_unlinked += 1
                        deterministic["reasons"] = list(deterministic.get("reasons", [])) + ["low_history_override"]
                        deterministic["history_quality"] = "low"
                        context = self.get_context(tv.path)
                        context["subsystem"] = subsystem
                        context["subsystems"] = tag_meta.get("subsystems") or [subsystem]
                        candidates.append(
                            {
                                "context": context,
                                "deterministic": deterministic,
                                "live_sample": {
                                    "path": tv.path,
                                    "value": tv.value,
                                    "quality": tv.quality,
                                    "timestamp": tv.timestamp,
                                    "data_type": tv.data_type,
                                },
                                "subsystem": subsystem,
                            }
                        )
                        sub_id = subsystem.get("id", "global:all")
                        candidate_subsystem_counts[sub_id] = candidate_subsystem_counts.get(sub_id, 0) + 1
                        low_history_candidate_count += 1
                continue

            prev_val = self._prev_values.get(tv.path)
            deterministic = compute_deviation_scores(
                current_value=tv.value,
                history_values=history,
                prev_value=prev_val,
                thresholds=thresholds,
            )
            curr_num = safe_float(tv.value)
            if curr_num is not None:
                self._prev_values[tv.path] = curr_num

            _update_subsystem_signal(subsystem, deterministic, tv.path)
            if is_linked:
                evaluated_linked += 1
            else:
                evaluated_unlinked += 1
            if abs(float(deterministic.get("z_score", 0.0))) >= 1.5:
                near_shift_count += 1
                if is_linked:
                    near_shift_linked += 1
                else:
                    near_shift_unlinked += 1

            if deterministic.get("candidate"):
                sub_bucket = subsystem_shift_signals.setdefault(
                    subsystem.get("id", "global:all"),
                    {
                        "subsystemId": subsystem.get("id", "global:all"),
                        "subsystemType": subsystem.get("type", "global"),
                        "subsystemName": subsystem.get("name", "all"),
                        "evaluated": 0,
                        "candidate": 0,
                        "nearShift": 0,
                        "sumAbsZ": 0.0,
                        "sumZ": 0.0,
                        "maxAbsZ": 0.0,
                        "sampleTag": tv.path,
                    },
                )
                sub_bucket["candidate"] += 1
                if is_linked:
                    candidate_linked += 1
                else:
                    candidate_unlinked += 1
                context = self.get_context(tv.path)
                context["subsystem"] = subsystem
                context["subsystems"] = tag_meta.get("subsystems") or [subsystem]
                candidates.append(
                    {
                        "context": context,
                        "deterministic": deterministic,
                        "live_sample": {
                            "path": tv.path,
                            "value": tv.value,
                            "quality": tv.quality,
                            "timestamp": tv.timestamp,
                            "data_type": tv.data_type,
                        },
                        "subsystem": subsystem,
                    }
                )
                sub_id = subsystem.get("id", "global:all")
                candidate_subsystem_counts[sub_id] = candidate_subsystem_counts.get(sub_id, 0) + 1

        if live_values and live_error_count == len(live_values):
            emitted = self.emit_provider_failure_event(
                "live_tag_provider_failed",
                f"Live tag provider failed for all reads ({live_error_count}/{len(live_values)}).",
                severity="high",
                category="quality-issue",
                details={"samples": live_error_samples},
            )
            if emitted:
                metrics["emitted"] += 1
        elif live_error_count > 0:
            emitted = self.emit_provider_failure_event(
                "live_tag_provider_partial_failure",
                f"Live tag provider partially failed ({live_error_count}/{len(live_values)} reads).",
                severity="medium",
                category="quality-issue",
                details={"samples": live_error_samples},
            )
            if emitted:
                metrics["emitted"] += 1

        if valid_live_count > 0 and history_error_count >= max(1, int(valid_live_count * 0.8)):
            emitted = self.emit_provider_failure_event(
                "history_provider_failed",
                f"History provider failed for most queries ({history_error_count}/{valid_live_count}).",
                severity="high",
                category="quality-issue",
                details={"samples": history_error_samples},
            )
            if emitted:
                metrics["emitted"] += 1
        elif history_error_count > 0:
            emitted = self.emit_provider_failure_event(
                "history_provider_partial_failure",
                f"History provider partially failed ({history_error_count}/{valid_live_count}).",
                severity="medium",
                category="quality-issue",
                details={"samples": history_error_samples},
            )
            if emitted:
                metrics["emitted"] += 1

        if valid_live_count > 0 and stale_filtered_count >= max(1, int(valid_live_count * 0.8)):
            emitted = self.emit_provider_failure_event(
                "live_timestamp_stale",
                f"Most live samples were stale ({stale_filtered_count}/{valid_live_count}).",
                severity="medium",
                category="quality-issue",
                details={"staleCount": stale_filtered_count, "validLiveCount": valid_live_count},
            )
            if emitted:
                metrics["emitted"] += 1

        if valid_live_count > 0 and quality_filtered_count >= max(1, int(valid_live_count * 0.8)):
            emitted = self.emit_provider_failure_event(
                "live_quality_bad",
                f"Most live samples had non-good quality ({quality_filtered_count}/{valid_live_count}).",
                severity="medium",
                category="quality-issue",
                details={"qualityFilteredCount": quality_filtered_count, "validLiveCount": valid_live_count},
            )
            if emitted:
                metrics["emitted"] += 1

        metrics["candidates"] = len(candidates)
        shortlisted: List[Dict[str, Any]] = []
        selected_per_subsystem: Dict[str, int] = {}
        for candidate in candidates:
            subsystem = candidate.get("subsystem") or _subsystem_ref("global", "all")
            sub_id = subsystem.get("id", "global:all")
            if selected_per_subsystem.get(sub_id, 0) >= max_candidates_per_subsystem:
                continue
            shortlisted.append(candidate)
            selected_per_subsystem[sub_id] = selected_per_subsystem.get(sub_id, 0) + 1
            if len(shortlisted) >= max_candidates_total:
                break

        llm_total = 0
        llm_per_subsystem: Dict[str, int] = {}
        dedup_suppressed_count = 0

        for candidate in shortlisted:
            subsystem = candidate.get("subsystem") or _subsystem_ref("global", "all")
            sub_id = subsystem.get("id", "global:all")
            use_llm = (
                llm_total < max_triage_total
                and llm_per_subsystem.get(sub_id, 0) < max_triage_per_subsystem
            )
            triage = (
                self.run_llm_triage(
                    candidate["context"],
                    candidate["deterministic"],
                    candidate["live_sample"],
                )
                if use_llm
                else {
                    "summary": (
                        f"Deviation on {candidate['context'].get('tag_name', candidate['context']['tag_path'])} "
                        f"in subsystem {subsystem.get('name', 'all')}"
                    ),
                    "category": candidate["deterministic"].get("category", "deviation"),
                    "severity": "medium",
                    "confidence": 0.5,
                    "verification_checks": [],
                    "probable_causes": [],
                    "safety_notes": [],
                    "rationale": "Triaged in deterministic-only mode due per-cycle/per-subsystem LLM caps.",
                    "related_entities": [],
                }
            )
            if use_llm:
                llm_total += 1
                llm_per_subsystem[sub_id] = llm_per_subsystem.get(sub_id, 0) + 1
            metrics["triaged"] += 1
            persisted = self.persist_event(
                candidate["context"],
                candidate["deterministic"],
                candidate["live_sample"],
                triage,
                subsystem=subsystem,
            )
            if persisted:
                metrics["emitted"] += 1
                self._emit_persisted_event(persisted)
            else:
                dedup_suppressed_count += 1

        top_candidates_by_subsystem = dict(
            sorted(candidate_subsystem_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        )
        top_shift_signals = sorted(
            subsystem_shift_signals.values(),
            key=lambda item: (
                int(item.get("candidate", 0)),
                float(item.get("maxAbsZ", 0.0)),
                int(item.get("nearShift", 0)),
                int(item.get("evaluated", 0)),
            ),
            reverse=True,
        )[:8]
        for item in top_shift_signals:
            evaluated = max(1, int(item.get("evaluated", 0)))
            item["avgAbsZ"] = round(float(item.get("sumAbsZ", 0.0)) / evaluated, 3)
            item["avgZ"] = round(float(item.get("sumZ", 0.0)) / evaluated, 3)
            item["shiftRatio"] = round(float(item.get("nearShift", 0)) / evaluated, 3)
            item["candidateRatio"] = round(float(item.get("candidate", 0)) / evaluated, 3)
            item.pop("sumAbsZ", None)
            item.pop("sumZ", None)

        metrics["diagnostics"] = {
            "monitoredTags": len(tag_paths),
            "linkedTags": linked_tag_count,
            "unlinkedTags": unlinked_tag_count,
            "validLiveCount": valid_live_count,
            "missingTimestampCount": missing_timestamp_count,
            "inferredTimestampCount": inferred_timestamp_count,
            "liveErrorCount": live_error_count,
            "liveErrorLinked": live_error_linked,
            "liveErrorUnlinked": live_error_unlinked,
            "qualityFilteredCount": quality_filtered_count,
            "qualityFilteredLinked": quality_filtered_linked,
            "qualityFilteredUnlinked": quality_filtered_unlinked,
            "staleFilteredCount": stale_filtered_count,
            "staleFilteredLinked": stale_filtered_linked,
            "staleFilteredUnlinked": stale_filtered_unlinked,
            "historyErrorCount": history_error_count,
            "historyErrorLinked": history_error_linked,
            "historyErrorUnlinked": history_error_unlinked,
            "insufficientHistoryCount": insufficient_history_count,
            "lowHistoryCandidateCount": low_history_candidate_count,
            "evaluatedLinked": evaluated_linked,
            "evaluatedUnlinked": evaluated_unlinked,
            "candidateLinked": candidate_linked,
            "candidateUnlinked": candidate_unlinked,
            "nearShiftCount": near_shift_count,
            "nearShiftLinked": near_shift_linked,
            "nearShiftUnlinked": near_shift_unlinked,
            "stalenessThresholdSec": int(thresholds.get("stalenessSec", 120)),
            "staleSamples": stale_samples,
            "timestampParseNote": "Naive timestamps are treated as local time by parse_timestamp().",
            "detectedSubsystemCount": len(detected_subsystems),
            "detectedSubsystems": detected_subsystems[:10],
            "candidateSubsystemCount": len(candidate_subsystem_counts),
            "candidateBySubsystem": top_candidates_by_subsystem,
            "subsystemShiftSignals": top_shift_signals,
            "maxCandidatesPerSubsystem": max_candidates_per_subsystem,
            "maxLlmTriagesPerSubsystem": max_triage_per_subsystem,
            "llmTriagedCount": llm_total,
            "dedupSuppressedCount": dedup_suppressed_count,
            "toolCalls": tool_calls,
        }
        metrics["cycleMs"] = int((time.time() - cycle_start) * 1000)
        return metrics

    def cleanup_retention(self) -> int:
        retention_days = int(self.config.get("retentionDays", 14))
        return self.graph.cleanup_anomaly_events(retention_days=retention_days)

    def run_forever(self) -> int:
        self.init_schema()
        self.upsert_run("running")
        emit("AGENT_STATUS", {
            "runId": self.run_id,
            "state": "running",
            "cycleMs": 0,
            "candidates": 0,
            "triaged": 0,
            "emitted": 0,
            "timestamp": utc_now_iso(),
        })

        poll_ms = int(self.config.get("pollIntervalMs", 15000))
        cleanup_every = max(1, int(self.config.get("cleanupEveryCycles", 40)))
        exit_code = 0
        reason = "stopped"

        while self._running:
            self._cycle_count += 1
            cycle_started = time.time()
            try:
                metrics = self.run_cycle()
                self.heartbeat(metrics)
                emit("AGENT_STATUS", {
                    "runId": self.run_id,
                    "state": "running",
                    "cycleMs": metrics["cycleMs"],
                    "candidates": metrics["candidates"],
                    "triaged": metrics["triaged"],
                    "emitted": metrics["emitted"],
                    "diagnostics": metrics.get("diagnostics", {}),
                    "timestamp": utc_now_iso(),
                })
                if self._cycle_count % cleanup_every == 0:
                    deleted = self.cleanup_retention()
                    if deleted > 0:
                        emit("AGENT_STATUS", {
                            "runId": self.run_id,
                            "state": "retention_cleanup",
                            "cycleMs": 0,
                            "candidates": 0,
                            "triaged": 0,
                            "emitted": deleted,
                            "timestamp": utc_now_iso(),
                        })
            except Exception as exc:
                reason = "failed"
                exit_code = 1
                emit("AGENT_ERROR", {
                    "runId": self.run_id,
                    "code": "cycle_error",
                    "message": str(exc),
                    "recoverable": True,
                    "timestamp": utc_now_iso(),
                })

            elapsed_ms = int((time.time() - cycle_started) * 1000)
            remaining = max(0, poll_ms - elapsed_ms) / 1000.0
            if remaining > 0:
                time.sleep(remaining)

        self.upsert_run("stopped" if reason != "failed" else "failed", reason=reason)
        emit("AGENT_COMPLETE", {
            "runId": self.run_id,
            "success": exit_code == 0,
            "reason": reason,
            "stoppedAt": utc_now_iso(),
        })
        return exit_code

    # -----------------------------
    # Single-operation helpers
    # -----------------------------
    def list_events(self, limit: int, state: Optional[str], severity: Optional[str], run_id: Optional[str]) -> Dict[str, Any]:
        events = self.graph.list_anomaly_events(limit=limit, state=state, severity=severity, run_id=run_id)
        return {"success": True, "events": events}

    def get_event(self, event_id: str) -> Dict[str, Any]:
        event = self.graph.get_anomaly_event(event_id)
        if not event:
            return {"success": False, "error": f"Event not found: {event_id}"}
        return {"success": True, "event": event}

    def ack_event(self, event_id: str, note: Optional[str]) -> Dict[str, Any]:
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $event_id})
                SET e.state = 'acknowledged',
                    e.acknowledged_at = datetime(),
                    e.ack_note = $note,
                    e.updated_at = datetime()
                RETURN count(e) AS cnt
                """,
                event_id=event_id,
                note=note or "",
            )
            record = result.single()
            if not record or record["cnt"] == 0:
                return {"success": False, "error": f"Event not found: {event_id}"}
        return {"success": True, "eventId": event_id}

    def clear_event(self, event_id: str, note: Optional[str]) -> Dict[str, Any]:
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $event_id})
                SET e.state = 'cleared',
                    e.cleared_at = datetime(),
                    e.clear_note = $note,
                    e.updated_at = datetime()
                RETURN count(e) AS cnt
                """,
                event_id=event_id,
                note=note or "",
            )
            record = result.single()
            if not record or record["cnt"] == 0:
                return {"success": False, "error": f"Event not found: {event_id}"}
        return {"success": True, "eventId": event_id}

    def get_status(self, run_id: str) -> Dict[str, Any]:
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (r:AgentRun {run_id: $run_id})
                RETURN r
                LIMIT 1
                """,
                run_id=run_id,
            )
            row = result.single()
            if not row:
                return {"success": False, "error": f"Run not found: {run_id}"}
            props = dict(row["r"])
            return {
                "success": True,
                "status": props.get("status"),
                "metrics": {
                    "cycleCount": props.get("cycle_count", 0),
                    "lastCycleMs": props.get("last_cycle_ms", 0),
                    "lastCandidates": props.get("last_candidates", 0),
                    "lastTriaged": props.get("last_triaged", 0),
                    "lastEmitted": props.get("last_emitted", 0),
                },
                "lastHeartbeatAt": props.get("last_heartbeat_at"),
                "run": props,
            }


def _load_fixture_cases(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data.get("cases", [])
    if isinstance(data, list):
        return data
    return []


def replay_fixtures(config_json: Optional[str], fixture_path: str) -> Dict[str, Any]:
    config = merge_defaults(json.loads(config_json) if config_json else {})
    path = Path(fixture_path)
    cases = _load_fixture_cases(path)
    thresholds = config.get("thresholds", {})
    passed = 0
    failures: List[Dict[str, Any]] = []

    for case in cases:
        result = compute_deviation_scores(
            current_value=case.get("current_value"),
            history_values=case.get("history_values", []),
            prev_value=case.get("prev_value"),
            thresholds=thresholds,
        )
        expected = bool(case.get("expected_candidate", False))
        if result.get("candidate") == expected:
            passed += 1
        else:
            failures.append(
                {
                    "id": case.get("id"),
                    "expected_candidate": expected,
                    "actual_candidate": result.get("candidate"),
                    "category": result.get("category"),
                    "reasons": result.get("reasons", []),
                }
            )

    return {
        "success": len(failures) == 0,
        "total": len(cases),
        "passed": passed,
        "failed": len(failures),
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Anomaly monitor worker")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Run continuous anomaly monitoring")
    p_run.add_argument("--run-id", help="Optional run id")
    p_run.add_argument("--config-json", default="{}", help="JSON config string")

    p_status = sub.add_parser("status", help="Get status for one run")
    p_status.add_argument("--run-id", required=True)

    p_list = sub.add_parser("list-events", help="List anomaly events")
    p_list.add_argument("--limit", type=int, default=100)
    p_list.add_argument("--state")
    p_list.add_argument("--severity")
    p_list.add_argument("--run-id")

    p_get = sub.add_parser("get-event", help="Get one anomaly event")
    p_get.add_argument("--event-id", required=True)

    p_ack = sub.add_parser("ack-event", help="Acknowledge one anomaly event")
    p_ack.add_argument("--event-id", required=True)
    p_ack.add_argument("--note")

    p_clear = sub.add_parser("clear-event", help="Clear one acknowledged anomaly event")
    p_clear.add_argument("--event-id", required=True)
    p_clear.add_argument("--note")

    p_cleanup = sub.add_parser("cleanup", help="Delete old anomaly events")
    p_cleanup.add_argument("--retention-days", type=int, default=14)

    p_replay = sub.add_parser("replay-fixtures", help="Validate deterministic scoring against fixtures")
    p_replay.add_argument("--fixture-file", required=True)
    p_replay.add_argument("--config-json", default="{}")

    args = parser.parse_args()

    if args.command == "replay-fixtures":
        result = replay_fixtures(args.config_json, args.fixture_file)
        print(json.dumps(result))
        return 0 if result["success"] else 1

    try:
        monitor = AnomalyMonitor(
            config=json.loads(getattr(args, "config_json", "{}") or "{}"),
            run_id=getattr(args, "run_id", None),
        )
    except Exception as exc:
        print(json.dumps({"success": False, "error": str(exc)}))
        return 1

    if args.command == "run":
        def _signal_handler(_signum, _frame):
            monitor._running = False

        signal.signal(signal.SIGTERM, _signal_handler)
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, _signal_handler)
        return monitor.run_forever()

    if args.command == "status":
        print(json.dumps(monitor.get_status(args.run_id), default=str))
        return 0

    if args.command == "list-events":
        print(json.dumps(monitor.list_events(args.limit, args.state, args.severity, args.run_id), default=str))
        return 0

    if args.command == "get-event":
        print(json.dumps(monitor.get_event(args.event_id), default=str))
        return 0

    if args.command == "ack-event":
        print(json.dumps(monitor.ack_event(args.event_id, args.note), default=str))
        return 0

    if args.command == "clear-event":
        print(json.dumps(monitor.clear_event(args.event_id, args.note), default=str))
        return 0

    if args.command == "cleanup":
        deleted = monitor.graph.cleanup_anomaly_events(args.retention_days)
        print(json.dumps({"success": True, "deleted": deleted}))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())

