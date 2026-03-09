#!/usr/bin/env python3
"""
Per-subsystem anomaly monitoring with coordinator + worker threads.

Architecture:
  AgentCoordinator (main thread)
    - Discovers subsystems from Neo4j ontology
    - Spawns/manages SubsystemAgent threads
    - Reads stdin for commands (start/stop individual agents)
    - Shared: Neo4j driver, IgnitionApiClient, thread-safe emit()

  SubsystemAgent (one thread per subsystem)
    - Own cycle loop, history cache, prev_values, ClaudeClient
    - Monitors only its assigned tags
    - Emits per-subsystem status/events via thread-safe emit()

CLI modes:
  run           Start coordinator with per-subsystem agents
  list-events   List persisted anomaly events
  get-event     Fetch one anomaly event
  ack-event     Acknowledge an event
  clear-event   Clear an acknowledged event
  deep-analyze  Run LLM triage on an existing event
  cleanup       Delete old events
  status        Get run status
  replay-fixtures  Validate scoring against fixtures
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

try:
    from dotenv import load_dotenv
except ImportError:
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

_api_semaphore = threading.Semaphore(2)  # max 2 concurrent Ignition API calls

_emit_queue: queue.Queue = queue.Queue()


def _emit_writer() -> None:
    """Dedicated thread that drains the emit queue to stdout."""
    while True:
        line = _emit_queue.get()
        if line is None:
            break
        try:
            sys.stdout.write(line)
            sys.stdout.flush()
        except Exception:
            pass


_emit_thread = threading.Thread(target=_emit_writer, daemon=True, name="emit-writer")
_emit_thread.start()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(prefix: str, payload: Dict[str, Any]) -> None:
    _emit_queue.put(f"[{prefix}] {json.dumps(payload, default=str)}\n")


DEFAULT_SUBSYSTEM_PRIORITY = ["view", "equipment", "group", "global"]


# ---------------------------------------------------------------------------
#  Subsystem helpers
# ---------------------------------------------------------------------------

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
    if len(parts) < 2:
        return None
    return parts[0]


def _last_segment(tag_path: Optional[str]) -> str:
    raw = str(tag_path or "").strip()
    if not raw:
        return ""
    if raw.startswith("[") and "]" in raw:
        raw = raw.split("]", 1)[1]
    raw = raw.strip("/")
    parts = [p.strip() for p in raw.split("/") if p.strip()]
    return parts[-1] if parts else raw


def _looks_like_tag_path(value: Optional[str]) -> bool:
    path = str(value or "").strip()
    if not path:
        return False
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
        ref = _subsystem_ref("global", "all")
        return [ref], ref

    refs: List[Dict[str, str]] = []
    seen: Set[str] = set()

    def add(kind: str, name: Optional[str]) -> None:
        if not name:
            return
        ref = _subsystem_ref(kind, name)
        if ref["id"] not in seen:
            seen.add(ref["id"])
            refs.append(ref)

    for v in tag_meta.get("views") or []:
        add("view", str(v))
    for e in tag_meta.get("equipment") or []:
        add("equipment", str(e))
    add("group", infer_tag_group(tag_meta.get("path"), tag_meta.get("folder_name")))

    if not refs:
        refs = [_subsystem_ref("global", "all")]

    ordered = [_canonical_subsystem_type(x) for x in (priority or DEFAULT_SUBSYSTEM_PRIORITY)]
    primary = refs[0]
    for kind in ordered:
        found = next((s for s in refs if s.get("type") == kind), None)
        if found:
            primary = found
            break

    return refs, primary


def _preview_value(value: Any, max_len: int = 120) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    text = str(value)
    return text if len(text) <= max_len else text[:max_len - 3] + "..."


# ---------------------------------------------------------------------------
#  Config
# ---------------------------------------------------------------------------

def merge_defaults(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = dict(config or {})
    thresholds = raw.get("thresholds", {}) if isinstance(raw.get("thresholds"), dict) else {}
    defaults = {
        "pollIntervalMs": 5000,
        "historyWindowMinutes": 360,
        "minHistoryPoints": 30,
        "maxMonitoredTags": 200,
        "maxCandidatesPerCycle": 25,
        "maxCandidatesPerSubsystem": 8,
        "maxLlmTriagesPerCycle": 0,
        "maxLlmTriagesPerSubsystem": 0,
        "dedupCooldownMinutes": 10,
        "retentionDays": 14,
        "cleanupEveryCycles": 40,
        "historyCacheTtlSec": 60,
        "tagCacheTtlSec": 60,
        "rediscoveryIntervalSec": 60,
        "scope": {
            "subsystemMode": "auto",
            "subsystemPriority": list(DEFAULT_SUBSYSTEM_PRIORITY),
            "subsystemInclude": [],
            "includeUnlinkedTags": False,
            "tagRegex": None,
            "equipmentTags": [],
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
    scope = cfg["scope"]
    mode = str(scope.get("subsystemMode") or "auto").strip().lower()
    scope["subsystemMode"] = mode if mode in {"auto", "global", "off", "disabled"} else "auto"
    if not isinstance(scope.get("subsystemPriority"), list) or not scope["subsystemPriority"]:
        scope["subsystemPriority"] = list(DEFAULT_SUBSYSTEM_PRIORITY)
    scope["subsystemPriority"] = [
        str(x).strip() for x in scope["subsystemPriority"] if str(x).strip()
    ] or list(DEFAULT_SUBSYSTEM_PRIORITY)
    if not isinstance(scope.get("subsystemInclude"), list):
        scope["subsystemInclude"] = []
    scope["subsystemInclude"] = [str(x).strip().lower() for x in scope["subsystemInclude"] if str(x).strip()]
    scope["includeUnlinkedTags"] = bool(scope.get("includeUnlinkedTags", False))
    return cfg


# ═══════════════════════════════════════════════════════════════════════════
#  SubsystemAgent — one per subsystem, runs in its own thread
# ═══════════════════════════════════════════════════════════════════════════

class SubsystemAgent(threading.Thread):
    """Monitors a single subsystem's tags in its own thread."""

    def __init__(
        self,
        *,
        subsystem_id: str,
        subsystem_type: str,
        subsystem_name: str,
        tag_metas: List[Dict[str, Any]],
        graph: Any,
        api: Any,
        config: Dict[str, Any],
        run_id: str,
        stagger_delay: float = 0.0,
    ):
        super().__init__(daemon=True, name=f"agent-{subsystem_id}")
        self.subsystem_id = subsystem_id
        self.subsystem_type = subsystem_type
        self.subsystem_name = subsystem_name
        self.tag_metas = list(tag_metas)
        self.graph = graph
        self.api = api
        self.config = config
        self.run_id = run_id
        self._stagger_delay = stagger_delay

        self._running = True
        self._paused = False
        self._cycle_count = 0
        self._total_candidates = 0
        self._total_triaged = 0
        self._total_emitted = 0
        self._cycle_times: List[int] = []
        self._prev_values: Dict[str, float] = {}
        self._history_cache: Dict[str, Dict[str, Any]] = {}
        self._context_cache: Dict[str, Dict[str, Any]] = {}
        self._context_cache_ts: Dict[str, float] = {}

        self.llm = None
        if bool(os.getenv("ANTHROPIC_API_KEY")):
            try:
                from claude_client import ClaudeClient
                self.llm = ClaudeClient(
                    enable_tools=False,
                    ignition_api_url=config.get("ignitionApiUrl"),
                    ignition_api_token=config.get("ignitionApiToken"),
                )
            except Exception:
                pass

    @property
    def agent_state(self) -> str:
        if not self._running:
            return "stopped"
        if self._paused:
            return "paused"
        return "running"

    @property
    def avg_cycle_ms(self) -> int:
        if not self._cycle_times:
            return 0
        return int(sum(self._cycle_times) / len(self._cycle_times))

    def update_tags(self, tag_metas: List[Dict[str, Any]]) -> None:
        self.tag_metas = list(tag_metas)

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def stop(self) -> None:
        self._running = False

    # -------------------------------------------------------------------
    #  Thread entry point
    # -------------------------------------------------------------------
    def run(self) -> None:
        poll_ms = int(self.config.get("pollIntervalMs", 1000))
        self._emit_status("agent_started", "cycle_start")

        if self._stagger_delay > 0:
            self._emit_progress("staggering", f"{int(self._stagger_delay * 1000)}ms")
            time.sleep(self._stagger_delay)

        while self._running:
            if self._paused:
                time.sleep(0.5)
                continue

            self._cycle_count += 1
            t0 = time.time()
            try:
                metrics = self._run_cycle()
                cycle_ms = int((time.time() - t0) * 1000)
                self._cycle_times.append(cycle_ms)
                if len(self._cycle_times) > 20:
                    self._cycle_times = self._cycle_times[-20:]

                self._emit_status(
                    "cycle_complete",
                    "ok",
                    cycle_ms=cycle_ms,
                    diagnostics=metrics.get("diagnostics", {}),
                    candidates=metrics.get("candidates", 0),
                    triaged=metrics.get("triaged", 0),
                    emitted=metrics.get("emitted", 0),
                    live_events=metrics.get("liveEvents", []),
                )
            except Exception as exc:
                cycle_ms = int((time.time() - t0) * 1000)
                emit("AGENT_ERROR", {
                    "runId": self.run_id,
                    "subsystemId": self.subsystem_id,
                    "code": "cycle_error",
                    "message": str(exc),
                    "recoverable": True,
                    "timestamp": utc_now_iso(),
                })
                self._emit_status("cycle_error", str(exc), cycle_ms=cycle_ms)

            elapsed = time.time() - t0
            remaining = max(0, poll_ms / 1000.0 - elapsed)
            if remaining > 0 and self._running:
                self._emit_progress("waiting", f"{int(remaining * 1000)}ms")
                time.sleep(remaining)

        self._emit_status("agent_stopped", "stopped")

    # -------------------------------------------------------------------
    #  Status emission
    # -------------------------------------------------------------------
    def _emit_status(
        self,
        phase: str,
        reason: str,
        cycle_ms: int = 0,
        diagnostics: Optional[Dict[str, Any]] = None,
        candidates: int = 0,
        triaged: int = 0,
        emitted: int = 0,
        live_events: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "runId": self.run_id,
            "subsystemId": self.subsystem_id,
            "state": self.agent_state,
            "cycleMs": cycle_ms,
            "candidates": candidates,
            "triaged": triaged,
            "emitted": emitted,
            "diagnostics": {
                "phase": phase,
                "reason": reason,
                "subsystemId": self.subsystem_id,
                "subsystemType": self.subsystem_type,
                "subsystemName": self.subsystem_name,
                "cycleCount": self._cycle_count,
                "avgCycleMs": self.avg_cycle_ms,
                "totalCandidates": self._total_candidates,
                "totalTriaged": self._total_triaged,
                "totalEmitted": self._total_emitted,
                "tagCount": len(self.tag_metas),
                **(diagnostics or {}),
            },
            "timestamp": utc_now_iso(),
        }
        if live_events is not None:
            payload["liveEvents"] = live_events
        emit("AGENT_STATUS", payload)

    # -------------------------------------------------------------------
    #  History fetching (per-agent cache)
    # -------------------------------------------------------------------
    def _extract_history_values(self, history_data: Any, tag_path: str) -> List[float]:
        values: List[float] = []
        if history_data is None or (isinstance(history_data, dict) and history_data.get("error")):
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
            if not rows and isinstance(history_data.get("tagHistory"), list):
                rows = history_data["tagHistory"]

        prefixed = self.api._ensure_provider_prefix(tag_path) if hasattr(self.api, "_ensure_provider_prefix") else tag_path
        stripped = tag_path
        if stripped.startswith("[") and "]" in stripped:
            stripped = stripped[stripped.index("]") + 1:]
        path_variants = {tag_path, prefixed, stripped}

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
                candidate = row["value"]
            else:
                matched = next((k for k in path_variants if k in row), None)
                if matched:
                    candidate = row[matched]
                elif len(row) <= 2:
                    for k, v in row.items():
                        if k.lower() in {"timestamp", "ts", "t", "time"}:
                            continue
                        candidate = v
                        break
            val = safe_float(candidate)
            if val is not None:
                values.append(val)
        return values

    def _fetch_history_batch(self, tag_paths: List[str]) -> Dict[str, Tuple[List[float], Optional[str]]]:
        ttl = float(self.config.get("historyCacheTtlSec", 30))
        now = time.time()
        results: Dict[str, Tuple[List[float], Optional[str]]] = {}
        uncached: List[str] = []

        for path in tag_paths:
            cached = self._history_cache.get(path)
            if cached and ttl > 0 and (now - cached["fetched_at"]) < ttl:
                results[path] = (list(cached["values"]), cached.get("error"))
            else:
                uncached.append(path)

        if not uncached:
            return results

        minutes = int(self.config.get("historyWindowMinutes", 360))
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(minutes=minutes)
        return_size = max(100, int(self.config.get("minHistoryPoints", 30)) * 4)

        for i in range(0, len(uncached), 20):
            batch = uncached[i:i + 20]
            with _api_semaphore:
                data = self.api.query_tag_history(
                    batch, start_dt.isoformat(), end_dt.isoformat(),
                    return_size=return_size, aggregation_mode="Average", return_format="Wide",
                )
            ts = time.time()

            if isinstance(data, dict) and data.get("error"):
                err = str(data["error"])
                for p in batch:
                    results[p] = ([], err)
                    self._history_cache[p] = {"values": [], "error": err, "fetched_at": ts}
                continue

            for p in batch:
                vals = self._extract_history_values(data, p)
                results[p] = (vals, None)
                self._history_cache[p] = {"values": vals, "error": None, "fetched_at": ts}

        return results

    # -------------------------------------------------------------------
    #  Context & triage
    # -------------------------------------------------------------------
    def _get_context(self, tag_path: str) -> Dict[str, Any]:
        ttl = 120.0
        now = time.time()
        cached_ts = self._context_cache_ts.get(tag_path, 0)
        if tag_path in self._context_cache and (now - cached_ts) < ttl:
            return dict(self._context_cache[tag_path])

        ctx = self._fetch_context_from_graph(tag_path)
        self._context_cache[tag_path] = ctx
        self._context_cache_ts[tag_path] = now
        return dict(ctx)

    def _fetch_context_from_graph(self, tag_path: str) -> Dict[str, Any]:
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
                RETURN t,
                       collect(DISTINCT v.name) AS views,
                       collect(DISTINCT eq.name) AS equipment,
                       collect(DISTINCT s.symptom) AS symptoms,
                       collect(DISTINCT fc.cause) AS causes
                LIMIT 1
                """,
                tag=tag_path,
            )
            record = result.single()
            fallback = session.run(
                """
                MATCH (v:View)-[:HAS_COMPONENT]->(vc:ViewComponent)-[r:BINDS_TO]->(n)
                WHERE r.tag_path = $tag
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(n)
                RETURN collect(DISTINCT v.name) AS views, collect(DISTINCT eq.name) AS equipment
                LIMIT 1
                """,
                tag=tag_path,
            ).single()
            fb_views = [x for x in (fallback["views"] or []) if x] if fallback else []
            fb_equip = [x for x in (fallback["equipment"] or []) if x] if fallback else []

            if not record:
                return {
                    "tag_path": tag_path,
                    "tag_name": _last_segment(tag_path) or tag_path,
                    "views": fb_views, "equipment": fb_equip,
                    "group": infer_tag_group(tag_path),
                    "symptoms": [], "causes": [],
                }
            node = record["t"]
            return {
                "tag_path": tag_path,
                "tag_name": node.get("name") if node else (_last_segment(tag_path) or tag_path),
                "views": sorted(set([x for x in record["views"] if x] + fb_views)),
                "equipment": sorted(set([x for x in record["equipment"] if x] + fb_equip)),
                "group": infer_tag_group(tag_path, node.get("folder_name") if node else None),
                "symptoms": [x for x in record["symptoms"] if x],
                "causes": [x for x in record["causes"] if x],
            }

    def _run_llm_triage(self, context: Dict, deterministic: Dict, live_sample: Dict) -> Dict[str, Any]:
        fallback = {
            "summary": f"Deviation on {context.get('tag_name', context['tag_path'])} in {self.subsystem_name}",
            "category": deterministic.get("category", "deviation"),
            "severity": "medium",
            "confidence": 0.5,
            "probable_causes": ["Signal deviates from historical baseline."],
            "verification_checks": [f"Check {context.get('tag_path')}"],
            "safety_notes": [],
            "rationale": "Deterministic-only triage.",
            "related_entities": [
                {"label": "Equipment", "name": e} for e in context.get("equipment", [])[:3]
            ],
        }
        if not self.llm:
            return fallback
        try:
            result = self.llm.query_json(
                system_prompt=(
                    "You are an industrial anomaly triage assistant. "
                    "Return ONLY valid JSON with keys: summary, category, severity, confidence, "
                    "probable_causes, verification_checks, safety_notes, rationale, related_entities."
                ),
                user_prompt=json.dumps({"context": context, "deterministic": deterministic, "live_sample": live_sample}, default=str),
                max_tokens=900,
                use_tools=False,
            )
            data = result.get("data")
            if isinstance(data, dict):
                merged = dict(fallback)
                merged.update({k: v for k, v in data.items() if v is not None})
                return merged
        except Exception:
            pass
        return fallback

    # -------------------------------------------------------------------
    #  Main cycle
    # -------------------------------------------------------------------
    def _emit_progress(self, step: str, detail: str = "") -> None:
        emit("AGENT_STATUS", {
            "runId": self.run_id,
            "subsystemId": self.subsystem_id,
            "state": self.agent_state,
            "diagnostics": {
                "phase": "cycle_progress",
                "step": step,
                "detail": detail,
                "subsystemId": self.subsystem_id,
                "cycleCount": self._cycle_count,
            },
            "timestamp": utc_now_iso(),
        })

    def _run_cycle(self) -> Dict[str, Any]:
        thresholds = self.config.get("thresholds", {})
        stale_sec = int(thresholds.get("stalenessSec", 120))
        min_history = int(self.config.get("minHistoryPoints", 30))
        max_candidates = int(self.config.get("maxCandidatesPerSubsystem", 8))
        max_llm = int(self.config.get("maxLlmTriagesPerSubsystem", 0))

        tag_paths = [t["path"] for t in self.tag_metas]
        if not tag_paths:
            return {"candidates": 0, "triaged": 0, "emitted": 0, "diagnostics": {"phase": "cycle_complete", "reason": "no_tags"}}

        self._emit_progress("reading_tags", f"{len(tag_paths)} tags")
        t_read = time.time()
        with _api_semaphore:
            live_values = self.api.read_tags(tag_paths)
        read_ms = int((time.time() - t_read) * 1000)
        now = datetime.now(timezone.utc)

        tags_for_history: List[Tuple[Any, Dict[str, Any]]] = []
        live_error_count = 0
        quality_filtered = 0
        stale_filtered = 0

        for idx, tv in enumerate(live_values):
            tag_meta = self.tag_metas[idx] if idx < len(self.tag_metas) else {"path": tv.path, "name": tv.path}
            if tv.error:
                live_error_count += 1
                continue
            if not is_quality_good(tv.quality):
                quality_filtered += 1
                continue
            if is_stale(tv.timestamp, stale_sec, now=now):
                stale_filtered += 1
                continue
            tags_for_history.append((tv, tag_meta))

        self._emit_progress("fetching_history", f"{len(tags_for_history)} tags")
        history_paths = [tv.path for tv, _ in tags_for_history]
        t_hist = time.time()
        history_results = self._fetch_history_batch(history_paths) if history_paths else {}
        hist_ms = int((time.time() - t_hist) * 1000)

        self._emit_progress("scoring", f"{len(tags_for_history)} tags (read={read_ms}ms hist={hist_ms}ms)")
        t_score = time.time()
        shift_signal = {
            "subsystemId": self.subsystem_id,
            "subsystemType": self.subsystem_type,
            "subsystemName": self.subsystem_name,
            "evaluated": 0, "candidate": 0, "nearShift": 0,
            "sumAbsZ": 0.0, "maxAbsZ": 0.0,
            "_tagEntries": [],
        }
        candidates: List[Dict] = []
        history_errors = 0
        insufficient_history = 0

        for tv, tag_meta in tags_for_history:
            history, hist_err = history_results.get(tv.path, ([], "No history"))
            if hist_err:
                history_errors += 1
                continue
            if len(history) < min_history and len(history) < 5:
                insufficient_history += 1
                continue

            prev_val = self._prev_values.get(tv.path)
            det = compute_deviation_scores(tv.value, history, prev_value=prev_val, thresholds=thresholds)
            curr = safe_float(tv.value)
            if curr is not None:
                self._prev_values[tv.path] = curr

            abs_z = abs(float(det.get("z_score", 0.0)))
            z = float(det.get("z_score", 0.0))
            shift_signal["evaluated"] += 1
            shift_signal["sumAbsZ"] += abs_z
            if abs_z > shift_signal["maxAbsZ"]:
                shift_signal["maxAbsZ"] = abs_z
            if abs_z >= 1.5:
                shift_signal["nearShift"] += 1

            tag_name = tv.path.rsplit("/", 1)[-1] if "/" in str(tv.path) else str(tv.path)
            cached_hist = self._history_cache.get(tv.path)
            sparkline = None
            avg_val = None
            if cached_hist and cached_hist.get("values"):
                vals = cached_hist["values"]
                avg_val = round(sum(vals) / len(vals), 2)
                if len(vals) <= 20:
                    sparkline = [round(v, 2) for v in vals]
                else:
                    step = len(vals) / 20
                    sparkline = [round(vals[int(i * step)], 2) for i in range(20)]

            shift_signal["_tagEntries"].append({
                "path": str(tv.path), "name": tag_name,
                "z": round(z, 3), "mad": round(float(det.get("mad_score", 0)), 3),
                "value": tv.value, "avg": avg_val, "sparkline": sparkline,
            })

            cat = det.get("category", "normal")
            if det.get("candidate") and cat != "stuck" and len(candidates) < max_candidates:
                shift_signal["candidate"] += 1
                context = self._get_context(tv.path)
                context["subsystem"] = _subsystem_ref(self.subsystem_type, self.subsystem_name)
                candidates.append({
                    "context": context, "deterministic": det,
                    "live_sample": {"path": tv.path, "value": tv.value, "quality": tv.quality, "timestamp": tv.timestamp},
                })

        score_ms = int((time.time() - t_score) * 1000)

        t_triage = time.time()
        live_events: List[Dict[str, Any]] = []
        now_iso = utc_now_iso()
        for cand in candidates:
            det = cand["deterministic"]
            ctx = cand["context"]
            ls = cand["live_sample"]
            severity = "low"
            abs_z = abs(float(det.get("z_score", 0)))
            if abs_z >= 8:
                severity = "critical"
            elif abs_z >= 5:
                severity = "high"
            elif abs_z >= 3:
                severity = "medium"
            live_events.append({
                "event_id": f"live-{self.subsystem_id}-{ls.get('path', '')}",
                "source_tag": ls.get("path", ""),
                "tag_name": ctx.get("tag_name") or ls.get("path", ""),
                "subsystem_id": self.subsystem_id,
                "subsystem_type": self.subsystem_type,
                "subsystem_name": self.subsystem_name,
                "state": "open",
                "severity": severity,
                "category": det.get("category", "deviation"),
                "summary": f"{det.get('category', 'Deviation')} on {ctx.get('tag_name', '?')} (z={det.get('z_score', 0):.1f})",
                "z_score": float(det.get("z_score", 0)),
                "mad_score": float(det.get("mad_score", 0)),
                "delta_rate": float(det.get("delta_rate", 0)),
                "confidence": 0.5,
                "deterministic_reasons_json": json.dumps(det.get("reasons", []), default=str),
                "live_value": str(ls.get("value")),
                "live_quality": ls.get("quality"),
                "live_timestamp": ls.get("timestamp"),
                "created_at": now_iso,
            })
        triage_ms = int((time.time() - t_triage) * 1000)

        self._total_candidates += len(candidates)
        self._total_emitted += len(live_events)

        evaluated = max(1, shift_signal["evaluated"])
        tag_entries = shift_signal.pop("_tagEntries", [])
        shift_signal["avgAbsZ"] = round(shift_signal["sumAbsZ"] / evaluated, 3)
        shift_signal["shiftRatio"] = round(shift_signal["nearShift"] / evaluated, 3)
        shift_signal["candidateRatio"] = round(shift_signal["candidate"] / evaluated, 3)
        shift_signal.pop("sumAbsZ", None)
        sorted_tags = sorted(tag_entries, key=lambda t: abs(t.get("z", 0)), reverse=True)
        shift_signal["tagSignals"] = sorted_tags

        return {
            "candidates": len(candidates),
            "triaged": len(live_events),
            "emitted": len(live_events),
            "liveEvents": live_events,
            "diagnostics": {
                "phase": "cycle_complete",
                "reason": "ok",
                "monitoredTags": len(tag_paths),
                "liveErrorCount": live_error_count,
                "qualityFilteredCount": quality_filtered,
                "staleFilteredCount": stale_filtered,
                "historyErrorCount": history_errors,
                "insufficientHistoryCount": insufficient_history,
                "evaluatedCount": shift_signal["evaluated"],
                "candidateCount": len(candidates),
                "subsystemShiftSignals": [shift_signal],
                "timingMs": {
                    "read": read_ms,
                    "history": hist_ms,
                    "score": score_ms,
                    "triage": triage_ms,
                },
            },
        }


# ═══════════════════════════════════════════════════════════════════════════
#  AgentCoordinator — manages subsystem agents
# ═══════════════════════════════════════════════════════════════════════════

class AgentCoordinator:
    """Discovers subsystems, spawns/manages SubsystemAgent threads."""

    def __init__(self, config: Dict[str, Any], run_id: Optional[str] = None):
        self.config = merge_defaults(config)
        self.run_id = run_id or f"agent-{int(time.time() * 1000)}"
        from ignition_api_client import IgnitionApiClient
        from neo4j_ontology import get_ontology_graph

        self.graph = get_ontology_graph()
        self.api = IgnitionApiClient(
            base_url=self.config.get("ignitionApiUrl") or os.getenv("IGNITION_API_URL"),
            api_token=self.config.get("ignitionApiToken") or os.getenv("IGNITION_API_TOKEN"),
            timeout=15.0,
        )
        self._running = True
        self.agents: Dict[str, SubsystemAgent] = {}

    # -------------------------------------------------------------------
    #  Schema / lifecycle
    # -------------------------------------------------------------------
    def _init_schema(self) -> None:
        self.graph.init_agent_monitoring_schema()

    def _upsert_run(self, status: str, reason: Optional[str] = None) -> None:
        with self.graph.session() as session:
            session.run(
                """
                MERGE (r:AgentRun {run_id: $run_id})
                SET r.status = $status, r.updated_at = datetime(),
                    r.last_heartbeat_at = datetime(),
                    r.config_json = $cfg,
                    r.started_at = coalesce(r.started_at, datetime()),
                    r.stopped_at = CASE WHEN $status IN ['stopped','failed'] THEN datetime() ELSE r.stopped_at END,
                    r.stop_reason = CASE WHEN $reason IS NULL THEN r.stop_reason ELSE $reason END
                """,
                run_id=self.run_id, status=status,
                cfg=json.dumps(self.config, default=str), reason=reason,
            )

    # -------------------------------------------------------------------
    #  Tag discovery
    # -------------------------------------------------------------------
    def _fetch_tags(self) -> List[Dict[str, Any]]:
        max_tags = int(self.config.get("maxMonitoredTags", 200))
        scope = self.config.get("scope", {})
        subsystem_mode = str(scope.get("subsystemMode") or "auto")
        subsystem_priority = scope.get("subsystemPriority") or list(DEFAULT_SUBSYSTEM_PRIORITY)
        include_unlinked = bool(scope.get("includeUnlinkedTags", False))
        tag_map: Dict[str, Dict[str, Any]] = {}

        def upsert(*, path: str, name: str, folder: str = "", views: List[str] = None, equipment: List[str] = None, source: str = "unknown"):
            path = path.strip()
            if not path:
                return
            entry = tag_map.setdefault(path, {
                "path": path, "name": name or _last_segment(path) or path,
                "folder_name": folder, "views": [], "equipment": [],
                "source": source, "bound_to_view": False,
            })
            if source == "view_binding":
                entry["bound_to_view"] = True
                entry["source"] = source
            if folder and not entry.get("folder_name"):
                entry["folder_name"] = folder
            if name and (not entry["name"] or entry["name"] == entry["path"]):
                entry["name"] = name
            for v in (views or []):
                if v and v not in entry["views"]:
                    entry["views"].append(v)
            for e in (equipment or []):
                if e and e not in entry["equipment"]:
                    entry["equipment"].append(e)

        with self.graph.session() as session:
            for r in session.run(
                """
                MATCH (v:View)-[:HAS_COMPONENT]->(c:ViewComponent)-[r:BINDS_TO]->(n)
                WHERE r.tag_path IS NOT NULL AND trim(r.tag_path) <> ''
                  AND toLower(coalesce(r.binding_type, 'tag')) = 'tag'
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(n)
                RETURN DISTINCT trim(r.tag_path) AS tag_path, coalesce(n.name,'') AS tag_name,
                       collect(DISTINCT v.name) AS views, collect(DISTINCT eq.name) AS equipment
                LIMIT $lim
                """, lim=max_tags * 4,
            ):
                p = str(r["tag_path"] or "").strip()
                if _looks_like_tag_path(p):
                    upsert(path=p, name=str(r["tag_name"] or _last_segment(p)),
                           folder=infer_tag_group(p) or "",
                           views=[x for x in (r["views"] or []) if x],
                           equipment=[x for x in (r["equipment"] or []) if x],
                           source="view_binding")

            for r in session.run(
                """
                MATCH (t:ScadaTag) WHERE t.opc_item_path IS NOT NULL AND trim(t.opc_item_path) <> ''
                OPTIONAL MATCH (c:ViewComponent)-[:BINDS_TO]->(t)
                OPTIONAL MATCH (v:View)-[:HAS_COMPONENT]->(c)
                OPTIONAL MATCH (eq:Equipment)-[*1..2]-(t)
                RETURN DISTINCT trim(t.opc_item_path) AS tag_path, coalesce(t.name,t.opc_item_path) AS tag_name,
                       coalesce(t.folder_name,'') AS folder_name,
                       collect(DISTINCT v.name) AS views, collect(DISTINCT eq.name) AS equipment
                LIMIT $lim
                """, lim=max_tags * 6,
            ):
                p = str(r["tag_path"] or "").strip()
                if _looks_like_tag_path(p):
                    upsert(path=p, name=str(r["tag_name"] or _last_segment(p)),
                           folder=str(r["folder_name"] or ""),
                           views=[x for x in (r["views"] or []) if x],
                           equipment=[x for x in (r["equipment"] or []) if x],
                           source="scada_tag")

        tags = list(tag_map.values())
        if not include_unlinked:
            linked = [t for t in tags if t.get("views") or t.get("equipment") or t.get("bound_to_view")]
            if linked:
                tags = linked

        for tag in tags:
            subs, primary = derive_subsystems_for_tag(tag, subsystem_mode=subsystem_mode, priority=subsystem_priority)
            tag["subsystems"] = subs
            tag["primary_subsystem"] = primary

        return tags[:max_tags]

    def _discover_subsystems(self) -> Dict[str, Dict[str, Any]]:
        tags = self._fetch_tags()
        subsystems: Dict[str, Dict[str, Any]] = {}
        for t in tags:
            sub = t.get("primary_subsystem") or _subsystem_ref("global", "all")
            sub_id = sub.get("id", "global:all")
            bucket = subsystems.setdefault(sub_id, {
                "type": sub.get("type", "global"),
                "name": sub.get("name", "all"),
                "tags": [],
            })
            bucket["tags"].append(t)
        return subsystems

    # -------------------------------------------------------------------
    #  Agent management
    # -------------------------------------------------------------------
    def _spawn_agent(self, sub_id: str, info: Dict[str, Any], stagger_delay: float = 0.0) -> SubsystemAgent:
        agent = SubsystemAgent(
            subsystem_id=sub_id,
            subsystem_type=info["type"],
            subsystem_name=info["name"],
            tag_metas=info["tags"],
            graph=self.graph,
            api=self.api,
            config=self.config,
            run_id=self.run_id,
            stagger_delay=stagger_delay,
        )
        agent.start()
        self.agents[sub_id] = agent
        return agent

    def _stop_agent(self, sub_id: str) -> None:
        agent = self.agents.pop(sub_id, None)
        if agent:
            agent.stop()

    def _stop_all(self) -> None:
        for agent in self.agents.values():
            agent.stop()
        for agent in list(self.agents.values()):
            agent.join(timeout=5)
        self.agents.clear()

    # -------------------------------------------------------------------
    #  Stdin command reader
    # -------------------------------------------------------------------
    def _stdin_reader(self) -> None:
        while self._running:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                cmd = json.loads(line)
                self._handle_command(cmd)
            except (json.JSONDecodeError, Exception):
                continue

    def _handle_command(self, cmd: Dict[str, Any]) -> None:
        action = cmd.get("cmd", "")
        sub_id = cmd.get("subsystemId", "")

        if action == "stop-all":
            self._running = False
        elif action == "stop-agent" and sub_id:
            agent = self.agents.get(sub_id)
            if agent:
                agent.pause()
                emit("AGENT_STATUS", {
                    "runId": self.run_id, "subsystemId": sub_id,
                    "state": "paused", "diagnostics": {"phase": "agent_paused", "reason": "user_request"},
                    "timestamp": utc_now_iso(),
                })
        elif action == "start-agent" and sub_id:
            agent = self.agents.get(sub_id)
            if agent:
                agent.resume()
                emit("AGENT_STATUS", {
                    "runId": self.run_id, "subsystemId": sub_id,
                    "state": "running", "diagnostics": {"phase": "agent_resumed", "reason": "user_request"},
                    "timestamp": utc_now_iso(),
                })
        elif action == "deep-analyze":
            event_data = cmd.get("event", {})
            threading.Thread(
                target=self._deep_analyze_inline,
                args=(event_data,),
                daemon=True,
                name="deep-analyze",
            ).start()

    # -------------------------------------------------------------------
    #  Deep analyze (inline, runs in background thread)
    # -------------------------------------------------------------------
    def _deep_analyze_inline(self, event_data: Dict[str, Any]) -> None:
        event_id = event_data.get("event_id", "?")
        tag_path = event_data.get("source_tag") or event_data.get("tag_name", "")
        sub_id = event_data.get("subsystem_id", "")
        if not tag_path:
            emit("AGENT_EVENT", {"runId": self.run_id, "deepAnalyze": True,
                "event": {**event_data, "deep_analyze_error": "No source_tag"}})
            return
        agent = self.agents.get(sub_id) if sub_id else None
        llm = None
        if agent and agent.llm:
            llm = agent.llm
        else:
            if bool(os.getenv("ANTHROPIC_API_KEY")):
                try:
                    from claude_client import ClaudeClient
                    llm = ClaudeClient(
                        enable_tools=False,
                        ignition_api_url=self.config.get("ignitionApiUrl"),
                        ignition_api_token=self.config.get("ignitionApiToken"),
                    )
                except Exception:
                    pass
        if not llm:
            emit("AGENT_EVENT", {"runId": self.run_id, "deepAnalyze": True,
                "event": {**event_data, "deep_analyze_error": "No LLM available (check ANTHROPIC_API_KEY)"}})
            return

        det = {
            "z_score": event_data.get("z_score", 0),
            "mad_score": event_data.get("mad_score", 0),
            "delta_rate": event_data.get("delta_rate", 0),
            "category": event_data.get("category", "deviation"),
            "reasons": json.loads(event_data.get("deterministic_reasons_json", "[]")),
        }
        context = {"tag_path": tag_path, "tag_name": event_data.get("tag_name", tag_path),
            "equipment": [], "views": [], "group": "", "symptoms": [], "causes": []}
        live_sample = {"path": tag_path, "value": event_data.get("live_value"),
            "quality": event_data.get("live_quality"), "timestamp": event_data.get("live_timestamp")}

        try:
            result = llm.query_json(
                system_prompt=(
                    "You are an industrial anomaly triage assistant. "
                    "Return ONLY valid JSON with keys: summary, category, severity, confidence, "
                    "probable_causes, verification_checks, safety_notes, rationale, related_entities."
                ),
                user_prompt=json.dumps({"context": context, "deterministic": det, "live_sample": live_sample}, default=str),
                max_tokens=900,
                use_tools=False,
            )
            data = result.get("data", {}) if isinstance(result, dict) else {}
            updated = dict(event_data)
            if isinstance(data, dict):
                updated["summary"] = data.get("summary", updated.get("summary", ""))
                updated["explanation"] = data.get("rationale", updated.get("explanation", ""))
                updated["probable_causes_json"] = json.dumps(data.get("probable_causes", []))
                updated["recommended_checks_json"] = json.dumps(data.get("verification_checks", []))
                updated["safety_notes_json"] = json.dumps(data.get("safety_notes", []))
                updated["severity"] = data.get("severity", updated.get("severity", "medium"))
                updated["confidence"] = data.get("confidence", updated.get("confidence", 0.5))
                updated["deep_analyzed"] = True
            emit("AGENT_EVENT", {"runId": self.run_id, "deepAnalyze": True, "event": updated})
        except Exception as exc:
            emit("AGENT_EVENT", {"runId": self.run_id, "deepAnalyze": True,
                "event": {**event_data, "deep_analyze_error": str(exc)}})

    # -------------------------------------------------------------------
    #  Main loop
    # -------------------------------------------------------------------
    def run(self) -> int:
        self._init_schema()
        self._upsert_run("running")

        emit("AGENT_STATUS", {
            "runId": self.run_id, "state": "running",
            "diagnostics": {"phase": "startup", "reason": "coordinator_started"},
            "timestamp": utc_now_iso(),
        })

        subsystems = self._discover_subsystems()
        tag_map: Dict[str, Any] = {}
        stagger_sec = 1.5  # seconds between each agent's first cycle
        for idx, (sub_id, info) in enumerate(subsystems.items()):
            tag_map[sub_id] = {
                "type": info["type"], "name": info["name"],
                "tags": [{"path": t["path"], "name": t.get("name", t["path"])} for t in info["tags"]],
            }
            self._spawn_agent(sub_id, info, stagger_delay=idx * stagger_sec)

        emit("AGENT_STATUS", {
            "runId": self.run_id, "state": "running",
            "diagnostics": {
                "phase": "agents_started",
                "reason": f"{len(self.agents)} subsystem agents spawned",
                "subsystemTagMap": tag_map,
                "agentCount": len(self.agents),
                "agentIds": list(self.agents.keys()),
            },
            "timestamp": utc_now_iso(),
        })

        stdin_thread = threading.Thread(target=self._stdin_reader, daemon=True, name="stdin-reader")
        stdin_thread.start()

        rediscovery_interval = float(self.config.get("rediscoveryIntervalSec", 60))
        cleanup_every = max(1, int(self.config.get("cleanupEveryCycles", 40)))
        last_rediscovery = time.time()
        watchdog_count = 0

        while self._running:
            time.sleep(2)
            watchdog_count += 1

            if time.time() - last_rediscovery >= rediscovery_interval:
                try:
                    new_subs = self._discover_subsystems()
                    new_ids = set(new_subs.keys())
                    old_ids = set(self.agents.keys())

                    for sub_id in new_ids - old_ids:
                        info = new_subs[sub_id]
                        self._spawn_agent(sub_id, info)
                        emit("AGENT_STATUS", {
                            "runId": self.run_id, "subsystemId": sub_id, "state": "running",
                            "diagnostics": {"phase": "agent_discovered", "reason": "new_subsystem"},
                            "timestamp": utc_now_iso(),
                        })

                    for sub_id in old_ids & new_ids:
                        agent = self.agents.get(sub_id)
                        if agent:
                            agent.update_tags(new_subs[sub_id]["tags"])

                    tag_map = {}
                    for sub_id, info in new_subs.items():
                        tag_map[sub_id] = {
                            "type": info["type"], "name": info["name"],
                            "tags": [{"path": t["path"], "name": t.get("name", t["path"])} for t in info["tags"]],
                        }
                    emit("AGENT_STATUS", {
                        "runId": self.run_id, "state": "running",
                        "diagnostics": {
                            "phase": "rediscovery_complete",
                            "reason": f"{len(new_subs)} subsystems",
                            "subsystemTagMap": tag_map,
                            "agentCount": len(self.agents),
                        },
                        "timestamp": utc_now_iso(),
                    })
                except Exception as exc:
                    emit("AGENT_ERROR", {
                        "runId": self.run_id, "code": "rediscovery_error",
                        "message": str(exc), "recoverable": True, "timestamp": utc_now_iso(),
                    })
                last_rediscovery = time.time()

            if watchdog_count % cleanup_every == 0:
                try:
                    deleted = self.graph.cleanup_anomaly_events(int(self.config.get("retentionDays", 14)))
                    if deleted > 0:
                        emit("AGENT_STATUS", {
                            "runId": self.run_id, "state": "running",
                            "diagnostics": {"phase": "retention_cleanup", "reason": f"deleted {deleted} old events"},
                            "timestamp": utc_now_iso(),
                        })
                except Exception:
                    pass

        self._stop_all()
        self._upsert_run("stopped", reason="stopped")
        emit("AGENT_COMPLETE", {
            "runId": self.run_id, "success": True, "reason": "stopped", "stoppedAt": utc_now_iso(),
        })
        return 0

    # -------------------------------------------------------------------
    #  Single-operation helpers (for CLI)
    # -------------------------------------------------------------------
    def list_events(self, limit: int, state: Optional[str] = None, severity: Optional[str] = None, run_id: Optional[str] = None) -> Dict:
        return {"success": True, "events": self.graph.list_anomaly_events(limit=limit, state=state, severity=severity, run_id=run_id)}

    def get_event(self, event_id: str) -> Dict:
        event = self.graph.get_anomaly_event(event_id)
        return {"success": True, "event": event} if event else {"success": False, "error": f"Not found: {event_id}"}

    def ack_event(self, event_id: str, note: Optional[str] = None) -> Dict:
        with self.graph.session() as session:
            row = session.run(
                "MATCH (e:AnomalyEvent {event_id: $eid}) SET e.state='acknowledged', e.acknowledged_at=datetime(), e.ack_note=$note, e.updated_at=datetime() RETURN count(e) AS cnt",
                eid=event_id, note=note or "",
            ).single()
            if not row or row["cnt"] == 0:
                return {"success": False, "error": f"Not found: {event_id}"}
        return {"success": True, "eventId": event_id}

    def clear_event(self, event_id: str, note: Optional[str] = None) -> Dict:
        with self.graph.session() as session:
            row = session.run(
                "MATCH (e:AnomalyEvent {event_id: $eid}) SET e.state='cleared', e.cleared_at=datetime(), e.clear_note=$note, e.updated_at=datetime() RETURN count(e) AS cnt",
                eid=event_id, note=note or "",
            ).single()
            if not row or row["cnt"] == 0:
                return {"success": False, "error": f"Not found: {event_id}"}
        return {"success": True, "eventId": event_id}

    def deep_analyze(self, event_id: str) -> Dict:
        event = self.graph.get_anomaly_event(event_id)
        if not event:
            return {"success": False, "error": f"Not found: {event_id}"}
        tag_path = event.get("source_tag") or event.get("tag_name", "")
        if not tag_path:
            return {"success": False, "error": "Event has no source_tag"}

        temp_agent = SubsystemAgent(
            subsystem_id=event.get("subsystem_id", "global:all"),
            subsystem_type=event.get("subsystem_type", "global"),
            subsystem_name=event.get("subsystem_name", "all"),
            tag_metas=[], graph=self.graph, api=self.api,
            config=self.config, run_id=self.run_id,
        )
        if not temp_agent.llm:
            return {"success": False, "error": "LLM client not configured"}

        context = temp_agent._get_context(tag_path)
        context["subsystem"] = _subsystem_ref(event.get("subsystem_type", "global"), event.get("subsystem_name", "all"))
        det = {
            "candidate": True,
            "z_score": float(event.get("z_score", 0)),
            "mad_score": float(event.get("mad_score", 0)),
            "delta_rate": float(event.get("delta_rate", 0)),
            "window_volatility": float(event.get("window_volatility", 0)),
            "reasons": json.loads(event.get("deterministic_reasons_json", "[]")),
            "category": event.get("category", "deviation"),
        }
        live = {"value": event.get("live_value"), "quality": event.get("live_quality"), "timestamp": event.get("live_timestamp")}
        triage = temp_agent._run_llm_triage(context, det, live)
        severity = SubsystemAgent._severity_from_scores(det, triage)

        with self.graph.session() as session:
            session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $eid})
                SET e.summary=$summary, e.explanation=$expl, e.severity=$sev,
                    e.confidence=$conf, e.recommended_checks_json=$checks,
                    e.probable_causes_json=$causes, e.safety_notes_json=$safety,
                    e.updated_at=$ts, e.llm_triaged=true
                """,
                eid=event_id, summary=triage.get("summary", ""),
                expl=triage.get("rationale", ""), sev=severity,
                conf=float(max(0.0, min(1.0, triage.get("confidence", 0.5)))),
                checks=json.dumps(triage.get("verification_checks", []), default=str),
                causes=json.dumps(triage.get("probable_causes", []), default=str),
                safety=json.dumps(triage.get("safety_notes", []), default=str),
                ts=utc_now_iso(),
            )
        return {"success": True, "event": self.graph.get_anomaly_event(event_id)}

    def get_status(self, run_id: str) -> Dict:
        with self.graph.session() as session:
            row = session.run("MATCH (r:AgentRun {run_id: $rid}) RETURN r LIMIT 1", rid=run_id).single()
            if not row:
                return {"success": False, "error": f"Run not found: {run_id}"}
            props = dict(row["r"])
            return {
                "success": True, "status": props.get("status"),
                "metrics": {
                    "cycleCount": props.get("cycle_count", 0),
                    "lastCycleMs": props.get("last_cycle_ms", 0),
                },
                "lastHeartbeatAt": props.get("last_heartbeat_at"), "run": props,
            }


# ═══════════════════════════════════════════════════════════════════════════
#  Fixture replay (standalone, no agent needed)
# ═══════════════════════════════════════════════════════════════════════════

def replay_fixtures(config_json: Optional[str], fixture_path: str) -> Dict[str, Any]:
    config = merge_defaults(json.loads(config_json) if config_json else {})
    cases = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    if isinstance(cases, dict):
        cases = cases.get("cases", [])
    thresholds = config.get("thresholds", {})
    passed = 0
    failures: List[Dict] = []
    for case in cases:
        result = compute_deviation_scores(case.get("current_value"), case.get("history_values", []),
                                          prev_value=case.get("prev_value"), thresholds=thresholds)
        expected = bool(case.get("expected_candidate", False))
        if result.get("candidate") == expected:
            passed += 1
        else:
            failures.append({"id": case.get("id"), "expected": expected, "actual": result.get("candidate"), "reasons": result.get("reasons", [])})
    return {"success": len(failures) == 0, "total": len(cases), "passed": passed, "failed": len(failures), "failures": failures}


# ═══════════════════════════════════════════════════════════════════════════
#  CLI entry point
# ═══════════════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(description="Per-subsystem anomaly monitor")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Run coordinator with per-subsystem agents")
    p_run.add_argument("--run-id")
    p_run.add_argument("--config-json", default="{}")

    sub.add_parser("status", help="Get run status").add_argument("--run-id", required=True)

    p_list = sub.add_parser("list-events", help="List anomaly events")
    p_list.add_argument("--limit", type=int, default=100)
    p_list.add_argument("--state")
    p_list.add_argument("--severity")
    p_list.add_argument("--run-id")

    sub.add_parser("get-event", help="Get one event").add_argument("--event-id", required=True)

    p_ack = sub.add_parser("ack-event", help="Acknowledge event")
    p_ack.add_argument("--event-id", required=True)
    p_ack.add_argument("--note")

    p_clear = sub.add_parser("clear-event", help="Clear event")
    p_clear.add_argument("--event-id", required=True)
    p_clear.add_argument("--note")

    p_deep = sub.add_parser("deep-analyze", help="LLM triage on existing event")
    p_deep.add_argument("--event-id", required=True)

    sub.add_parser("cleanup", help="Delete old events").add_argument("--retention-days", type=int, default=14)

    p_replay = sub.add_parser("replay-fixtures", help="Validate scoring")
    p_replay.add_argument("--fixture-file", required=True)
    p_replay.add_argument("--config-json", default="{}")

    args = parser.parse_args()

    if args.command == "replay-fixtures":
        print(json.dumps(replay_fixtures(args.config_json, args.fixture_file)))
        return 0

    try:
        coordinator = AgentCoordinator(
            config=json.loads(getattr(args, "config_json", "{}") or "{}"),
            run_id=getattr(args, "run_id", None),
        )
    except Exception as exc:
        print(json.dumps({"success": False, "error": str(exc)}))
        return 1

    if args.command == "run":
        signal.signal(signal.SIGTERM, lambda *_: setattr(coordinator, '_running', False))
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, lambda *_: setattr(coordinator, '_running', False))
        return coordinator.run()

    if args.command == "status":
        print(json.dumps(coordinator.get_status(args.run_id), default=str))
    elif args.command == "list-events":
        print(json.dumps(coordinator.list_events(args.limit, args.state, args.severity, getattr(args, "run_id", None)), default=str))
    elif args.command == "get-event":
        print(json.dumps(coordinator.get_event(args.event_id), default=str))
    elif args.command == "ack-event":
        print(json.dumps(coordinator.ack_event(args.event_id, args.note), default=str))
    elif args.command == "clear-event":
        print(json.dumps(coordinator.clear_event(args.event_id, args.note), default=str))
    elif args.command == "deep-analyze":
        print(json.dumps(coordinator.deep_analyze(args.event_id), default=str))
    elif args.command == "cleanup":
        deleted = coordinator.graph.cleanup_anomaly_events(args.retention_days)
        print(json.dumps({"success": True, "deleted": deleted}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
