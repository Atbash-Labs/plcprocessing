#!/usr/bin/env python3
"""
Anomaly monitoring worker for the Agents tab.

Long-running process that:
1. Reads live tag values from Ignition gateway
2. Scores deviations against historical windows (Stage A)
3. Triages candidates via ontology-aware LLM (Stage B)
4. Persists anomaly events to Neo4j
5. Streams status/events to Electron via stdout JSON lines

Protocol:
  stdin  <- JSON config on start
  stdout -> JSON lines: {type: "status"|"event"|"error"|"complete", ...}
  stderr -> debug/verbose output

Event schema version: 1
"""

import os
import sys
import json
import time
import uuid
import signal
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

from dotenv import load_dotenv
load_dotenv()

from neo4j_ontology import OntologyGraph, get_ontology_graph
from ignition_api_client import IgnitionApiClient
from anomaly_rules import (
    ThresholdConfig,
    DeviationScores,
    compute_deviation_scores,
    score_tag_batch,
    filter_candidates,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_SCHEMA_VERSION = 1
DEFAULT_POLL_INTERVAL_MS = 15000
DEFAULT_HISTORY_WINDOW_MIN = 360
DEFAULT_MIN_HISTORY_POINTS = 30
DEFAULT_MAX_CANDIDATES_PER_CYCLE = 25
DEFAULT_MAX_LLM_TRIAGES_PER_CYCLE = 5
DEDUP_COOLDOWN_SEC = 300  # 5 min cooldown per tag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def emit(msg_type: str, payload: dict):
    """Emit a JSON line to stdout for Electron consumption."""
    payload["type"] = msg_type
    payload["ts"] = datetime.now(timezone.utc).isoformat()
    line = json.dumps(payload, default=str)
    print(line, flush=True)


def debug(msg: str):
    """Print debug message to stderr."""
    print(f"[DEBUG] {msg}", file=sys.stderr, flush=True)


def new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Neo4j persistence for AgentRun / AnomalyEvent
# ---------------------------------------------------------------------------

class AnomalyStore:
    """Persist agent runs and anomaly events to Neo4j."""

    def __init__(self, graph: OntologyGraph):
        self._graph = graph

    def ensure_schema(self):
        """Create indexes/constraints for agent monitoring nodes."""
        with self._graph.session() as session:
            statements = [
                "CREATE CONSTRAINT agentrun_id IF NOT EXISTS FOR (r:AgentRun) REQUIRE r.run_id IS UNIQUE",
                "CREATE CONSTRAINT anomalyevent_id IF NOT EXISTS FOR (e:AnomalyEvent) REQUIRE e.event_id IS UNIQUE",
                "CREATE INDEX anomalyevent_created IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.created_at)",
                "CREATE INDEX anomalyevent_state IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.state)",
                "CREATE INDEX anomalyevent_severity IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.severity)",
                "CREATE INDEX anomalyevent_dedup IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.dedup_key)",
            ]
            for stmt in statements:
                try:
                    session.run(stmt)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        debug(f"Schema warning: {e}")

    def create_run(self, run_id: str, config_json: str) -> None:
        with self._graph.session() as session:
            session.run(
                """
                CREATE (r:AgentRun {
                    run_id: $run_id,
                    status: 'running',
                    started_at: datetime(),
                    config_json: $config_json,
                    last_heartbeat_at: datetime(),
                    cycle_count: 0
                })
                """,
                run_id=run_id,
                config_json=config_json,
            )

    def heartbeat(self, run_id: str, cycle_count: int) -> None:
        with self._graph.session() as session:
            session.run(
                """
                MATCH (r:AgentRun {run_id: $run_id})
                SET r.last_heartbeat_at = datetime(),
                    r.cycle_count = $cycle_count
                """,
                run_id=run_id,
                cycle_count=cycle_count,
            )

    def stop_run(self, run_id: str, reason: str = "user_stop") -> None:
        with self._graph.session() as session:
            session.run(
                """
                MATCH (r:AgentRun {run_id: $run_id})
                SET r.status = 'stopped',
                    r.stopped_at = datetime(),
                    r.stop_reason = $reason
                """,
                run_id=run_id,
                reason=reason,
            )

    def persist_event(
        self,
        run_id: str,
        event_id: str,
        severity: str,
        confidence: float,
        category: str,
        summary: str,
        explanation: str,
        recommended_checks: List[str],
        z_score: Optional[float],
        mad_score: Optional[float],
        delta_rate: Optional[float],
        source_tag: str,
        dedup_key: str,
        equipment_name: Optional[str] = None,
        related_entities: Optional[List[Dict]] = None,
    ) -> None:
        """Persist an AnomalyEvent and link to run, tag, and equipment."""
        with self._graph.session() as session:
            # Create event node
            session.run(
                """
                CREATE (e:AnomalyEvent {
                    event_id: $event_id,
                    run_id: $run_id,
                    event_schema_version: $schema_version,
                    state: 'open',
                    severity: $severity,
                    confidence: $confidence,
                    category: $category,
                    summary: $summary,
                    explanation: $explanation,
                    recommended_checks_json: $checks_json,
                    z_score: $z_score,
                    mad_score: $mad_score,
                    delta_rate: $delta_rate,
                    source_tag: $source_tag,
                    dedup_key: $dedup_key,
                    created_at: datetime(),
                    updated_at: datetime()
                })
                """,
                event_id=event_id,
                run_id=run_id,
                schema_version=EVENT_SCHEMA_VERSION,
                severity=severity,
                confidence=confidence,
                category=category,
                summary=summary,
                explanation=explanation,
                checks_json=json.dumps(recommended_checks),
                z_score=z_score,
                mad_score=mad_score,
                delta_rate=delta_rate,
                source_tag=source_tag,
                dedup_key=dedup_key,
            )

            # Link to AgentRun
            session.run(
                """
                MATCH (r:AgentRun {run_id: $run_id}),
                      (e:AnomalyEvent {event_id: $event_id})
                MERGE (r)-[:EMITTED]->(e)
                """,
                run_id=run_id,
                event_id=event_id,
            )

            # Link to ScadaTag if exists
            session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $event_id})
                OPTIONAL MATCH (t:ScadaTag)
                WHERE t.path = $source_tag OR t.name = $source_tag
                FOREACH (_ IN CASE WHEN t IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (e)-[:OBSERVED_ON]->(t)
                )
                """,
                event_id=event_id,
                source_tag=source_tag,
            )

            # Link to Equipment if known
            if equipment_name:
                session.run(
                    """
                    MATCH (e:AnomalyEvent {event_id: $event_id})
                    OPTIONAL MATCH (eq:Equipment {name: $eq_name})
                    FOREACH (_ IN CASE WHEN eq IS NOT NULL THEN [1] ELSE [] END |
                        MERGE (e)-[:AFFECTS]->(eq)
                    )
                    """,
                    event_id=event_id,
                    eq_name=equipment_name,
                )

            # Link to related entities (FaultSymptom, FaultCause, etc.)
            if related_entities:
                for rel in related_entities:
                    label = rel.get("label", "")
                    name = rel.get("name", "")
                    if label and name:
                        try:
                            session.run(
                                f"""
                                MATCH (e:AnomalyEvent {{event_id: $event_id}})
                                OPTIONAL MATCH (n:{label} {{name: $name}})
                                FOREACH (_ IN CASE WHEN n IS NOT NULL THEN [1] ELSE [] END |
                                    MERGE (e)-[:RELATED_TO]->(n)
                                )
                                """,
                                event_id=event_id,
                                name=name,
                            )
                        except Exception:
                            pass

    def check_dedup(self, dedup_key: str, cooldown_sec: float) -> bool:
        """Return True if an event with this dedup_key exists within cooldown window."""
        with self._graph.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent {dedup_key: $key})
                WHERE e.created_at > datetime() - duration({seconds: $cooldown})
                RETURN count(e) as cnt
                """,
                key=dedup_key,
                cooldown=int(cooldown_sec),
            )
            record = result.single()
            return record["cnt"] > 0 if record else False

    def list_events(
        self,
        run_id: Optional[str] = None,
        state: Optional[str] = None,
        severity: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict]:
        """List anomaly events with optional filters."""
        conditions = []
        params: Dict[str, Any] = {"limit": limit, "offset": offset}

        if run_id:
            conditions.append("e.run_id = $run_id")
            params["run_id"] = run_id
        if state:
            conditions.append("e.state = $state")
            params["state"] = state
        if severity:
            conditions.append("e.severity = $severity")
            params["severity"] = severity

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        with self._graph.session() as session:
            result = session.run(
                f"""
                MATCH (e:AnomalyEvent)
                {where}
                RETURN e
                ORDER BY e.created_at DESC
                SKIP $offset LIMIT $limit
                """,
                **params,
            )
            events = []
            for record in result:
                node = record["e"]
                events.append(dict(node))
            return events

    def get_event(self, event_id: str) -> Optional[Dict]:
        """Get single event with all relationships."""
        with self._graph.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $event_id})
                OPTIONAL MATCH (e)-[:OBSERVED_ON]->(t)
                OPTIONAL MATCH (e)-[:AFFECTS]->(eq)
                OPTIONAL MATCH (e)-[:RELATED_TO]->(rel)
                RETURN e,
                       collect(DISTINCT {label: labels(t)[0], name: t.name}) as tags,
                       collect(DISTINCT {label: labels(eq)[0], name: eq.name}) as equipment,
                       collect(DISTINCT {label: labels(rel)[0], name: rel.name}) as related
                """,
                event_id=event_id,
            )
            record = result.single()
            if not record:
                return None
            event = dict(record["e"])
            event["observed_tags"] = [r for r in record["tags"] if r.get("name")]
            event["affected_equipment"] = [r for r in record["equipment"] if r.get("name")]
            event["related_entities"] = [r for r in record["related"] if r.get("name")]
            return event


# ---------------------------------------------------------------------------
# Ontology context gathering for LLM triage
# ---------------------------------------------------------------------------

def gather_ontology_context(graph: OntologyGraph, tag_path: str) -> Dict[str, Any]:
    """
    Gather ontology neighborhood for a tag to provide context for LLM triage.

    Returns compact dict with equipment, symptoms, causes, patterns, safety elements.
    """
    context: Dict[str, Any] = {
        "tag_path": tag_path,
        "equipment": [],
        "symptoms": [],
        "causes": [],
        "patterns": [],
        "safety_elements": [],
        "related_tags": [],
    }

    with graph.session() as session:
        # Find equipment associated with the tag
        result = session.run(
            """
            OPTIONAL MATCH (t:ScadaTag)-[:MONITORED_BY|BINDS_TO|HAS_TAG*1..2]-(eq:Equipment)
            WHERE t.path = $path OR t.name = $path
            WITH eq WHERE eq IS NOT NULL
            RETURN DISTINCT eq.name as name, eq.type as type, eq.purpose as purpose
            LIMIT 5
            """,
            path=tag_path,
        )
        for r in result:
            if r["name"]:
                context["equipment"].append({
                    "name": r["name"],
                    "type": r["type"],
                    "purpose": r["purpose"],
                })

        # Find fault symptoms related to equipment
        eq_names = [e["name"] for e in context["equipment"]]
        if eq_names:
            result = session.run(
                """
                MATCH (eq:Equipment)-[:HAS_SYMPTOM|EXHIBITS*1..2]->(s:FaultSymptom)
                WHERE eq.name IN $names
                RETURN DISTINCT s.symptom as symptom
                LIMIT 10
                """,
                names=eq_names,
            )
            context["symptoms"] = [r["symptom"] for r in result if r["symptom"]]

            # Fault causes
            result = session.run(
                """
                MATCH (eq:Equipment)-[:HAS_SYMPTOM|EXHIBITS*1..2]->(s:FaultSymptom)
                      -[:CAUSED_BY|HAS_CAUSE]->(c:FaultCause)
                WHERE eq.name IN $names
                RETURN DISTINCT c.cause as cause, c.name as name
                LIMIT 10
                """,
                names=eq_names,
            )
            context["causes"] = [
                r["cause"] or r["name"] for r in result if r["cause"] or r["name"]
            ]

            # Control patterns
            result = session.run(
                """
                MATCH (eq:Equipment)<-[:APPLIES_TO|CONTROLS*1..2]-(p:ControlPattern)
                WHERE eq.name IN $names
                RETURN DISTINCT p.pattern_name as name, p.description as desc
                LIMIT 5
                """,
                names=eq_names,
            )
            context["patterns"] = [
                {"name": r["name"], "description": r["desc"]}
                for r in result if r["name"]
            ]

            # Safety elements
            result = session.run(
                """
                MATCH (eq:Equipment)-[:SAFETY_CRITICAL|HAS_SAFETY*1..2]->(se:SafetyElement)
                WHERE eq.name IN $names
                RETURN DISTINCT se.name as name, se.type as type
                LIMIT 5
                """,
                names=eq_names,
            )
            context["safety_elements"] = [
                {"name": r["name"], "type": r["type"]}
                for r in result if r["name"]
            ]

        # Related tags on same equipment
        if eq_names:
            result = session.run(
                """
                MATCH (eq:Equipment)-[:HAS_TAG|MONITORED_BY*1..2]->(t:ScadaTag)
                WHERE eq.name IN $names AND (t.path <> $path AND t.name <> $path)
                RETURN DISTINCT t.path as path, t.name as name
                LIMIT 10
                """,
                names=eq_names,
                path=tag_path,
            )
            context["related_tags"] = [
                r["path"] or r["name"] for r in result if r["path"] or r["name"]
            ]

    return context


# ---------------------------------------------------------------------------
# LLM triage
# ---------------------------------------------------------------------------

def run_llm_triage(
    scores: DeviationScores,
    ontology_context: Dict[str, Any],
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run Stage B: ontology-aware LLM triage on an anomaly candidate.

    Returns structured triage result with classification, causes, checks, safety.
    Falls back to deterministic-only result if LLM unavailable.
    """
    import anthropic

    triage_result = {
        "classification": scores.category or "unknown",
        "probable_causes": [],
        "verification_checks": [],
        "safety_relevant": False,
        "urgency": "low",
        "confidence_rationale": "",
        "llm_available": False,
    }

    # Build compact prompt
    prompt = _build_triage_prompt(scores, ontology_context)

    try:
        client = anthropic.Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are an industrial process anomaly analyst. Analyze the anomaly candidate "
                "and return ONLY a JSON object with these fields:\n"
                '- "classification": one of "spike", "drift", "stuck", "state-conflict", "quality-issue"\n'
                '- "probable_causes": array of ranked cause strings (most likely first)\n'
                '- "verification_checks": array of specific checks an operator should perform\n'
                '- "safety_relevant": boolean\n'
                '- "urgency": one of "critical", "high", "medium", "low"\n'
                '- "confidence_rationale": brief explanation of your confidence level\n'
                "Return ONLY valid JSON, no markdown fences or extra text."
            ),
        )

        text = response.content[0].text.strip()
        # Parse JSON from response (handle markdown fences)
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        parsed = json.loads(text)
        triage_result.update(parsed)
        triage_result["llm_available"] = True

    except json.JSONDecodeError:
        debug(f"LLM returned non-JSON for {scores.tag_path}, using deterministic fallback")
        triage_result["confidence_rationale"] = "LLM response was not parseable; using deterministic classification only"
    except Exception as e:
        debug(f"LLM triage failed for {scores.tag_path}: {e}")
        triage_result["confidence_rationale"] = f"LLM unavailable ({type(e).__name__}); deterministic-only classification"

    # Ensure safety flag if safety elements exist in context
    if ontology_context.get("safety_elements"):
        triage_result["safety_relevant"] = True
        if triage_result["urgency"] == "low":
            triage_result["urgency"] = "medium"

    return triage_result


def _build_triage_prompt(scores: DeviationScores, ctx: Dict[str, Any]) -> str:
    """Build a compact triage prompt for the LLM."""
    parts = [
        f"Tag: {scores.tag_path}",
        f"Current value: {scores.current_value}",
        f"Category hint: {scores.category or 'unknown'}",
        f"Z-score: {scores.z_score:.2f}" if scores.z_score is not None else "",
        f"MAD score: {scores.mad_score:.2f}" if scores.mad_score is not None else "",
        f"Delta rate: {scores.delta_rate:.2f}" if scores.delta_rate is not None else "",
        f"Window mean: {scores.window_mean:.2f}" if scores.window_mean is not None else "",
        f"Window std: {scores.window_std:.4f}" if scores.window_std is not None else "",
        f"History points: {scores.history_points}",
    ]

    if ctx.get("equipment"):
        eq_strs = [f"{e['name']} ({e.get('type', 'unknown')})" for e in ctx["equipment"]]
        parts.append(f"Equipment: {', '.join(eq_strs)}")

    if ctx.get("symptoms"):
        parts.append(f"Known symptoms: {'; '.join(ctx['symptoms'][:5])}")

    if ctx.get("causes"):
        parts.append(f"Known causes: {'; '.join(ctx['causes'][:5])}")

    if ctx.get("safety_elements"):
        se_strs = [f"{s['name']} ({s.get('type', '')})" for s in ctx["safety_elements"]]
        parts.append(f"Safety elements nearby: {', '.join(se_strs)}")

    if ctx.get("patterns"):
        pat_strs = [p["name"] for p in ctx["patterns"]]
        parts.append(f"Control patterns: {', '.join(pat_strs)}")

    return "\n".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Tag discovery from ontology
# ---------------------------------------------------------------------------

def discover_monitored_tags(graph: OntologyGraph, scope: Dict[str, Any]) -> List[str]:
    """
    Discover which tags to monitor based on scope config.

    Scope options:
        tagPaths: explicit list of tag paths
        tagRegex: regex filter on tag path
        equipmentNames: monitor all tags on these equipment
        project: monitor all tags in project
    """
    tag_paths = set()

    with graph.session() as session:
        # Explicit tag paths
        if scope.get("tagPaths"):
            tag_paths.update(scope["tagPaths"])

        # Tags on specific equipment
        if scope.get("equipmentNames"):
            result = session.run(
                """
                MATCH (eq:Equipment)-[:HAS_TAG|MONITORED_BY*1..2]->(t:ScadaTag)
                WHERE eq.name IN $names
                RETURN DISTINCT coalesce(t.path, t.name) as path
                """,
                names=scope["equipmentNames"],
            )
            for r in result:
                if r["path"]:
                    tag_paths.add(r["path"])

        # Tags in project
        if scope.get("project"):
            result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE t.project = $project
                RETURN DISTINCT coalesce(t.path, t.name) as path
                LIMIT 500
                """,
                project=scope["project"],
            )
            for r in result:
                if r["path"]:
                    tag_paths.add(r["path"])

        # Regex filter
        if scope.get("tagRegex"):
            result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE coalesce(t.path, t.name) =~ $regex
                RETURN DISTINCT coalesce(t.path, t.name) as path
                LIMIT 500
                """,
                regex=scope["tagRegex"],
            )
            for r in result:
                if r["path"]:
                    tag_paths.add(r["path"])

        # Default: all ScadaTags (limited)
        if not tag_paths:
            result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE t.path IS NOT NULL
                RETURN DISTINCT t.path as path
                LIMIT 200
                """
            )
            for r in result:
                if r["path"]:
                    tag_paths.add(r["path"])

    return sorted(tag_paths)


# ---------------------------------------------------------------------------
# History fetching (simulated from Ignition or Neo4j stored values)
# ---------------------------------------------------------------------------

def fetch_history_window(
    api: IgnitionApiClient,
    tag_paths: List[str],
    window_minutes: int = 360,
) -> Dict[str, List[float]]:
    """
    Fetch historical values for tags.

    In V1, uses current live reads as proxy if no history API available.
    The live_enricher stores live_value on ScadaTag nodes; we can read those
    plus any accumulated samples if Ignition history queries are available.

    Falls back to empty list if history is unavailable.
    """
    history: Dict[str, List[float]] = {}

    # Try batch read of current values as baseline
    # Real implementation would use query_tag_history
    if api.is_configured:
        try:
            readings = api.read_tags(tag_paths)
            for tv in readings:
                if tv.value is not None and tv.error is None:
                    try:
                        val = float(tv.value)
                        # Single point - insufficient for statistical analysis
                        # but used as seed; real history comes from Ignition historian
                        history[tv.path] = history.get(tv.path, [])
                        history[tv.path].append(val)
                    except (TypeError, ValueError):
                        pass
        except Exception as e:
            debug(f"History fetch failed: {e}")

    return history


# ---------------------------------------------------------------------------
# Main monitoring loop
# ---------------------------------------------------------------------------

class AnomalyMonitor:
    """Core monitoring loop orchestrator."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.run_id = new_id()
        self.running = False
        self.cycle_count = 0

        # Parse config
        self.poll_interval_ms = config.get("pollIntervalMs", DEFAULT_POLL_INTERVAL_MS)
        self.history_window_min = config.get("historyWindowMinutes", DEFAULT_HISTORY_WINDOW_MIN)
        self.min_history_points = config.get("minHistoryPoints", DEFAULT_MIN_HISTORY_POINTS)
        self.max_candidates = config.get("maxCandidatesPerCycle", DEFAULT_MAX_CANDIDATES_PER_CYCLE)
        self.max_llm_triages = config.get("maxLlmTriagesPerCycle", DEFAULT_MAX_LLM_TRIAGES_PER_CYCLE)

        # Thresholds
        thresholds = config.get("thresholds", {})
        self.threshold_config = ThresholdConfig(
            z_threshold=thresholds.get("z", 3.0),
            mad_threshold=thresholds.get("mad", 3.5),
            rate_threshold=thresholds.get("rate", 50.0),
            staleness_sec=thresholds.get("stalenessSec", 120.0),
            min_history_points=self.min_history_points,
        )

        # Scope
        self.scope = config.get("scope", {})

        # State
        self.previous_values: Dict[str, float] = {}
        self.history_cache: Dict[str, List[float]] = {}
        self.dedup_tracker: Dict[str, float] = {}  # tag -> last emit timestamp

        # Connections (initialized on start)
        self._graph: Optional[OntologyGraph] = None
        self._api: Optional[IgnitionApiClient] = None
        self._store: Optional[AnomalyStore] = None

    def start(self):
        """Start the monitoring loop."""
        self.running = True

        # Initialize connections
        try:
            self._graph = get_ontology_graph()
            self._store = AnomalyStore(self._graph)
            self._store.ensure_schema()
            self._api = IgnitionApiClient()
        except Exception as e:
            emit("error", {
                "runId": self.run_id,
                "code": "INIT_FAILED",
                "message": str(e),
                "recoverable": False,
            })
            return

        # Check Ignition API availability
        api_available = self._api.is_configured
        if not api_available:
            debug("Ignition API not configured - running in degraded mode")

        # Create run record
        try:
            self._store.create_run(self.run_id, json.dumps(self.config))
        except Exception as e:
            debug(f"Failed to create run record: {e}")

        # Discover tags to monitor
        try:
            tag_paths = discover_monitored_tags(self._graph, self.scope)
        except Exception as e:
            debug(f"Tag discovery failed: {e}")
            tag_paths = []

        if not tag_paths:
            debug("No tags discovered for monitoring")

        emit("status", {
            "runId": self.run_id,
            "state": "running",
            "tagCount": len(tag_paths),
            "apiAvailable": api_available,
        })

        # Emit start confirmation
        debug(f"Monitor started: run_id={self.run_id}, tags={len(tag_paths)}")

        # Main loop
        try:
            while self.running:
                cycle_start = time.time()
                self.cycle_count += 1

                try:
                    self._run_cycle(tag_paths)
                except Exception as e:
                    debug(f"Cycle {self.cycle_count} error: {e}")
                    traceback.print_exc(file=sys.stderr)
                    emit("error", {
                        "runId": self.run_id,
                        "code": "CYCLE_ERROR",
                        "message": str(e),
                        "recoverable": True,
                    })

                # Heartbeat
                try:
                    self._store.heartbeat(self.run_id, self.cycle_count)
                except Exception:
                    pass

                emit("status", {
                    "runId": self.run_id,
                    "state": "running",
                    "cycleCount": self.cycle_count,
                    "cycleMs": int((time.time() - cycle_start) * 1000),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

                # Sleep for interval
                elapsed_ms = (time.time() - cycle_start) * 1000
                sleep_ms = max(0, self.poll_interval_ms - elapsed_ms)
                if sleep_ms > 0 and self.running:
                    time.sleep(sleep_ms / 1000.0)

        except KeyboardInterrupt:
            debug("Monitor interrupted")
        finally:
            self._shutdown("completed")

    def stop(self):
        """Signal the monitor to stop."""
        self.running = False

    def _shutdown(self, reason: str):
        """Clean shutdown."""
        try:
            if self._store:
                self._store.stop_run(self.run_id, reason)
        except Exception:
            pass

        emit("complete", {
            "runId": self.run_id,
            "success": True,
            "reason": reason,
        })

        try:
            if self._graph:
                self._graph.close()
        except Exception:
            pass

    def _run_cycle(self, tag_paths: List[str]):
        """Execute one monitoring cycle."""
        if not tag_paths:
            return

        # Step 1: Read current values
        readings = []
        if self._api and self._api.is_configured:
            try:
                # Batch read in chunks of 50
                for i in range(0, len(tag_paths), 50):
                    chunk = tag_paths[i:i + 50]
                    tvs = self._api.read_tags(chunk)
                    for tv in tvs:
                        readings.append({
                            "path": tv.path,
                            "value": tv.value,
                            "quality": tv.quality,
                            "timestamp": tv.timestamp,
                        })
            except Exception as e:
                debug(f"Tag read failed: {e}")
                emit("error", {
                    "runId": self.run_id,
                    "code": "IGNITION_READ_FAILED",
                    "message": str(e),
                    "recoverable": True,
                })
                return

        if not readings:
            return

        # Step 2: Update history cache
        for reading in readings:
            path = reading["path"]
            if reading["value"] is not None:
                try:
                    val = float(reading["value"])
                    if path not in self.history_cache:
                        self.history_cache[path] = []
                    self.history_cache[path].append(val)
                    # Bound cache size (keep ~6h of data at 15s intervals = ~1440 points)
                    max_points = max(2000, self.min_history_points * 10)
                    if len(self.history_cache[path]) > max_points:
                        self.history_cache[path] = self.history_cache[path][-max_points:]
                except (TypeError, ValueError):
                    pass

        # Step 3: Score deviations (Stage A)
        all_scores = score_tag_batch(
            tag_readings=readings,
            history_map=self.history_cache,
            previous_values=self.previous_values,
            config=self.threshold_config,
        )

        # Update previous values
        for reading in readings:
            if reading["value"] is not None:
                try:
                    self.previous_values[reading["path"]] = float(reading["value"])
                except (TypeError, ValueError):
                    pass

        # Step 4: Filter candidates
        candidates = filter_candidates(all_scores, self.max_candidates)

        if not candidates:
            return

        # Step 5: Dedup check and LLM triage (Stage B)
        triaged_count = 0
        for candidate in candidates:
            if triaged_count >= self.max_llm_triages:
                break

            # Dedup: check cooldown
            dedup_key = f"{candidate.tag_path}:{candidate.category}"
            last_emit = self.dedup_tracker.get(dedup_key, 0)
            if time.time() - last_emit < DEDUP_COOLDOWN_SEC:
                continue

            # Check Neo4j dedup
            try:
                if self._store.check_dedup(dedup_key, DEDUP_COOLDOWN_SEC):
                    self.dedup_tracker[dedup_key] = time.time()
                    continue
            except Exception:
                pass

            # Gather ontology context
            try:
                context = gather_ontology_context(self._graph, candidate.tag_path)
            except Exception as e:
                debug(f"Ontology context failed for {candidate.tag_path}: {e}")
                context = {"tag_path": candidate.tag_path}

            # Run LLM triage
            try:
                triage = run_llm_triage(candidate, context)
            except Exception as e:
                debug(f"LLM triage failed: {e}")
                triage = {
                    "classification": candidate.category or "unknown",
                    "probable_causes": [],
                    "verification_checks": [],
                    "safety_relevant": bool(context.get("safety_elements")),
                    "urgency": "medium" if context.get("safety_elements") else "low",
                    "confidence_rationale": "Deterministic-only (LLM failed)",
                    "llm_available": False,
                }

            triaged_count += 1

            # Determine severity from urgency
            urgency = triage.get("urgency", "low")
            severity_map = {"critical": "critical", "high": "high", "medium": "medium", "low": "low"}
            severity = severity_map.get(urgency, "low")

            # Build summary
            summary = (
                f"{triage.get('classification', candidate.category or 'anomaly').title()} "
                f"detected on {candidate.tag_path}"
            )
            if candidate.z_score is not None:
                summary += f" (z={candidate.z_score:.1f})"

            # Build explanation
            explanation_parts = []
            if triage.get("probable_causes"):
                explanation_parts.append("Probable causes: " + "; ".join(triage["probable_causes"][:3]))
            if triage.get("confidence_rationale"):
                explanation_parts.append(triage["confidence_rationale"])
            explanation = " | ".join(explanation_parts) if explanation_parts else "No explanation available"

            # Persist event
            event_id = new_id()
            eq_name = context.get("equipment", [{}])[0].get("name") if context.get("equipment") else None

            try:
                self._store.persist_event(
                    run_id=self.run_id,
                    event_id=event_id,
                    severity=severity,
                    confidence=0.8 if triage.get("llm_available") else 0.5,
                    category=triage.get("classification", candidate.category or "unknown"),
                    summary=summary,
                    explanation=explanation,
                    recommended_checks=triage.get("verification_checks", []),
                    z_score=candidate.z_score,
                    mad_score=candidate.mad_score,
                    delta_rate=candidate.delta_rate,
                    source_tag=candidate.tag_path,
                    dedup_key=dedup_key,
                    equipment_name=eq_name,
                )
            except Exception as e:
                debug(f"Event persist failed: {e}")

            # Update dedup tracker
            self.dedup_tracker[dedup_key] = time.time()

            # Emit event to UI
            emit("event", {
                "runId": self.run_id,
                "eventId": event_id,
                "severity": severity,
                "category": triage.get("classification", candidate.category),
                "summary": summary,
                "explanation": explanation,
                "sourceTag": candidate.tag_path,
                "equipment": eq_name,
                "zScore": candidate.z_score,
                "madScore": candidate.mad_score,
                "safetyRelevant": triage.get("safety_relevant", False),
                "urgency": urgency,
                "checks": triage.get("verification_checks", []),
                "llmAvailable": triage.get("llm_available", False),
                "createdAt": datetime.now(timezone.utc).isoformat(),
            })


# ---------------------------------------------------------------------------
# CLI commands for Electron IPC
# ---------------------------------------------------------------------------

def cmd_start(config: Dict[str, Any]) -> Dict[str, Any]:
    """Start monitoring (blocks until stopped)."""
    monitor = AnomalyMonitor(config)

    # Handle SIGTERM/SIGINT gracefully
    def handle_signal(signum, frame):
        debug(f"Received signal {signum}, stopping monitor")
        monitor.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Emit start response immediately
    emit("status", {
        "runId": monitor.run_id,
        "state": "starting",
    })

    monitor.start()
    return {"success": True, "runId": monitor.run_id}


def cmd_list_events(filters: Dict[str, Any]) -> Dict[str, Any]:
    """List anomaly events."""
    try:
        graph = get_ontology_graph()
        store = AnomalyStore(graph)
        events = store.list_events(
            run_id=filters.get("runId"),
            state=filters.get("state"),
            severity=filters.get("severity"),
            limit=filters.get("limit", 50),
            offset=filters.get("offset", 0),
        )
        graph.close()
        return {"success": True, "events": events}
    except Exception as e:
        return {"success": False, "error": str(e)}


def cmd_get_event(event_id: str) -> Dict[str, Any]:
    """Get single event details."""
    try:
        graph = get_ontology_graph()
        store = AnomalyStore(graph)
        event = store.get_event(event_id)
        graph.close()
        if event:
            return {"success": True, "event": event}
        return {"success": False, "error": "Event not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Anomaly monitoring worker")
    parser.add_argument("command", choices=["start", "list-events", "get-event"],
                        help="Command to execute")
    parser.add_argument("--config", help="JSON config string (for start)")
    parser.add_argument("--filters", help="JSON filters (for list-events)")
    parser.add_argument("--event-id", help="Event ID (for get-event)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

    if args.command == "start":
        # Read config from stdin or --config
        if args.config:
            config = json.loads(args.config)
        else:
            raw = sys.stdin.read()
            config = json.loads(raw) if raw.strip() else {}

        cmd_start(config)

    elif args.command == "list-events":
        if args.filters:
            filters = json.loads(args.filters)
        else:
            raw = sys.stdin.read()
            filters = json.loads(raw) if raw.strip() else {}

        result = cmd_list_events(filters)
        print(json.dumps(result, default=str))

    elif args.command == "get-event":
        if not args.event_id:
            print(json.dumps({"success": False, "error": "Event ID required"}))
            sys.exit(1)
        result = cmd_get_event(args.event_id)
        print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
