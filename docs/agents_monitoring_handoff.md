# Long-Running Agents Monitoring Handoff

## Summary

This handoff documents the implemented V1 monitoring capability:

- New **Agents** tab in Electron UI for starting/stopping long-running monitoring.
- Continuous Python worker (`anomaly_monitor.py`) with:
  - deterministic historical-deviation scoring,
  - quality/staleness gates,
  - optional LLM triage,
  - Neo4j persistence for `AgentRun` and `AnomalyEvent`,
  - event dedup and retention cleanup.
- IPC surface and stream channels from Electron main to renderer:
  - `agents:start`, `agents:status`, `agents:stop`,
  - `agents:list-events`, `agents:get-event`, `agents:ack-event`, `agents:cleanup`,
  - channels: `agent-status`, `agent-event`, `agent-error`, `agent-complete`.
- Graph drill-down integration with anomaly node support.

## Files Changed

### Electron

- `electron-ui/index.html`
  - Added **Agents** nav button.
  - Added `tab-agents` page shell with controls, filters, feed, and detail panel.
  - Added graph filter option for anomaly layer.

- `electron-ui/styles.css`
  - Added Agents tab styles (`agents-*`, `status-chip`, feed cards, detail panel).

- `electron-ui/preload.js`
  - Added `agents*` API bridge methods.
  - Added event listeners for `agent-status/event/error/complete`.

- `electron-ui/main.js`
  - Added background agent runtime management (`activeAgentRun`).
  - Added stream parser for monitor stdout markers (`[AGENT_STATUS]`, etc.).
  - Added full `agents:*` IPC handlers.
  - Added graceful stop handling on app shutdown.

- `electron-ui/renderer.js`
  - Added Agents tab state management.
  - Added start/stop/refresh/cleanup/ack handlers.
  - Added realtime feed updates from agent channels.
  - Added event detail rendering and graph drill-down action.

### Python backend

- `scripts/anomaly_rules.py` (new)
  - Deterministic scoring logic (`z`, `MAD`, rate, drift trend, flatline).
  - Quality/staleness helpers and dedup key generator.

- `scripts/anomaly_monitor.py` (new)
  - Long-running monitoring worker with CLI subcommands:
    - `run`, `status`, `list-events`, `get-event`, `ack-event`, `cleanup`, `replay-fixtures`.
  - Neo4j persistence + dedup + retention cleanup.
  - Optional LLM triage with structured JSON fallback.

- `scripts/ignition_api_client.py`
  - Added `query_tag_history(...)` and local-time-to-UTC conversion helper.

- `scripts/neo4j_ontology.py`
  - Added monitoring schema constraints/indexes for `AgentRun` / `AnomalyEvent`.
  - Added helper methods: list/get/cleanup anomaly events.
  - Added CLI commands:
    - `init-agent-schema`
    - `list-anomaly-events`
    - `get-anomaly-event`
    - `cleanup-anomaly-events`

- `scripts/graph_api.py`
  - Added node groups/colors for `AgentRun` and `AnomalyEvent`.
  - Extended neighbor center-node lookup to support `event_id` and `run_id`.

### Fixtures

- `scripts/fixtures/anomaly_replay_cases.json` (new)
  - Deterministic replay cases:
    - normal baseline,
    - sudden spike,
    - slow drift,
    - flatline/stuck.

## Runtime Commands

### Deterministic replay validation

```bash
python3 scripts/anomaly_monitor.py replay-fixtures --fixture-file scripts/fixtures/anomaly_replay_cases.json
```

### Monitor worker manual run

```bash
python3 scripts/anomaly_monitor.py run --run-id demo-run --config-json '{"pollIntervalMs":1000}'
```

### Event operations

```bash
python3 scripts/anomaly_monitor.py list-events --limit 50
python3 scripts/anomaly_monitor.py get-event --event-id <event_id>
python3 scripts/anomaly_monitor.py ack-event --event-id <event_id> --note "Reviewed by operator"
python3 scripts/anomaly_monitor.py cleanup --retention-days 14
```

## Known Environment Requirements

The Python environment must include packages from `requirements.txt`:

- `neo4j`
- `anthropic` (for LLM triage; deterministic fallback works without API key)
- `python-dotenv`
- `requests`

If `ANTHROPIC_API_KEY` is absent, triage automatically falls back to deterministic explanations.

## Validation Status

- Syntax checks passed:
  - Python (`py_compile`) for all modified scripts.
  - JS syntax checks (`node --check`) for Electron files.
- Fixture replay passed:
  - `4/4` deterministic scenarios.

Live end-to-end validation against actual Ignition + Neo4j + Anthropic requires connected runtime services.

