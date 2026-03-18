# PLC Ontology Assistant

A full-stack platform for PLC/SCADA knowledge management, combining multi-vendor PLC DevOps tooling with a Neo4j-backed ontology graph, AI-assisted troubleshooting, live anomaly monitoring, and an Electron desktop UI.

## Key Capabilities

- **Multi-vendor PLC DevOps** -- Export, diff, merge, and import PLC code for CODESYS, Rockwell/Allen-Bradley, and Siemens TIA Portal / AX
- **Knowledge Graph** -- Neo4j ontology covering PLC logic, SCADA tags, HMI views, equipment hierarchies, and MES/ISA-95 constructs
- **AI Troubleshooting** -- Claude-powered agent with full graph context, live tag reads, and anomaly history
- **Anomaly Monitoring** -- Per-subsystem live tag monitoring with configurable scoring rules and optional LLM triage
- **Ignition SCADA Integration** -- Gateway REST API client for live/historical tag values, alarm data, and incremental ontology enrichment
- **MES / ISA-95** -- Batch, material, equipment, CCP, and process-deviation modelling at Levels 3-4
- **MCP Server** -- Exposes ontology tools over Model Context Protocol for Claude Desktop, Cursor, or other MCP clients
- **Desktop App** -- Electron UI for graph visualization, CRUD editing, troubleshooting chat, and monitoring dashboards

## Project Structure

```
.
├── electron-ui/              # Electron desktop application
│   ├── main.js               #   Main process, IPC, Python spawn
│   ├── preload.js            #   contextBridge API
│   ├── renderer.js           #   Renderer logic
│   ├── graph-renderer.js     #   Graph visualization
│   ├── index.html / styles.css
│   └── package.json
│
├── scripts/                  # Python backend (~50 modules)
│   │
│   │ ── PLC Export / Import ──
│   ├── codesys_export.py            # Export via CODESYS scripting API
│   ├── codesys_export_from_xml.py   # Export from PLCOpenXML (no CODESYS needed)
│   ├── codesys_import.py            # Import into CODESYS project
│   ├── codesys_import_external.py   # Headless import wrapper
│   ├── codesys_diff.py              # Generate unified diffs
│   ├── codesys_apply.py             # Apply diffs to text files
│   ├── l5x_export.py               # Rockwell L5X → .sc export
│   ├── sc_parser.py                 # Parse Rockwell .sc files
│   ├── siemens_parser.py            # Parse Siemens .st files
│   ├── siemens_project_parser.py    # Parse Siemens TIA project trees
│   ├── tia_xml_parser.py            # Parse Siemens TIA XML exports
│   │
│   │ ── Ontology / Graph ──
│   ├── neo4j_ontology.py            # Neo4j graph interface (core)
│   ├── ontology_analyzer.py         # LLM-based PLC code analysis
│   ├── unified_ontology.py          # Merge PLC + SCADA ontologies
│   ├── ignition_ontology.py         # Ignition backup → ontology
│   ├── ignition_parser.py           # Parse Ignition project exports
│   ├── workbench_parser.py          # Axilon Workbench backup parser
│   ├── workbench_ingest.py          # Axilon Workbench → Neo4j ingest
│   ├── process_semantics.py         # Process-semantic layer
│   ├── ontology_viewer.py           # HTML graph visualization
│   ├── ontology_graphviz.py         # GraphViz DOT output
│   ├── mes_ontology.py              # MES/ISA-95 schema extension
│   ├── migrate_to_neo4j.py          # JSON → Neo4j migration
│   │
│   │ ── AI / Troubleshooting ──
│   ├── claude_client.py             # Claude API client with Neo4j tools
│   ├── troubleshoot.py              # Interactive troubleshooting CLI
│   ├── troubleshooting_ontology.py  # Troubleshooting enrichment
│   ├── rca_enrichment.py            # Root-cause-analysis enrichment
│   ├── sif_enrichment.py            # SIF RCA enrichment
│   │
│   │ ── Live Data / Monitoring ──
│   ├── ignition_api_client.py       # Ignition gateway REST client
│   ├── live_enricher.py             # Enrich ontology with live values
│   ├── anomaly_monitor.py           # Per-subsystem anomaly monitor
│   ├── anomaly_rules.py             # Anomaly scoring rules
│   │
│   │ ── MES / ERP ──
│   ├── load_pharma_sample.py        # Pharma MES sample data
│   ├── load_proveit_sample.py       # ProveIT sample data
│   ├── load_safety_aois.py          # Safety AOI data
│   ├── oee_sample_data.py           # OEE sample data
│   │
│   │ ── Integration ──
│   ├── mcp_server.py                # MCP server (stdio / SSE)
│   ├── graph_api.py                 # Graph CRUD API for Electron UI
│   ├── dispatcher.py                # PyInstaller entry point
│   ├── db_client.py                 # SQL database client
│   ├── artifact_ingest.py           # P&ID / SOP ingestion (GPT)
│   ├── artifact_linker.py           # Link artifacts to ontology
│   ├── artifact_models.py           # Artifact data models
│   ├── dexpi_converter.py           # DEXPI P&ID conversion
│   ├── diff_processor.py            # Ignition diff processing
│   └── incremental_analyzer.py      # Incremental Ignition analysis
│
├── codesys-export.ps1        # PowerShell driver scripts
├── codesys-diff.ps1
├── codesys-apply.ps1
├── codesys-import.ps1
├── codesys-merge.ps1
├── l5x-export.ps1
├── l5x-ontology.ps1
├── siemens-export.ps1
├── build.ps1                 # Full build (PyInstaller + Electron)
│
├── tests/                    # Unit and integration tests
├── docs/                     # Additional documentation
├── PLC/                      # Sample Rockwell L5X files
├── legacy/                   # Deprecated XML-based tools
│
├── requirements.txt          # Python dependencies
├── requirements-dev.txt      # Dev dependencies (pytest)
├── pyinstaller.spec          # PyInstaller spec
└── pytest.ini                # Pytest config
```

## Getting Started

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.10+ | |
| Node.js | 18+ | For Electron UI |
| Neo4j | 5.x | Community or Enterprise |
| CODESYS | V3.5 SP21+ | Only for CODESYS import/export |
| PowerShell | 5.1+ | For driver scripts |

### Installation

```bash
# Clone the repo
git clone <repo-url> && cd plcprocessing

# Create a virtual environment and install dependencies
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt

# (Optional) Install dev dependencies
pip install -r requirements-dev.txt
```

### Configuration

Create a `.env` file in the project root:

```ini
# Neo4j (required for ontology features)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

# Anthropic Claude (required for AI features)
ANTHROPIC_API_KEY=sk-ant-...

# Ignition SCADA (optional)
IGNITION_API_URL=http://localhost:9074
IGNITION_API_TOKEN=              # optional Bearer token

# OpenAI (optional, for artifact extraction)
OPENAI_API_KEY=sk-...
```

### Run the Electron UI

```bash
cd electron-ui
npm install
npm start                     # dev mode
```

### Run Tests

```bash
python -m pytest              # all tests
python -m pytest tests/unit   # unit tests only
```

## PLC DevOps Workflows

### Why These Tools?

CODESYS V3.x GUI PLCOpenXML export/import is broken -- exported XML files cannot be re-imported reliably. These tools use the CODESYS Scripting API for direct project manipulation, giving you a reliable text-based workflow for version control with full support for additions, modifications, and deletions.

### CODESYS

```powershell
# Export from PLCOpenXML (no CODESYS installation needed)
.\codesys-export.ps1 -FromXml "MyProject.xml" -OutputDir ".\export"

# Generate diff between two exports
.\codesys-diff.ps1 -BaseDir ".\export_v1" -TargetDir ".\export_v2" -OutputDir ".\diffs"

# Apply diffs to an export
.\codesys-apply.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -OutputDir ".\merged"

# Import into CODESYS project (preview first!)
.\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\merged" -DryRun
.\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\merged"

# One-command merge workflow
.\codesys-merge.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -ProjectPath "MyProject.project" -DryRun
```

### Rockwell / Allen-Bradley

```powershell
# Export L5X files to structured code (.sc)
.\l5x-export.ps1 -Input "Motor_Control.L5X" -OutputDir ".\export"
.\l5x-export.ps1 -Input ".\PLC" -OutputDir ".\export"

# Run ontology analysis on exported files
.\l5x-ontology.ps1 -Input ".\export" -OutputDir ".\analysis"
```

### Siemens TIA Portal / AX

```powershell
# Parse Siemens ST files
.\siemens-export.ps1 -Input ".\siemens_project"

# Parse and run ontology analysis
.\siemens-export.ps1 -Input ".\siemens_project" -Analyze
```

### Authoritative Import Behavior

The CODESYS import is **authoritative** -- the import directory is the source of truth:

- POUs/GVLs not in import directory are **deleted** from the project
- Methods not in import directory are **deleted** from their POU
- GVL content is **fully replaced** (variables not in file are removed)

Always use `--dry-run` / `-DryRun` to preview before applying.

## Ontology & Knowledge Graph

The Neo4j knowledge graph models the full hierarchy of a plant's automation stack:

| Layer | Node Types |
|-------|------------|
| **PLC Logic** | AOI, Program, FunctionBlock, Tag, UDT, Routine |
| **SCADA / HMI** | View, Component, Binding, Alarm, AlarmPipeline |
| **Equipment** | Equipment, Subsystem, Instrument |
| **Troubleshooting** | FaultSymptom, FaultCause, RepairAction |
| **MES / ISA-95** | Material, Batch, ProductionOrder, CCP, ProcessDeviation |
| **Siemens** | TiaProject, PLCDevice, HMIDevice, HMIAlarm |

### Ingest Sources

```bash
# Rockwell L5X → ontology
python scripts/ontology_analyzer.py ".\PLC" -v

# Siemens ST → ontology
python scripts/ontology_analyzer.py ".\siemens_project" --siemens -v

# Ignition SCADA backup → ontology
python scripts/ignition_ontology.py "ignition_backup.zip"

# Axilon Workbench backup → ontology
python scripts/workbench_ingest.py "workbench_backup/"

# Merge PLC + SCADA into unified graph
python scripts/unified_ontology.py

# Load MES sample data
python scripts/load_pharma_sample.py
```

### Visualization

```bash
# Interactive HTML visualization
python scripts/ontology_viewer.py

# GraphViz DOT export
python scripts/ontology_graphviz.py -o graph.dot
```

## AI Troubleshooting

The Claude-based troubleshooting agent has access to the full ontology graph, live Ignition tag values, anomaly event history, and MES data. It uses Claude's tool-use capability to dynamically query the graph and build context.

```bash
# Interactive CLI
python scripts/troubleshoot.py "Why is ConveyorMotor01 faulting?"

# Programmatic usage
python -c "
from scripts.claude_client import ClaudeClient
client = ClaudeClient()
response = client.chat('What alarms are active on Line 2?')
print(response)
"
```

Available tools the agent can use:
- `get_schema` -- Discover graph node labels, relationship types, and properties
- `run_query` -- Execute Cypher queries against the ontology
- `get_node` -- Fetch a specific node by label and name
- `read_tag_live` / `read_tag_history` -- Read live and historical Ignition tag values
- `list_anomaly_events` / `score_anomaly` -- Query and score anomaly events
- `run_db_query` -- Execute read-only SQL queries against connected databases

## Anomaly Monitoring

Per-subsystem live monitoring that discovers equipment from the ontology, polls Ignition tags, scores deviations, and optionally triages anomalies with Claude.

```bash
# Start the monitor
python scripts/anomaly_monitor.py run --run-id production-line-1

# List persisted anomaly events
python scripts/anomaly_monitor.py list-events

# Deep-analyze an event with LLM
python scripts/anomaly_monitor.py deep-analyze --event-id <uuid>

# Acknowledge / clear events
python scripts/anomaly_monitor.py ack-event --event-id <uuid>
python scripts/anomaly_monitor.py clear-event --event-id <uuid>
```

## MCP Server

Exposes the same ontology tools the Claude agent uses internally, so any MCP-compatible client can query the graph, read live tags, and manage anomaly events.

```bash
# stdio transport (default, for Claude Desktop / Cursor)
python scripts/mcp_server.py

# SSE transport on port 8080
python scripts/mcp_server.py --transport sse
```

## File Formats

### CODESYS Exported Text Files

| Extension | Type |
|-----------|------|
| `NAME.prg.st` | Program |
| `NAME.fb.st` | Function Block |
| `NAME.fun.st` | Function |
| `NAME.gvl.st` | Global Variable List |
| `POU_METHOD.meth.st` | Method (in POU) |

### Rockwell/Allen-Bradley Exported Files

| Extension | Type |
|-----------|------|
| `NAME.aoi.sc` | Add-On Instruction |
| `NAME.udt.sc` | User Defined Type |

### Siemens TIA Portal / AX Source Constructs

| Construct | Maps To |
|-----------|---------|
| `CLASS ... END_CLASS` | Function Block (FB) |
| `TYPE ... STRUCT ... END_TYPE` | User Defined Type (UDT) |
| `PROGRAM ... END_PROGRAM` | Program |
| `CONFIGURATION ... END_CONFIGURATION` | Configuration |
| `METHOD ... END_METHOD` | Routine (within a CLASS) |
| `NAMESPACE ... END_NAMESPACE` | Grouping / package |

### Diff Files

| Extension | Meaning |
|-----------|---------|
| `*.diff` | Unified diff (modifications) |
| `*.added` | New file content |
| `*.removed` | Marker for deletion |
| `diff_summary.txt` | Summary statistics |

## Building the Desktop App

The `build.ps1` script creates a distributable package by bundling the Python backend with PyInstaller and packaging the Electron app with electron-builder.

```powershell
.\build.ps1                   # Full build
.\build.ps1 -SkipPython       # Rebuild Electron only
.\build.ps1 -SkipElectron     # Rebuild Python backend only
```

Output:
- `build\python-backend\` -- Bundled Python backend
- `dist\win-unpacked\PLC Ontology Assistant.exe` -- Standalone app

Place your `.env` file next to the `.exe` when running the packaged app.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | Python 3.10+ |
| Graph Database | Neo4j 5.x |
| AI | Anthropic Claude (troubleshooting, analysis), OpenAI GPT (artifact extraction) |
| Desktop App | Electron 28 |
| SCADA | Ignition (gateway REST API) |
| Build | PyInstaller, electron-builder |
| PLC Platforms | CODESYS, Rockwell Logix 5000, Siemens TIA Portal / AX |

## Documentation

- [Import Instructions](docs/import_instructions.md) -- Detailed CODESYS import guide
- [Ontology Evolution Plan](docs/ontology_evolution_plan.md) -- Roadmap for graph schema changes
- [Workbench Ingest Spec](docs/workbench_ingest_spec.md) -- Axilon Workbench ingest specification
- [Agents & Monitoring Handoff](docs/agents_monitoring_handoff.md) -- Agent architecture and monitoring handoff

## License

MIT
