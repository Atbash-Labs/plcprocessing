# MES/ERP Ontology Extension - Integration Guide

This extension adds ISA-95 Level 3-4 (MES/ERP) capabilities to the existing PLC/SCADA ontology.

## Quick Start

```bash
# 1. Create MES schema
python mes_ontology.py --create-schema

# 2. Load sample pharma data
python load_pharma_sample.py --load-all -v

# 3. Link Equipment to your existing AOIs
python load_pharma_sample.py --link-equipment -v

# 4. (Optional) Run RCA enrichment on equipment
python rca_enrichment.py --enrich-all-equipment -v
```

## Files Added

| File | Purpose |
|------|---------|
| `mes_ontology.py` | Core extension - adds MES methods to OntologyGraph + new tools for ClaudeClient |
| `rca_enrichment.py` | RCA enrichment generator - uses Claude to add troubleshooting context |
| `load_pharma_sample.py` | Sample data loader - creates Axilumab mAb production data |

## Architecture

### New Node Types

```
Material          - SAP material master (raw materials, intermediates, finished products)
Batch             - Production batches with quality status
ProductionOrder   - Manufacturing orders
Operation         - Steps in production orders
CriticalControlPoint (CCP) - GMP-critical parameters with limits
ProcessDeviation  - Quality deviations (created when they occur)
FunctionalLocation - Plant hierarchy
Vendor            - Qualified suppliers
```

### Key Integration Relationship

```
Equipment -[CONTROLLED_BY]-> AOI
```

This is the bridge between MES and PLC layers. It enables queries like:
- "What batches were affected by this PLC fault?"
- "Which tags should I check for this CCP violation?"

### New Relationships

```
Equipment -[CONTROLLED_BY]-> AOI           # MES → PLC bridge
Equipment -[LOCATED_IN]-> FunctionalLocation
CriticalControlPoint -[MONITORED_BY]-> Equipment
Operation -[EXECUTED_ON]-> Equipment
ProductionOrder -[PRODUCES]-> Material
Batch -[BATCH_OF]-> Material
ProcessDeviation -[AFFECTS_BATCH]-> Batch
ProcessDeviation -[OCCURRED_ON]-> Equipment
ProcessDeviation -[VIOLATES]-> CriticalControlPoint
```

## Usage

### Extend Existing Graph

```python
from neo4j_ontology import get_ontology_graph
from mes_ontology import extend_ontology

graph = get_ontology_graph()
extend_ontology(graph)

# Now graph has MES methods:
graph.create_material("MAT-001", "Widget", "FERT")
graph.create_batch("BATCH-001", "MAT-001", 100.0)
graph.create_ccp("CCP-001", "Temperature", 37.0, 36.0, 38.0, equipment_name="BR-500-001")

# And RCA query methods:
context = graph.get_batch_rca_context("BATCH-001")
equipment = graph.get_equipment_rca_context("BR-500-001")
results = graph.search_by_symptom_extended("temperature drifting")
```

### Add MES Tools to Claude Client

```python
from claude_client import ClaudeClient
from mes_ontology import integrate_with_claude_client, MES_TOOL_DEFINITIONS

client = ClaudeClient(enable_tools=True)
integrate_with_claude_client(client)

# Add MES tool definitions to your tool list
all_tools = OntologyTools.TOOL_DEFINITIONS + MES_TOOL_DEFINITIONS

# Now Claude can use:
# - get_batch_context
# - get_equipment_rca
# - get_ccp_context
# - search_by_symptom
# - trace_tag_impact
# - get_process_ccps
# - get_open_deviations
```

### Update System Prompt

Add this to your Claude system prompt:

```python
from mes_ontology import MES_SYSTEM_PROMPT_EXTENSION

system_prompt = EXISTING_SYSTEM_PROMPT + MES_SYSTEM_PROMPT_EXTENSION
```

## RCA Enrichment

RCA enrichments are stored as properties ON existing nodes, not as parallel structures:

```python
# Equipment gains:
equipment.rca_enriched = true
equipment.rca_failure_modes = [...]      # JSON array
equipment.rca_operator_observations = [...] # What operators say
equipment.rca_diagnostic_sequence = [...]   # Steps referencing actual tags

# CCP gains:
ccp.rca_enriched = true
ccp.rca_violation_scenarios = [...]  # Ways it could be violated
ccp.rca_operator_language = [...]    # How violations are described
ccp.rca_diagnostic_steps = [...]     # Investigation steps
```

### Generate Enrichments

```python
from rca_enrichment import RCAEnrichmentGenerator

generator = RCAEnrichmentGenerator()

# Enrich specific equipment
generator.enrich_equipment("BR-500-001", verbose=True)

# Enrich all CCPs
generator.enrich_all_ccps(verbose=True)
```

## Sample Queries

### Get Batch Investigation Context

```cypher
MATCH (b:Batch {charg: 'HCC2601001'})
OPTIONAL MATCH (po:ProductionOrder {batch: b.charg})-[:HAS_OPERATION]->(op:Operation)
OPTIONAL MATCH (op)-[:EXECUTED_ON]->(e:Equipment)
OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(aoi:AOI)
OPTIONAL MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
OPTIONAL MATCH (c:CriticalControlPoint)-[:MONITORED_BY]->(e)
RETURN b, po, collect(DISTINCT op) as operations, 
       collect(DISTINCT e.name) as equipment,
       collect(DISTINCT tag.name) as plc_tags,
       collect(DISTINCT c.ccp_id) as ccps
```

### Trace Tag to Business Impact

```cypher
MATCH (tag:Tag {name: 'BR500_01_Temperature_PV'})
MATCH (aoi:AOI)-[:HAS_TAG]->(tag)
MATCH (e:Equipment)-[:CONTROLLED_BY]->(aoi)
OPTIONAL MATCH (c:CriticalControlPoint)-[:MONITORED_BY]->(e)
OPTIONAL MATCH (op:Operation)-[:EXECUTED_ON]->(e)
OPTIONAL MATCH (po:ProductionOrder)-[:HAS_OPERATION]->(op)
RETURN tag.name, aoi.name, e.name, 
       collect(DISTINCT c.ccp_id) as affected_ccps,
       collect(DISTINCT po.aufnr) as affected_orders
```

### Find Equipment for CCP Violation

```cypher
MATCH (c:CriticalControlPoint {ccp_id: 'CCP-BR-TEMP'})
MATCH (c)-[:MONITORED_BY]->(e:Equipment)
MATCH (e)-[:CONTROLLED_BY]->(aoi:AOI)
MATCH (aoi)-[:HAS_TAG]->(tag:Tag)
WHERE tag.name CONTAINS 'Temperature'
RETURN c, e.name, aoi.name, collect(tag.name) as tags_to_check
```

## Linking to Existing AOIs

After loading PLC data with your existing ontology generator, run:

```bash
python load_pharma_sample.py --link-equipment -v
```

This creates `Equipment -[CONTROLLED_BY]-> AOI` relationships based on naming patterns.

You can customize the mappings in `load_pharma_sample.py`:

```python
potential_mappings = {
    "BR-500-001": ["Bioreactor_Control", "BR_Control"],  # Equipment -> possible AOI names
    "CHR-PA-001": ["Chromatography_Control", "CHR_Control"],
    # ...
}
```

## Testing

```bash
# Test MES queries
python mes_ontology.py --test-queries

# Check schema
python -c "
from neo4j_ontology import get_ontology_graph
from mes_ontology import extend_ontology

g = get_ontology_graph()
extend_ontology(g)

# Test query
result = g.get_batch_rca_context('HCC2601001')
print(result)
g.close()
"
```

## Philosophy

This extension follows Axilon's architecture:

1. **Deterministic extraction**: Node structure comes from actual configs (SAP exports, etc.)
2. **Semantic enrichment**: Claude adds "why" and "how" through RCA enrichment
3. **Unified identity**: Equipment is the canonical link between MES and PLC
4. **Properties on nodes**: RCA enrichments are stored on existing nodes, not as parallel structures
