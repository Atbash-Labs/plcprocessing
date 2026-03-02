# Test Flow: Agents Monitoring + Ingest

This repository now includes a lightweight test scaffold using `pytest`.

## Layout

- `tests/unit/`
  - `test_anomaly_rules.py`  
    Unit tests for deterministic anomaly scoring and quality/staleness gates.
  - `test_ingest_workbench_parser.py`  
    Unit tests for workbench ingest parsing.
  - `test_ingest_siemens_parser.py`  
    Unit tests for Siemens `.st` ingest parsing.

- `tests/integration/`
  - `simulated_ignition_server.py`  
    Local simulated live/history webserver implementing:
    - `/system/webdev/Axilon/getTags`
    - `/system/webdev/Axilon/queryTagHistory`
  - `test_live_value_sim_server.py`  
    Integration tests for `IgnitionApiClient` + anomaly scoring with simulated live values.

## Run all tests

```bash
python3 -m pytest
```

## Run only unit tests

```bash
python3 -m pytest tests/unit
```

## Run only integration tests

```bash
python3 -m pytest tests/integration
```

## Notes

- Integration tests are fully local and do **not** require a real Ignition gateway.
- LLM services are not required for these tests.
- Neo4j is not required for this test suite.

