from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
INTEGRATION_DIR = REPO_ROOT / "tests" / "integration"

for path in (SCRIPTS_DIR, INTEGRATION_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


@pytest.fixture
def sim_ignition():
    from simulated_ignition_server import (
        start_simulated_ignition_server,
        stop_simulated_ignition_server,
    )

    server, thread, state, base_url = start_simulated_ignition_server()
    try:
        yield {
            "server": server,
            "thread": thread,
            "state": state,
            "base_url": base_url,
        }
    finally:
        stop_simulated_ignition_server(server, thread)
