#!/usr/bin/env python3
"""
Simulated Ignition WebDev endpoints for local integration tests.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Tuple
from urllib.parse import parse_qs, urlparse


def _utc_iso(offset_minutes: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()


@dataclass
class SimulatedIgnitionState:
    fail_live_reads: bool = False
    fail_history_reads: bool = False
    live_tags: Dict[str, Dict] = field(default_factory=dict)
    tag_history: Dict[str, List[Tuple[str, float]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.live_tags:
            self.live_tags = {
                "[default]Line/Throughput": {
                    "value": 95.0,
                    "quality": "Good",
                    "timestamp": _utc_iso(),
                    "dataType": "Float8",
                },
                "[default]Line/Temperature": {
                    "value": 42.0,
                    "quality": "Good",
                    "timestamp": _utc_iso(),
                    "dataType": "Float8",
                },
            }
        if not self.tag_history:
            base = [49.9, 50.1, 50.0, 50.2, 50.1, 49.8, 50.3, 50.0, 49.9, 50.2]
            self.tag_history = {
                "[default]Line/Throughput": [
                    (_utc_iso(offset_minutes=-(len(base) - i)), value)
                    for i, value in enumerate(base)
                ],
                "[default]Line/Temperature": [
                    (_utc_iso(offset_minutes=-(len(base) - i)), 41.5 + (i * 0.1))
                    for i in range(len(base))
                ],
            }


class _IgnitionHandler(BaseHTTPRequestHandler):
    state: SimulatedIgnitionState

    def _send_json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler naming
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/system/webdev/Axilon/getTags":
            if self.state.fail_live_reads:
                self._send_json({"error": "simulated live provider failure"}, status=503)
                return

            raw = query.get("tagPaths", [""])[0]
            tag_paths = [p.strip() for p in raw.split(",") if p.strip()]
            tags = []
            for tag_path in tag_paths:
                data = self.state.live_tags.get(tag_path)
                if not data:
                    tags.append(
                        {
                            "tagPath": tag_path,
                            "value": None,
                            "quality": "Bad",
                            "isGood": False,
                            "timestamp": _utc_iso(),
                            "dataType": "Unknown",
                        }
                    )
                    continue
                tags.append(
                    {
                        "tagPath": tag_path,
                        "value": data.get("value"),
                        "quality": data.get("quality", "Good"),
                        "isGood": str(data.get("quality", "Good")).lower() == "good",
                        "timestamp": data.get("timestamp", _utc_iso()),
                        "dataType": data.get("dataType", "Unknown"),
                    }
                )
            self._send_json({"success": True, "count": len(tags), "tags": tags})
            return

        if path == "/system/webdev/Axilon/queryTagHistory":
            if self.state.fail_history_reads:
                self._send_json({"error": "simulated history provider failure"}, status=503)
                return

            raw = query.get("tagPaths", [""])[0]
            tag_paths = [p.strip() for p in raw.split(",") if p.strip()]
            rows = []

            primary_path = tag_paths[0] if tag_paths else "[default]Line/Throughput"
            primary_hist = self.state.tag_history.get(primary_path, [])
            for ts, _ in primary_hist:
                row = {"timestamp": ts}
                for tag_path in tag_paths:
                    values = self.state.tag_history.get(tag_path, [])
                    match_val = None
                    for hist_ts, hist_val in values:
                        if hist_ts == ts:
                            match_val = hist_val
                            break
                    if match_val is None and values:
                        match_val = values[-1][1]
                    row[tag_path] = match_val
                rows.append(row)

            self._send_json(
                {
                    "success": True,
                    "rows": rows,
                    "tagPaths": tag_paths,
                    "returnFormat": "Wide",
                }
            )
            return

        self._send_json({"error": f"unsupported endpoint: {path}"}, status=404)

    def log_message(self, format, *args):  # noqa: A003 - stdlib signature
        # Silence default HTTP request logs during tests.
        return


def start_simulated_ignition_server() -> tuple[HTTPServer, threading.Thread, SimulatedIgnitionState, str]:
    state = SimulatedIgnitionState()
    handler_cls = type(
        "IgnitionHandlerWithState",
        (_IgnitionHandler,),
        {"state": state},
    )
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    return server, thread, state, base_url


def stop_simulated_ignition_server(server: HTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=3)

