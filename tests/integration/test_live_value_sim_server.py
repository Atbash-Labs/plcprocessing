from datetime import datetime, timedelta, timezone

from anomaly_rules import compute_deviation_scores
from ignition_api_client import IgnitionApiClient

def test_read_tags_history_and_detect_spike(sim_ignition):
    state = sim_ignition["state"]
    state.fail_live_reads = False
    state.fail_history_reads = False

    client = IgnitionApiClient(base_url=sim_ignition["base_url"], api_token="token")
    try:
        tag_path = "[default]Line/Throughput"
        tv = client.read_tag(tag_path)
        assert tv.error is None
        assert tv.quality == "Good"
        assert float(tv.value) == 95.0

        start = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat()
        end = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        history = client.query_tag_history([tag_path], start, end, return_size=100)
        assert isinstance(history, dict)
        assert "rows" in history

        history_values = [
            row[tag_path]
            for row in history["rows"]
            if isinstance(row, dict) and tag_path in row and row[tag_path] is not None
        ]
        assert len(history_values) > 5

        score = compute_deviation_scores(
            current_value=tv.value,
            history_values=history_values,
            prev_value=55.0,
            thresholds={"z": 3.0, "mad": 3.5, "rate": 10.0},
        )
        assert score["candidate"]
        assert score["category"] in {"spike", "deviation", "drift"}
    finally:
        client.close()


def test_live_provider_failure_surfaces_as_read_error(sim_ignition):
    state = sim_ignition["state"]
    state.fail_live_reads = True

    client = IgnitionApiClient(base_url=sim_ignition["base_url"], api_token="token")
    try:
        tv = client.read_tag("[default]Line/Throughput")
        assert tv.error is not None
        assert "failed" in tv.error.lower()
    finally:
        client.close()


def test_history_provider_failure_surfaces_error_payload(sim_ignition):
    state = sim_ignition["state"]
    state.fail_history_reads = True

    client = IgnitionApiClient(base_url=sim_ignition["base_url"], api_token="token")
    try:
        start = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat()
        end = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        history = client.query_tag_history(
            ["[default]Line/Throughput"],
            start,
            end,
            return_size=100,
        )
        assert isinstance(history, dict)
        assert "error" in history
    finally:
        client.close()

