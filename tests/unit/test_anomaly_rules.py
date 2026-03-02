from datetime import datetime, timedelta, timezone

import pytest

from anomaly_rules import compute_deviation_scores, is_quality_good, is_stale, parse_timestamp


def test_detects_sharp_rise_and_sharp_drop():
    baseline = [50.0, 49.9, 50.1, 50.2, 49.8, 50.0, 50.1, 49.9, 50.0, 50.2] * 3

    rise = compute_deviation_scores(
        current_value=95.0,
        history_values=baseline,
        prev_value=52.0,
        thresholds={"z": 3.0, "mad": 3.5, "rate": 10.0},
    )
    drop = compute_deviation_scores(
        current_value=12.0,
        history_values=baseline,
        prev_value=49.0,
        thresholds={"z": 3.0, "mad": 3.5, "rate": 10.0},
    )

    assert rise["candidate"]
    assert drop["candidate"]


def test_detects_flatline_stuck_pattern():
    flat = [72.0] * 30
    result = compute_deviation_scores(
        current_value=72.0,
        history_values=flat,
        prev_value=72.0,
        thresholds={"z": 3.0, "mad": 3.5, "rate": 1.0, "stuck_window_size": 20},
    )
    assert result["candidate"]
    assert "flatline_detected" in result["reasons"]
    assert result["category"] == "stuck"


@pytest.mark.parametrize(
    "quality,expected",
    [("Good", True), ("OK", True), ("Bad", False), (None, False)],
)
def test_quality_helper(quality, expected):
    assert is_quality_good(quality) is expected


def test_staleness_helper():
    recent_ts = datetime.now(timezone.utc).isoformat()
    old_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
    assert not is_stale(recent_ts, staleness_sec=300)
    assert is_stale(old_ts, staleness_sec=300)


def test_staleness_accepts_epoch_seconds_and_millis():
    now = datetime.now(timezone.utc)
    recent = int(now.timestamp())
    recent_ms = int(now.timestamp() * 1000)
    assert not is_stale(str(recent), staleness_sec=300, now=now)
    assert not is_stale(str(recent_ms), staleness_sec=300, now=now)


def test_parse_timestamp_naive_assumed_local_time():
    local_now = datetime.now().replace(microsecond=0)
    parsed = parse_timestamp(local_now.isoformat())
    assert parsed is not None


def test_non_numeric_current_value_is_rejected():
    result = compute_deviation_scores(
        current_value="not-a-number",
        history_values=[1, 2, 3, 4, 5],
        prev_value=3,
    )
    assert not result["candidate"]
    assert result["category"] == "invalid_value"

