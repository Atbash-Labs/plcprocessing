#!/usr/bin/env python3
"""
Deterministic anomaly scoring primitives for monitoring agents.

This module intentionally avoids external dependencies so it can run in
packaged/offline environments.
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone
from statistics import mean, median, pstdev
from typing import Any, Dict, List, Optional


def safe_float(value: Any) -> Optional[float]:
    """Best-effort conversion to float."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-like timestamp to UTC-aware datetime."""
    if not ts:
        return None
    text = str(ts).strip()
    if not text:
        return None
    # Handle unix epoch (seconds or milliseconds) represented as numeric text.
    if text.isdigit():
        try:
            raw = int(text)
            if raw > 10_000_000_000:  # likely milliseconds
                raw = raw / 1000.0
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        # Ignition often returns naive local timestamps; assume local timezone.
        local_tz = datetime.now().astimezone().tzinfo or timezone.utc
        dt = dt.replace(tzinfo=local_tz)
    return dt.astimezone(timezone.utc)


def is_quality_good(quality: Optional[str]) -> bool:
    """Conservative quality gate."""
    if quality is None:
        return False
    q = str(quality).strip().lower()
    if not q:
        return False
    if "good" in q or "ok" in q or q in {"192"}:
        return True
    return False


def is_stale(timestamp: Optional[str], staleness_sec: int, now: Optional[datetime] = None) -> bool:
    """Return True if sample timestamp is stale or invalid."""
    if staleness_sec <= 0:
        return False
    parsed = parse_timestamp(timestamp)
    if parsed is None:
        return True
    baseline = now or datetime.now(timezone.utc)
    age = (baseline - parsed).total_seconds()
    return age > staleness_sec


def _mad(values: List[float]) -> float:
    """Median absolute deviation."""
    if not values:
        return 0.0
    med = median(values)
    abs_dev = [abs(v - med) for v in values]
    return median(abs_dev) if abs_dev else 0.0


def _percentile_rank(values: List[float], current: float) -> float:
    """Approximate percentile rank of current within values."""
    if not values:
        return 0.0
    less_or_equal = sum(1 for v in values if v <= current)
    return less_or_equal / len(values)


def compute_deviation_scores(
    current_value: Any,
    history_values: List[Any],
    prev_value: Any = None,
    thresholds: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Compute deterministic anomaly scores and candidate flags.

    Threshold defaults are intentionally conservative and should be configured
    per process during rollout.
    """
    cfg = {
        "z": 3.0,
        "mad": 3.5,
        "rate": 0.0,
        "flatline_std_epsilon": 1e-6,
        "stuck_window_size": 20,
    }
    if thresholds:
        cfg.update({k: v for k, v in thresholds.items() if v is not None})

    current = safe_float(current_value)
    hist = [v for v in (safe_float(x) for x in history_values) if v is not None]
    previous = safe_float(prev_value)

    result: Dict[str, Any] = {
        "candidate": False,
        "reasons": [],
        "category": "normal",
        "z_score": 0.0,
        "mad_score": 0.0,
        "delta_rate": 0.0,
        "window_volatility": 0.0,
        "percentile_rank": 0.0,
        "drift_score": 0.0,
        "history_points": len(hist),
    }

    if current is None:
        result["category"] = "invalid_value"
        result["reasons"].append("current_value_not_numeric")
        return result
    if not hist:
        result["category"] = "insufficient_history"
        result["reasons"].append("history_empty")
        return result

    mu = mean(hist)
    sigma = pstdev(hist) if len(hist) > 1 else 0.0
    sigma = max(sigma, 1e-9)
    z_score = (current - mu) / sigma
    result["z_score"] = z_score
    result["window_volatility"] = sigma
    result["percentile_rank"] = _percentile_rank(hist, current)

    mad = _mad(hist)
    mad_denom = max(mad * 1.4826, 1e-9)
    mad_score = abs(current - median(hist)) / mad_denom
    result["mad_score"] = mad_score

    if previous is not None:
        result["delta_rate"] = abs(current - previous)

    if abs(z_score) >= float(cfg["z"]):
        result["candidate"] = True
        result["reasons"].append("z_score_threshold")
    if mad_score >= float(cfg["mad"]):
        result["candidate"] = True
        result["reasons"].append("mad_score_threshold")
    if float(cfg["rate"]) > 0 and result["delta_rate"] >= float(cfg["rate"]):
        result["candidate"] = True
        result["reasons"].append("delta_rate_threshold")

    if len(hist) >= 20:
        midpoint = len(hist) // 2
        first_half = hist[:midpoint]
        second_half = hist[midpoint:]
        trend_delta = abs(mean(second_half) - mean(first_half))
        trend_score = trend_delta / sigma
        result["drift_score"] = trend_score
        if trend_score >= 1.25 and (result["percentile_rank"] >= 0.85 or result["percentile_rank"] <= 0.15):
            result["candidate"] = True
            result["reasons"].append("drift_trend")

    recent = hist[-int(max(3, cfg["stuck_window_size"])) :]
    recent_std = pstdev(recent) if len(recent) > 1 else 0.0
    if recent_std <= float(cfg["flatline_std_epsilon"]):
        if previous is not None and abs(current - previous) <= float(cfg["flatline_std_epsilon"]):
            result["candidate"] = True
            result["reasons"].append("flatline_detected")
            result["category"] = "stuck"

    if result["category"] == "normal" and result["candidate"]:
        if "flatline_detected" in result["reasons"]:
            result["category"] = "stuck"
        elif result["delta_rate"] > 0 and "delta_rate_threshold" in result["reasons"]:
            result["category"] = "spike"
        elif "drift_trend" in result["reasons"]:
            result["category"] = "drift"
        elif abs(z_score) > 0 and len(hist) >= 20:
            # Drift-like heuristic for sustained tail position with moderate rate
            if result["percentile_rank"] >= 0.95 or result["percentile_rank"] <= 0.05:
                result["category"] = "drift"
            else:
                result["category"] = "spike"
        else:
            result["category"] = "deviation"

    return result


def dedup_key(tag_path: str, category: str, bucket_minutes: int = 10) -> str:
    """Create a deterministic dedup signature for event cooldown windows."""
    now = datetime.now(timezone.utc)
    bucket = int(now.timestamp() // max(1, bucket_minutes * 60))
    raw = f"{tag_path}|{category}|{bucket}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

