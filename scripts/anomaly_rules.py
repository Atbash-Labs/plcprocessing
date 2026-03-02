#!/usr/bin/env python3
"""
Deterministic Stage A anomaly scoring engine.

Computes statistical deviation features for tag values against historical windows.
Designed for testability and isolation from the monitoring loop.
"""

import math
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field


@dataclass
class DeviationScores:
    """Computed deviation features for a single tag sample."""
    tag_path: str
    current_value: Optional[float] = None
    z_score: Optional[float] = None
    mad_score: Optional[float] = None
    delta_rate: Optional[float] = None
    window_volatility: Optional[float] = None
    percentile_position: Optional[float] = None
    history_points: int = 0
    window_mean: Optional[float] = None
    window_std: Optional[float] = None
    window_median: Optional[float] = None
    quality: str = "Good"
    timestamp: Optional[str] = None
    is_candidate: bool = False
    skip_reason: Optional[str] = None
    category: Optional[str] = None  # spike, drift, stuck, quality-issue


@dataclass
class ThresholdConfig:
    """Configurable thresholds for anomaly detection."""
    z_threshold: float = 3.0
    mad_threshold: float = 3.5
    rate_threshold: float = 50.0  # absolute units per sample interval
    staleness_sec: float = 120.0
    min_history_points: int = 30
    stuck_std_threshold: float = 0.001  # near-zero std -> stuck sensor


def _median(values: List[float]) -> float:
    """Compute median of a sorted list."""
    n = len(values)
    if n == 0:
        return 0.0
    s = sorted(values)
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2.0
    return s[mid]


def _mad(values: List[float], median_val: float) -> float:
    """Compute Median Absolute Deviation."""
    if not values:
        return 0.0
    deviations = sorted(abs(v - median_val) for v in values)
    return _median(deviations)


def compute_deviation_scores(
    tag_path: str,
    current_value: Any,
    history_values: List[float],
    quality: str = "Good",
    timestamp: Optional[str] = None,
    previous_value: Optional[float] = None,
    config: Optional[ThresholdConfig] = None,
) -> DeviationScores:
    """
    Compute all deviation features for a single tag.

    Args:
        tag_path: Tag identifier
        current_value: Current live reading
        history_values: List of numeric historical values
        quality: OPC quality string (Good, Bad, Uncertain)
        timestamp: ISO timestamp of current reading
        previous_value: Previous sample value (for delta_rate)
        config: Threshold configuration

    Returns:
        DeviationScores with all computed features and candidate flag
    """
    if config is None:
        config = ThresholdConfig()

    scores = DeviationScores(
        tag_path=tag_path,
        quality=quality,
        timestamp=timestamp,
    )

    # Gate: bad quality
    if quality and quality.lower() not in ("good", "192", "good [192]"):
        scores.skip_reason = f"bad_quality:{quality}"
        return scores

    # Gate: non-numeric value
    try:
        current_num = float(current_value)
    except (TypeError, ValueError):
        scores.skip_reason = "non_numeric_value"
        return scores

    scores.current_value = current_num

    # Gate: insufficient history
    scores.history_points = len(history_values)
    if scores.history_points < config.min_history_points:
        scores.skip_reason = "insufficient_history"
        return scores

    # Compute window statistics
    n = len(history_values)
    mean_val = sum(history_values) / n
    variance = sum((v - mean_val) ** 2 for v in history_values) / n
    std_val = math.sqrt(variance)
    median_val = _median(history_values)
    mad_val = _mad(history_values, median_val)

    scores.window_mean = mean_val
    scores.window_std = std_val
    scores.window_median = median_val

    # z-score with epsilon guard
    epsilon = 1e-10
    scores.z_score = (current_num - mean_val) / (std_val + epsilon)

    # MAD score (1.4826 constant for consistency with normal distribution)
    mad_scaled = mad_val * 1.4826
    scores.mad_score = (current_num - median_val) / (mad_scaled + epsilon)

    # Delta rate (first derivative against previous sample)
    if previous_value is not None:
        try:
            scores.delta_rate = abs(current_num - float(previous_value))
        except (TypeError, ValueError):
            scores.delta_rate = None
    else:
        scores.delta_rate = None

    # Window volatility and percentile position
    scores.window_volatility = std_val
    sorted_hist = sorted(history_values)
    position = sum(1 for v in sorted_hist if v <= current_num)
    scores.percentile_position = position / n if n > 0 else 0.5

    # Classify anomaly type
    scores.is_candidate = False

    # Check for stuck sensor (flatline)
    if std_val < config.stuck_std_threshold and n >= config.min_history_points:
        # History is flat - if current differs at all, it's notable
        if abs(current_num - mean_val) > config.stuck_std_threshold * 10:
            scores.is_candidate = True
            scores.category = "spike"  # breaking out of flatline
        elif std_val < config.stuck_std_threshold:
            # Check if current continues the flatline pattern
            scores.category = "stuck"
            scores.is_candidate = True
        return scores

    # Standard threshold checks
    if abs(scores.z_score) >= config.z_threshold:
        scores.is_candidate = True
        scores.category = "spike" if abs(scores.z_score) > config.z_threshold * 1.5 else "drift"

    if scores.mad_score is not None and abs(scores.mad_score) >= config.mad_threshold:
        scores.is_candidate = True
        if scores.category is None:
            scores.category = "spike"

    if scores.delta_rate is not None and scores.delta_rate >= config.rate_threshold:
        scores.is_candidate = True
        if scores.category is None:
            scores.category = "spike"

    return scores


def score_tag_batch(
    tag_readings: List[Dict[str, Any]],
    history_map: Dict[str, List[float]],
    previous_values: Optional[Dict[str, float]] = None,
    config: Optional[ThresholdConfig] = None,
) -> List[DeviationScores]:
    """
    Score a batch of tag readings against their historical windows.

    Args:
        tag_readings: List of dicts with keys: path, value, quality, timestamp
        history_map: Map of tag_path -> list of historical numeric values
        previous_values: Map of tag_path -> previous sample value
        config: Threshold configuration

    Returns:
        List of DeviationScores for all tags
    """
    if config is None:
        config = ThresholdConfig()
    if previous_values is None:
        previous_values = {}

    results = []
    for reading in tag_readings:
        path = reading.get("path", "")
        history = history_map.get(path, [])
        prev = previous_values.get(path)

        scores = compute_deviation_scores(
            tag_path=path,
            current_value=reading.get("value"),
            history_values=history,
            quality=reading.get("quality", "Good"),
            timestamp=reading.get("timestamp"),
            previous_value=prev,
            config=config,
        )
        results.append(scores)

    return results


def filter_candidates(
    scores: List[DeviationScores],
    max_candidates: int = 25,
) -> List[DeviationScores]:
    """
    Filter and rank anomaly candidates from scored results.

    Returns only candidates, sorted by severity (highest z_score first).
    Limited to max_candidates to prevent backlog.
    """
    candidates = [s for s in scores if s.is_candidate]

    # Sort by absolute z_score descending (most anomalous first)
    candidates.sort(
        key=lambda s: abs(s.z_score) if s.z_score is not None else 0.0,
        reverse=True,
    )

    return candidates[:max_candidates]
