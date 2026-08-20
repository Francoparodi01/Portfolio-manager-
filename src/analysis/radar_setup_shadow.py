"""Point-in-time Radar discovery/setup measurements.

This module is audit-only. Its scores are hypotheses to validate against
prospective outcomes; they never alter Radar ranking, planner or execution.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import math
from statistics import median
from typing import Any, Mapping, Sequence

try:
    import pandas as pd
except ImportError:  # pragma: no cover - scheduler image includes pandas
    pd = None


RADAR_SETUP_SHADOW_VERSION = "radar-setup-shadow-v1"
RADAR_SETUP_TRIGGER_WINDOW_SESSIONS = 10
RADAR_SETUP_CONTROL_TICKERS = {"QQQ", "SPY"}


@dataclass(frozen=True)
class RadarSetupShadowMeasurement:
    version: str
    trend_component_score: float | None
    relative_strength_component_score: float | None
    compression_component_score: float | None
    setup_component_score: float | None
    discovery_score: float | None
    setup_score: float | None
    composite_score: float | None
    discovery_percentile: float | None
    setup_percentile: float | None
    composite_percentile: float | None
    readiness_state: str
    trigger_price: float | None
    invalidation_price: float | None
    target_price: float | None
    risk_reward: float | None
    trigger_confirmed: bool
    feature_quality_flag: str
    features: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update({
            "informational_only": True,
            "promotion_eligible": False,
            "affects_radar_ranking": False,
            "affects_analysis": False,
            "affects_execution": False,
        })
        return payload


def build_radar_setup_shadow_universe(
    *,
    tickers: Sequence[str],
    history_frames: Mapping[str, Any],
    asset_types: Mapping[str, str] | None = None,
    screening_metrics: Mapping[str, Any] | None = None,
    candidates: Mapping[str, Any] | None = None,
) -> dict[str, RadarSetupShadowMeasurement]:
    """Build independent 0-25 components from information available at capture."""
    if pd is None:
        return {}

    normalized_tickers = list(dict.fromkeys(
        str(ticker).upper().strip() for ticker in tickers if str(ticker).strip()
    ))
    types = {str(key).upper(): str(value or "UNKNOWN").upper()
             for key, value in dict(asset_types or {}).items()}
    screens = {str(key).upper(): value
               for key, value in dict(screening_metrics or {}).items()}
    candidate_map = {str(key).upper(): value
                     for key, value in dict(candidates or {}).items()}

    raw: dict[str, dict[str, Any]] = {}
    for ticker in normalized_tickers:
        if ticker in RADAR_SETUP_CONTROL_TICKERS:
            continue
        raw[ticker] = _extract_point_in_time_features(
            ticker=ticker,
            frame=history_frames.get(ticker),
            asset_type=types.get(ticker, "UNKNOWN"),
            screen=screens.get(ticker),
            candidate=candidate_map.get(ticker),
        )

    benchmark_returns = {
        benchmark: _return_features(history_frames.get(benchmark))
        for benchmark in sorted(RADAR_SETUP_CONTROL_TICKERS)
    }
    percentile_20 = _cross_sectional_percentiles(raw, "momentum_20")
    percentile_60 = _cross_sectional_percentiles(raw, "momentum_60")

    drafts: dict[str, dict[str, Any]] = {}
    for ticker, features in raw.items():
        features["universe_percentile_20"] = percentile_20.get(ticker)
        features["universe_percentile_60"] = percentile_60.get(ticker)
        _attach_benchmark_features(features, benchmark_returns)

        trend = _trend_component(features)
        relative_strength = _relative_strength_component(features)
        compression = _compression_component(features)
        setup = _setup_component(features)
        discovery = _sum_if_complete(trend, relative_strength)
        setup_score = _sum_if_complete(compression, setup)
        composite = _sum_if_complete(discovery, setup_score)
        readiness = _readiness_state(features)
        warnings = list(features.pop("warnings", []))
        quality = _feature_quality(features, warnings)

        drafts[ticker] = {
            "version": RADAR_SETUP_SHADOW_VERSION,
            "trend_component_score": _round_score(trend),
            "relative_strength_component_score": _round_score(relative_strength),
            "compression_component_score": _round_score(compression),
            "setup_component_score": _round_score(setup),
            "discovery_score": _round_score(discovery),
            "setup_score": _round_score(setup_score),
            "composite_score": _round_score(composite),
            "readiness_state": readiness,
            "trigger_price": _round_price(features.get("trigger_price")),
            "invalidation_price": _round_price(features.get("invalidation_price")),
            "target_price": _round_price(features.get("target_price")),
            "risk_reward": _round_optional(features.get("risk_reward"), 4),
            "trigger_confirmed": bool(features.get("trigger_confirmed", False)),
            "feature_quality_flag": quality,
            "features": {key: _json_value(value) for key, value in features.items()},
            "warnings": tuple(dict.fromkeys(warnings)),
        }

    discovery_percentiles = _draft_percentiles(drafts, "discovery_score")
    setup_percentiles = _draft_percentiles(drafts, "setup_score")
    composite_percentiles = _draft_percentiles(drafts, "composite_score")
    return {
        ticker: RadarSetupShadowMeasurement(
            **draft,
            discovery_percentile=discovery_percentiles.get(ticker),
            setup_percentile=setup_percentiles.get(ticker),
            composite_percentile=composite_percentiles.get(ticker),
        )
        for ticker, draft in drafts.items()
    }


def resolve_setup_event(
    *,
    reference_ts: datetime,
    reference_price: float,
    readiness_state: str,
    trigger_price: float,
    invalidation_price: float,
    candles: Sequence[Mapping[str, Any]],
    trigger_window_sessions: int = RADAR_SETUP_TRIGGER_WINDOW_SESSIONS,
) -> dict[str, Any] | None:
    """Resolve the first prospective trigger/invalidation without intraday guesses."""
    state = str(readiness_state or "").upper()
    reference_at = _aware_datetime(reference_ts)
    if state == "TRIGGERED":
        return {
            "event_status": "TRIGGERED_AT_CAPTURE",
            "event_ts": reference_at,
            "event_price": float(reference_price),
            "sessions_from_discovery": 0,
            "trigger_volume_ratio": None,
        }
    if state == "EXTENDED":
        return {
            "event_status": "EXTENDED_AT_CAPTURE",
            "event_ts": reference_at,
            "event_price": None,
            "sessions_from_discovery": 0,
            "trigger_volume_ratio": None,
        }

    ordered = sorted(
        (
            dict(row) for row in candles
            if row.get("ts") is not None and _aware_datetime(row["ts"]) > reference_at
        ),
        key=lambda row: _aware_datetime(row["ts"]),
    )
    future = ordered[:max(int(trigger_window_sessions), 1)]
    for session_index, row in enumerate(future, start=1):
        close = _row_number(row, "close_price", "close")
        high = _row_number(row, "high_price", "high") or close
        low = _row_number(row, "low_price", "low") or close
        if high is None or low is None:
            continue
        trigger_hit = high >= float(trigger_price)
        invalidation_hit = low <= float(invalidation_price)
        event_ts = _aware_datetime(row["ts"])
        volume_ratio = _event_volume_ratio(candles, event_ts)
        if trigger_hit and invalidation_hit:
            return {
                "event_status": "AMBIGUOUS_SAME_SESSION",
                "event_ts": event_ts,
                "event_price": None,
                "sessions_from_discovery": session_index,
                "trigger_volume_ratio": volume_ratio,
            }
        if invalidation_hit:
            return {
                "event_status": "INVALIDATED_BEFORE_TRIGGER",
                "event_ts": event_ts,
                "event_price": float(invalidation_price),
                "sessions_from_discovery": session_index,
                "trigger_volume_ratio": volume_ratio,
            }
        if trigger_hit:
            return {
                "event_status": "TRIGGERED_AFTER_DISCOVERY",
                "event_ts": event_ts,
                "event_price": float(trigger_price),
                "sessions_from_discovery": session_index,
                "trigger_volume_ratio": volume_ratio,
            }
    if len(future) >= int(trigger_window_sessions):
        return {
            "event_status": "EXPIRED_NO_TRIGGER",
            "event_ts": _aware_datetime(future[-1]["ts"]),
            "event_price": None,
            "sessions_from_discovery": int(trigger_window_sessions),
            "trigger_volume_ratio": None,
        }
    return None


def _extract_point_in_time_features(
    *,
    ticker: str,
    frame: Any,
    asset_type: str,
    screen: Any,
    candidate: Any,
) -> dict[str, Any]:
    features: dict[str, Any] = {
        "ticker": ticker,
        "asset_type": asset_type,
        "comparison_basis": "LOCAL_SAME_RUN",
        "warnings": [],
    }
    normalized = _normalize_frame(frame)
    if normalized is None or len(normalized) < 40:
        features["history_sessions"] = 0 if normalized is None else len(normalized)
        features["warnings"].append("insufficient_price_history")
        return features

    close = normalized["close"]
    high = normalized.get("high")
    low = normalized.get("low")
    volume = normalized.get("volume")
    features["history_sessions"] = len(normalized)
    features["close"] = _last(close)
    features["momentum_20"] = _return_over(close, 20)
    features["momentum_60"] = _return_over(close, 60)

    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    features.update({
        "sma_20": _last(sma20),
        "sma_50": _last(sma50),
        "sma_200": _last(sma200),
        "sma_20_slope_10": _series_slope(sma20, 10),
        "sma_50_slope_10": _series_slope(sma50, 10),
    })
    current = features["close"]
    features["distance_sma20"] = _ratio_delta(current, features["sma_20"])
    features["distance_sma50"] = _ratio_delta(current, features["sma_50"])
    features["distance_sma200"] = _ratio_delta(current, features["sma_200"])

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_width = (4.0 * bb_std) / bb_mid.replace(0, float("nan"))
    features["bb_width"] = _last(bb_width)
    features["bb_width_time_percentile"] = _time_percentile(bb_width)

    if high is None or low is None:
        features["warnings"].append("missing_high_low_history")
    else:
        atr = _atr_series(high, low, close)
        atr_pct = atr / close.replace(0, float("nan"))
        adx, di_plus, di_minus = _adx_series(high, low, close)
        features.update({
            "atr_14": _last(atr),
            "atr_pct": _last(atr_pct),
            "atr_pct_time_percentile": _time_percentile(atr_pct),
            "adx_14": _last(adx),
            "di_plus": _last(di_plus),
            "di_minus": _last(di_minus),
            "higher_high_20": _higher_window(high, 20, use_min=False),
            "higher_low_20": _higher_window(low, 20, use_min=True),
        })
        _attach_range_and_setup_features(features, high, low, close, atr)

    _attach_volume_features(features, volume)
    if asset_type == "CEDEAR":
        features["warnings"].append("cedear_ccl_not_separated")

    features["screen_rs_benchmark"] = str(
        getattr(screen, "rs_benchmark_ticker", "") or ""
    ).upper() or None
    features["screen_rs_20"] = _finite_or_none(
        getattr(screen, "rs_vs_spy_20d", None)
    )
    asymmetry = getattr(candidate, "asymmetry", None)
    features["operational_rr_reference"] = _finite_or_none(
        getattr(asymmetry, "risk_reward", None)
    )
    return features


def _attach_range_and_setup_features(features, high, low, close, atr) -> None:
    current = float(features["close"])
    range_5 = _window_range(high, low, 5)
    range_20 = _window_range(high, low, 20)
    features["range_5_pct"] = range_5 / current if range_5 is not None else None
    features["range_20_pct"] = range_20 / current if range_20 is not None else None
    features["range_contraction_ratio"] = (
        range_5 / range_20 if range_5 is not None and range_20 and range_20 > 0 else None
    )
    recent_ranges = [_slice_range(high, low, offset) for offset in (0, 5, 10)]
    features["successive_range_contractions"] = sum(
        1 for left, right in zip(recent_ranges, recent_ranges[1:])
        if left is not None and right is not None and left < right
    )

    if len(high) < 21:
        features["warnings"].append("insufficient_setup_history")
        return
    trigger = _finite_or_none(high.iloc[-21:-1].max())
    support = _finite_or_none(low.tail(10).min())
    base_low = _finite_or_none(low.tail(20).min())
    atr_value = _last(atr)
    if trigger is None or support is None or base_low is None or atr_value is None:
        features["warnings"].append("setup_levels_unavailable")
        return

    invalidation = max(support, current - 1.5 * atr_value)
    if invalidation >= current:
        invalidation = current * 0.99
    entry_basis = max(current, trigger)
    target = trigger + max(trigger - base_low, 0.0)
    risk = entry_basis - invalidation
    reward = target - entry_basis
    rr = reward / risk if risk > 0 and reward > 0 else 0.0
    distance = current / trigger - 1.0 if trigger > 0 else None
    features.update({
        "trigger_price": trigger,
        "distance_to_trigger": distance,
        "invalidation_price": invalidation,
        "target_price": target,
        "risk_pct": risk / entry_basis if entry_basis > 0 else None,
        "risk_reward": rr,
        "base_range_pct": range_20 / current if range_20 is not None else None,
    })


def _attach_volume_features(features: dict[str, Any], volume: Any) -> None:
    if volume is None or len(volume) < 20:
        features["volume_quality_20"] = None
        features["warnings"].append("volume_history_unavailable")
        return
    recent = volume.tail(20)
    quality = float(((recent.notna()) & (recent > 0)).mean())
    features["volume_quality_20"] = quality
    if quality < 0.80:
        features["warnings"].append("volume_quality_below_80pct")
        return
    mean_20 = _finite_or_none(recent.mean())
    mean_5 = _finite_or_none(recent.tail(5).mean())
    latest = _finite_or_none(recent.iloc[-1])
    features["volume_contraction_ratio"] = (
        mean_5 / mean_20 if mean_5 is not None and mean_20 and mean_20 > 0 else None
    )
    features["volume_ratio_latest_20"] = (
        latest / mean_20 if latest is not None and mean_20 and mean_20 > 0 else None
    )


def _attach_benchmark_features(
    features: dict[str, Any],
    benchmark_returns: Mapping[str, Mapping[str, float | None]],
) -> None:
    momentum_20 = features.get("momentum_20")
    momentum_60 = features.get("momentum_60")
    available_20: list[float] = []
    available_60: list[float] = []
    for benchmark, returns in benchmark_returns.items():
        ret20 = returns.get("momentum_20")
        ret60 = returns.get("momentum_60")
        excess20 = _subtract(momentum_20, ret20)
        excess60 = _subtract(momentum_60, ret60)
        features[f"excess_vs_{benchmark.lower()}_20"] = excess20
        features[f"excess_vs_{benchmark.lower()}_60"] = excess60
        if excess20 is not None:
            available_20.append(excess20)
        if excess60 is not None:
            available_60.append(excess60)
    features["benchmark_excess_20"] = median(available_20) if available_20 else None
    features["benchmark_excess_60"] = median(available_60) if available_60 else None
    if not available_20 or not available_60:
        features["warnings"].append("benchmark_history_incomplete")


def _trend_component(features: Mapping[str, Any]) -> float | None:
    if int(features.get("history_sessions") or 0) < 60 or features.get("close") is None:
        return None
    close = float(features["close"])
    score = 0.0
    sma20 = features.get("sma_20")
    sma50 = features.get("sma_50")
    sma200 = features.get("sma_200")
    score += 2.0 if sma20 and close > sma20 else 0.0
    score += 2.0 if sma20 and sma50 and sma20 > sma50 else 0.0
    score += 2.0 if sma50 and sma200 and sma50 > sma200 else 0.0
    score += 1.0 if sma200 and close > sma200 else 0.0
    score += 3.0 * _scale(features.get("sma_20_slope_10"), -0.02, 0.04)
    score += 3.0 * _scale(features.get("sma_50_slope_10"), -0.015, 0.03)
    if _greater(features.get("di_plus"), features.get("di_minus")):
        score += 5.0 * _scale(features.get("adx_14"), 15.0, 35.0)
    score += 2.0 if features.get("higher_high_20") else 0.0
    score += 2.0 if features.get("higher_low_20") else 0.0
    score += _extension_score(features.get("distance_sma20"))
    return min(max(score, 0.0), 25.0)


def _relative_strength_component(features: Mapping[str, Any]) -> float | None:
    p20 = features.get("universe_percentile_20")
    p60 = features.get("universe_percentile_60")
    if p20 is None or p60 is None:
        return None
    score = 10.0 * float(p20) + 8.0 * float(p60)
    score += 2.0 * _scale(features.get("benchmark_excess_20"), -0.05, 0.10)
    score += 2.0 * _scale(features.get("benchmark_excess_60"), -0.10, 0.20)
    score += 1.5 if _positive(features.get("momentum_20")) else 0.0
    score += 1.5 if _positive(features.get("momentum_60")) else 0.0
    return min(max(score, 0.0), 25.0)


def _compression_component(features: Mapping[str, Any]) -> float | None:
    bb_percentile = features.get("bb_width_time_percentile")
    atr_percentile = features.get("atr_pct_time_percentile")
    contraction = features.get("range_contraction_ratio")
    if bb_percentile is None or atr_percentile is None or contraction is None:
        return None
    score = 7.0 * (1.0 - float(bb_percentile))
    score += 6.0 * (1.0 - float(atr_percentile))
    score += 5.0 * (1.0 - _scale(contraction, 0.35, 0.80))
    score += 1.5 * min(int(features.get("successive_range_contractions") or 0), 2)
    if float(features.get("volume_quality_20") or 0.0) >= 0.80:
        score += 4.0 * (1.0 - _scale(features.get("volume_contraction_ratio"), 0.60, 1.0))
    return min(max(score, 0.0), 25.0)


def _setup_component(features: Mapping[str, Any]) -> float | None:
    distance = features.get("distance_to_trigger")
    base_range = features.get("base_range_pct")
    rr = features.get("risk_reward")
    risk_pct = features.get("risk_pct")
    if None in (distance, base_range, rr, risk_pct):
        return None
    score = _trigger_proximity_score(float(distance))
    score += 5.0 * (1.0 - _scale(base_range, 0.08, 0.25))
    score += 6.0 * _scale(rr, 1.0, 3.0)
    score += _risk_clarity_score(float(risk_pct))
    crossed = 0.0 <= float(distance) <= 0.06
    volume_ratio = features.get("volume_ratio_latest_20")
    volume_quality = float(features.get("volume_quality_20") or 0.0)
    confirmed = crossed and volume_quality >= 0.80 and _at_least(volume_ratio, 1.20)
    if crossed:
        score += 4.0 if confirmed else (3.0 if _at_least(volume_ratio, 1.0) else 2.0)
    if isinstance(features, dict):
        features["trigger_confirmed"] = confirmed
    return min(max(score, 0.0), 25.0)


def _readiness_state(features: Mapping[str, Any]) -> str:
    distance = features.get("distance_to_trigger")
    if distance is None or features.get("invalidation_price") is None:
        return "DATA_INSUFFICIENT"
    distance = float(distance)
    if distance > 0.06:
        return "EXTENDED"
    if distance >= 0.0:
        return "TRIGGERED"
    if distance >= -0.05:
        return "PRE_BREAKOUT"
    return "WATCH"


def _feature_quality(features: Mapping[str, Any], warnings: list[str]) -> str:
    sessions = int(features.get("history_sessions") or 0)
    if sessions < 60 or features.get("distance_to_trigger") is None:
        return "INSUFFICIENT"
    if sessions < 200 or float(features.get("volume_quality_20") or 0.0) < 0.80:
        return "PARTIAL"
    return "GOOD"


def _cross_sectional_percentiles(
    raw: Mapping[str, Mapping[str, Any]], key: str
) -> dict[str, float]:
    all_values = [
        float(features[key]) for features in raw.values()
        if _finite_or_none(features.get(key)) is not None
    ]
    by_type: dict[str, list[float]] = {}
    for features in raw.values():
        value = _finite_or_none(features.get(key))
        if value is not None:
            by_type.setdefault(str(features.get("asset_type") or "UNKNOWN"), []).append(value)
    result: dict[str, float] = {}
    for ticker, features in raw.items():
        value = _finite_or_none(features.get(key))
        if value is None:
            continue
        peers = by_type.get(str(features.get("asset_type") or "UNKNOWN"), [])
        if len(peers) < 10:
            peers = all_values
        percentile = _percentile_rank(value, peers)
        if percentile is not None:
            result[ticker] = percentile
    return result


def _draft_percentiles(
    drafts: Mapping[str, Mapping[str, Any]], key: str
) -> dict[str, float]:
    peers = [
        float(draft[key]) for draft in drafts.values()
        if _finite_or_none(draft.get(key)) is not None
    ]
    return {
        ticker: percentile
        for ticker, draft in drafts.items()
        if (value := _finite_or_none(draft.get(key))) is not None
        and (percentile := _percentile_rank(value, peers)) is not None
    }


def _return_features(frame: Any) -> dict[str, float | None]:
    normalized = _normalize_frame(frame)
    if normalized is None:
        return {"momentum_20": None, "momentum_60": None}
    close = normalized["close"]
    return {
        "momentum_20": _return_over(close, 20),
        "momentum_60": _return_over(close, 60),
    }


def _normalize_frame(frame: Any):
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        data = pd.DataFrame(index=frame.index)
        data["close"] = _numeric_column(frame, "Close")
        if "High" in frame.columns:
            data["high"] = _numeric_column(frame, "High")
        if "Low" in frame.columns:
            data["low"] = _numeric_column(frame, "Low")
        if "Volume" in frame.columns:
            data["volume"] = _numeric_column(frame, "Volume")
        data = data.dropna(subset=["close"])
        return data if not data.empty else None
    except (KeyError, TypeError, ValueError):
        return None


def _numeric_column(frame: Any, name: str):
    values = frame[name]
    if getattr(values, "ndim", 1) > 1:
        values = values.iloc[:, 0]
    return pd.to_numeric(values, errors="coerce")


def _atr_series(high, low, close, period: int = 14):
    previous = close.shift(1)
    true_range = pd.concat([
        (high - low).abs(),
        (high - previous).abs(),
        (low - previous).abs(),
    ], axis=1).max(axis=1)
    return true_range.rolling(period).mean()


def _adx_series(high, low, close, period: int = 14):
    previous = close.shift(1)
    true_range = pd.concat([
        (high - low).abs(),
        (high - previous).abs(),
        (low - previous).abs(),
    ], axis=1).max(axis=1)
    up = high - high.shift(1)
    down = low.shift(1) - low
    dm_plus = up.where((up > down) & (up > 0), 0.0)
    dm_minus = down.where((down > up) & (down > 0), 0.0)
    atr_wilder = true_range.ewm(alpha=1 / period, adjust=False).mean()
    di_plus = 100 * dm_plus.ewm(alpha=1 / period, adjust=False).mean() / (atr_wilder + 1e-9)
    di_minus = 100 * dm_minus.ewm(alpha=1 / period, adjust=False).mean() / (atr_wilder + 1e-9)
    dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus + 1e-9)
    return dx.ewm(alpha=1 / period, adjust=False).mean(), di_plus, di_minus


def _time_percentile(series: Any, lookback: int = 126) -> float | None:
    valid = series.dropna().tail(lookback)
    if len(valid) < 20:
        return None
    return _percentile_rank(float(valid.iloc[-1]), [float(value) for value in valid])


def _percentile_rank(value: float, peers: Sequence[float]) -> float | None:
    clean = sorted(float(item) for item in peers if math.isfinite(float(item)))
    if not clean:
        return None
    if len(clean) == 1:
        return 1.0
    lower = sum(item < value for item in clean)
    equal = sum(item == value for item in clean)
    average_rank_zero_based = lower + (max(equal, 1) - 1) / 2.0
    return average_rank_zero_based / (len(clean) - 1)


def _window_range(high, low, size: int) -> float | None:
    if len(high) < size or len(low) < size:
        return None
    high_value = _finite_or_none(high.tail(size).max())
    low_value = _finite_or_none(low.tail(size).min())
    return high_value - low_value if high_value is not None and low_value is not None else None


def _slice_range(high, low, offset: int, size: int = 5) -> float | None:
    end = len(high) - offset
    start = end - size
    if start < 0 or end <= 0:
        return None
    high_value = _finite_or_none(high.iloc[start:end].max())
    low_value = _finite_or_none(low.iloc[start:end].min())
    return high_value - low_value if high_value is not None and low_value is not None else None


def _higher_window(series: Any, size: int, *, use_min: bool) -> bool | None:
    if len(series) < size * 2:
        return None
    aggregate = "min" if use_min else "max"
    current = _finite_or_none(getattr(series.tail(size), aggregate)())
    previous = _finite_or_none(getattr(series.iloc[-size * 2:-size], aggregate)())
    return current > previous if current is not None and previous is not None else None


def _return_over(series: Any, sessions: int) -> float | None:
    if len(series) <= sessions:
        return None
    current = _finite_or_none(series.iloc[-1])
    previous = _finite_or_none(series.iloc[-sessions - 1])
    if current is None or previous is None or previous <= 0:
        return None
    return current / previous - 1.0


def _series_slope(series: Any, sessions: int) -> float | None:
    valid = series.dropna()
    if len(valid) <= sessions:
        return None
    current = _finite_or_none(valid.iloc[-1])
    previous = _finite_or_none(valid.iloc[-sessions - 1])
    if current is None or previous is None or previous == 0:
        return None
    return current / previous - 1.0


def _trigger_proximity_score(distance: float) -> float:
    if -0.05 <= distance < 0:
        return 7.0 * (1.0 - abs(distance) / 0.05)
    if 0.0 <= distance <= 0.02:
        return 7.0
    if 0.02 < distance <= 0.06:
        return 7.0 * (1.0 - (distance - 0.02) / 0.04)
    return 0.0


def _risk_clarity_score(risk_pct: float) -> float:
    if 0.02 <= risk_pct <= 0.12:
        return 3.0
    if 0.01 <= risk_pct < 0.02:
        return 3.0 * (risk_pct - 0.01) / 0.01
    if 0.12 < risk_pct <= 0.20:
        return 3.0 * (1.0 - (risk_pct - 0.12) / 0.08)
    return 0.0


def _extension_score(distance: Any) -> float:
    value = _finite_or_none(distance)
    if value is None:
        return 0.0
    if -0.02 <= value <= 0.05:
        return 3.0
    if -0.08 <= value < -0.02:
        return 3.0 * (value + 0.08) / 0.06
    if 0.05 < value <= 0.10:
        return 3.0 * (1.0 - (value - 0.05) / 0.05)
    return 0.0


def _scale(value: Any, low: float, high: float) -> float:
    normalized = _finite_or_none(value)
    if normalized is None or high <= low:
        return 0.0
    return min(max((normalized - low) / (high - low), 0.0), 1.0)


def _last(series: Any) -> float | None:
    valid = series.dropna()
    return _finite_or_none(valid.iloc[-1]) if not valid.empty else None


def _finite_or_none(value: Any) -> float | None:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if math.isfinite(normalized) else None


def _ratio_delta(value: Any, base: Any) -> float | None:
    numerator = _finite_or_none(value)
    denominator = _finite_or_none(base)
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator - 1.0


def _subtract(left: Any, right: Any) -> float | None:
    left_value = _finite_or_none(left)
    right_value = _finite_or_none(right)
    return left_value - right_value if left_value is not None and right_value is not None else None


def _sum_if_complete(left: float | None, right: float | None) -> float | None:
    return left + right if left is not None and right is not None else None


def _greater(left: Any, right: Any) -> bool:
    left_value = _finite_or_none(left)
    right_value = _finite_or_none(right)
    return left_value is not None and right_value is not None and left_value > right_value


def _positive(value: Any) -> bool:
    normalized = _finite_or_none(value)
    return normalized is not None and normalized > 0


def _at_least(value: Any, threshold: float) -> bool:
    normalized = _finite_or_none(value)
    return normalized is not None and normalized >= threshold


def _round_score(value: float | None) -> float | None:
    return _round_optional(value, 4)


def _round_price(value: Any) -> float | None:
    return _round_optional(value, 6)


def _round_optional(value: Any, digits: int) -> float | None:
    normalized = _finite_or_none(value)
    return round(normalized, digits) if normalized is not None else None


def _json_value(value: Any) -> Any:
    normalized = _finite_or_none(value)
    if normalized is not None and not isinstance(value, (str, bool)):
        return round(normalized, 8)
    return value


def _aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _row_number(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _finite_or_none(row.get(key))
        if value is not None:
            return value
    return None


def _event_volume_ratio(candles: Sequence[Mapping[str, Any]], event_ts: datetime) -> float | None:
    event_at = _aware_datetime(event_ts)
    ordered = sorted(
        (dict(row) for row in candles if row.get("ts") is not None),
        key=lambda row: _aware_datetime(row["ts"]),
    )
    prior: list[float] = []
    event_volume = None
    for row in ordered:
        ts = _aware_datetime(row["ts"])
        volume = _row_number(row, "volume")
        if ts < event_at and volume is not None and volume > 0:
            prior.append(volume)
        elif ts == event_at:
            event_volume = volume
            break
    baseline = median(prior[-20:]) if len(prior) >= 10 else None
    if event_volume is None or baseline is None or baseline <= 0:
        return None
    return event_volume / baseline


__all__ = [
    "RADAR_SETUP_SHADOW_VERSION",
    "RADAR_SETUP_TRIGGER_WINDOW_SESSIONS",
    "RadarSetupShadowMeasurement",
    "build_radar_setup_shadow_universe",
    "resolve_setup_event",
]
