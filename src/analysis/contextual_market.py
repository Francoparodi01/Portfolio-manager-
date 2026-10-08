"""Deterministic, PIT-safe contextual market evidence.

This module is descriptive and SHADOW_ONLY. It neither emits a trading score
nor changes synthesis, optimizer, planner or execution decisions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from src.analysis.contextual_contracts import frame_quality


CONTEXTUAL_MARKET_VERSION = "contextual-market-v1"
CONTEXTUAL_SNAPSHOT_SCHEMA = "contextual-market-snapshot-v1"
RVOL_WINDOW = 20
BREAKOUT_WINDOW = 20
SWING_WINDOW = 2
STRUCTURE_LOOKBACK = 120
RVOL_EXPANSION = 1.50
RVOL_CONTRACTION = 0.70
BREAKOUT_CONFIRMATION_RVOL = 1.20
RS_WINDOWS = (20, 60, 120)


class ContextIdentityError(ValueError):
    """Raised when a series cannot represent one unambiguous instrument."""


@dataclass(frozen=True)
class ContextualSnapshot:
    snapshot_id: str
    schema_version: str
    definition_version: str
    mode: str
    affects_analysis: bool
    affects_execution: bool
    cutoff: str
    identity: dict[str, Any]
    sources: dict[str, Any]
    timestamps: dict[str, Any]
    quality: dict[str, Any]
    components: dict[str, Any]
    invalidators: list[dict[str, Any]]
    input_digests: dict[str, str]
    missingness: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return _iso(value)
    if isinstance(value, np.generic):
        return _canonical(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )
    return sha256(encoded.encode("utf-8")).hexdigest()


def _identity(frame: pd.DataFrame) -> dict[str, Any]:
    identity = dict(getattr(frame, "attrs", {}).get("series_identity", {}) or {})
    required = ("ticker", "asset_type", "currency", "venue", "interval")
    missing = [key for key in required if not identity.get(key)]
    if missing:
        raise ContextIdentityError("MISSING_SERIES_IDENTITY:" + ",".join(missing))
    if identity["interval"] != "1d":
        raise ContextIdentityError("CONTEXT_REQUIRES_DAILY_SERIES")
    return identity


def _pit_frame(frame: pd.DataFrame, cutoff: Any) -> tuple[pd.DataFrame, list[str]]:
    """Select only fully timestamped evidence known by ``cutoff``.

    Unknown availability is excluded instead of inferred from the candle label.
    This can yield an empty frame for legacy data, which is returned as UNKNOWN.
    """
    if not isinstance(frame.index, pd.DatetimeIndex) or frame.index.tz is None:
        raise ValueError("CONTEXT_REQUIRES_TZ_AWARE_DATETIME_INDEX")
    _identity(frame)
    cutoff_ts = pd.Timestamp(cutoff)
    if cutoff_ts.tzinfo is None:
        raise ValueError("CONTEXT_REQUIRES_TZ_AWARE_CUTOFF")
    result = frame.sort_index().copy()
    result.attrs = dict(frame.attrs)
    missing = []
    mask = result.index <= cutoff_ts
    temporal = {
        "AvailableAt": "UNKNOWN_AVAILABLE_AT",
        "RetrievedAt": "UNKNOWN_SCRAPED_AT",
        "BarEnd": "UNKNOWN_BAR_END",
    }
    for column, reason in temporal.items():
        if column not in result:
            missing.append(reason)
            mask &= False
            continue
        values = pd.to_datetime(result[column], utc=True, errors="coerce")
        if values.isna().any():
            missing.append(reason)
        mask &= values.notna() & (values <= cutoff_ts)
    if "IsClosed" not in result:
        missing.append("UNKNOWN_IS_CLOSED")
        mask &= False
    else:
        closed = result["IsClosed"].astype("boolean")
        if closed.isna().any():
            missing.append("UNKNOWN_IS_CLOSED")
        mask &= closed.fillna(False).astype(bool)
    result = result.loc[mask].copy()
    result.attrs = dict(frame.attrs)
    result.attrs["cutoff"] = cutoff_ts
    if "Source" in result:
        counts = {
            str(source): int(count)
            for source, count in result.Source.value_counts().sort_index().items()
        }
        result.attrs["candle_source_counts"] = counts
        result.attrs["candle_sources"] = tuple(counts)
    if "ProviderSymbol" in result:
        result.attrs["provider_symbols"] = sorted(
            {str(value) for value in result.ProviderSymbol.dropna()}
        )
    return result, sorted(set(missing))


def _confirmed_swings(frame: pd.DataFrame, window: int = SWING_WINDOW) -> tuple[list[dict], list[dict]]:
    highs, lows = [], []
    if len(frame) < window * 2 + 1:
        return highs, lows
    high = pd.to_numeric(frame["High"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(frame["Low"], errors="coerce").to_numpy(dtype=float)
    for index in range(window, len(frame) - window):
        high_window = high[index - window:index + window + 1]
        low_window = low[index - window:index + window + 1]
        if np.isfinite(high_window).all() and high[index] == high_window.max() and np.sum(high_window == high[index]) == 1:
            highs.append({"timestamp": _iso(frame.index[index]), "value": float(high[index])})
        if np.isfinite(low_window).all() and low[index] == low_window.min() and np.sum(low_window == low[index]) == 1:
            lows.append({"timestamp": _iso(frame.index[index]), "value": float(low[index])})
    return highs, lows


def market_structure(frame: pd.DataFrame, *, window: int = SWING_WINDOW,
                     lookback: int = STRUCTURE_LOOKBACK) -> dict[str, Any]:
    sample = frame.tail(lookback)
    highs, lows = _confirmed_swings(sample, window)
    close = _number(sample.Close.iloc[-1]) if len(sample) and "Close" in sample else None
    high_pattern = low_pattern = "UNKNOWN"
    if len(highs) >= 2:
        high_pattern = "HH" if highs[-1]["value"] > highs[-2]["value"] else (
            "LH" if highs[-1]["value"] < highs[-2]["value"] else "EH"
        )
    if len(lows) >= 2:
        low_pattern = "HL" if lows[-1]["value"] > lows[-2]["value"] else (
            "LL" if lows[-1]["value"] < lows[-2]["value"] else "EL"
        )
    if high_pattern == "HH" and low_pattern == "HL":
        trend = "UPTREND"
    elif high_pattern == "LH" and low_pattern == "LL":
        trend = "DOWNTREND"
    elif "UNKNOWN" in (high_pattern, low_pattern):
        trend = "UNKNOWN"
    else:
        trend = "MIXED"
    support = lows[-1] if lows else None
    resistance = highs[-1] if highs else None
    return {
        "trend": trend,
        "structure": f"{high_pattern}_{low_pattern}",
        "swing_window": window,
        "lookback_bars": min(len(frame), lookback),
        "confirmed_swing_highs": highs[-4:],
        "confirmed_swing_lows": lows[-4:],
        "support": support,
        "resistance": resistance,
        "close": close,
        "distance_support": (
            (close - support["value"]) / close
            if close and support else None
        ),
        "distance_resistance": (
            (resistance["value"] - close) / close
            if close and resistance else None
        ),
        "as_of": _iso(sample.index[-1]) if len(sample) else None,
        "status": "VALID" if trend != "UNKNOWN" else "UNKNOWN",
    }


def weekly_bars(frame: pd.DataFrame, cutoff: Any) -> pd.DataFrame:
    if frame.empty:
        result = frame.copy()
        result.attrs = dict(frame.attrs)
        return result
    numeric = frame[["Open", "High", "Low", "Close", "Volume"]].apply(pd.to_numeric, errors="coerce")
    grouped = numeric.resample("W-FRI", label="right", closed="right").agg({
        "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum",
    })
    grouped = grouped.dropna(subset=["Open", "High", "Low", "Close"])
    cutoff_ts = pd.Timestamp(cutoff)
    grouped = grouped.loc[grouped.index <= cutoff_ts]
    grouped.attrs = dict(frame.attrs)
    return grouped


def rvol_context(frame: pd.DataFrame, *, window: int = RVOL_WINDOW) -> dict[str, Any]:
    output = {
        "definition": "current_closed_daily_volume / arithmetic_mean(previous_20_closed_daily_volumes)",
        "window": window, "current_volume": None, "expected_volume": None,
        "rvol": None, "state": "UNKNOWN", "status": "UNKNOWN", "reason": None,
    }
    if "Volume" not in frame or len(frame) < window + 1:
        output["reason"] = "INSUFFICIENT_COMPARABLE_VOLUME"
        return output
    volume = pd.to_numeric(frame.Volume, errors="coerce")
    current = _number(volume.iloc[-1])
    previous = volume.iloc[-window - 1:-1]
    if current is None or current <= 0:
        output["reason"] = "CURRENT_VOLUME_MISSING_OR_NONPOSITIVE"
        return output
    if len(previous) != window or previous.isna().any() or not np.isfinite(previous).all() or (previous <= 0).any():
        output["reason"] = "REFERENCE_VOLUME_MISSING_OR_NONPOSITIVE"
        return output
    expected = float(previous.mean())
    if expected <= 0:
        output["reason"] = "REFERENCE_VOLUME_NONPOSITIVE"
        return output
    rvol = current / expected
    state = "EXPANSION" if rvol >= RVOL_EXPANSION else (
        "CONTRACTION" if rvol <= RVOL_CONTRACTION else "NORMAL"
    )
    output.update(current_volume=current, expected_volume=expected, rvol=rvol,
                  state=state, status="VALID", reason=None)
    return output


def breakout_context(frame: pd.DataFrame, volume: Mapping[str, Any],
                     *, window: int = BREAKOUT_WINDOW) -> dict[str, Any]:
    output = {"state": "UNKNOWN", "reference_high": None, "reference_low": None,
              "lookback": window, "status": "UNKNOWN", "reason": None}
    if len(frame) < window + 1:
        output["reason"] = "INSUFFICIENT_BREAKOUT_LOOKBACK"
        return output
    previous = frame.iloc[-window - 1:-1]
    close = _number(frame.Close.iloc[-1])
    high = _number(pd.to_numeric(previous.High, errors="coerce").max())
    low = _number(pd.to_numeric(previous.Low, errors="coerce").min())
    if close is None or high is None or low is None:
        output["reason"] = "INVALID_BREAKOUT_PRICE"
        return output
    state = "IN_RANGE"
    if close > high:
        if volume.get("status") != "VALID":
            state = "BREAKOUT_VOLUME_UNKNOWN"
        elif volume["rvol"] >= BREAKOUT_CONFIRMATION_RVOL:
            state = "BREAKOUT_CONFIRMED"
        else:
            state = "BREAKOUT_WITHOUT_VOLUME"
    elif close < low:
        state = "BREAKDOWN"
    output.update(state=state, reference_high=high, reference_low=low,
                  status="VALID", reason=None, close=close)
    return output


def _daily_close(frame: pd.DataFrame) -> pd.Series:
    values = pd.to_numeric(frame.Close, errors="coerce")
    index = frame.index.tz_convert("UTC").normalize()
    return pd.Series(values.to_numpy(), index=index).groupby(level=0).last().sort_index()


def relative_strength(asset: pd.DataFrame, benchmark: pd.DataFrame,
                      *, benchmark_name: str, windows: tuple[int, ...] = RS_WINDOWS) -> dict[str, Any]:
    asset_identity, benchmark_identity = _identity(asset), _identity(benchmark)
    output = {
        "benchmark": benchmark_name,
        "benchmark_identity": benchmark_identity,
        "definition": "aligned_session_relative_price_change_and_excess_simple_return",
        "windows": {}, "status": "UNKNOWN", "reason": None,
    }
    if asset_identity["currency"] != benchmark_identity["currency"] and not (
        asset.attrs.get("currency_normalized") and benchmark.attrs.get("currency_normalized")
    ):
        output["reason"] = "INCOMPATIBLE_BENCHMARK_CURRENCY"
        return output
    aligned = pd.concat([_daily_close(asset).rename("asset"), _daily_close(benchmark).rename("benchmark")], axis=1).dropna()
    for window in windows:
        item = {"sessions": window, "relative_price_change": None,
                "asset_return": None, "benchmark_return": None,
                "excess_return": None, "status": "UNKNOWN", "reason": None}
        if len(aligned) < window + 1:
            item["reason"] = "INSUFFICIENT_ALIGNED_SESSIONS"
        else:
            sample = aligned.iloc[-window - 1:]
            a0, a1 = _number(sample.asset.iloc[0]), _number(sample.asset.iloc[-1])
            b0, b1 = _number(sample.benchmark.iloc[0]), _number(sample.benchmark.iloc[-1])
            if not all(value is not None and value > 0 for value in (a0, a1, b0, b1)):
                item["reason"] = "INVALID_RELATIVE_STRENGTH_PRICE"
            else:
                asset_return = a1 / a0 - 1
                benchmark_return = b1 / b0 - 1
                item.update(
                    relative_price_change=(a1 / b1) / (a0 / b0) - 1,
                    asset_return=asset_return,
                    benchmark_return=benchmark_return,
                    excess_return=asset_return - benchmark_return,
                    status="VALID", reason=None,
                    start=_iso(sample.index[0]), end=_iso(sample.index[-1]),
                )
        output["windows"][str(window)] = item
    valid = sum(item["status"] == "VALID" for item in output["windows"].values())
    if valid:
        output.update(status="VALID" if valid == len(windows) else "PARTIAL", reason=None)
    else:
        output["reason"] = output["reason"] or "NO_VALID_RELATIVE_STRENGTH_WINDOW"
    return output


def _invalidators(signal_action: str, daily: Mapping[str, Any], weekly: Mapping[str, Any],
                  volume: Mapping[str, Any], breakout: Mapping[str, Any],
                  relative: Mapping[str, Any]) -> list[dict[str, Any]]:
    def item(code: str, status: str, reason: str, observed: Any = None) -> dict[str, Any]:
        return {"code": code, "status": status, "reason": reason, "observed": observed}

    result = []
    breakout_state = breakout.get("state")
    if breakout_state == "BREAKOUT_WITHOUT_VOLUME":
        result.append(item("BREAKOUT_VOLUME", "FAIL", "BREAKOUT_WITHOUT_VOLUME", breakout_state))
    elif breakout_state in {"BREAKOUT_VOLUME_UNKNOWN", "UNKNOWN"}:
        result.append(item("BREAKOUT_VOLUME", "UNKNOWN", "BREAKOUT_VOLUME_UNKNOWN", breakout_state))
    else:
        result.append(item("BREAKOUT_VOLUME", "PASS", "NO_UNCONFIRMED_BREAKOUT", breakout_state))

    weekly_trend = weekly.get("trend")
    if signal_action not in {"BUY", "ACCUMULATE"}:
        result.append(item("BUY_WEEKLY_STRUCTURE", "PASS", "SIGNAL_IS_NOT_BUY", weekly_trend))
    elif weekly_trend == "DOWNTREND":
        result.append(item("BUY_WEEKLY_STRUCTURE", "FAIL", "BUY_AGAINST_WEEKLY_DOWNTREND", weekly_trend))
    elif weekly_trend == "MIXED":
        result.append(item("BUY_WEEKLY_STRUCTURE", "WARN", "BUY_WITH_MIXED_WEEKLY_STRUCTURE", weekly_trend))
    elif weekly_trend == "UNKNOWN":
        result.append(item("BUY_WEEKLY_STRUCTURE", "UNKNOWN", "WEEKLY_STRUCTURE_UNKNOWN", weekly_trend))
    else:
        result.append(item("BUY_WEEKLY_STRUCTURE", "PASS", "WEEKLY_STRUCTURE_NOT_DETERIORATED", weekly_trend))

    general = relative.get("general", {})
    window20 = general.get("windows", {}).get("20", {})
    window60 = general.get("windows", {}).get("60", {})
    if window20.get("status") != "VALID":
        result.append(item("RELATIVE_STRENGTH", "UNKNOWN", "RS20_UNKNOWN"))
    elif window20["relative_price_change"] < 0 and window60.get("status") == "VALID" and window60["relative_price_change"] < 0:
        result.append(item("RELATIVE_STRENGTH", "FAIL", "RS20_AND_RS60_DETERIORATING",
                           {"rs20": window20["relative_price_change"], "rs60": window60["relative_price_change"]}))
    elif window20["relative_price_change"] < 0:
        result.append(item("RELATIVE_STRENGTH", "WARN", "RS20_DETERIORATING", window20["relative_price_change"]))
    else:
        result.append(item("RELATIVE_STRENGTH", "PASS", "RS20_NOT_DETERIORATING", window20["relative_price_change"]))

    distance = daily.get("distance_support")
    if distance is None:
        result.append(item("STRUCTURAL_SUPPORT", "UNKNOWN", "SUPPORT_UNKNOWN"))
    elif distance < 0:
        result.append(item("STRUCTURAL_SUPPORT", "FAIL", "PRICE_BELOW_STRUCTURAL_SUPPORT", distance))
    else:
        result.append(item("STRUCTURAL_SUPPORT", "PASS", "PRICE_NOT_BELOW_STRUCTURAL_SUPPORT", distance))

    if signal_action in {"BUY", "ACCUMULATE"} and volume.get("status") != "VALID":
        result.append(item("VOLUME_CONFIRMATION", "UNKNOWN", "BUY_VOLUME_UNKNOWN"))
    elif signal_action in {"BUY", "ACCUMULATE"} and volume.get("rvol", 0) < RVOL_CONTRACTION:
        result.append(item("VOLUME_CONFIRMATION", "WARN", "BUY_ON_CONTRACTING_VOLUME", volume.get("rvol")))
    else:
        result.append(item("VOLUME_CONFIRMATION", "PASS", "NO_BUY_VOLUME_WARNING", volume.get("rvol")))
    return result


def evaluate_contextual_invalidators(
    signal_action: str,
    daily: Mapping[str, Any],
    weekly: Mapping[str, Any],
    volume: Mapping[str, Any],
    breakout: Mapping[str, Any],
    relative: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Expose the shared E2 invalidator contract to read-only replay tooling.

    The wrapper deliberately returns the same descriptive PASS/WARN/FAIL/UNKNOWN
    records used by ``build_contextual_snapshot``.  It does not derive a score,
    change a signal or grant execution authority.
    """
    return _invalidators(
        signal_action,
        daily,
        weekly,
        volume,
        breakout,
        relative,
    )


def build_contextual_snapshot(
    asset_frame: pd.DataFrame,
    *,
    cutoff: Any,
    signal_action: str,
    benchmarks: Mapping[str, pd.DataFrame] | None = None,
    general_benchmark: str = "SPY",
    sector_benchmark: str | None = None,
) -> ContextualSnapshot:
    cutoff_ts = pd.Timestamp(cutoff)
    asset, missing = _pit_frame(asset_frame, cutoff_ts)
    identity = _identity(asset_frame)
    daily = market_structure(asset)
    weekly_frame = weekly_bars(asset, cutoff_ts)
    weekly = market_structure(weekly_frame)
    volume = rvol_context(asset)
    breakout = breakout_context(asset, volume)

    relative: dict[str, Any] = {}
    benchmark_sources, benchmark_digests = {}, {}
    for role, name in (("general", general_benchmark), ("sector", sector_benchmark)):
        if not name:
            continue
        source = (benchmarks or {}).get(name)
        if source is None:
            relative[role] = {"benchmark": name, "status": "UNKNOWN", "reason": "BENCHMARK_NOT_PROVIDED", "windows": {}}
            missing.append(f"{role.upper()}_BENCHMARK_NOT_PROVIDED:{name}")
            continue
        try:
            pit_benchmark, benchmark_missing = _pit_frame(source, cutoff_ts)
            missing.extend(f"{role.upper()}_{reason}" for reason in benchmark_missing)
            relative[role] = relative_strength(asset, pit_benchmark, benchmark_name=name)
            benchmark_sources[role] = {
                "name": name, "identity": _identity(source),
                "sources": list(source.attrs.get("candle_sources", ())),
            }
            benchmark_digests[role] = _digest(pit_benchmark.reset_index().to_dict("records"))
        except (ContextIdentityError, ValueError) as exc:
            relative[role] = {"benchmark": name, "status": "UNKNOWN", "reason": str(exc), "windows": {}}
            missing.append(f"{role.upper()}_BENCHMARK_INVALID:{exc}")

    components = {
        "trend_daily": daily["trend"], "trend_weekly": weekly["trend"],
        "structure_daily": daily, "structure_weekly": weekly,
        "relative_strength": relative, "rvol": volume.get("rvol"),
        "volume": volume, "volume_state": volume["state"],
        "breakout_state": breakout["state"], "breakout": breakout,
        "distance_support": daily["distance_support"],
        "distance_resistance": daily["distance_resistance"],
    }
    coverage = [
        daily["status"] == "VALID", weekly["status"] == "VALID",
        volume["status"] == "VALID", breakout["status"] == "VALID",
        relative.get("general", {}).get("status") in {"VALID", "PARTIAL"},
    ]
    ratio = sum(coverage) / len(coverage)
    components["context_confidence"] = {
        "value": ratio,
        "status": "HIGH" if ratio >= 0.8 else "MEDIUM" if ratio >= 0.5 else "LOW",
        "definition": "available_required_components / 5; descriptive coverage, not directional conviction",
        "components_available": sum(coverage), "components_required": len(coverage),
    }
    invalidators = evaluate_contextual_invalidators(
        signal_action,
        daily,
        weekly,
        volume,
        breakout,
        relative,
    )
    quality = frame_quality(asset, cutoff=cutoff_ts) if len(asset) else {
        "version": "contextual-e1-v1", "price_status": "INVALID",
        "volume_status": "PARTIAL", "provenance_status": "PARTIAL",
        "price_reasons": ["NO_PIT_ELIGIBLE_BARS"], "volume_reasons": ["NO_PIT_ELIGIBLE_BARS"],
        "provenance_reasons": ["NO_PIT_ELIGIBLE_BARS"], "bar_count": 0,
    }
    timestamps = {
        "cutoff": cutoff_ts.isoformat(),
        "last_asset_bar": _iso(asset.index[-1]) if len(asset) else None,
        "last_weekly_bar": _iso(weekly_frame.index[-1]) if len(weekly_frame) else None,
        "last_available_at": _iso(asset.AvailableAt.iloc[-1]) if len(asset) else None,
        "last_scraped_at": _iso(asset.RetrievedAt.iloc[-1]) if len(asset) else None,
    }
    sources = {
        "asset": list(asset_frame.attrs.get("candle_sources", ())),
        "provider_symbols": list(asset_frame.attrs.get("provider_symbols", ())),
        "benchmarks": benchmark_sources,
    }
    digests = {
        "asset": _digest(asset.reset_index().to_dict("records")),
        **{f"benchmark_{role}": value for role, value in benchmark_digests.items()},
    }
    payload = {
        "schema_version": CONTEXTUAL_SNAPSHOT_SCHEMA, "mode": "SHADOW_ONLY",
        "definition_version": CONTEXTUAL_MARKET_VERSION,
        "cutoff": cutoff_ts.isoformat(), "identity": identity, "sources": sources,
        "timestamps": timestamps, "quality": quality, "components": components,
        "invalidators": invalidators, "input_digests": digests,
        "missingness": sorted(set(missing)),
    }
    return ContextualSnapshot(
        snapshot_id=f"context:{_digest(payload)[:24]}",
        schema_version=CONTEXTUAL_SNAPSHOT_SCHEMA,
        definition_version=CONTEXTUAL_MARKET_VERSION,
        mode="SHADOW_ONLY", affects_analysis=False, affects_execution=False,
        cutoff=cutoff_ts.isoformat(), identity=identity, sources=sources,
        timestamps=timestamps, quality=quality, components=components,
        invalidators=invalidators, input_digests=digests,
        missingness=sorted(set(missing)),
    )


def attach_contextual_shadow(result: Any, snapshot: ContextualSnapshot) -> None:
    """Attach audit evidence without changing any productive result field."""
    result.contextual_market_shadow = snapshot.to_dict()


def render_contextual_diagnostic(frame: pd.DataFrame, snapshot: ContextualSnapshot,
                                 output: str | Path) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    pit, _ = _pit_frame(frame, snapshot.cutoff)
    if pit.empty:
        raise ValueError("NO_PIT_ELIGIBLE_BARS_FOR_DIAGNOSTIC")
    sample = pit.tail(120)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig, (price_ax, volume_ax) = plt.subplots(
        2, 1, figsize=(12, 7), sharex=True,
        gridspec_kw={"height_ratios": [3, 1]}, constrained_layout=True,
    )
    price_ax.plot(sample.index, sample.Close, color="#1f4e79", linewidth=1.8, label="Close")
    daily = snapshot.components["structure_daily"]
    for key, color, style in (("support", "#2e7d32", "--"), ("resistance", "#c62828", "--")):
        level = daily.get(key)
        if level:
            price_ax.axhline(level["value"], color=color, linestyle=style, linewidth=1.2,
                             label=f"{key.title()} {level['value']:.2f}")
    for swing in daily.get("confirmed_swing_highs", []):
        price_ax.scatter(pd.Timestamp(swing["timestamp"]), swing["value"], marker="v", color="#c62828", s=35)
    for swing in daily.get("confirmed_swing_lows", []):
        price_ax.scatter(pd.Timestamp(swing["timestamp"]), swing["value"], marker="^", color="#2e7d32", s=35)
    decision_at = pd.Timestamp(snapshot.cutoff)
    price_ax.axvline(decision_at, color="#6a1b9a", linewidth=1.2, label="Decision cutoff")
    price_ax.set_title(f"{snapshot.identity['ticker']} contextual diagnosis | SHADOW_ONLY")
    price_ax.set_ylabel(f"Price ({snapshot.identity['currency']})")
    price_ax.legend(loc="best", fontsize=8)
    volume = pd.to_numeric(sample.Volume, errors="coerce")
    expected = volume.shift(1).rolling(RVOL_WINDOW, min_periods=RVOL_WINDOW).mean()
    volume_ax.bar(sample.index, volume, color="#90a4ae", width=0.8, label="Volume")
    volume_ax.plot(sample.index, expected, color="#ef6c00", linewidth=1.4, label="Previous-20 mean")
    volume_ax.axvline(decision_at, color="#6a1b9a", linewidth=1.2)
    volume_ax.set_ylabel(snapshot.identity.get("volume_unit") or "Volume")
    volume_ax.legend(loc="best", fontsize=8)
    fig.savefig(output, dpi=150)
    plt.close(fig)
    return output


__all__ = [
    "BREAKOUT_CONFIRMATION_RVOL", "CONTEXTUAL_MARKET_VERSION",
    "CONTEXTUAL_SNAPSHOT_SCHEMA", "ContextIdentityError", "ContextualSnapshot",
    "attach_contextual_shadow", "breakout_context", "build_contextual_snapshot",
    "evaluate_contextual_invalidators",
    "market_structure", "relative_strength", "render_contextual_diagnostic",
    "rvol_context", "weekly_bars",
]
