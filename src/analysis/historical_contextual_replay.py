"""Read-only retrospective replay over reconstructed historical holdings.

The replay intentionally keeps three evidence classes separate:

* observed portfolio rows from the validated reconstruction;
* historical market bars selected from one stable BYMA/provider series;
* E2 contextual diagnostics computed with only bars at or before each date.

Most historical TradingView bars were ingested after the dates they describe.
Consequently this module labels every result ``RETROSPECTIVE_NOT_PIT``.  It can
measure how today's deterministic technical/contextual definitions behave on
past prices, but it cannot claim to reproduce what Quantia knew at the time.

The contextual result is SHADOW_ONLY and reuses the productive technical signal
unchanged.  It never proposes a different order, target, quantity or cash path.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from hashlib import sha256
import json
import math
from statistics import mean, median
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from src.analysis.contextual_contracts import frame_quality
from src.analysis.contextual_market import (
    breakout_context,
    evaluate_contextual_invalidators,
    market_structure,
    relative_strength,
    rvol_context,
    weekly_bars,
)
from src.analysis.technical import analyze_ticker_from_frame


REPLAY_SCHEMA = "historical-contextual-replay-v1"
REPLAY_METHOD_VERSION = "holdings-eod-technical-context-v1"
DATA_STATUS = "RETROSPECTIVE_MARKET_HISTORY_NOT_PIT"
AUTHORITY_MODE = "SHADOW_ONLY"
SOURCE_PREFERENCE = (
    "TRADINGVIEW_BYMA",
    "COCOS",
    "YAHOO_BYMA",
    "internal_snapshot",
)
NON_SECURITY_LABELS = {"USESPECIE", "ARSCABLE"}
HORIZONS = (5, 10, 20)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _canonical(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def digest(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return sha256(encoded.encode("utf-8")).hexdigest()


def _normalise_date(value: Any) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _source_rank(source: str) -> tuple[int, str]:
    try:
        return SOURCE_PREFERENCE.index(source), source
    except ValueError:
        return len(SOURCE_PREFERENCE), source


def select_market_series(
    rows: Iterable[Mapping[str, Any]],
    *,
    ticker: str,
    asset_type: str,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    """Select one deterministic provider/instrument series for a ticker.

    A source with multiple divergent provider symbols is rejected instead of
    merged.  The selection favours the production historical source order and
    then the longest valid daily series.
    """
    ticker = str(ticker or "").strip().upper()
    candidates: list[
        tuple[tuple[int, int, int, str], pd.DataFrame, dict[str, Any]]
    ] = []
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for raw in rows:
        if str(raw.get("ticker") or "").strip().upper() != ticker:
            continue
        source = str(raw.get("source") or "UNKNOWN")
        symbol = str(raw.get("long_ticker") or raw.get("provider_symbol") or ticker)
        grouped[(source, symbol)].append(raw)

    for (source, symbol), items in grouped.items():
        prepared = []
        for item in items:
            ts = pd.to_datetime(item.get("ts"), errors="coerce", utc=True)
            values = {
                name: _number(item.get(column))
                for name, column in (
                    ("Open", "open_price"),
                    ("High", "high_price"),
                    ("Low", "low_price"),
                    ("Close", "close_price"),
                    ("Volume", "volume"),
                )
            }
            if pd.isna(ts) or any(values[name] is None or values[name] <= 0 for name in ("Open", "High", "Low", "Close")):
                continue
            if values["High"] < max(values["Open"], values["Low"], values["Close"]):
                continue
            if values["Low"] > min(values["Open"], values["High"], values["Close"]):
                continue
            prepared.append(
                {
                    "ts": ts,
                    **values,
                    "Source": source,
                    "ProviderSymbol": symbol,
                    "RetrievedAt": pd.to_datetime(
                        item.get("scraped_at"), errors="coerce", utc=True
                    ),
                }
            )
        if not prepared:
            continue
        frame = pd.DataFrame(prepared).sort_values("ts")
        frame["session_date"] = frame["ts"].dt.date
        divergent_days = 0
        chosen = []
        for _, day_rows in frame.groupby("session_date", sort=True):
            price_variants = day_rows[["Open", "High", "Low", "Close", "Volume"]].drop_duplicates()
            if len(price_variants) > 1:
                divergent_days += 1
                continue
            chosen.append(day_rows.iloc[-1])
        if not chosen:
            continue
        frame = pd.DataFrame(chosen).drop(columns=["session_date"]).set_index("ts").sort_index()
        valid_volume = int(
            (pd.to_numeric(frame["Volume"], errors="coerce").fillna(0) > 0).sum()
        )
        frame.attrs["series_identity"] = {
            "ticker": ticker,
            "asset_type": str(asset_type or "UNKNOWN"),
            "currency": "ARS",
            "venue": "BYMA",
            "interval": "1d",
            "volume_unit": None,
            "calendar": None,
            "adjustment_policy": None,
            "depositary_ratio": None,
            "instrument_id": f"BYMA:{str(asset_type or 'UNKNOWN').upper()}:{ticker}:ARS",
        }
        frame.attrs["candle_sources"] = (source,)
        frame.attrs["candle_source_counts"] = {source: len(frame)}
        frame.attrs["provider_symbols"] = [symbol]
        frame.attrs["selection_policy"] = "retrospective_source_priority_v1"
        frame.attrs["calendar_validation"] = None
        frame.attrs["has_reconstructed_candles"] = source == "internal_snapshot"
        info = {
            "ticker": ticker,
            "source": source,
            "provider_symbol": symbol,
            "rows": len(frame),
            "valid_volume_rows": valid_volume,
            "divergent_days_excluded": divergent_days,
            "first_session": frame.index[0].date().isoformat(),
            "last_session": frame.index[-1].date().isoformat(),
        }
        rank = (_source_rank(source)[0], -valid_volume, -len(frame), symbol)
        candidates.append((rank, frame, info))

    if not candidates:
        return None, {
            "ticker": ticker,
            "status": "MISSING",
            "reason": "NO_VALID_MARKET_SERIES",
        }
    _, frame, info = sorted(candidates, key=lambda item: item[0])[0]
    info["status"] = "SELECTED"
    info["alternatives"] = len(candidates) - 1
    return frame, info


def _slice_through_session(frame: pd.DataFrame, session: date) -> pd.DataFrame:
    mask = pd.Index([value.date() <= session for value in frame.index])
    sliced = frame.loc[mask].copy()
    sliced.attrs = dict(frame.attrs)
    return sliced


def _session_on_or_before(frame: pd.DataFrame, observed_date: date) -> date | None:
    sessions = [value.date() for value in frame.index if value.date() <= observed_date]
    return sessions[-1] if sessions else None


def _context_severity(invalidators: Iterable[Mapping[str, Any]]) -> str:
    statuses = {str(item.get("status") or "UNKNOWN").upper() for item in invalidators}
    for level in ("FAIL", "WARN", "UNKNOWN"):
        if level in statuses:
            return level
    return "PASS"


def build_retrospective_context(
    asset: pd.DataFrame,
    benchmark: pd.DataFrame,
    *,
    cutoff: Any,
    signal_action: str,
) -> dict[str, Any]:
    """Compute E2 dimensions from date-sliced bars without claiming PIT lineage."""
    cutoff_ts = pd.Timestamp(cutoff)
    if cutoff_ts.tzinfo is None:
        cutoff_ts = cutoff_ts.tz_localize("UTC")
    asset = asset.loc[asset.index <= cutoff_ts].copy()
    asset.attrs = dict(getattr(asset, "attrs", {}))
    benchmark = benchmark.loc[benchmark.index <= cutoff_ts].copy()
    benchmark.attrs = dict(getattr(benchmark, "attrs", {}))

    daily = market_structure(asset)
    weekly_frame = weekly_bars(asset, cutoff_ts)
    weekly = market_structure(weekly_frame)
    volume = rvol_context(asset)
    breakout = breakout_context(asset, volume)
    relative = {
        "general": relative_strength(
            asset,
            benchmark,
            benchmark_name="SPY",
        )
    }
    invalidators = evaluate_contextual_invalidators(
        signal_action,
        daily,
        weekly,
        volume,
        breakout,
        relative,
    )
    coverage = [
        daily["status"] == "VALID",
        weekly["status"] == "VALID",
        volume["status"] == "VALID",
        breakout["status"] == "VALID",
        relative["general"].get("status") in {"VALID", "PARTIAL"},
    ]
    confidence_value = sum(coverage) / len(coverage)
    confidence_status = (
        "HIGH" if confidence_value >= 0.8 else
        "MEDIUM" if confidence_value >= 0.5 else
        "LOW"
    )
    quality = frame_quality(asset, cutoff=cutoff_ts)
    late_ingested = 0
    if "RetrievedAt" in asset:
        retrieved = pd.to_datetime(asset["RetrievedAt"], utc=True, errors="coerce")
        late_ingested = int((retrieved.notna() & (retrieved > cutoff_ts)).sum())
    missingness = [
        "RETROSPECTIVE_MARKET_HISTORY_NOT_PIT",
        "AVAILABLE_AT_UNKNOWN_FOR_LEGACY_SERIES",
        "BAR_END_UNKNOWN_FOR_LEGACY_SERIES",
        "IS_CLOSED_INFERRED_FROM_DAILY_HISTORY_NOT_PERSISTED",
        "VOLUME_UNIT_UNKNOWN",
        "EXCHANGE_CALENDAR_VERSION_UNKNOWN",
        "ADJUSTMENT_POLICY_UNKNOWN",
        "SECTOR_BENCHMARK_NOT_CONFIGURED",
    ]
    if asset.attrs.get("series_identity", {}).get("asset_type") == "CEDEAR":
        missingness.append("DEPOSITARY_RATIO_UNKNOWN")
    if late_ingested:
        missingness.append("INGESTED_AFTER_HISTORICAL_CUTOFF")
    components = {
        "trend_daily": daily["trend"],
        "trend_weekly": weekly["trend"],
        "structure_daily": daily,
        "structure_weekly": weekly,
        "relative_strength": relative,
        "rvol": volume.get("rvol"),
        "volume": volume,
        "volume_state": volume["state"],
        "breakout_state": breakout["state"],
        "breakout": breakout,
        "distance_support": daily.get("distance_support"),
        "distance_resistance": daily.get("distance_resistance"),
        "context_confidence": {
            "value": confidence_value,
            "status": confidence_status,
            "definition": "available_required_components / 5; descriptive coverage, not directional conviction",
            "components_available": sum(coverage),
            "components_required": len(coverage),
        },
    }
    inputs = asset.reset_index().to_dict("records")
    benchmark_inputs = benchmark.reset_index().to_dict("records")
    payload = {
        "schema_version": "historical-contextual-snapshot-v1",
        "definition_version": "contextual-market-v1",
        "replay_method_version": REPLAY_METHOD_VERSION,
        "mode": AUTHORITY_MODE,
        "affects_analysis": False,
        "affects_execution": False,
        "data_status": DATA_STATUS,
        "cutoff": cutoff_ts.isoformat(),
        "identity": asset.attrs.get("series_identity", {}),
        "sources": {
            "asset": list(asset.attrs.get("candle_sources", ())),
            "provider_symbols": list(asset.attrs.get("provider_symbols", ())),
            "general_benchmark": "SPY",
        },
        "timestamps": {
            "last_asset_bar": asset.index[-1].isoformat() if len(asset) else None,
            "last_benchmark_bar": benchmark.index[-1].isoformat() if len(benchmark) else None,
            "historical_cutoff": cutoff_ts.isoformat(),
            "late_ingested_input_rows": late_ingested,
        },
        "quality": quality,
        "components": components,
        "invalidators": invalidators,
        "missingness": sorted(set(missingness)),
        "input_digests": {
            "asset": digest(inputs),
            "benchmark_spy": digest(benchmark_inputs),
        },
    }
    payload["snapshot_hash"] = digest(payload)
    payload["snapshot_id"] = f"historical-context:{payload['snapshot_hash'][:24]}"
    payload["severity"] = _context_severity(invalidators)
    return payload


def forward_outcome(
    asset: pd.DataFrame,
    market_sessions: list[date],
    *,
    as_of_session: date,
    signal: str,
    horizon: int,
) -> dict[str, Any]:
    """Next-session-open to H-th-session-close gross price outcome."""
    future = [session for session in market_sessions if session > as_of_session]
    if len(future) < horizon:
        return {
            "status": "IMMATURE",
            "horizon_sessions": horizon,
            "entry_session": future[0].isoformat() if future else None,
            "exit_session": None,
            "asset_return": None,
            "directional_or_hold_return": None,
        }
    path = future[:horizon]
    by_date = {value.date(): row for value, row in asset.iterrows()}
    if any(session not in by_date for session in path):
        return {
            "status": "MISSING_EXACT_SESSION_PRICE",
            "horizon_sessions": horizon,
            "entry_session": path[0].isoformat(),
            "exit_session": path[-1].isoformat(),
            "asset_return": None,
            "directional_or_hold_return": None,
        }
    rows = [by_date[session] for session in path]
    prices = [
        (_number(row.get("Open")), _number(row.get("Close")))
        for row in rows
    ]
    if any(opening is None or closing is None or opening <= 0 or closing <= 0 for opening, closing in prices):
        return {
            "status": "INVALID_PRICE",
            "horizon_sessions": horizon,
            "entry_session": path[0].isoformat(),
            "exit_session": path[-1].isoformat(),
            "asset_return": None,
            "directional_or_hold_return": None,
        }
    discontinuity = any(abs(closing / opening - 1) > 0.30 for opening, closing in prices)
    discontinuity |= any(
        abs(current[0] / previous[1] - 1) > 0.30
        for previous, current in zip(prices, prices[1:])
    )
    if discontinuity:
        return {
            "status": "UNVERIFIED_DISCONTINUITY",
            "horizon_sessions": horizon,
            "entry_session": path[0].isoformat(),
            "exit_session": path[-1].isoformat(),
            "asset_return": None,
            "directional_or_hold_return": None,
        }
    asset_return = prices[-1][1] / prices[0][0] - 1
    action = str(signal or "HOLD").upper()
    directional = -asset_return if action == "SELL" else asset_return
    return {
        "status": "EVALUATED",
        "horizon_sessions": horizon,
        "entry_session": path[0].isoformat(),
        "exit_session": path[-1].isoformat(),
        "entry_price": prices[0][0],
        "exit_price": prices[-1][1],
        "asset_return": asset_return,
        "directional_or_hold_return": directional,
        "semantics": "SELL_AVOIDANCE" if action == "SELL" else (
            "BUY_DIRECTIONAL" if action == "BUY" else "HELD_POSITION_EXPOSURE"
        ),
        "cost_bps": 0.0,
    }


def replay_holdings(
    canonical_rows: Iterable[Mapping[str, Any]],
    market_rows: Iterable[Mapping[str, Any]],
    event_rows: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Replay the technical baseline and contextual shadow for observed holdings."""
    canonical = [dict(row) for row in canonical_rows]
    events = {
        (str(row.get("event_date") or row.get("date")), str(row.get("ticker") or "").upper()): dict(row)
        for row in event_rows
    }
    asset_types: dict[str, str] = {}
    for row in canonical:
        ticker = str(row.get("ticker") or "").upper()
        candidate = str(row.get("asset_type") or "UNKNOWN").upper()
        if ticker and ticker not in asset_types:
            asset_types[ticker] = candidate

    all_market_rows = [dict(row) for row in market_rows]
    series: dict[str, pd.DataFrame] = {}
    selections: dict[str, dict[str, Any]] = {}
    requested = sorted(set(asset_types) | {"SPY"})
    for ticker in requested:
        frame, selection = select_market_series(
            all_market_rows,
            ticker=ticker,
            asset_type=asset_types.get(ticker, "CEDEAR" if ticker == "SPY" else "UNKNOWN"),
        )
        selections[ticker] = selection
        if frame is not None:
            series[ticker] = frame

    benchmark = series.get("SPY")
    if benchmark is None:
        raise ValueError("SPY benchmark series is required")
    market_sessions = sorted({value.date() for value in benchmark.index})
    seen_metric_keys: set[tuple[str, date]] = set()
    results: list[dict[str, Any]] = []

    for row in sorted(canonical, key=lambda item: (str(item.get("date")), str(item.get("ticker")))):
        ticker = str(row.get("ticker") or "").strip().upper()
        observed_date = _normalise_date(row.get("date"))
        if not ticker or observed_date is None:
            continue
        result: dict[str, Any] = {
            "date": observed_date.isoformat(),
            "ticker": ticker,
            "asset_type": row.get("asset_type"),
            "currency": row.get("currency"),
            "quantity_observed": _number(row.get("quantity_observed")),
            "price_observed": _number(row.get("price_observed")),
            "market_value_observed": _number(row.get("market_value_observed")),
            "weight_observed": _number(row.get("weight_final")),
            "account_weight_observed": (
                _number(row.get("market_value_observed")) / _number(row.get("account_total_recalc_ars"))
                if _number(row.get("market_value_observed")) is not None
                and _number(row.get("account_total_recalc_ars")) not in (None, 0)
                else None
            ),
            "cash_observed_ars": _number(row.get("cash_observed_ars")),
            "invested_total_observed_ars": _number(row.get("invested_total_observed_ars")),
            "account_total_recalc_ars": _number(row.get("account_total_recalc_ars")),
            "portfolio_snapshot_id": row.get("snapshot_id"),
            "portfolio_snapshot_scraped_at": row.get("snapshot_scraped_at"),
            "portfolio_owner_chat_id": _number(row.get("owner_chat_id")),
            "portfolio_confidence": row.get("confidence"),
            "portfolio_quality_flags": row.get("quality_flags"),
            "data_status": DATA_STATUS,
            "mode": AUTHORITY_MODE,
            "affects_analysis": False,
            "affects_execution": False,
            "old_analysis_version": "technical.py-current-at-replay",
            "new_analysis_version": "contextual-market-v1-shadow",
        }
        event = events.get((observed_date.isoformat(), ticker), {})
        result["position_event_classification"] = event.get("classification")
        result["position_event_confidence"] = event.get("confidence")

        if ticker in NON_SECURITY_LABELS or str(row.get("asset_type") or "").upper() == "UNKNOWN":
            result.update(
                analysis_status="EXCLUDED_NON_SECURITY_LABEL",
                metric_eligible=False,
                metric_exclusion_reason="NON_SECURITY_LABEL",
            )
            results.append(result)
            continue
        asset_full = series.get(ticker)
        if asset_full is None:
            result.update(
                analysis_status="MISSING_MARKET_SERIES",
                metric_eligible=False,
                metric_exclusion_reason="MISSING_MARKET_SERIES",
            )
            results.append(result)
            continue
        session = _session_on_or_before(benchmark, observed_date)
        if session is None:
            result.update(
                analysis_status="NO_MARKET_SESSION_ON_OR_BEFORE_DATE",
                metric_eligible=False,
                metric_exclusion_reason="NO_MARKET_SESSION",
            )
            results.append(result)
            continue
        cutoff = pd.to_datetime(row.get("snapshot_scraped_at"), errors="coerce", utc=True)
        if pd.isna(cutoff):
            cutoff = pd.Timestamp(session, tz="UTC") + pd.Timedelta(hours=23, minutes=59)
            result["historical_cutoff_basis"] = "DATE_END_FALLBACK"
        else:
            result["historical_cutoff_basis"] = "OBSERVED_PORTFOLIO_SNAPSHOT"
        asset = _slice_through_session(asset_full, session)
        benchmark_slice = _slice_through_session(benchmark, session)
        asset.attrs["cutoff"] = cutoff
        benchmark_slice.attrs["cutoff"] = cutoff
        result["market_session"] = session.isoformat()
        result["market_source"] = selections[ticker].get("source")
        result["provider_symbol"] = selections[ticker].get("provider_symbol")
        result["historical_cutoff"] = cutoff.isoformat()
        result["input_bar_count"] = len(asset)

        signal = analyze_ticker_from_frame(ticker, asset) if len(asset) >= 60 else None
        if signal is None:
            result.update(
                analysis_status="INSUFFICIENT_TECHNICAL_HISTORY",
                metric_eligible=False,
                metric_exclusion_reason="INSUFFICIENT_TECHNICAL_HISTORY",
            )
            results.append(result)
            continue
        context = build_retrospective_context(
            asset,
            benchmark_slice,
            cutoff=cutoff,
            signal_action=signal.signal,
        )
        metric_key = (ticker, session)
        duplicate_inputs = metric_key in seen_metric_keys
        seen_metric_keys.add(metric_key)
        portfolio_confidence = str(row.get("confidence") or "UNKNOWN").upper()
        metric_eligible = not duplicate_inputs and portfolio_confidence in {"HIGH", "MEDIUM"}
        result.update(
            analysis_status="COMPLETE",
            metric_eligible=metric_eligible,
            metric_exclusion_reason=(
                "DUPLICATE_TICKER_MARKET_SESSION" if duplicate_inputs else
                "LOW_PORTFOLIO_CONFIDENCE" if portfolio_confidence not in {"HIGH", "MEDIUM"} else
                None
            ),
            old_signal=signal.signal,
            old_score_raw=signal.score_raw,
            old_strength=signal.strength,
            old_technical_regime=signal.technical_regime,
            new_signal=signal.signal,
            new_score_raw=signal.score_raw,
            signal_changed=False,
            score_changed=False,
            context_severity=context["severity"],
            context_confidence=context["components"]["context_confidence"]["status"],
            context_confidence_value=context["components"]["context_confidence"]["value"],
            trend_daily=context["components"]["trend_daily"],
            trend_weekly=context["components"]["trend_weekly"],
            rvol=context["components"]["rvol"],
            volume_state=context["components"]["volume_state"],
            breakout_state=context["components"]["breakout_state"],
            rs20=(
                context["components"]["relative_strength"]["general"]
                .get("windows", {}).get("20", {}).get("relative_price_change")
            ),
            rs60=(
                context["components"]["relative_strength"]["general"]
                .get("windows", {}).get("60", {}).get("relative_price_change")
            ),
            rs120=(
                context["components"]["relative_strength"]["general"]
                .get("windows", {}).get("120", {}).get("relative_price_change")
            ),
            distance_support=context["components"]["distance_support"],
            distance_resistance=context["components"]["distance_resistance"],
            invalidators=context["invalidators"],
            contextual_snapshot=context,
            contextual_snapshot_hash=context["snapshot_hash"],
            contextual_snapshot_id=context["snapshot_id"],
            late_ingested_input_rows=context["timestamps"]["late_ingested_input_rows"],
        )
        for horizon in HORIZONS:
            outcome = forward_outcome(
                asset_full,
                market_sessions,
                as_of_session=session,
                signal=signal.signal,
                horizon=horizon,
            )
            result[f"outcome_{horizon}d"] = outcome
            result[f"outcome_{horizon}d_status"] = outcome["status"]
            result[f"asset_return_{horizon}d"] = outcome.get("asset_return")
            result[f"directional_or_hold_return_{horizon}d"] = outcome.get(
                "directional_or_hold_return"
            )
        results.append(result)

    return {
        "schema": REPLAY_SCHEMA,
        "method_version": REPLAY_METHOD_VERSION,
        "data_status": DATA_STATUS,
        "mode": AUTHORITY_MODE,
        "affects_analysis": False,
        "affects_execution": False,
        "source_selections": selections,
        "results": results,
    }


def _metric(values: Iterable[Any]) -> dict[str, Any]:
    clean = [value for raw in values if (value := _number(raw)) is not None]
    return {
        "n": len(clean),
        "mean": mean(clean) if clean else None,
        "median": median(clean) if clean else None,
        "positive": sum(value > 0 for value in clean),
        "positive_rate": (
            sum(value > 0 for value in clean) / len(clean) if clean else None
        ),
        "min": min(clean) if clean else None,
        "max": max(clean) if clean else None,
    }


def summarize_replay(payload: Mapping[str, Any]) -> dict[str, Any]:
    results = [dict(row) for row in payload.get("results", [])]
    complete = [row for row in results if row.get("analysis_status") == "COMPLETE"]
    metric_rows = [row for row in complete if row.get("metric_eligible") is True]
    evaluated_5d = [
        row for row in metric_rows if row.get("outcome_5d_status") == "EVALUATED"
    ]
    action_5d = [row for row in evaluated_5d if row.get("old_signal") in {"BUY", "SELL"}]
    hold_5d = [row for row in evaluated_5d if row.get("old_signal") == "HOLD"]

    by_signal = {}
    for signal in ("BUY", "SELL", "HOLD"):
        rows = [row for row in evaluated_5d if row.get("old_signal") == signal]
        by_signal[signal] = {
            "directional_or_hold": _metric(
                row.get("directional_or_hold_return_5d") for row in rows
            ),
            "asset_return": _metric(row.get("asset_return_5d") for row in rows),
        }
    by_context = {}
    for severity in ("PASS", "WARN", "FAIL", "UNKNOWN"):
        rows = [row for row in evaluated_5d if row.get("context_severity") == severity]
        by_context[severity] = {
            "directional_or_hold": _metric(
                row.get("directional_or_hold_return_5d") for row in rows
            ),
            "asset_return": _metric(row.get("asset_return_5d") for row in rows),
        }

    daily_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evaluated_5d:
        daily_groups[str(row.get("market_session"))].append(row)
    daily_static = []
    for session, rows in sorted(daily_groups.items()):
        coverage = sum(_number(row.get("account_weight_observed")) or 0.0 for row in rows)
        value = sum(
            (_number(row.get("account_weight_observed")) or 0.0)
            * (_number(row.get("asset_return_5d")) or 0.0)
            for row in rows
        )
        daily_static.append(
            {
                "market_session": session,
                "positions": len(rows),
                "account_weight_covered": coverage,
                "static_hold_return_5d": value if coverage >= 0.80 else None,
                "status": "EVALUATED" if coverage >= 0.80 else "INSUFFICIENT_WEIGHT_COVERAGE",
            }
        )

    exclusion_counts = Counter(
        str(row.get("metric_exclusion_reason") or "NONE")
        for row in results
        if row.get("metric_eligible") is not True
    )
    status_counts = Counter(str(row.get("analysis_status")) for row in results)
    signal_counts = Counter(str(row.get("old_signal")) for row in metric_rows)
    severity_counts = Counter(str(row.get("context_severity")) for row in metric_rows)
    outcome_status_counts = Counter(str(row.get("outcome_5d_status")) for row in metric_rows)
    late_rows = sum(int(row.get("late_ingested_input_rows") or 0) for row in complete)
    all_equal = all(
        row.get("old_signal") == row.get("new_signal")
        and _number(row.get("old_score_raw")) == _number(row.get("new_score_raw"))
        for row in complete
    )
    return {
        "population": {
            "canonical_rows": len(results),
            "complete_rows": len(complete),
            "metric_eligible_rows": len(metric_rows),
            "evaluated_5d_rows": len(evaluated_5d),
            "action_5d_rows": len(action_5d),
            "hold_5d_rows": len(hold_5d),
            "observed_dates": len({row.get("date") for row in results}),
            "market_sessions": len({row.get("market_session") for row in metric_rows}),
            "tickers": len({row.get("ticker") for row in results}),
            "analyzed_tickers": len({row.get("ticker") for row in complete}),
        },
        "analysis_status_counts": dict(sorted(status_counts.items())),
        "metric_exclusion_counts": dict(sorted(exclusion_counts.items())),
        "signal_counts": dict(sorted(signal_counts.items())),
        "context_severity_counts": dict(sorted(severity_counts.items())),
        "outcome_5d_status_counts": dict(sorted(outcome_status_counts.items())),
        "old_analysis": {
            "all_directional_or_hold_5d": _metric(
                row.get("directional_or_hold_return_5d") for row in evaluated_5d
            ),
            "action_only_directional_5d": _metric(
                row.get("directional_or_hold_return_5d") for row in action_5d
            ),
            "hold_exposure_5d": _metric(row.get("asset_return_5d") for row in hold_5d),
            "by_signal": by_signal,
        },
        "contextual_analysis": {
            "by_severity": by_context,
            "context_has_economic_authority": False,
            "decision_uplift_measurable": False,
            "reason": "E2 is descriptive SHADOW_ONLY and preserves the baseline signal",
        },
        "daily_static_hold_5d": daily_static,
        "non_regression": {
            "scores_equal": all_equal,
            "signals_equal": all_equal,
            "decisions_changed": 0,
            "orders_created": 0,
            "quantities_changed": 0,
            "cash_changed": False,
            "affects_analysis": False,
            "affects_execution": False,
        },
        "quality": {
            "data_status": DATA_STATUS,
            "late_ingested_input_rows_across_replays": late_rows,
            "pit_eligible": False,
            "current_code_replay": True,
            "historical_macro_replayed": False,
            "historical_sentiment_replayed": False,
            "historical_optimizer_replayed": False,
            "historical_planner_replayed": False,
            "outcomes_are_realized_pnl": False,
        },
    }


def report_markdown(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    population = summary["population"]
    old = summary["old_analysis"]
    context = summary["contextual_analysis"]

    def pct(value: Any) -> str:
        number = _number(value)
        return "N/D" if number is None else f"{number * 100:+.2f}%"

    def metric_line(label: str, metric: Mapping[str, Any]) -> str:
        return (
            f"| {label} | {metric.get('n', 0)} | {pct(metric.get('mean'))} | "
            f"{pct(metric.get('median'))} | {pct(metric.get('positive_rate'))} |"
        )

    lines = [
        "# Replay histórico de portfolio: análisis base vs contexto E2",
        "",
        f"Run: `{report['run_id']}`  ",
        f"Reconstrucción: `{report['reconstruction_id']}`  ",
        f"Código: `{report['code_version']}`  ",
        f"Período: {report['period']['start']} a {report['period']['end']}  ",
        f"Estado de evidencia: **{report['data_status']}**",
        "",
        "## Alcance",
        "",
        "El análisis base vuelve a ejecutar la capa técnica determinística actual sobre cada posición observada. El análisis nuevo adjunta E2 —estructura, volumen, RVOL, fuerza relativa e invalidadores— sin modificar la señal. No reconstruye macro, sentiment, optimizer ni planner históricos porque no existen vintages completos y verificables para cada corte.",
        "",
        "Las velas históricas principales fueron ingeridas después de las fechas que describen. Por eso esta entrega es un replay retrospectivo sin lookahead de barras por fecha, pero **no una reproducción PIT de lo que Quantia conocía entonces**.",
        "",
        "## Cobertura",
        "",
        f"- Filas canónicas preservadas: **{population['canonical_rows']}**.",
        f"- Filas con análisis completo: **{population['complete_rows']}**.",
        f"- Observaciones únicas ticker/sesión elegibles para métricas: **{population['metric_eligible_rows']}**.",
        f"- Outcomes maduros a cinco ruedas: **{population['evaluated_5d_rows']}**.",
        f"- Fechas observadas: **{population['observed_dates']}**; sesiones de mercado: **{population['market_sessions']}**.",
        f"- Tickers preservados: **{population['tickers']}**; analizados: **{population['analyzed_tickers']}**.",
        "",
        "Los snapshots de fines de semana o fechas que compartían la misma última rueda se conservaron, pero no se contaron dos veces en las métricas.",
        "",
        "## Resultado del análisis base a cinco ruedas",
        "",
        "| Cohorte | n | Media | Mediana | Positivos |",
        "|---|---:|---:|---:|---:|",
        metric_line("BUY/SELL", old["action_only_directional_5d"]),
        metric_line("HOLD como exposición mantenida", old["hold_exposure_5d"]),
        metric_line("Total descriptivo", old["all_directional_or_hold_5d"]),
        "",
        "Para SELL, el outcome direccional es el retorno evitado; para BUY es el retorno del activo; para HOLD es el retorno de mantener una posición ya existente. No son PnL realizado, no incluyen comisiones y las ventanas superpuestas no son operaciones independientes.",
        "",
        "### Por señal",
        "",
        "| Señal base | n | Media | Mediana | Positivos |",
        "|---|---:|---:|---:|---:|",
    ]
    for signal in ("BUY", "SELL", "HOLD"):
        lines.append(metric_line(signal, old["by_signal"][signal]["directional_or_hold"]))
    lines.extend(
        [
            "",
            "## Contexto E2 shadow",
            "",
            "| Severidad contextual | n | Retorno medio del activo 5D | Mediana | Positivos |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for severity in ("PASS", "WARN", "FAIL", "UNKNOWN"):
        lines.append(metric_line(severity, context["by_severity"][severity]["asset_return"]))
    lines.extend(
        [
            "",
            "La tabla mide si las advertencias contextuales separan cohortes con distinto retorno posterior. No mide un incremento de performance de una estrategia nueva: el contexto no cambia decisiones y no existe una regla económica autorizada para transformar FAIL/WARN en órdenes.",
            "",
            "## No regresión",
            "",
            f"- Scores iguales: **{summary['non_regression']['scores_equal']}**.",
            f"- Señales iguales: **{summary['non_regression']['signals_equal']}**.",
            "- Decisiones cambiadas: **0**.",
            "- Órdenes creadas: **0**.",
            "- Cantidades y cash modificados: **0**.",
            "- `SHADOW_ONLY`, `affects_analysis=false`, `affects_execution=false`.",
            "",
            "## Datos preservados",
            "",
        (
            "La reconstrucción original se importó en tablas separadas y append-only. "
            if report.get("persistence_status") == "COMPLETE" else
            "La reconstrucción original se mantuvo en los archivos canónicos y el replay se ejecutó sin persistir. "
        ) + "No se actualizó `portfolio_snapshots`, `positions`, `decision_log`, `execution_plans`, `order_intents`, cash ni ningún registro productivo. Cantidad, precio, market value, peso, snapshot, owner y confidence observados permanecen disponibles junto al resultado del replay.",
            "",
            "YPFD conserva el evento 5→50 como `CORPORATE_ACTION_SPLIT`; no se lo cuenta como compra. La serie TradingView seleccionada ya presenta el histórico de precios comparable alrededor del cambio 1:10, y cualquier discontinuidad diaria superior a 30% hace fallar el outcome de forma cerrada.",
            "",
            "## Limitaciones",
            "",
            "- No es un backtest PIT estricto porque `scraped_at` de gran parte del histórico es posterior al cutoff.",
            "- Ejecuta el código técnico/contextual actual, no binarios históricos ni configuración versionada de cada fecha.",
            "- No reconstruye macro, sentiment, universo, optimizer o planner históricos.",
            "- No simula cantidades compradas/vendidas ni una curva de cash.",
            "- No representa PnL real de cuenta ni reconcilia comisiones, impuestos, dividendos o flujos externos.",
            "- Las observaciones diarias y sus ventanas de cinco ruedas se superponen; los porcentajes son descriptivos.",
            "",
            "## Conclusión",
            "",
            "El replay es apto para evaluar la cobertura del análisis actual sobre las tenencias históricas y para estudiar si los invalidadores E2 separan contextos más débiles. No autoriza reemplazar el análisis productivo ni permite afirmar mejora económica del análisis nuevo mientras siga siendo descriptivo y no exista validación prospectiva de una política concreta.",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "AUTHORITY_MODE",
    "DATA_STATUS",
    "HORIZONS",
    "REPLAY_METHOD_VERSION",
    "REPLAY_SCHEMA",
    "build_retrospective_context",
    "digest",
    "forward_outcome",
    "report_markdown",
    "replay_holdings",
    "select_market_series",
    "summarize_replay",
]
