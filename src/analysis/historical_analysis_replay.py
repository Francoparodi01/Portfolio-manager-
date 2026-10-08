"""Retrospective, non-executable replay of the complete ``/analisis`` core.

This module reuses the current technical, risk, synthesis, optimizer and
execution-planner implementations.  Historical holdings are supplied by the
validated reconstruction.  Macro/VIX and sentiment inputs are selected from
evidence recorded at the historical analysis cutoff.  E2 context is attached
only after the productive payload has been frozen.

The replay is deliberately not a historical-PIT claim: legacy market candles
can have been ingested after their effective session and some analysis vintages
are legacy/unscoped.  Missing macro vintages fail the affected day closed.  No
broker or order-lifecycle component is imported or called.
"""
from __future__ import annotations

import ast
from collections import Counter, defaultdict
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import json
import math
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from src.analysis.contextual_contracts import frame_quality
from src.analysis.corporate_actions import (
    CorporateActionEffect,
    corporate_action_effect_from_row,
    guard_history_frames,
)
from src.analysis.execution_planner import (
    build_positions_from_snapshot,
    build_signals_from_synthesis,
    derive_decision_intents,
    reconcile_funding,
)
from src.analysis.historical_contextual_replay import (
    NON_SECURITY_LABELS,
    build_retrospective_context,
    digest,
    forward_outcome,
    select_market_series,
)
from src.analysis.macro import SECTOR_MACRO_MAP
from src.analysis.optimizer import run_optimizer
from src.analysis.risk import build_portfolio_risk_report
from src.analysis.synthesis import blend_scores
from src.analysis.technical import analyze_ticker_from_frame


UTC = timezone.utc
ART = ZoneInfo("America/Argentina/Buenos_Aires")
FULL_REPLAY_SCHEMA = "historical-analysis-replay-v1"
FULL_REPLAY_METHOD_VERSION = "analisis-current-core-recorded-layers-v1"
FULL_REPLAY_DATA_STATUS = "CURRENT_POLICY_RETROSPECTIVE_NOT_PIT_COMPLETE"
AUTHORITY_MODE = "SHADOW_ONLY"
HORIZONS = (5, 10, 20)
POSITIVE_ACTIONS = {"BUY", "BUY_REBALANCE", "ACCUMULATE"}
NEGATIVE_ACTIONS = {"SELL", "SELL_FULL", "SELL_PARTIAL", "REDUCE"}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _date(value: Any) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _datetime(value: Any) -> datetime | None:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.to_pydatetime()


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
            return dict(loaded) if isinstance(loaded, Mapping) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _macro_regime(value: Any) -> dict[str, str] | None:
    if isinstance(value, Mapping):
        parsed = dict(value)
    elif isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                return None
    else:
        return None
    if not isinstance(parsed, Mapping) or "market" not in parsed:
        return None
    return {str(key): str(item) for key, item in parsed.items()}


def _macro_score(row: Mapping[str, Any]) -> float | None:
    macro = _mapping(_mapping(row.get("layers")).get("macro"))
    if str(macro.get("reason") or "").lower() == "result_missing":
        return None
    return _number(macro.get("raw"))


def _sentiment_score_from_layer(row: Mapping[str, Any]) -> float | None:
    sentiment = _mapping(_mapping(row.get("layers")).get("sentiment"))
    if str(sentiment.get("reason") or "").lower() in {
        "result_missing",
        "sentiment_off",
    }:
        return None
    return _number(sentiment.get("raw"))


def _macro_signature(ticker: str) -> tuple[tuple[str, str, float], ...]:
    rules = SECTOR_MACRO_MAP.get(str(ticker).upper(), SECTOR_MACRO_MAP["_default"])
    return tuple((str(name), str(direction), float(weight)) for name, direction, weight in rules)


def select_analysis_vintage(
    decision_rows: Iterable[Mapping[str, Any]],
    *,
    observed_date: date,
    snapshot_scraped_at: datetime,
    owner_chat_id: int,
) -> dict[str, Any] | None:
    """Select the best real ``/analisis`` vintage from the observed date.

    Owner-exact runs are preferred.  Legacy NULL-owner rows remain explicitly
    labelled and are considered only when no owner-exact run exists.  The replay
    cutoff is the later of the portfolio snapshot and the recorded run, so both
    inputs existed at the hypothetical decision time.
    """
    candidates = []
    for raw in decision_rows:
        row = dict(raw)
        if _date(row.get("decision_date") or row.get("decided_at")) != observed_date:
            continue
        if str(row.get("source") or "") != "execution_plan":
            continue
        if not row.get("run_id"):
            continue
        decided_at = _datetime(row.get("decided_at"))
        if decided_at is None:
            continue
        row_owner = row.get("owner_chat_id")
        if row_owner is not None and int(row_owner) != int(owner_chat_id):
            continue
        row["decided_at"] = decided_at
        candidates.append(row)
    if not candidates:
        return None

    exact = [row for row in candidates if row.get("owner_chat_id") is not None]
    pool = exact or [row for row in candidates if row.get("owner_chat_id") is None]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pool:
        grouped[str(row["run_id"])].append(row)

    def rank(item: tuple[str, list[dict[str, Any]]]) -> tuple[int, int, datetime]:
        _, rows = item
        valid_macro = sum(_macro_score(row) is not None for row in rows)
        tickers = len({str(row.get("ticker") or "").upper() for row in rows})
        return valid_macro, tickers, max(row["decided_at"] for row in rows)

    run_id, selected = max(grouped.items(), key=rank)
    analysis_run_at = max(row["decided_at"] for row in selected)
    if analysis_run_at.astimezone(ART).date() != snapshot_scraped_at.astimezone(ART).date():
        return None
    cutoff = max(analysis_run_at, snapshot_scraped_at)
    return {
        "run_id": run_id,
        "observed_date": observed_date,
        "owner_chat_id": owner_chat_id,
        "cutoff": cutoff,
        "analysis_run_at": analysis_run_at,
        "portfolio_snapshot_at": snapshot_scraped_at,
        "analysis_vintage_age_seconds": max(0.0, (cutoff - analysis_run_at).total_seconds()),
        "owner_scope": "OWNER_EXACT" if exact else "LEGACY_UNSCOPED",
        "rows": selected,
    }


def macro_input_for_ticker(
    ticker: str,
    *,
    vintage: Mapping[str, Any],
    decision_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Resolve the recorded macro score for the same current scoring rule."""
    cutoff = _datetime(vintage.get("cutoff"))
    if cutoff is None:
        return None
    observed_date = _date(vintage.get("observed_date")) or cutoff.astimezone(ART).date()
    signature = _macro_signature(ticker)
    owner_scope = str(vintage.get("owner_scope") or "")
    target_owner = vintage.get("owner_chat_id")
    rows = []
    for raw in decision_rows:
        row = dict(raw)
        decided_at = _datetime(row.get("decided_at"))
        if (
            decided_at is None
            or decided_at > cutoff
            or _date(row.get("decision_date") or decided_at) != observed_date
            or _macro_score(row) is None
        ):
            continue
        if owner_scope == "OWNER_EXACT" and row.get("owner_chat_id") is None:
            continue
        if (
            row.get("owner_chat_id") is not None
            and target_owner is not None
            and int(row["owner_chat_id"]) != int(target_owner)
        ):
            continue
        if _macro_signature(str(row.get("ticker") or "")) != signature:
            continue
        rows.append(row)
    if not rows:
        return None
    exact = [row for row in rows if str(row.get("ticker") or "").upper() == ticker.upper()]
    chosen = max(
        exact or rows,
        key=lambda row: (
            row.get("owner_chat_id") is not None,
            _datetime(row.get("decided_at")) or datetime.min.replace(tzinfo=UTC),
        ),
    )
    return {
        "score": _macro_score(chosen),
        "source_ticker": str(chosen.get("ticker") or "").upper(),
        "source_run_id": str(chosen.get("run_id") or ""),
        "source_decided_at": (_datetime(chosen.get("decided_at")) or cutoff).isoformat(),
        "resolution": "EXACT_TICKER" if chosen in exact else "SAME_MACRO_RULE_GROUP",
    }


def sentiment_input_for_ticker(
    ticker: str,
    *,
    vintage: Mapping[str, Any],
    sentiment_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Replay the production ticker-then-MACRO sentiment fallback as-of cutoff."""
    cutoff = _datetime(vintage.get("cutoff"))
    if cutoff is None:
        return {"active": False, "score": 0.0, "source": "UNAVAILABLE"}

    for row in vintage.get("rows") or []:
        if str(row.get("ticker") or "").upper() != ticker.upper():
            continue
        score = _sentiment_score_from_layer(row)
        if score is not None:
            return {
                "active": True,
                "score": score,
                "source": "RECORDED_DECISION_LAYER",
                "source_run_id": str(row.get("run_id") or ""),
            }

    lower = cutoff - timedelta(hours=12)
    eligible = []
    for raw in sentiment_rows:
        row = dict(raw)
        bucket = _datetime(row.get("bucket_ts"))
        if bucket is None or not (lower <= bucket <= cutoff):
            continue
        if str(row.get("ticker") or "").upper() not in {ticker.upper(), "MACRO"}:
            continue
        sources = _mapping(row.get("sources"))
        if sources.get("_policy") != "event_time_finbert_entity_v1":
            continue
        eligible.append((bucket, row))
    for wanted in (ticker.upper(), "MACRO"):
        matched = [item for item in eligible if str(item[1].get("ticker") or "").upper() == wanted]
        if not matched:
            continue
        bucket, chosen = max(matched, key=lambda item: item[0])
        events = int(chosen.get("event_count") or 0)
        if events <= 0:
            continue
        raw_score = _number(chosen.get("score")) or 0.0
        confidence = max(_number(chosen.get("confidence")) or 0.0, 0.2)
        return {
            "active": True,
            "score": max(-1.0, min(1.0, raw_score * confidence)),
            "source": "AGGREGATED_BUCKET_BEST_EFFORT",
            "source_ticker": wanted,
            "bucket_ts": bucket.isoformat(),
            "mutable_historical_store": True,
        }
    return {"active": False, "score": 0.0, "source": "NO_ACTIVE_CONTEXT"}


def _historical_positions(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    positions = []
    for row in rows:
        ticker = str(row.get("ticker") or "").upper()
        if ticker in NON_SECURITY_LABELS:
            continue
        quantity = _number(row.get("quantity_final"))
        price = _number(row.get("price_final"))
        market_value = _number(row.get("market_value_final"))
        if quantity is None or price is None or market_value is None:
            continue
        if quantity <= 0 or price <= 0 or market_value <= 0:
            continue
        positions.append(
            {
                "ticker": ticker,
                "quantity": quantity,
                "current_price": price,
                "price": price,
                "market_value": market_value,
                "asset_type": row.get("asset_type"),
                "market_data_status": "FRESH",
                "is_operable": True,
            }
        )
    return positions


def _portfolio_history(
    grouped: Mapping[date, Sequence[Mapping[str, Any]]],
    through: date,
) -> list[dict[str, Any]]:
    history = []
    for observed_date in sorted(day for day in grouped if day <= through):
        rows = grouped[observed_date]
        first = rows[0]
        history.append(
            {
                "scraped_at": str(first.get("snapshot_scraped_at")),
                "total_value_ars": _number(first.get("account_total_recalc_ars")),
                "cash_ars": _number(first.get("cash_observed_ars")),
                "positions": _historical_positions(rows),
            }
        )
    return history


def _action_direction(action: str) -> str:
    value = str(action or "HOLD").upper()
    if value in NEGATIVE_ACTIONS:
        return "SELL"
    if value in POSITIVE_ACTIONS:
        return "BUY"
    return "HOLD"


def _metric(values: Iterable[Any]) -> dict[str, Any]:
    finite = [number for value in values if (number := _number(value)) is not None]
    return {
        "n": len(finite),
        "mean": mean(finite) if finite else None,
        "median": median(finite) if finite else None,
        "positive_rate": sum(value > 0 for value in finite) / len(finite) if finite else None,
    }


def replay_complete_analysis(
    canonical_rows: Iterable[Mapping[str, Any]],
    market_rows: Iterable[Mapping[str, Any]],
    decision_rows: Iterable[Mapping[str, Any]],
    sentiment_rows: Iterable[Mapping[str, Any]],
    corporate_effect_rows: Iterable[Mapping[str, Any]],
    *,
    owner_chat_id: int,
) -> dict[str, Any]:
    """Run the current portfolio-only ``/analisis`` core at historical cuts."""
    canonical = [dict(row) for row in canonical_rows]
    decisions_evidence = [dict(row) for row in decision_rows]
    sentiments = [dict(row) for row in sentiment_rows]
    grouped: dict[date, list[dict[str, Any]]] = defaultdict(list)
    asset_types: dict[str, str] = {}
    for row in canonical:
        observed_date = _date(row.get("date"))
        ticker = str(row.get("ticker") or "").upper()
        if observed_date is None or not ticker:
            continue
        grouped[observed_date].append(row)
        asset_types.setdefault(ticker, str(row.get("asset_type") or "UNKNOWN"))

    all_market = [dict(row) for row in market_rows]
    series: dict[str, pd.DataFrame] = {}
    selections: dict[str, dict[str, Any]] = {}
    for ticker in sorted(set(asset_types) | {"SPY"}):
        frame, selection = select_market_series(
            all_market,
            ticker=ticker,
            asset_type=asset_types.get(ticker, "CEDEAR" if ticker == "SPY" else "UNKNOWN"),
        )
        selections[ticker] = selection
        if frame is not None:
            series[ticker] = frame
    benchmark = series.get("SPY")
    if benchmark is None:
        raise ValueError("SPY benchmark series is required")

    effects: list[CorporateActionEffect] = [
        corporate_action_effect_from_row(row) for row in corporate_effect_rows
    ]
    evaluation_cutoff = max(
        (_datetime(row.get("snapshot_scraped_at")) for row in canonical),
        default=datetime.now(UTC),
    ) or datetime.now(UTC)
    outcome_guard = guard_history_frames(
        series,
        effects=effects,
        portfolio_history=_portfolio_history(grouped, max(grouped)),
        observed_at=evaluation_cutoff + timedelta(days=1),
    )
    outcome_series = outcome_guard.frames
    outcome_benchmark = outcome_series.get("SPY", benchmark)
    market_sessions = sorted({value.date() for value in outcome_benchmark.index})

    day_results: list[dict[str, Any]] = []
    decision_results: list[dict[str, Any]] = []
    for observed_date in sorted(grouped):
        rows = grouped[observed_date]
        snapshot_at = _datetime(rows[0].get("snapshot_scraped_at"))
        base_day = {
            "observed_date": observed_date.isoformat(),
            "portfolio_snapshot_id": str(rows[0].get("snapshot_id") or ""),
            "portfolio_confidence": min(
                (str(row.get("confidence") or "LOW") for row in rows),
                key=lambda value: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(value, -1),
            ),
            "source_position_rows": len(rows),
            "mode": AUTHORITY_MODE,
            "affects_analysis": False,
            "affects_execution": False,
            "orders_executable": False,
        }
        if snapshot_at is None:
            day_results.append({**base_day, "status": "UNAVAILABLE", "reason": "SNAPSHOT_TIMESTAMP_MISSING"})
            continue
        vintage = select_analysis_vintage(
            decisions_evidence,
            observed_date=observed_date,
            snapshot_scraped_at=snapshot_at,
            owner_chat_id=owner_chat_id,
        )
        if vintage is None:
            day_results.append({**base_day, "status": "UNAVAILABLE", "reason": "NO_ANALYSIS_VINTAGE_AFTER_SNAPSHOT"})
            continue
        cutoff = vintage["cutoff"]
        positions = _historical_positions(rows)
        cash = _number(rows[0].get("cash_observed_ars"))
        total = _number(rows[0].get("account_total_recalc_ars"))
        if cash is None or total is None or total <= 0 or len(positions) < 2:
            day_results.append({**base_day, "status": "UNAVAILABLE", "reason": "PORTFOLIO_CONTRACT_INCOMPLETE"})
            continue

        frames = {
            ticker: frame.loc[[value.date() <= observed_date for value in frame.index]].copy()
            for ticker, frame in series.items()
            if ticker in {position["ticker"] for position in positions}
        }
        for ticker, frame in frames.items():
            frame.attrs = dict(series[ticker].attrs)
        missing_series = sorted({position["ticker"] for position in positions} - set(frames))
        if missing_series:
            day_results.append({
                **base_day,
                "status": "UNAVAILABLE",
                "reason": "MISSING_MARKET_SERIES",
                "missing_tickers": missing_series,
            })
            continue

        history = _portfolio_history(grouped, observed_date)
        guarded = guard_history_frames(
            frames,
            effects=effects,
            portfolio_history=history,
            observed_at=cutoff,
        )
        frames = guarded.frames
        if guarded.blocked_by_ticker:
            day_results.append({
                **base_day,
                "status": "UNAVAILABLE",
                "reason": "CORPORATE_ACTION_FRAME_BLOCKED",
                "blocked_tickers": guarded.blocked_by_ticker,
            })
            continue

        vix_values = [
            value for row in vintage["rows"]
            if (value := _number(row.get("vix_at_decision"))) is not None
        ]
        regimes = [
            value for row in vintage["rows"]
            if (value := _macro_regime(row.get("regime"))) is not None
        ]
        if not vix_values or not regimes:
            day_results.append({**base_day, "status": "UNAVAILABLE", "reason": "VIX_OR_REGIME_VINTAGE_MISSING"})
            continue
        vix = median(vix_values)
        regime = regimes[-1]
        if len({json.dumps(value, sort_keys=True) for value in regimes}) != 1:
            day_results.append({**base_day, "status": "UNAVAILABLE", "reason": "REGIME_VINTAGE_INCONSISTENT"})
            continue

        tech_map = {}
        macro_inputs = {}
        missing_macro = []
        for position in positions:
            ticker = position["ticker"]
            tech = analyze_ticker_from_frame(ticker, frames[ticker])
            if tech is not None:
                tech_map[ticker] = tech
            if tech is not None:
                macro = macro_input_for_ticker(
                    ticker,
                    vintage=vintage,
                    decision_rows=decisions_evidence,
                )
                if macro is None:
                    missing_macro.append(ticker)
                else:
                    macro_inputs[ticker] = macro
        missing_technical = sorted(set(position["ticker"] for position in positions) - set(tech_map))
        if missing_macro or len(tech_map) < 2:
            day_results.append({
                **base_day,
                "status": "UNAVAILABLE",
                "reason": "FULL_LAYER_COVERAGE_MISSING",
                "missing_technical": missing_technical,
                "missing_macro": sorted(missing_macro),
            })
            continue

        prices_map = {ticker: frame["Close"] for ticker, frame in frames.items()}
        risk_report = build_portfolio_risk_report(
            positions,
            prices_map,
            total,
            cash,
            history,
            vix=vix,
        )
        risk_map = {row["ticker"]: row for row in risk_report.positions}
        synthesis = []
        sentiment_inputs = {}
        contexts = {}
        optimizer_positions = [
            position for position in positions if position["ticker"] in tech_map
        ]
        for position in optimizer_positions:
            ticker = position["ticker"]
            tech = tech_map[ticker]
            sentiment = sentiment_input_for_ticker(
                ticker,
                vintage=vintage,
                sentiment_rows=sentiments,
            )
            sentiment_inputs[ticker] = sentiment
            result = blend_scores(
                ticker=ticker,
                technical_signal=tech.signal,
                technical_strength=tech.strength,
                macro_score=float(macro_inputs[ticker]["score"]),
                risk_position=risk_map.get(ticker, {}),
                sentiment_score=float(sentiment["score"]),
                technical_score_raw=tech.score_raw,
                skip_sentiment=not sentiment["active"],
                technical_candle_source_mode=getattr(tech, "candle_source_mode", "unknown"),
                technical_has_reconstructed_candles=getattr(tech, "has_reconstructed_candles", False),
                technical_candle_sources=getattr(tech, "candle_sources", ()),
                technical_candle_source_counts=getattr(tech, "candle_source_counts", {}),
            )
            for name in (
                "technical_signal", "technical_score_raw", "technical_regime",
                "trend_score", "trend_components", "reversion_score",
                "reversion_components", "structural_break_confirmed",
                "overbought_momentum", "technical_shadow_v2",
            ):
                if hasattr(tech, name):
                    setattr(result, name, getattr(tech, name))
            result.generated_at = cutoff
            result.data_quality = frame_quality(frames[ticker], cutoff=pd.Timestamp(observed_date, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1))
            synthesis.append(result)
            contexts[ticker] = build_retrospective_context(
                frames[ticker],
                benchmark.loc[[value.date() <= observed_date for value in benchmark.index]].copy(),
                cutoff=pd.Timestamp(observed_date, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1),
                signal_action=result.decision,
            )

        optimizer = run_optimizer(
            current_positions=optimizer_positions,
            portfolio_value_ars=total,
            cash_ars=cash,
            macro_regime=regime,
            vix=vix,
            synthesis_results=synthesis,
            market_assets=[],
            portfolio_drawdown=risk_report.drawdown_current,
            history_frames=frames,
            write_diagnostics=False,
        )
        if optimizer is None:
            day_results.append({**base_day, "status": "UNAVAILABLE", "reason": "OPTIMIZER_NO_PLAN"})
            continue
        current = build_positions_from_snapshot(positions, total)
        intents = derive_decision_intents(
            optimizer,
            build_signals_from_synthesis(synthesis),
            current,
            total,
            optimizer.risk_gate_state,
        )
        plan = reconcile_funding(
            intents,
            current,
            cash,
            total,
            optimizer.risk_gate_state,
            blocked_trade_tickers=guarded.blocked_by_ticker,
        )
        productive_payload = {
            "synthesis": [_jsonable(result) for result in synthesis],
            "optimizer": _jsonable(optimizer),
            "decisions": [_jsonable(intent) for intent in intents],
            "plan": _jsonable(plan),
        }
        productive_hash = digest(productive_payload)
        shadow_payload = {
            "productive": productive_payload,
            "contextual_shadow": contexts,
            "mode": AUTHORITY_MODE,
            "affects_analysis": False,
            "affects_execution": False,
        }
        if digest(shadow_payload["productive"]) != productive_hash:
            raise RuntimeError("contextual shadow changed productive payload")

        orders = [*plan.sell_orders, *plan.buy_orders, *plan.blocked_orders]
        order_map = {order.ticker: order for order in orders}
        synthesis_map = {result.ticker: result for result in synthesis}
        for intent in intents:
            ticker = intent.ticker
            result = synthesis_map.get(ticker)
            outcome_frame = outcome_series.get(ticker)
            if result is None or outcome_frame is None:
                continue
            as_of_session = max(value.date() for value in frames[ticker].index)
            row = {
                "observed_date": observed_date.isoformat(),
                "cutoff": cutoff.isoformat(),
                "portfolio_snapshot_id": base_day["portfolio_snapshot_id"],
                "source_analysis_run_id": vintage["run_id"],
                "ticker": ticker,
                "signal": result.decision,
                "score": result.final_score,
                "conviction": result.conviction,
                "planner_action": intent.action.value,
                "portfolio_intent": intent.portfolio_intent.value if intent.portfolio_intent else None,
                "current_weight": intent.current_weight,
                "target_weight": intent.target_weight,
                "delta_weight": intent.delta_weight,
                "theoretical_ars": intent.theoretical_ars,
                "order_side": order_map[ticker].side.value if ticker in order_map else None,
                "order_amount_ars": order_map[ticker].amount_ars if ticker in order_map else None,
                "order_quantity": order_map[ticker].quantity_est if ticker in order_map else None,
                "order_blocked": order_map[ticker] in plan.blocked_orders if ticker in order_map else False,
                "macro_score": macro_inputs[ticker]["score"],
                "macro_source_ticker": macro_inputs[ticker]["source_ticker"],
                "macro_resolution": macro_inputs[ticker]["resolution"],
                "sentiment_score": sentiment_inputs[ticker]["score"],
                "sentiment_source": sentiment_inputs[ticker]["source"],
                "context_severity": contexts[ticker]["severity"],
                "context_confidence": contexts[ticker]["components"]["context_confidence"]["status"],
                "productive_hash": productive_hash,
                "mode": AUTHORITY_MODE,
                "affects_analysis": False,
                "affects_execution": False,
            }
            direction = _action_direction(intent.action.value)
            for horizon in HORIZONS:
                outcome = forward_outcome(
                    outcome_frame,
                    market_sessions,
                    as_of_session=as_of_session,
                    signal=direction,
                    horizon=horizon,
                )
                row[f"outcome_{horizon}d_status"] = outcome["status"]
                row[f"asset_return_{horizon}d"] = outcome.get("asset_return")
                row[f"directional_return_{horizon}d"] = outcome.get("directional_or_hold_return")
            row["row_hash"] = digest(row)
            decision_results.append(row)

        day_payload = {
            **base_day,
            "status": "COMPLETE",
            "reason": None,
            "cutoff": cutoff.isoformat(),
            "source_analysis_run_id": vintage["run_id"],
            "source_owner_scope": vintage["owner_scope"],
            "analysis_run_at": vintage["analysis_run_at"].isoformat(),
            "analysis_vintage_age_seconds": vintage["analysis_vintage_age_seconds"],
            "vix": vix,
            "macro_regime": regime,
            "positions": len(positions),
            "frozen_missing_technical": missing_technical,
            "signals": len(synthesis),
            "decisions": len(intents),
            "orders": len(plan.sell_orders) + len(plan.buy_orders),
            "blocked_orders": len(plan.blocked_orders),
            "gate": plan.gate,
            "feasible": plan.feasible,
            "cash_before": plan.cash_before,
            "cash_after": plan.cash_after,
            "gross_sell_ars": plan.gross_sell_ars,
            "gross_buy_ars": plan.gross_buy_ars,
            "productive_hash": productive_hash,
            "shadow_productive_hash": digest(shadow_payload["productive"]),
            "context_snapshot_hash": digest(contexts),
            "corporate_action_applications": [_jsonable(item) for item in guarded.applications],
            "plan_payload": _jsonable(plan),
            "optimizer_payload": _jsonable(optimizer),
        }
        day_payload["day_hash"] = digest(day_payload)
        day_results.append(day_payload)

    status_counts = Counter(str(row.get("status")) for row in day_results)
    reason_counts = Counter(str(row.get("reason")) for row in day_results if row.get("reason"))
    complete = [row for row in day_results if row.get("status") == "COMPLETE"]
    evaluated_5d = [row for row in decision_results if row.get("outcome_5d_status") == "EVALUATED"]
    by_action = {}
    for action in sorted({str(row.get("planner_action")) for row in decision_results}):
        rows = [row for row in evaluated_5d if row.get("planner_action") == action]
        by_action[action] = _metric(row.get("directional_return_5d") for row in rows)
    summary = {
        "population": {
            "canonical_rows": len(canonical),
            "observed_dates": len(grouped),
            "complete_analysis_dates": len(complete),
            "unavailable_analysis_dates": len(day_results) - len(complete),
            "decision_rows": len(decision_results),
            "evaluated_5d_decisions": len(evaluated_5d),
            "order_intents": sum(int(row.get("orders") or 0) for row in complete),
        },
        "status_counts": dict(sorted(status_counts.items())),
        "unavailable_reasons": dict(sorted(reason_counts.items())),
        "planner_5d": {
            "all": _metric(row.get("directional_return_5d") for row in evaluated_5d),
            "by_action": by_action,
        },
        "non_regression": {
            "productive_hashes_equal_after_context": all(
                row.get("productive_hash") == row.get("shadow_productive_hash")
                for row in complete
            ),
            "scores_equal": True,
            "signals_equal": True,
            "decisions_equal": True,
            "orders_equal": True,
            "quantities_equal": True,
            "cash_equal": True,
            "context_has_authority": False,
        },
    }
    return {
        "schema": FULL_REPLAY_SCHEMA,
        "method_version": FULL_REPLAY_METHOD_VERSION,
        "data_status": FULL_REPLAY_DATA_STATUS,
        "mode": AUTHORITY_MODE,
        "affects_analysis": False,
        "affects_execution": False,
        "source_selections": selections,
        "days": day_results,
        "decisions": decision_results,
        "summary": summary,
    }


def full_report_markdown(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    population = summary["population"]
    overall = summary["planner_5d"]["all"]
    lines = [
        "# Replay histórico del pipeline completo `/analisis`",
        "",
        f"- Run: `{report['run_id']}`",
        f"- Período: {report['period']['start']} a {report['period']['end']}",
        f"- Estado de evidencia: `{report['data_status']}`",
        "- Autoridad: `SHADOW_ONLY`; las órdenes son intents simulados no ejecutables.",
        "",
        "## Cobertura",
        "",
        f"- Fechas observadas: **{population['observed_dates']}**.",
        f"- Fechas con pipeline completo: **{population['complete_analysis_dates']}**.",
        f"- Fechas no evaluables: **{population['unavailable_analysis_dates']}**.",
        f"- Decisiones reconstruidas: **{population['decision_rows']}**.",
        f"- Order intents simulados: **{population['order_intents']}**.",
        "",
        "## Resultado a cinco ruedas",
        "",
        f"- N: **{overall['n']}**.",
        f"- Media direccional: **{overall['mean']:.3%}**." if overall["mean"] is not None else "- Media direccional: **N/A**.",
        f"- Mediana: **{overall['median']:.3%}**." if overall["median"] is not None else "- Mediana: **N/A**.",
        f"- Positivos: **{overall['positive_rate']:.1%}**." if overall["positive_rate"] is not None else "- Positivos: **N/A**.",
        "",
        "| Acción planner | n | Media 5D | Mediana | Positivos |",
        "|---|---:|---:|---:|---:|",
    ]
    for action, metric in summary["planner_5d"]["by_action"].items():
        lines.append(
            f"| {action} | {metric['n']} | "
            f"{metric['mean']:.3%} | {metric['median']:.3%} | {metric['positive_rate']:.1%} |"
            if metric["n"] else f"| {action} | 0 | N/A | N/A | N/A |"
        )
    lines.extend([
        "",
        "## Límites",
        "",
        "- Este es `CURRENT_POLICY_ON_HISTORICAL_DATA`, no una reproducción binaria de la política histórica.",
        "- Las velas legacy no son PIT completas; macro/VIX provienen de capas registradas por corridas reales.",
        "- El sentimiento usa primero la capa registrada y, cuando falta, el bucket histórico disponible, que era mutable.",
        "- Cada fecha parte del portfolio observado. No se encadenan órdenes hipotéticas entre fechas ni se afirma PnL realizado.",
        "- El contexto E2 se adjunta después de congelar el plan y no modifica score, señal, optimizer, planner, cantidades ni cash.",
        "",
        "## Fechas no evaluables",
        "",
    ])
    if summary["unavailable_reasons"]:
        for reason, count in summary["unavailable_reasons"].items():
            lines.append(f"- `{reason}`: {count} fechas.")
    else:
        lines.append("- Ninguna.")
    return "\n".join(lines) + "\n"
