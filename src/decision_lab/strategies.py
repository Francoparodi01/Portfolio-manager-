"""Versioned offline adapters. No network, database, training or parameter search.

quantia_core_v1 is the frozen core-kernel experiment (portfolio-only, no Radar).
Recorded full production plans are a different estimand, never its substitute.
"""

from __future__ import annotations

from dataclasses import asdict, fields
from datetime import datetime
from decimal import Decimal
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
import platform
import sys

from .models import FrozenPlan, HistoricalState, Order, StrategySpec, canonical, digest
from .state import InsufficientEvidence, require_kind


ROOT = Path(__file__).resolve().parents[2]
IMPLEMENTATION_FILES = (
    "src/analysis/technical.py",
    "src/analysis/risk.py",
    "src/analysis/macro.py",
    "src/analysis/synthesis.py",
    "src/analysis/optimizer.py",
    "src/analysis/execution_planner.py",
    "src/analysis/technical_shadow_v2.py",
    "src/analysis/technical_buy_shadow_v3.py",
    "src/decision_lab/strategies.py",
    "src/decision_lab/state.py",
    "src/decision_lab/models.py",
    "src/decision_lab/counterfactuals.py",
    "src/decision_lab/statistics.py",
    "src/decision_lab/runner.py",
    "src/analysis/date_block_statistics.py",
)


def implementation_manifest():
    dependencies = {}
    for package in (
        "numpy",
        "pandas",
        "PyPortfolioOpt",
        "cvxpy",
        "scipy",
        "scikit-learn",
    ):
        try:
            dependencies[package] = version(package)
        except PackageNotFoundError:
            dependencies[package] = "NOT_INSTALLED"
    return {
        "files": {
            name: digest((ROOT / name).read_text(encoding="utf-8"))
            for name in IMPLEMENTATION_FILES
        },
        "dependencies": dependencies,
        "python": sys.version,
        "platform": platform.platform(),
    }


def current_spec(
    adapter: str,
    *,
    registered_at: datetime,
    code_version: str,
    config_hash: str,
    strategy_version: str | None = None,
) -> StrategySpec:
    manifest = implementation_manifest()
    return StrategySpec(
        strategy_version=strategy_version or adapter,
        adapter=adapter,
        available_at=registered_at,
        config_available_at=registered_at,
        code_version=code_version,
        config_hash=config_hash,
        planner_version=manifest["files"]["src/analysis/execution_planner.py"],
        optimizer_version=manifest["files"]["src/analysis/optimizer.py"],
        risk_policy_version=manifest["files"]["src/analysis/risk.py"],
        implementation_hash=digest(manifest),
    )


def _check_version(state, spec, mode):
    if mode == "HISTORICAL_POLICY_REPLAY":
        if spec.available_at > state.as_of or spec.config_available_at > state.as_of:
            raise InsufficientEvidence("POLICY_OR_CONFIG_NOT_AVAILABLE_AT_T")
        if any(
            x.lower() in {"unknown", "none", ""}
            for x in (
                spec.code_version,
                spec.strategy_version,
                spec.planner_version,
                spec.optimizer_version,
                spec.risk_policy_version,
            )
        ):
            raise InsufficientEvidence("UNKNOWN_HISTORICAL_VERSION")
    if spec.training_end and spec.training_end >= state.as_of:
        raise InsufficientEvidence("TRAINING_REACHES_EVALUATION")
    if spec.implementation_hash != digest(implementation_manifest()):
        raise InsufficientEvidence("RUNTIME_DIFFERS_FROM_FROZEN_IMPLEMENTATION")
    if state.quality.level == "INVALID":
        raise InsufficientEvidence("INVALID_STATE")
    if mode == "RECORDED_PLAN_EVALUATION" and spec.adapter != "recorded_plan_v1":
        # HOLD is a neutral comparison with the same recorded opportunities.
        if spec.adapter != "hold_v1":
            raise ValueError(
                "recorded plan mode cannot pretend to rerun a different strategy"
            )


def _recorded(state):
    record = require_kind(state, "PLAN")[-1]
    p = record.payload
    if not p.get("complete", False):
        raise InsufficientEvidence("RECORDED_PLAN_INCOMPLETE")
    if (
        p.get("portfolio_snapshot_id")
        and p["portfolio_snapshot_id"] != state.portfolio_snapshot_id
    ):
        raise InsufficientEvidence("PLAN_PORTFOLIO_LINK_MISMATCH")
    observed_cash = (
        state.cash_ars.quantize(Decimal(1))
        if p.get("cash_precision") == "ROUND_HALF_EVEN_WHOLE_ARS"
        else state.cash_ars
    )
    if abs(Decimal(str(p["cash_before"])) - observed_cash) > Decimal("0.05"):
        raise InsufficientEvidence("PLAN_PORTFOLIO_CASH_MISMATCH")
    orders = tuple(Order.model_validate(row) for row in p["orders"])
    return (
        orders,
        bool(p["feasible"]),
        {
            "record_id": record.record_id,
            "record_hash": record.content_hash,
            "historical_strategy_version": p.get(
                "historical_strategy_version", "UNKNOWN"
            ),
            "scope": "RECORDED_PROPOSAL_NOT_REEXECUTED_STRATEGY",
            "decisions": p.get("decisions", []),
            "segments": p.get("segments", {}),
        },
    )


def _core(state):
    import pandas as pd
    from src.analysis.technical import analyze_ticker_from_frame
    from src.analysis.risk import build_portfolio_risk_report
    from src.analysis.macro import (
        MacroSnapshot,
        score_macro_for_ticker,
        get_macro_regime,
    )
    from src.analysis.synthesis import blend_scores
    from src.analysis.optimizer import run_optimizer
    from src.analysis.execution_planner import (
        build_signals_from_synthesis,
        build_positions_from_snapshot,
        derive_decision_intents,
        reconcile_funding,
    )

    config = require_kind(state, "CONFIG")[-1].payload
    if config.get("scope") != "PORTFOLIO_ONLY_NO_RADAR":
        raise InsufficientEvidence("EXPLICIT_CORE_SCOPE_REQUIRED")
    for required in (
        "lookback_sessions",
        "sentiment_enabled",
        "portfolio_history",
        "events_complete",
    ):
        if required not in config:
            raise InsufficientEvidence(f"CONFIG_REQUIRED:{required}")
    if not config["events_complete"]:
        raise InsufficientEvidence("EVENT_GUARDS_INCOMPLETE")
    if state.universe_quality not in {"EXACT", "RECONSTRUCTED"}:
        raise InsufficientEvidence("UNIVERSE_NOT_RECONSTRUCTED")
    universe = require_kind(state, "UNIVERSE")[-1].payload
    permitted = {
        r["ticker"]
        for r in universe["instruments"]
        if r.get("enabled") and r.get("operable")
    }
    if any(p.ticker not in permitted for p in state.positions):
        raise InsufficientEvidence("HELD_INSTRUMENT_NOT_IN_FROZEN_UNIVERSE")
    macro_data = require_kind(state, "MACRO")[-1].payload
    if macro_data.get("missing_fields"):
        raise InsufficientEvidence("MACRO_INPUTS_INCOMPLETE")
    macro_fields = {f.name for f in fields(MacroSnapshot)}
    macro = MacroSnapshot(
        **{
            k: v
            for k, v in macro_data.items()
            if k in macro_fields and k != "fetched_at"
        },
        fetched_at=state.as_of,
    )
    if (
        macro.vix is None
        or macro.sp500_trend is None
        or macro.ccl is None
        or macro.riesgo_pais is None
    ):
        raise InsufficientEvidence("MACRO_GATE_INPUTS_MISSING")
    grouped = {p.ticker: [] for p in state.positions}
    for e in state.records:
        if e.kind == "BAR" and e.payload["ticker"] in grouped:
            grouped[e.payload["ticker"]].append((e.effective_at, e.payload))
    frames = {}
    lookback = int(config["lookback_sessions"])
    if lookback < 60:
        raise InsufficientEvidence("CORE_REQUIRES_60_SESSIONS")
    for ticker, rows in grouped.items():
        rows.sort(key=lambda x: x[0])
        rows = rows[-lookback:]
        if len(rows) != lookback or any(
            any(row.get(k) is None for k in ("open", "high", "low", "close", "volume"))
            for _, row in rows
        ):
            raise InsufficientEvidence(f"INCOMPLETE_OHLCV:{ticker}")
        frames[ticker] = pd.DataFrame(
            [
                {
                    k: float(row[v])
                    for k, v in {
                        "Open": "open",
                        "High": "high",
                        "Low": "low",
                        "Close": "close",
                        "Volume": "volume",
                    }.items()
                }
                for _, row in rows
            ],
            index=[row["session_id"] for _, row in rows],
        )
        frames[ticker].attrs["candle_sources"] = tuple(
            sorted({row.get("provider", "FROZEN") for _, row in rows})
        )
    if not frames or len({tuple(f.index) for f in frames.values()}) != 1:
        raise InsufficientEvidence("UNALIGNED_PANEL_NO_ZERO_FILL_ALLOWED")
    positions = [
        {
            "ticker": p.ticker,
            "quantity": float(p.quantity),
            "current_price": float(p.mark_ars),
            "price": float(p.mark_ars),
            "market_value": float(p.quantity * p.mark_ars),
        }
        for p in state.positions
    ]
    for row in config["portfolio_history"]:
        if datetime.fromisoformat(row["scraped_at"]) > state.as_of:
            raise InsufficientEvidence("FUTURE_PORTFOLIO_HISTORY")
    risk = build_portfolio_risk_report(
        positions,
        {t: f["Close"] for t, f in frames.items()},
        float(state.capital_base_ars),
        float(state.cash_ars),
        config["portfolio_history"],
        vix=macro.vix,
    )
    risk_map = {r["ticker"]: r for r in risk.positions}
    sentiments = {
        e.payload["ticker"]: e.payload for e in state.records if e.kind == "SENTIMENT"
    }
    results = []
    for p in state.positions:
        tech = analyze_ticker_from_frame(p.ticker, frames[p.ticker])
        if tech is None or p.ticker not in risk_map:
            raise InsufficientEvidence(f"CORE_INPUT_NOT_EVALUABLE:{p.ticker}")
        sent = sentiments.get(p.ticker, sentiments.get("MACRO"))
        if config["sentiment_enabled"] and sent is None:
            raise InsufficientEvidence(f"SENTIMENT_NOT_AVAILABLE:{p.ticker}")
        macro_score, _ = score_macro_for_ticker(p.ticker, macro)
        result = blend_scores(
            p.ticker,
            tech.signal,
            tech.strength,
            macro_score,
            risk_map[p.ticker],
            (
                float(sent["score"]) * max(float(sent["confidence"]), 0.2)
                if sent and config["sentiment_enabled"]
                else 0
            ),
            technical_score_raw=tech.score_raw,
            skip_sentiment=not config["sentiment_enabled"],
            technical_candle_source_mode=tech.candle_source_mode,
            technical_candle_sources=tech.candle_sources,
        )
        for name in (
            "technical_regime",
            "trend_score",
            "reversion_score",
            "structural_break_confirmed",
            "overbought_momentum",
            "technical_shadow_v2",
        ):
            setattr(result, name, getattr(tech, name))
        result.generated_at = state.as_of
        results.append(result)
    report = run_optimizer(
        positions,
        float(state.capital_base_ars),
        float(state.cash_ars),
        get_macro_regime(macro),
        macro.vix,
        results,
        [],
        portfolio_drawdown=risk.drawdown_current,
        history_frames=frames,
        write_diagnostics=False,
    )
    if report is None:
        raise InsufficientEvidence("OPTIMIZER_DID_NOT_PRODUCE_PLAN")
    current = build_positions_from_snapshot(positions, float(state.capital_base_ars))
    decisions = derive_decision_intents(
        report,
        build_signals_from_synthesis(results),
        current,
        float(state.capital_base_ars),
        report.risk_gate_state,
    )
    buy_blocks, trade_blocks = {}, {}
    for e in state.records:
        if e.kind == "EVENT" and e.payload.get("blocks_buys"):
            buy_blocks[e.payload["ticker"]] = e.payload["reason"]
        if e.kind == "CORPORATE_ACTION" and e.payload.get("blocks_trading"):
            trade_blocks[e.payload["ticker"]] = e.payload["reason"]
    plan = reconcile_funding(
        decisions,
        current,
        float(state.cash_ars),
        float(state.capital_base_ars),
        report.risk_gate_state,
        blocked_buy_tickers=buy_blocks,
        blocked_trade_tickers=trade_blocks,
    )
    decision_map = {d.ticker: d for d in decisions}
    orders = []
    for row in [*plan.sell_orders, *plan.buy_orders, *plan.blocked_orders]:
        d = decision_map.get(row.ticker)
        blocked = row in plan.blocked_orders
        orders.append(
            Order(
                ticker=row.ticker,
                side=row.side.value,
                quantity=Decimal(str(row.quantity_est)),
                reference_price=Decimal(str(row.reference_price)),
                target_amount_ars=Decimal(str(row.amount_ars)),
                executable=not blocked,
                blocked=blocked,
                action=row.action.value,
                current_weight=d.current_weight if d else None,
                target_weight=d.target_weight if d else None,
                reason=row.reason,
                restriction=row.block_code,
                priority=row.priority,
                funded_by=tuple(row.funded_by),
            )
        )
    return (
        tuple(orders),
        plan.feasible,
        {
            "scope": "FROZEN_CORE_KERNELS_PORTFOLIO_ONLY_NO_RADAR",
            "decisions": [asdict(d) for d in decisions],
            "signals": [asdict(r) for r in results],
            "optimizer": asdict(report.optimization),
            "warnings": plan.warnings,
            "gate": plan.gate,
        },
    )


def freeze_plan(
    state: HistoricalState, strategy: StrategySpec, mode: str
) -> FrozenPlan:
    _check_version(state, strategy, mode)
    if mode == "HISTORICAL_POLICY_REPLAY" and strategy.adapter != "recorded_plan_v1":
        registration = require_kind(state, "POLICY")[-1]
        if registration.quality != "POINT_IN_TIME_SAFE" or registration.payload.get(
            "registered_strategy_hash"
        ) != digest(strategy):
            raise InsufficientEvidence("HISTORICAL_POLICY_REGISTRATION_NOT_EVIDENCED")
    if strategy.adapter == "recorded_plan_v1":
        if mode != "RECORDED_PLAN_EVALUATION":
            raise InsufficientEvidence("RECORDED_PLAN_IS_NOT_POLICY_REEXECUTION")
        orders, feasible, mechanism = _recorded(state)
    elif strategy.adapter == "hold_v1":
        orders, feasible, mechanism = (), True, {"scope": "NO_TRADE_CHALLENGER"}
    else:
        config = require_kind(state, "CONFIG")[-1]
        if digest(config.payload) != strategy.config_hash:
            raise InsufficientEvidence("CONFIG_HASH_MISMATCH")
        orders, feasible, mechanism = _core(state)
    payload = {
        "strategy": strategy.model_dump(mode="json"),
        "state_id": state.state_id,
        "mode": mode,
        "orders": [r.model_dump(mode="json") for r in orders],
        "feasible": feasible,
        "mechanism": mechanism,
    }
    return FrozenPlan(
        strategy=strategy,
        mode=mode,
        state_id=state.state_id,
        orders=orders,
        affected_tickers=tuple(
            sorted(
                {
                    r.ticker
                    for r in orders
                    if r.executable
                    and not r.blocked
                    and r.quantity is not None
                    and r.quantity > 0
                }
            )
        ),
        mechanism_json=canonical(mechanism),
        feasible=feasible,
        plan_hash=digest(payload),
    )
