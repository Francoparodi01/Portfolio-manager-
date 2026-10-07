from pathlib import Path
from types import SimpleNamespace

import pytest

from src.analysis.contextual_telegram import (
    ContextualShadowSafetyError,
    SHADOW_PLAN_PAYLOAD_VERSION,
    assert_neutral,
    assert_runtime_is_shadow_only,
    assert_shadow_plan_envelope,
    productive_view,
    render_contextual_telegram,
)


def _runtime():
    result = SimpleNamespace(
        ticker="IREN", decision="HOLD", final_score=0.09,
        conviction=0.3333, confidence=0.3333,
    )
    decision = SimpleNamespace(
        ticker="IREN", action=SimpleNamespace(value="BUY"),
        current_weight=0.10, target_weight=0.15, delta_weight=0.05,
        score=0.09, conviction=0.3333,
        portfolio_intent=SimpleNamespace(value="INCREASE"),
    )
    order = SimpleNamespace(
        ticker="IREN", side=SimpleNamespace(value="BUY"),
        action=SimpleNamespace(value="BUY"), amount_ars=50_000,
        theoretical_ars=50_000, quantity_est=10, reference_price=5_000,
        status=SimpleNamespace(value="PLANNED"), block_code=None,
    )
    plan = SimpleNamespace(
        decisions=[decision], sell_orders=[], buy_orders=[order],
        blocked_orders=[], cash_before=100_000, gross_sell_ars=0,
        net_sell_ars=0, gross_buy_ars=50_000, fee_sell_ars=0,
        fee_buy_ars=300, cash_after=49_700, gate="OPEN", feasible=True,
    )
    return {
        "analysis_run_id": "run-1", "results": [result],
        "execution_plan": plan, "cash_ars": 100_000,
        "no_persist": True, "run_intent": "exploratory",
    }


def _context():
    return {
        "components": {
            "trend_daily": "DOWNTREND",
            "trend_weekly": "MIXED",
            "rvol": 1.45,
            "volume_state": "NORMAL",
            "breakout_state": "IN_RANGE",
            "relative_strength": {
                "general": {
                    "windows": {
                        "20": {"relative_price_change": 0.08},
                        "60": {"relative_price_change": -0.02},
                        "120": {"relative_price_change": None},
                    }
                }
            },
            "context_confidence": {"value": 0.8, "status": "HIGH"},
        },
        "invalidators": [
            {"status": "WARN", "reason": "BUY_WITH_MIXED_WEEKLY_STRUCTURE"}
        ],
        "missingness": ["VOLUME_UNIT_UNKNOWN"],
    }


def test_productive_view_preserves_signal_and_portfolio_decision_separately():
    view = productive_view(_runtime())
    assert view["signals"] == [{
        "ticker": "IREN", "decision": "HOLD",
        "final_score": 0.09, "conviction": 0.3333,
    }]
    assert view["decisions"][0]["action"] == "BUY"
    assert view["orders"]["buy_orders"][0]["quantity_est"] == 10.0


def test_shadow_runtime_is_fail_closed_on_persistence_or_wrong_intent():
    runtime = _runtime()
    assert_runtime_is_shadow_only(runtime)
    with pytest.raises(ContextualShadowSafetyError, match="PERSISTENCE_ENABLED"):
        assert_runtime_is_shadow_only({**runtime, "no_persist": False})
    with pytest.raises(ContextualShadowSafetyError, match="RUN_INTENT"):
        assert_runtime_is_shadow_only({**runtime, "run_intent": "formal_plan"})


def test_shadow_neutrality_detects_plan_change():
    before = productive_view(_runtime())
    assert all(assert_neutral(before, before).values())
    after = {**before, "cash": {**before["cash"], "cash_after": 1.0}}
    with pytest.raises(ContextualShadowSafetyError, match="CONTEXT_CHANGED"):
        assert_neutral(before, after)


def test_shadow_plan_envelope_forbids_order_intents():
    row = {
        "source": "execution_plan", "gate": "SHADOW_ONLY",
        "authority_mode": "SHADOW_ONLY",
        "affects_analysis": False, "affects_execution": False,
        "feasible": False, "payload_version": SHADOW_PLAN_PAYLOAD_VERSION,
        "cash_before": 100.0, "cash_after": 100.0,
        "gross_sell_ars": 0.0, "fee_sell_ars": 0.0, "net_sell_ars": 0.0,
        "gross_buy_ars": 0.0, "fee_buy_ars": 0.0,
    }
    assert_shadow_plan_envelope(row, 0)
    with pytest.raises(ContextualShadowSafetyError, match="INVALID_SHADOW_PLAN"):
        assert_shadow_plan_envelope(row, 1)


def test_contextual_telegram_format_separates_productive_and_shadow_views():
    report = render_contextual_telegram({
        "portfolio_value_ars": 1_000_000,
        "cash_ars": 100_000,
        "cutoff": "2026-10-07T15:00:00+00:00",
        "run_id": "run-1", "capture_id": "capture-1", "plan_id": "plan-1",
        "evidence_count": 518, "context_confidence": "HIGH",
        "assets": [{
            "ticker": "IREN", "productive_action": "BUY", "signal": "HOLD",
            "score": 0.09, "conviction": 0.3333, "context": _context(),
            "missingness": ["SECTOR_BENCHMARK_NOT_CONFIGURED"],
        }],
    })
    assert "ANÁLISIS CONTEXTUAL — SHADOW" in report
    assert "Productivo: <b>BUY</b>" in report
    assert "Contexto [SHADOW]" in report
    assert "Daily: <b>DOWNTREND</b>" in report
    assert "RVOL: <code>1.45</code>" in report
    assert "WARN" in report
    assert "Impacto contextual: <b>NONE · SHADOW ONLY</b>" in report
    assert "affects_analysis=false · affects_execution=false" in report


def test_migration_protects_shadow_plan_without_changing_productive_source():
    sql = (
        Path(__file__).parents[1]
        / "migrations"
        / "20261009_telegram_contextual_shadow.sql"
    ).read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS authority_mode" in sql
    assert "authority_mode <> 'SHADOW_ONLY'" in sql
    assert "contextual shadow plans cannot create order intents" in sql
    assert "BEFORE INSERT OR UPDATE ON order_intents" in sql


def test_bot_registers_new_command_without_replacing_analisis():
    source = (Path(__file__).parents[1] / "scripts" / "telegram_bot.py").read_text(
        encoding="utf-8"
    )
    assert 'CommandHandler("analisis",         analysis_handler)' in source
    assert 'CommandHandler("analisis_contextual", contextual_analysis_handler)' in source
    assert '"scripts/run_contextual_shadow.py"' in source
