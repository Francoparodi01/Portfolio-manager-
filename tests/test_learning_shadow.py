from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.analysis.learning_shadow import (
    CLEAN_MISSED_OPPORTUNITY,
    EXCLUDED_BASIS,
    MARKET_DRIVEN_WIN,
    MISSING_OUTCOME,
    NON_POSITIVE_COUNTERFACTUAL,
    PENDING,
    PLANNER_BLOCKED,
    POSITIVE_BELOW_THRESHOLD,
    POTENTIAL_FALSE_NEGATIVE,
    RADAR_BLOCKED,
    RISKY_COUNTERFACTUAL_WIN,
    LearningShadowCase,
    build_metric_rows,
    build_rule_candidates,
    classify_block_reason,
    classify_counterfactual,
)
from src.analysis.learning_shadow_store import LOAD_BLOCKED_CASE_INPUTS_SQL


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 7, tzinfo=timezone.utc)


def test_counterfactual_classification_keeps_material_threshold_explicit():
    common = {
        "outcome_basis": "canonical_cocos",
        "decided_at": NOW - timedelta(days=20),
        "horizon_days": 5,
        "as_of": NOW,
        "material_return_bps": 75,
    }
    assert classify_counterfactual(directional_outcome=0.02, **common) == POTENTIAL_FALSE_NEGATIVE
    assert classify_counterfactual(directional_outcome=0.004, **common) == POSITIVE_BELOW_THRESHOLD
    assert classify_counterfactual(directional_outcome=-0.01, **common) == NON_POSITIVE_COUNTERFACTUAL


def test_missing_pending_and_noncanonical_outcomes_are_not_false_negatives():
    recent = NOW - timedelta(days=3)
    old = NOW - timedelta(days=20)
    assert classify_counterfactual(
        directional_outcome=None,
        outcome_basis=None,
        decided_at=recent,
        horizon_days=5,
        as_of=NOW,
    ) == PENDING
    assert classify_counterfactual(
        directional_outcome=None,
        outcome_basis=None,
        decided_at=old,
        horizon_days=5,
        as_of=NOW,
    ) == MISSING_OUTCOME
    assert classify_counterfactual(
        directional_outcome=0.20,
        outcome_basis="legacy_external",
        decided_at=old,
        horizon_days=5,
        as_of=NOW,
    ) == EXCLUDED_BASIS
    assert classify_counterfactual(
        directional_outcome=0.20,
        outcome_basis=None,
        decided_at=old,
        horizon_days=5,
        as_of=NOW,
    ) == EXCLUDED_BASIS


def test_case_prefers_the_next_executable_outcome_and_records_horizon_mismatch():
    case = LearningShadowCase.from_mapping(
        {
            "owner_chat_id": 0,
            "decision_log_id": 17,
            "ticker": "MU",
            "decision": "BUY",
            "decided_at": NOW - timedelta(days=20),
            "horizon_days": 5,
            "shadow_horizon_sessions": 5,
            "block_reason": "BUY_SCORE_GUARD",
            "outcome_basis": "canonical_cocos",
            "nominal_outcome": -0.02,
            "executable_outcome": 0.03,
            "shadow_forecast_id": 91,
            "shadow_as_of_ts": NOW - timedelta(days=21),
            "shadow_expected_return": 0.01,
            "shadow_probability_up": 0.55,
            "shadow_action": "ENTRY_WATCH",
            "shadow_direction_correct": True,
        },
        as_of=NOW,
    )

    assert case.directional_outcome == 0.03
    assert case.outcome_source == "next_executable"
    assert case.classification == POTENTIAL_FALSE_NEGATIVE
    assert case.shadow_supports_direction is True
    assert "calendar days" in case.metadata["horizon_note"]


def test_metrics_separate_maturity_false_negatives_and_shadow_coverage():
    base = {
        "owner_chat_id": 0,
        "ticker": "MU",
        "decision": "BUY",
        "decided_at": NOW - timedelta(days=20),
        "horizon_days": 5,
        "shadow_horizon_sessions": 5,
        "outcome_basis": "canonical_cocos",
        "nominal_outcome": None,
        "shadow_as_of_ts": NOW - timedelta(days=21),
        "shadow_probability_up": 0.55,
        "shadow_action": "ENTRY_WATCH",
        "shadow_direction_correct": True,
        "source": "execution_plan",
        "status": "BLOCKED",
        "metric_scope": "blocked_audit",
        "path_sessions": 4,
        "mae": -0.02,
        "mfe": 0.04,
        "benchmark_outcome": 0.01,
    }
    rows = [
        {**base, "decision_log_id": 1, "executable_outcome": 0.03, "shadow_forecast_id": 11, "shadow_expected_return": 0.01},
        {**base, "decision_log_id": 2, "executable_outcome": -0.02, "shadow_forecast_id": None, "shadow_expected_return": None},
    ]
    cases = [LearningShadowCase.from_mapping(row, as_of=NOW) for row in rows]
    metric = next(
        row
        for row in build_metric_rows(cases)
        if row["horizon_days"] == 5
        and row["case_population"] == PLANNER_BLOCKED
    )

    assert metric["matured_cases"] == 2
    assert metric["potential_false_negatives"] == 1
    assert metric["potential_false_negative_rate"] == 0.5
    assert metric["shadow_coverage_rate"] == 0.5
    assert (
        metric["clean_missed_opportunities"]
        + metric["risky_counterfactual_wins"]
        + metric["market_driven_wins"]
        + metric["uncontrolled_counterfactual_wins"]
        + metric["insufficient_potential_wins"]
        == metric["potential_false_negatives"]
    )


def test_population_and_reason_taxonomy_do_not_mix_planner_with_radar():
    common = {
        "owner_chat_id": 0,
        "ticker": "BMA",
        "decision": "BUY",
        "decided_at": NOW - timedelta(days=20),
        "horizon_days": 5,
        "shadow_horizon_sessions": 5,
        "outcome_basis": "canonical_cocos",
        "executable_outcome": 0.03,
        "path_sessions": 4,
        "mae": -0.02,
        "mfe": 0.04,
        "benchmark_outcome": 0.01,
        "status": "BLOCKED",
    }
    planner = LearningShadowCase.from_mapping(
        {
            **common,
            "decision_log_id": 1,
            "source": "execution_plan",
            "metric_scope": "blocked_audit",
            "block_reason": "Compra en WATCH: se\u00f1al insuficiente +0.016",
        },
        as_of=NOW,
    )
    radar = LearningShadowCase.from_mapping(
        {
            **common,
            "decision_log_id": 2,
            "source": "radar",
            "metric_scope": "radar_audit",
            "block_reason": "R/R insuficiente",
        },
        as_of=NOW,
    )

    assert planner.case_population == PLANNER_BLOCKED
    assert planner.block_category == "SCORE_GUARD"
    assert radar.case_population == RADAR_BLOCKED
    assert radar.block_category == "RISK_REWARD"
    assert classify_block_reason("Sizing $110 menor a un nominal") == "MIN_TRADE_OR_NOMINAL"
    assert classify_block_reason("Se\u00f1al positiva - delta insuficiente para operar") == "MIN_WEIGHT_DELTA"


def test_review_label_requires_clean_path_and_positive_benchmark_alpha():
    base = {
        "owner_chat_id": 0,
        "ticker": "BMA",
        "decision": "BUY",
        "decided_at": NOW - timedelta(days=20),
        "horizon_days": 5,
        "shadow_horizon_sessions": 5,
        "outcome_basis": "canonical_cocos",
        "executable_outcome": 0.03,
        "path_sessions": 4,
        "mfe": 0.04,
        "status": "BLOCKED",
        "source": "execution_plan",
        "metric_scope": "blocked_audit",
    }
    clean = LearningShadowCase.from_mapping(
        {**base, "decision_log_id": 1, "mae": -0.02, "benchmark_outcome": 0.01},
        as_of=NOW,
    )
    risky = LearningShadowCase.from_mapping(
        {**base, "decision_log_id": 2, "mae": -0.08, "benchmark_outcome": 0.01},
        as_of=NOW,
    )
    market = LearningShadowCase.from_mapping(
        {**base, "decision_log_id": 3, "mae": -0.02, "benchmark_outcome": 0.04},
        as_of=NOW,
    )

    assert clean.review_label == CLEAN_MISSED_OPPORTUNITY
    assert risky.review_label == RISKY_COUNTERFACTUAL_WIN
    assert market.review_label == MARKET_DRIVEN_WIN


def test_rule_candidates_are_shadow_only_and_require_a_real_sample():
    cases = []
    for index in range(20):
        cases.append(LearningShadowCase.from_mapping(
            {
                "owner_chat_id": 0,
                "decision_log_id": index + 1,
                "ticker": "BMA",
                "decision": "BUY",
                "decided_at": NOW - timedelta(days=30 - index),
                "horizon_days": 5,
                "shadow_horizon_sessions": 5,
                "block_reason": "BUY_SCORE_GUARD",
                "outcome_basis": "canonical_cocos",
                "executable_outcome": 0.03 if index < 5 else -0.01,
                "path_sessions": 4,
                "mae": -0.02,
                "mfe": 0.04,
                "benchmark_outcome": 0.01,
                "source": "execution_plan",
                "status": "BLOCKED",
                "metric_scope": "blocked_audit",
            },
            as_of=NOW,
        ))

    candidates = build_rule_candidates(cases)
    assert len(candidates) == 1
    proposed = candidates[0]["proposed_rule"]
    assert proposed["mode"] == "shadow_only"
    assert proposed["live_threshold_change"] is False
    assert proposed["requires_human_approval"] is True


def test_shadow_match_is_predecision_and_store_cannot_write_operational_tables():
    assert "f.as_of_ts <= e.decided_at" in LOAD_BLOCKED_CASE_INPUTS_SQL
    source = (ROOT / "src" / "analysis" / "learning_shadow_store.py").read_text(encoding="utf-8")
    assert not re.search(r"INSERT\s+INTO\s+decision_log", source, re.I)
    assert not re.search(r"UPDATE\s+decision_log", source, re.I)
    assert not re.search(r"DELETE\s+FROM\s+decision_log", source, re.I)
    assert not re.search(r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+shadow_thesis_", source, re.I)
    assert "dl.status = 'BLOCKED'" in LOAD_BLOCKED_CASE_INPUTS_SQL
    assert "mc.source = 'COCOS'" in LOAD_BLOCKED_CASE_INPUTS_SQL
    assert "PARTITION BY mc.ticker" in LOAD_BLOCKED_CASE_INPUTS_SQL
    assert "bench.max_abs_gap >= 0.35" in LOAD_BLOCKED_CASE_INPUTS_SQL
    assert "LEAST(0, MIN" in LOAD_BLOCKED_CASE_INPUTS_SQL
    assert "GREATEST(0, MAX" in LOAD_BLOCKED_CASE_INPUTS_SQL


def test_schema_and_scheduler_keep_learning_shadow_isolated_and_after_outcomes():
    schema = (ROOT / "init.sql").read_text(encoding="utf-8")
    scheduler = (ROOT / "src" / "scheduler" / "runner.py").read_text(encoding="utf-8")
    script = (ROOT / "scripts" / "run_learning_shadow.py").read_text(encoding="utf-8")
    review_script = (ROOT / "scripts" / "review_learning_candidate.py").read_text(encoding="utf-8")
    monitor_api = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS learning_shadow_runs" in schema
    assert "CREATE TABLE IF NOT EXISTS learning_shadow_cases" in schema
    assert "CREATE TABLE IF NOT EXISTS learning_shadow_metric_snapshots" in schema
    assert "learning_shadow_metric_snapshots_v2" in schema
    assert "learning_shadow_rule_candidates" in schema
    assert 'id="learning_shadow_daily"' in scheduler
    assert '_business_day_cron(hour=21, minute=40)' in scheduler
    assert scheduler.index('id="update_outcomes_daily"') < scheduler.index('id="learning_shadow_daily"')
    assert "execution_planner" not in script
    assert "optimizer" not in script
    assert "UPDATE learning_shadow_rule_candidates" in review_script
    assert not re.search(r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+decision_log", review_script, re.I)
    assert 'app.router.add_get("/api/learning-shadow", learning_shadow_v2_view)' in monitor_api
