from src.analysis.net_decision_report import (
    ESTIMATED_COST_RATE,
    aggregate_runs,
    enrich_decision,
    render_net_decision_report,
    summarize_rows,
    write_run_csv,
)


def _row(**overrides):
    base = {
        "id": 1,
        "decided_at": "2026-08-01T13:00:00-03:00",
        "run_id": "run-1",
        "ticker": "NVDA",
        "decision": "BUY",
        "source": "execution_plan",
        "status": "EXECUTED",
        "decision_type": "executable",
        "metric_scope": "primary",
        "is_primary_metric": True,
        "executed_amount_ars": 100_000,
        "theoretical_amount_ars": 120_000,
        "fill_count": 1,
        "fee_count": 0,
        "fill_amount_ars": 100_000,
        "fill_fees_ars": 0,
        "outcome_5d": 0.10,
        "outcome_10d": None,
        "outcome_20d": None,
        "outcome_40d": None,
    }
    base.update(overrides)
    return base


def test_estimated_policy_cost_is_subtracted_once_from_directional_outcome():
    row = enrich_decision(_row())

    assert row["scope"] == "real_bot"
    assert row["cost_basis"] == "estimated_policy"
    assert row["cost_rate"] == ESTIMATED_COST_RATE
    assert row["net_5d"] == 0.10 - ESTIMATED_COST_RATE
    assert row["net_pnl_5d_ars"] == 100_000 * (0.10 - ESTIMATED_COST_RATE)


def test_complete_fill_fees_override_policy_estimate():
    row = enrich_decision(_row(fee_count=1, fill_fees_ars=500))

    assert row["cost_basis"] == "actual_fill_fees"
    assert row["cost_rate"] == 0.005
    assert row["net_5d"] == 0.095


def test_plans_and_blocked_rows_are_not_classified_as_real():
    plan = enrich_decision(_row(status="APPROVED", is_primary_metric=False, metric_scope="planner_audit"))
    blocked = enrich_decision(
        _row(status="BLOCKED", is_primary_metric=False, metric_scope="planner_audit")
    )

    assert plan["scope"] == "plan"
    assert blocked["scope"] == "blocked"


def test_primary_flag_does_not_turn_an_approved_plan_into_a_fill():
    plan = enrich_decision(
        _row(status="APPROVED", is_primary_metric=True, metric_scope="planner_audit")
    )

    assert plan["scope"] == "plan"


def test_weighted_run_net_uses_decision_amounts():
    first = enrich_decision(_row())
    second = enrich_decision(
        _row(id=2, ticker="QQQ", fill_amount_ars=300_000, executed_amount_ars=300_000, outcome_5d=0.0)
    )

    summary = summarize_rows([first, second])
    runs = aggregate_runs([first, second])

    expected = ((100_000 * first["net_5d"]) + (300_000 * second["net_5d"])) / 400_000
    assert summary["net_5d"] == expected
    assert summary["avg_net_5d"] == (first["net_5d"] + second["net_5d"]) / 2
    assert runs[0]["net_5d"] == expected
    assert summary["hit_5d"] == 0.5


def test_renderer_states_cost_limit_and_evidence_separation():
    real = enrich_decision(_row())
    plan = enrich_decision(
        _row(id=2, status="APPROVED", is_primary_metric=False, metric_scope="planner_audit")
    )
    rows = [real, plan]
    data = {
        "days": 180,
        "rows": rows,
        "runs": aggregate_runs(rows),
        "scopes": {
            scope: summarize_rows(row for row in rows if row["scope"] == scope)
            for scope in (
                "real_bot", "real_manual", "plan", "blocked", "radar", "theoretical", "audit"
            )
        },
        "actual_fee_rows": 0,
        "with_run_id": 2,
    }

    report = render_net_decision_report(data)

    assert "fills con fees reales: <b>0</b>" in report
    assert "Sin fee real se estima 0.75%" in report
    assert "EJECUCION REAL" in report
    assert "Plan sin fill" in report


def test_run_csv_has_one_row_per_analysis(tmp_path):
    rows = [enrich_decision(_row()), enrich_decision(_row(id=2, run_id="run-2"))]
    path = write_run_csv(aggregate_runs(rows), tmp_path / "runs.csv")

    content = path.read_text(encoding="utf-8-sig")
    assert content.count("\n") == 3
    assert "run-1" in content
    assert "run-2" in content
