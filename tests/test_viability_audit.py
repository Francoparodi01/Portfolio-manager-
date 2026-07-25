import pandas as pd

from src.analysis.viability_audit import (
    ViabilityAuditConfig,
    render_viability_chart,
    render_viability_audit,
    run_viability_audit_sync,
)


def _row(
    *,
    source: str,
    status: str,
    final_score: float,
    outcome_5d: float,
    executable_outcome_5d=None,
):
    return {
        "ticker": "TEST",
        "decision": "BUY",
        "final_score": final_score,
        "layers": {
            "trend_shadow": {"score": final_score * 0.8},
            "reversion_shadow": {"score": final_score * 0.2},
        },
        "source": source,
        "status": status,
        "decision_type": "executable",
        "outcome_basis": "canonical_cocos",
        "outcome_5d": outcome_5d,
        "outcome_10d": None,
        "outcome_20d": None,
        "outcome_40d": None,
        "executable_outcome_5d": executable_outcome_5d,
    }


def test_viability_audit_separates_bot_and_manual_primary_metrics():
    rows = []
    for score, outcome in [
        (0.05, 0.020),
        (0.07, 0.030),
        (0.09, 0.040),
        (0.11, 0.050),
        (0.13, 0.060),
    ]:
        rows.append(
            _row(
                source="execution_plan",
                status="EXECUTED",
                final_score=score,
                outcome_5d=outcome,
            )
        )

    for score, outcome in [
        (0.04, -0.020),
        (0.06, 0.000),
        (0.08, -0.030),
        (0.10, 0.010),
        (0.12, -0.040),
    ]:
        rows.append(
            _row(
                source="broker_movement",
                status="EXECUTED_MANUAL",
                final_score=score,
                outcome_5d=outcome,
            )
        )

    report = run_viability_audit_sync(
        pd.DataFrame(rows),
        ViabilityAuditConfig(
            database_url="postgresql://unused",
            horizons=("5d",),
            min_sample=5,
            cost_bps=50,
        ),
    )

    bot = report.metrics["bot_only"]["5d"]
    manual = report.metrics["manual_only"]["5d"]

    assert bot.n == 5
    assert manual.n == 5
    assert bot.ic_final and bot.ic_final > 0
    assert bot.ic_trend and bot.ic_trend > 0
    assert bot.net_ev and manual.net_ev
    assert bot.net_ev > manual.net_ev
    assert bot.max_drawdown is not None and manual.max_drawdown is not None
    assert bot.max_drawdown > manual.max_drawdown
    assert all(g.passed is True for g in report.gates)
    assert "VIABLE PARA 180D" in report.verdict


def test_viability_audit_prefers_executable_outcome_when_present():
    report = run_viability_audit_sync(
        pd.DataFrame(
            [
                _row(
                    source="execution_plan",
                    status="EXECUTED",
                    final_score=0.1,
                    outcome_5d=-0.20,
                    executable_outcome_5d=0.05,
                )
            ]
        ),
        ViabilityAuditConfig(
            database_url="postgresql://unused",
            horizons=("5d",),
            min_sample=1,
            cost_bps=0,
        ),
    )

    assert report.metrics["bot_only"]["5d"].net_ev == 0.05


def test_viability_render_states_no_threshold_change():
    report = run_viability_audit_sync(
        pd.DataFrame(
            [
                _row(
                    source="execution_plan",
                    status="EXECUTED_MANUAL",
                    final_score=0.1,
                    outcome_5d=0.02,
                )
            ]
        ),
        ViabilityAuditConfig(
            database_url="postgresql://unused",
            horizons=("5d",),
            min_sample=1,
        ),
    )

    text = render_viability_audit(report)

    assert report.metrics["bot_only"]["5d"].n == 0
    assert report.metrics["manual_only"]["5d"].n == 1
    assert "Guards y thresholds quedan intactos" in text


def test_viability_chart_renders_png(tmp_path):
    report = run_viability_audit_sync(
        pd.DataFrame(
            [
                _row(
                    source="execution_plan",
                    status="EXECUTED",
                    final_score=score,
                    outcome_5d=outcome,
                )
                for score, outcome in [
                    (0.05, 0.02),
                    (0.07, 0.03),
                    (0.09, 0.04),
                    (0.11, 0.05),
                    (0.13, 0.06),
                ]
            ]
        ),
        ViabilityAuditConfig(
            database_url="postgresql://unused",
            horizons=("5d",),
            min_sample=5,
            cost_bps=50,
        ),
    )

    chart_path = render_viability_chart(report, tmp_path / "viability.png")

    assert chart_path.exists()
    assert chart_path.read_bytes().startswith(b"\x89PNG")
