from src.analysis.decision_market_audit import (
    BENCHMARK_SQL,
    EXTREMES_SQL,
    FOLLOWED_SQL,
    QUALITY_SQL,
    SUMMARY_SQL,
    BenchmarkMetric,
    CohortHorizonMetric,
    DecisionMarketAuditReport,
    ExtremeDecision,
    FollowedMetric,
    QualityRow,
    render_decision_market_audit,
    report_to_json,
)


def _metric(cohort: str, horizon: str, *, avg_return: float, score_corr: float | None):
    return CohortHorizonMetric(
        cohort=cohort,
        horizon=horizon,
        total_decisions=40,
        matured=33 if horizon == "5d" else 28,
        with_live_fill=34 if cohort == "execution_plan_executed" else 0,
        hit_rate=0.636 if horizon == "5d" else 0.643,
        avg_return=avg_return,
        net_avg_return=avg_return - 0.0075,
        median_return=0.01,
        avg_win=0.06,
        avg_loss=-0.05,
        worst=-0.2,
        best=0.22,
        avg_score=-0.05,
        score_corr=score_corr,
        tickers=18,
    )


def _report():
    return DecisionMarketAuditReport(
        generated_at="2026-09-02T22:01:34+00:00",
        days=180,
        cost_bps=75.0,
        benchmarks=("SPY", "QQQ"),
        quality=[
            QualityRow("decision_log", 926, "2026-04-09T18:00:00+00:00", "2026-09-02T19:50:26+00:00"),
            QualityRow("broker_fills", 245, "2026-04-09T03:00:00+00:00", "2026-08-27T17:12:14+00:00"),
            QualityRow("plan_execution_attributions", 36, "2026-05-27T03:00:00+00:00", "2026-08-14T15:19:05+00:00"),
        ],
        summary=[
            _metric("execution_plan_executed", "5d", avg_return=0.0223, score_corr=-0.31),
            _metric("execution_plan_executed", "20d", avg_return=0.0549, score_corr=-0.49),
            _metric("execution_plan_blocked", "5d", avg_return=-0.0034, score_corr=0.03),
            _metric("execution_plan_blocked", "20d", avg_return=-0.0043, score_corr=0.02),
            _metric("execution_plan_approved", "5d", avg_return=-0.0024, score_corr=0.05),
            _metric("execution_plan_approved", "20d", avg_return=0.0213, score_corr=-0.03),
            _metric("manual_or_broker_real", "5d", avg_return=-0.0068, score_corr=None),
            _metric("manual_or_broker_real", "20d", avg_return=-0.0230, score_corr=None),
            _metric("radar_idea", "5d", avg_return=0.0250, score_corr=-0.01),
            _metric("radar_idea", "20d", avg_return=-0.0200, score_corr=-0.30),
        ],
        benchmark_summary=[
            BenchmarkMetric("execution_plan_approved", "BUY", "5d", "SPY", 42, 0.0148, 0.0060, 0.0088, 0.619, 0.667),
            BenchmarkMetric("execution_plan_approved", "BUY", "20d", "SPY", 35, 0.0232, 0.0373, -0.0141, 0.514, 0.971),
        ],
        followed_summary=[
            FollowedMetric("FOLLOWED", "CONFIRMED_SEQUENCE", 12, 12, 0.583, -0.022, 0.044, 0.057, -0.009, 1.07, 11)
        ],
        best_execution_plan_5d=[
            ExtremeDecision(741, "2026-08-04T02:26:39+00:00", "TEAM", "BUY", 0.0802, "APPROVED", "executable", 0.4755, 0.5634, 0.6801)
        ],
        worst_execution_plan_5d=[
            ExtremeDecision(275, "2026-06-05T00:06:10+00:00", "QCOM", "BUY", 0.1039, "APPROVED", "executable", -0.2118, -0.1260, -0.1368)
        ],
        warnings=[],
        timesfm3_shadow_next_step="Agregar timesfm3_tminus1_shadow sin tocar decisiones reales.",
    )


def test_render_decision_market_audit_keeps_operational_boundary():
    text = render_decision_market_audit(_report())

    assert "Decision vs Market Audit" in text
    assert "Plan ejecutado 5d: n=33" in text
    assert "no modifica score, optimizer, planner, orders ni fills" in text
    assert "final_score se reporta como diagnostico" in text
    assert "TimesFM-3 queda fuera de ejecucion" in text


def test_report_json_includes_generated_warnings():
    payload = report_to_json(_report())

    assert "final_score no ordena bien el outcome" in payload
    assert "execution_plan_executed" in payload
    assert "buy_alpha_vs_benchmark" in payload


def test_audit_sql_fragments_are_read_only():
    sql = "\n".join([SUMMARY_SQL, QUALITY_SQL, FOLLOWED_SQL, EXTREMES_SQL, BENCHMARK_SQL]).lower()

    for forbidden in (" insert ", " update ", " delete ", " alter ", " drop ", " truncate ", " create "):
        assert forbidden not in sql
