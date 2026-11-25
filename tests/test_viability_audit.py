import asyncio

import pandas as pd

from src.analysis.viability_audit import (
    ViabilityAuditConfig,
    load_viability_decision_log,
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
    # Five undated rows cannot carry statistical evidence: the gates stay open.
    gates = {g.name: g.passed for g in report.gates}
    assert gates["muestra bot-only 5d"] is True
    assert gates["IC bot-only 5d positivo"] is None
    assert gates["EV neto bot-only 5d positivo"] is None
    assert report.verdict_level == "INCONCLUSIVE"
    assert "NO CONCLUYENTE" in report.verdict


def test_viability_audit_reports_followed_scope_without_changing_bot_gates():
    rows = [
        _row(
            source="execution_plan",
            status="EXECUTED",
            final_score=0.1,
            outcome_5d=0.02,
        ),
        _row(
            source="plan_execution_attribution",
            status="EXECUTED_FOLLOWED",
            final_score=0.1,
            outcome_5d=0.05,
        ),
    ]
    frame = pd.DataFrame(rows)
    frame.attrs["followed_summary"] = {
        "attributions": 2,
        "eligible": 1,
        "ambiguous": 1,
        "plan_links": 3,
    }

    report = run_viability_audit_sync(
        frame,
        ViabilityAuditConfig(
            database_url="postgresql://unused",
            horizons=("5d",),
            min_sample=1,
            cost_bps=0,
        ),
    )

    assert report.metrics["bot_only"]["5d"].n == 1
    assert report.metrics["followed"]["5d"].n == 1
    assert report.metrics["followed"]["5d"].net_ev == 0.05
    assert report.followed_summary["ambiguous"] == 1


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


def test_viability_render_is_mobile_compact_and_scope_explicit():
    rows = []
    for source, status, outcome in [
        ("execution_plan", "EXECUTED", 0.03),
        ("plan_execution_attribution", "EXECUTED_FOLLOWED", 0.04),
        ("broker_movement", "EXECUTED_MANUAL", -0.02),
    ]:
        rows.append(
            _row(
                source=source,
                status=status,
                final_score=0.1,
                outcome_5d=outcome,
            )
        )
    frame = pd.DataFrame(rows)
    frame.attrs["followed_summary"] = {
        "attributions": 2,
        "eligible": 1,
        "ambiguous": 1,
        "plan_links": 3,
    }
    report = run_viability_audit_sync(
        frame,
        ViabilityAuditConfig(
            database_url="postgresql://unused",
            horizons=("5d",),
            min_sample=1,
            cost_bps=75,
        ),
    )

    text = render_viability_audit(report)

    assert len(text) < 4096
    assert "Resultados netos por horizonte" in text
    assert "🤖 Bot" in text
    assert "🧭 Seguido" in text
    assert "👤 Manual" in text
    assert "Gates bot-only" in text
    assert "ambiguas excluidas" in text


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


def test_viability_loader_excludes_decisions_backed_only_by_superseded_fills(monkeypatch):
    class _Connection:
        def __init__(self):
            self.statements = []

        async def fetch(self, statement, *args):
            self.statements.append(statement)
            if "information_schema.columns" in statement:
                return [
                    {"column_name": name}
                    for name in [
                        "id",
                        "decided_at",
                        "ticker",
                        "decision",
                        "final_score",
                        "layers",
                        "source",
                        "status",
                        "decision_type",
                        "metric_scope",
                        "outcome_basis",
                        "outcome_5d",
                    ]
                ]
            return []

        async def fetchval(self, statement, *args):
            return False

        async def close(self):
            return None

    conn = _Connection()

    async def _connect(_dsn):
        return conn

    monkeypatch.setattr("src.analysis.viability_audit.asyncpg.connect", _connect)

    frame = asyncio.run(
        load_viability_decision_log(
            ViabilityAuditConfig(database_url="postgresql://unused", horizons=("5d",))
        )
    )

    assert frame.empty
    decision_query = conn.statements[-1]
    assert "superseded_by_real" in decision_query
    assert "live_bf" in decision_query


def test_viability_loader_uses_fill_lineage_to_exclude_followed_manual_rows(monkeypatch):
    class _Connection:
        def __init__(self):
            self.statements = []

        async def fetch(self, statement, *args):
            self.statements.append(statement)
            if "information_schema.columns" in statement:
                return [
                    {"column_name": name}
                    for name in [
                        "id",
                        "decided_at",
                        "ticker",
                        "decision",
                        "final_score",
                        "layers",
                        "source",
                        "status",
                        "decision_type",
                        "metric_scope",
                        "outcome_basis",
                        "outcome_5d",
                    ]
                ]
            return []

        async def fetchval(self, statement, *args):
            return True

        async def fetchrow(self, statement, *args):
            return None

        async def close(self):
            return None

    conn = _Connection()

    async def _connect(_dsn):
        return conn

    monkeypatch.setattr("src.analysis.viability_audit.asyncpg.connect", _connect)

    asyncio.run(
        load_viability_decision_log(
            ViabilityAuditConfig(database_url="postgresql://unused", horizons=("5d",))
        )
    )

    assert any(
        "JOIN broker_fills bf" in statement
        and "bf.external_fill_id = movement.external_movement_id" in statement
        for statement in conn.statements
    )


def _dated_rows(*, source, status, n_dates, signal, noise_seed, decision="BUY", start="2026-03-02"):
    import numpy as np

    rng = np.random.default_rng(noise_seed)
    days = pd.bdate_range(start, periods=n_dates)
    rows = []
    for day in days:
        for _ in range(2):
            score = float(rng.uniform(0.02, 0.20))
            outcome = signal * score + float(rng.normal(0.0, 0.02))
            row = _row(source=source, status=status, final_score=score, outcome_5d=outcome)
            row["decided_at"] = day.tz_localize("America/Argentina/Buenos_Aires") + pd.Timedelta(hours=12)
            row["decision"] = decision
            rows.append(row)
    return rows


def _config(min_sample=30):
    return ViabilityAuditConfig(
        database_url="postgresql://unused", horizons=("5d",), min_sample=min_sample, cost_bps=75
    )


def test_viability_gates_pass_only_when_the_interval_clears_zero():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=60, signal=0.5, noise_seed=1)
    rows += [
        {**row, "outcome_5d": row["outcome_5d"] - 0.08}
        for row in _dated_rows(
            source="broker_movement", status="EXECUTED_MANUAL", n_dates=60, signal=0.0, noise_seed=2
        )
    ]
    frame = pd.DataFrame(rows)
    frame.attrs["benchmark_candles"] = _bench_candles(daily=0.0)
    report = run_viability_audit_sync(frame, _config())
    bot = report.metrics["bot_only"]["5d"]
    assert bot.n_dates == 60
    assert bot.net_ev_interval["lower"] > 0
    assert bot.ic_interval["lower"] > 0
    assert all(g.passed is True for g in report.gates), report.gates
    assert report.verdict_level == "PASS"
    assert "NO VALIDADO PARA ESCALAR CAPITAL" in report.verdict
    assert "🟢" in render_viability_audit(report)


def test_viability_positive_point_estimate_with_noise_is_not_a_pass():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=40, signal=0.0, noise_seed=3)
    for row in rows:
        row["outcome_5d"] = row["outcome_5d"] * 3 + 0.0085  # EV neto ~ +0.1%, very noisy
    report = run_viability_audit_sync(pd.DataFrame(rows), _config())
    gates = {g.name: g.passed for g in report.gates}
    assert gates["EV neto bot-only 5d positivo"] is None
    assert gates["IC bot-only 5d positivo"] is None
    assert report.verdict_level == "INCONCLUSIVE"
    rendered = render_viability_audit(report)
    assert "🟡" in rendered and "✅ <b>" not in rendered and "al 95%" in rendered


def test_viability_failing_verdict_never_gets_the_green_icon():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=60, signal=0.0, noise_seed=4)
    for row in rows:
        row["outcome_5d"] -= 0.05
    report = run_viability_audit_sync(pd.DataFrame(rows), _config())
    assert report.verdict_level == "FAIL"
    assert "NO VALIDADO" in report.verdict
    rendered = render_viability_audit(report)
    assert "🔴" in rendered
    assert "✅ <b>" not in rendered and "🟢" not in rendered


def test_viability_ic_aligns_sell_scores_with_directional_outcomes():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=30, signal=0.5, noise_seed=5)
    for row in rows[::2]:
        # A SELL is taken on a negative score and its stored outcome is already directional.
        row["decision"] = "SELL"
        row["final_score"] = -row["final_score"]
    report = run_viability_audit_sync(pd.DataFrame(rows), _config(min_sample=10))
    assert report.metrics["bot_only"]["5d"].ic_final > 0.3


def test_viability_drawdown_follows_time_and_position_fraction():
    rows = []
    for day, outcome in [("2026-03-03", 0.05), ("2026-03-02", -0.20), ("2026-03-04", 0.05)]:
        row = _row(source="execution_plan", status="EXECUTED", final_score=0.1, outcome_5d=outcome)
        row["decided_at"] = pd.Timestamp(day, tz="UTC")
        rows.append(row)
    report = run_viability_audit_sync(
        pd.DataFrame(rows),
        ViabilityAuditConfig(database_url="postgresql://unused", horizons=("5d",), min_sample=1, cost_bps=0),
    )
    # Chronological: -20% first, then recovery; at 10% per trade the drawdown is -2%, not -20%.
    assert abs(report.metrics["bot_only"]["5d"].max_drawdown - (-0.02)) < 1e-9


def _bench_candles(start="2026-02-20", days=140, daily=0.002):
    from datetime import date, timedelta

    first = date.fromisoformat(start)
    out, close = [], 100.0
    for i in range(days):
        day = first + timedelta(days=i)
        if day.weekday() < 5:
            out.append((day, close))
            close *= 1 + daily
    return out


def test_benchmark_excess_is_net_ev_minus_market_over_the_same_window():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=60, signal=0.0, noise_seed=6)
    for row in rows:
        row["outcome_5d"] = 0.0175 + row["outcome_5d"] * 0.1  # +1% net, steady
    frame = pd.DataFrame(rows)
    frame.attrs["benchmark_candles"] = _bench_candles(daily=0.004)  # market ~ +1.2% per 5 calendar days
    report = run_viability_audit_sync(frame, _config())
    bot = report.metrics["bot_only"]["5d"]
    assert bot.n_excess == bot.n
    assert bot.excess_ev < bot.net_ev - 0.008
    gates = {g.name: g.passed for g in report.gates}
    # Positive net EV, but it does not beat simply holding the benchmark.
    assert gates["EV neto bot-only 5d positivo"] is True
    assert gates["EV neto bot-only 5d sobre benchmark"] is not True
    assert report.verdict_level != "PASS"
    assert "exceso" in render_viability_audit(report)


def test_sell_excess_adds_the_benchmark_return():
    from src.analysis.viability_audit import _attach_benchmark_returns

    frame = pd.DataFrame(
        {"decided_at": [pd.Timestamp("2026-03-02 15:00", tz="UTC")] * 2, "decision": ["BUY", "SELL"]}
    )
    _attach_benchmark_returns(frame, [(pd.Timestamp("2026-03-02").date(), 100.0),
                                      (pd.Timestamp("2026-03-09").date(), 110.0)], ("5d",))
    assert frame["benchmark_return_5d"].round(6).tolist() == [0.1, 0.1]
    rows = [
        {**_row(source="execution_plan", status="EXECUTED", final_score=0.1, outcome_5d=0.12),
         "decided_at": pd.Timestamp("2026-03-02 15:00", tz="UTC"), "decision": "BUY"},
        {**_row(source="execution_plan", status="EXECUTED", final_score=-0.1, outcome_5d=0.02),
         "decided_at": pd.Timestamp("2026-03-02 15:00", tz="UTC"), "decision": "SELL"},
    ]
    data = pd.DataFrame(rows)
    data.attrs["benchmark_candles"] = [(pd.Timestamp("2026-03-02").date(), 100.0),
                                       (pd.Timestamp("2026-03-09").date(), 110.0)]
    report = run_viability_audit_sync(
        data, ViabilityAuditConfig(database_url="postgresql://unused", horizons=("5d",), min_sample=1, cost_bps=0)
    )
    # BUY: 12% - 10% = 2%. SELL avoided a +10% market: 2% + 10% = 12%. Mean 7%.
    assert abs(report.metrics["bot_only"]["5d"].excess_ev - 0.07) < 1e-9


def test_missing_benchmark_keeps_the_verdict_open():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=60, signal=0.5, noise_seed=1)
    report = run_viability_audit_sync(pd.DataFrame(rows), _config())
    gates = {g.name: g.passed for g in report.gates}
    assert gates["EV neto bot-only 5d sobre benchmark"] is None
    assert report.verdict_level == "INCONCLUSIVE"
    assert "Sin velas del benchmark" in render_viability_audit(report)


def test_prospective_cohort_only_counts_decisions_after_the_freeze():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=30, signal=0.5, noise_seed=7)
    config = ViabilityAuditConfig(
        database_url="postgresql://unused", horizons=("5d",), min_sample=10, cost_bps=75,
        frozen_since="2026-03-30",
    )
    report = run_viability_audit_sync(pd.DataFrame(rows), config)
    after = sum(1 for r in rows if r["decided_at"] >= pd.Timestamp("2026-03-30", tz="America/Argentina/Buenos_Aires"))
    assert report.prospective.n == after
    assert 0 < after < len(rows)
    assert "Prospectivo · reglas congeladas desde 2026-03-30" in render_viability_audit(report)


def test_missing_manual_cohort_does_not_block_a_favourable_verdict():
    frame = pd.DataFrame(
        _dated_rows(source="execution_plan", status="EXECUTED", n_dates=60, signal=0.5, noise_seed=1)
    )
    frame.attrs["benchmark_candles"] = _bench_candles(daily=0.0)
    report = run_viability_audit_sync(frame, _config())
    assert {g.name: g.passed for g in report.gates}["comparacion contra manual-only 5d"] is None
    assert report.verdict_level == "PASS"
