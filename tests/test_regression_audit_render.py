from datetime import datetime, timezone

import pandas as pd

from src.analysis.regression_audit import (
    RegressionAuditConfig,
    RegressionAuditReport,
    RegressionModelResult,
    normalize_decision_frame,
    render_regression_audit,
    render_regression_audit_compact,
    run_regression_audit_sync,
)


def _model(
    *,
    horizon: str,
    model_name: str,
    n: int,
    r2: float | None,
) -> RegressionModelResult:
    return RegressionModelResult(
        horizon=horizon,
        model_name=model_name,
        target_col=f"directional_{horizon}",
        features=["final_score"],
        n=n,
        r2=r2,
        adj_r2=None,
        rmse=0.08 if r2 is not None else None,
        intercept=0.01 if r2 is not None else None,
        coefficients={"final_score": 0.12} if r2 is not None else {},
        pvalues={"final_score": 0.04} if r2 is not None else {},
        score_coef=0.12 if r2 is not None else None,
        score_pvalue=0.04 if r2 is not None else None,
        ic=0.22 if r2 is not None else None,
        suggested_buy_threshold=None,
        threshold_reason="muestra insuficiente" if r2 is None else None,
        expected_return_at_buy_min_008=0.03 if r2 is not None else None,
        notes=["Feature risk_score omitida por ser constante o vacía."],
    )


def _report() -> RegressionAuditReport:
    bucket = pd.DataFrame(
        [
            {
                "bucket": "POS_OPERABLE",
                "n": 9,
                "avg_score": 0.108,
                "avg_return": 0.025,
                "hit_rate": 0.78,
                "reliability": "baja",
            }
        ]
    )
    return RegressionAuditReport(
        generated_at=datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc),
        rows_loaded=48,
        rows_usable=37,
        cost_threshold=0.0075,
        target_mode="directional",
        mode="optimizer",
        actions_used=["BUY", "SELL"],
        source_counts={"optimizer": 27, "execution_plan": 21},
        status_counts={"THEORETICAL": 25, "APPROVED": 2},
        models=[
            _model(horizon="5d", model_name="baseline_score", n=24, r2=0.01),
            _model(horizon="10d", model_name="baseline_score", n=9, r2=None),
        ],
        bucket_tables={"5d": bucket},
        warnings=["risk_score está todo en 0."],
    )


def _table_lines_after(report_text: str, heading: str) -> list[str]:
    tail = report_text.split(heading, maxsplit=1)[1]
    inside_pre = tail.split("<pre>", maxsplit=1)[1].split("</pre>", maxsplit=1)[0]
    return [line for line in inside_pre.strip("\n").splitlines() if line]


def test_full_regression_render_uses_one_aligned_scorecard():
    text = render_regression_audit(_report())

    assert "<b>RESUMEN DE MODELOS</b>" in text
    assert "<b>BUCKETS POR SCORE</b>" in text
    assert "Modelo:" not in text
    assert "HORIZONTE 5D" not in text

    rows = _table_lines_after(text, "<b>RESUMEN DE MODELOS</b>")
    assert len({len(row) for row in rows}) == 1


def test_compact_regression_render_keeps_aligned_single_output():
    text = render_regression_audit_compact(_report())

    assert "<b>RESUMEN DE MODELOS</b>" in text
    assert "<b>BUCKETS POR SCORE</b>" in text
    assert "Notas diagnósticas" not in text

    rows = _table_lines_after(text, "<b>BUCKETS POR SCORE</b>")
    assert len({len(row) for row in rows}) == 1


def test_regression_loader_accepts_asyncpg_dsn_scheme(monkeypatch):
    seen = {}

    class _Conn:
        async def fetch(self, query, *args):
            if "information_schema.columns" in query:
                return [{"column_name": "id"}, {"column_name": "decided_at"}]
            return []

        async def close(self):
            return None

    async def _connect(dsn):
        seen["dsn"] = dsn
        return _Conn()

    monkeypatch.setattr("src.analysis.regression_audit.asyncpg.connect", _connect)

    from src.analysis.regression_audit import load_decision_log
    import asyncio

    asyncio.run(load_decision_log("postgresql+asyncpg://user:pass@host/db"))

    assert seen["dsn"] == "postgresql://user:pass@host/db"


def test_regression_loader_keeps_pending_rows_without_canonical_outcome(monkeypatch):
    seen = {}

    class _Conn:
        async def fetch(self, query, *args):
            if "information_schema.columns" in query:
                return [
                    {"column_name": "id"},
                    {"column_name": "decided_at"},
                    {"column_name": "ticker"},
                    {"column_name": "decision"},
                    {"column_name": "final_score"},
                    {"column_name": "outcome_5d"},
                    {"column_name": "outcome_basis"},
                    {"column_name": "source"},
                    {"column_name": "status"},
                    {"column_name": "decision_type"},
                ]
            seen["query"] = query
            return [
                {
                    "id": 1,
                    "decided_at": datetime(2026, 5, 20, tzinfo=timezone.utc),
                    "ticker": "NVDA",
                    "decision": "BUY",
                    "final_score": 0.12,
                    "outcome_5d": None,
                    "outcome_basis": None,
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                }
            ]

        async def close(self):
            return None

    async def _connect(_dsn):
        return _Conn()

    monkeypatch.setattr("src.analysis.regression_audit.asyncpg.connect", _connect)

    from src.analysis.regression_audit import load_decision_log
    import asyncio

    df = asyncio.run(load_decision_log("postgresql://user:pass@host/db"))

    assert len(df) == 1
    assert "outcome_basis = 'canonical_cocos'" not in seen["query"]


def test_regression_summary_counts_match_selected_mode():
    df = pd.DataFrame(
        [
            {
                "ticker": "NVDA",
                "decision": "BUY",
                "final_score": 0.1,
                "outcome_5d": None,
                "source": "optimizer",
                "status": "THEORETICAL",
                "decision_type": "theoretical",
            },
            {
                "ticker": "AMD",
                "decision": "BUY",
                "final_score": 0.05,
                "outcome_5d": None,
                "source": "execution_plan",
                "status": "BLOCKED",
                "decision_type": "blocked",
            },
        ]
    )

    report = run_regression_audit_sync(
        df,
        RegressionAuditConfig(database_url="unused", mode="optimizer", horizons=("5d",)),
    )

    assert report.rows_loaded == 1
    assert report.source_counts == {"optimizer": 1}
    assert report.status_counts == {"THEORETICAL": 1}


def test_regression_normalizer_turns_null_text_status_into_unknown():
    df = pd.DataFrame(
        [
            {
                "decision": "BUY",
                "source": None,
                "status": None,
                "decision_type": None,
            },
            {
                "decision": "SELL",
                "source": "nan",
                "status": "None",
                "decision_type": "",
            },
        ]
    )

    normalized = normalize_decision_frame(df)

    assert normalized["source"].tolist() == ["sin_source", "sin_source"]
    assert normalized["status"].tolist() == ["UNKNOWN", "UNKNOWN"]
    assert normalized["decision_type"].tolist() == ["unknown", "unknown"]
