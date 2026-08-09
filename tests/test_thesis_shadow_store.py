from __future__ import annotations

import asyncio

from src.analysis.thesis_shadow import render_shadow_report, render_shadow_telegram_report
from src.analysis.thesis_shadow_store import (
    MAX_ABS_REALIZED_RETURN_FOR_METRICS,
    ShadowThesisStore,
)


class _FakeAcquire:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn

    async def __aenter__(self) -> "_FakeConnection":
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class _FakeConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args: tuple[object, ...] = ()

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.query = query
        self.args = args
        return [
            {
                "horizon_sessions": 5,
                "samples": 42,
                "excluded_samples": 3,
                "excluded_tickers": ["BAYN", "C.I."],
                "directional_accuracy": 0.61,
                "mean_absolute_error": 0.054,
                "mean_expected_return": 0.012,
                "mean_realized_return": 0.018,
            }
        ]


def test_evaluation_metrics_applies_realized_return_quality_gate():
    conn = _FakeConnection()
    store = ShadowThesisStore(_FakePool(conn))

    metrics = asyncio.run(store.evaluation_metrics(owner_chat_id=123))

    normalized_sql = " ".join(conn.query.split())
    assert conn.args == (123, MAX_ABS_REALIZED_RETURN_FOR_METRICS)
    assert "ABS(o.realized_return) <= $2::double precision" in normalized_sql
    assert "COUNT(*) FILTER (WHERE is_sane)::integer AS samples" in normalized_sql
    assert "COUNT(*) FILTER (WHERE NOT is_sane)::integer AS excluded_samples" in normalized_sql
    assert "ARRAY_AGG(DISTINCT ticker ORDER BY ticker)" in normalized_sql
    assert metrics[0]["samples"] == 42
    assert metrics[0]["excluded_samples"] == 3
    assert metrics[0]["excluded_tickers"] == ["BAYN", "C.I."]
    assert MAX_ABS_REALIZED_RETURN_FOR_METRICS == 1.0


def test_shadow_reports_log_quality_gate_exclusions_next_to_metrics():
    metrics = [
        {
            "horizon_sessions": 5,
            "samples": 42,
            "excluded_samples": 3,
            "excluded_tickers": ["BAYN", "C.I."],
            "directional_accuracy": 0.61,
            "mean_absolute_error": 0.054,
        }
    ]

    report = render_shadow_report([], metrics=metrics)
    telegram_report = render_shadow_telegram_report([], metrics=metrics)

    assert "5r: n=42 direccion=61.0% MAE=5.4% excluidos=3 (BAYN, C.I.)" in report
    assert "excluidos 3 (BAYN, C.I.)" in telegram_report
