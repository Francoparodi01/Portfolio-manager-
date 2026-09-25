from __future__ import annotations

import asyncio

from scripts import telegram_bot_meta as meta


COMPACT_REPORT = """
🧠 CIERRE DE RUEDA — 24/09 22:06 ART
💼 $2.892.597 ARS | Cash $3.832 ARS | Régimen: NORMAL
━━━ CARTERA ━━━
🔴 MU -0.110 T+0.060 M-0.070 S+0.001 R=RANGE trend=+0.600 12.0%→0.0% SELL_PARTIAL
🔴 YPFD -0.085 T+0.016 M+0.001 S-0.002 R=TRANSITIONAL trend=-0.344 4.5%→2.1% SELL_PARTIAL
🔴 IREN -0.050 T+0.118 M-0.066 S-0.002 R=RANGE trend=+0.319 12.0%→1.9% SELL_PARTIAL
"""


def test_sync_cached_analysis_ingests_report_with_cache_source(monkeypatch):
    async def fake_loader(report_type: str, chat_id: int):
        assert report_type == "analysis"
        assert chat_id == 123
        return {"report_text": COMPACT_REPORT}

    calls: list[tuple[str, str]] = []

    def fake_ingest(report: str, *, source: str):
        calls.append((report, source))
        return {
            "status": "SHADOW_ONLY",
            "run_id": "analysis-report-20260925T010600Z",
            "candidate_count": 3,
            "records_written": 9,
        }

    monkeypatch.setattr(meta.base, "_load_cached_report", fake_loader)
    monkeypatch.setattr(meta, "ingest_analysis_report", fake_ingest)

    summary = asyncio.run(meta._sync_cached_analysis_meta(123))

    assert summary["candidate_count"] == 3
    assert summary["records_written"] == 9
    assert calls == [(COMPACT_REPORT.strip(), "telegram-analysis-cache")]


def test_sync_cached_analysis_is_noop_without_artifact(monkeypatch):
    async def fake_loader(report_type: str, chat_id: int):
        return None

    called = False

    def fake_ingest(report: str, *, source: str):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(meta.base, "_load_cached_report", fake_loader)
    monkeypatch.setattr(meta, "ingest_analysis_report", fake_ingest)

    summary = asyncio.run(meta._sync_cached_analysis_meta(456))

    assert summary["status"] == "NO_CACHED_ANALYSIS"
    assert summary["records_written"] == 0
    assert called is False
