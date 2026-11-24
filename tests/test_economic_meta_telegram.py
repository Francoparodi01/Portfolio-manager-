from __future__ import annotations

import json

from src.analysis.economic_meta_telegram import render_latest_meta, render_meta_status


def _write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def test_render_latest_meta_uses_latest_run_and_is_read_only(tmp_path):
    path = tmp_path / "shadow.jsonl"
    rows = [
        {
            "run_id": "old-run",
            "as_of": "2026-09-23T12:00:00+00:00",
            "ticker": "OLD",
            "candidate_action": "BUY",
            "candidate_score": 0.20,
            "policy_name": "META-A",
            "decision": "ALLOW_SHADOW",
            "rejection_reason": None,
        },
        {
            "run_id": "new-run",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": "NVDA",
            "candidate_action": "SELL",
            "candidate_score": -0.14,
            "policy_name": "META-A",
            "decision": "ALLOW_SHADOW",
            "rejection_reason": None,
        },
        {
            "run_id": "new-run",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": "NVDA",
            "candidate_action": "SELL",
            "candidate_score": -0.14,
            "policy_name": "META-B",
            "decision": "ALLOW_SHADOW",
            "rejection_reason": None,
        },
        {
            "run_id": "new-run",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": "NVDA",
            "candidate_action": "SELL",
            "candidate_score": -0.14,
            "policy_name": "META-C",
            "decision": "REJECT_TO_HOLD",
            "rejection_reason": "EDGE_EVIDENCE_REQUIRED",
        },
    ]
    _write_rows(path, rows)

    text = render_latest_meta(path=path)

    assert "new-run" in text
    assert "NVDA · SELL · score -0.140" in text
    assert "A✅" in text
    assert "B✅" in text
    assert "C⏸(falta edge vs HOLD)" in text
    assert "OLD" not in text
    assert "Capital effect: NO" in text
    assert "Vista read-only" in text


def test_render_latest_meta_prefers_auto_analysis_over_newer_manual_probe(tmp_path):
    path = tmp_path / "shadow.jsonl"
    rows = [
        {
            "run_id": "formal-run",
            "as_of": "2026-09-24T17:00:00+00:00",
            "ticker": "MU",
            "candidate_action": "SELL",
            "candidate_score": -0.11,
            "policy_name": policy,
            "decision": "REJECT_TO_HOLD",
            "rejection_reason": "SCORE_BELOW_PREREGISTERED_GATE",
            "opportunity_id": "analysis:formal-run:0:MU:decision_log",
        }
        for policy in ("META-A", "META-B", "META-C")
    ]
    rows.extend(
        {
            "run_id": "prueba-telegram",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": "NVDA",
            "candidate_action": "SELL",
            "candidate_score": -0.14,
            "policy_name": policy,
            "decision": "ALLOW_SHADOW",
            "rejection_reason": None,
            "opportunity_id": None,
        }
        for policy in ("META-A", "META-B", "META-C")
    )
    _write_rows(path, rows)

    text = render_latest_meta(path=path)

    assert "formal-run" in text
    assert "Fuente: análisis automático" in text
    assert "MU · SELL" in text
    assert "prueba-telegram" not in text
    assert "NVDA" not in text


def test_render_latest_meta_filters_ticker(tmp_path):
    path = tmp_path / "shadow.jsonl"
    rows = [
        {
            "run_id": "run-1",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": ticker,
            "candidate_action": "HOLD",
            "candidate_score": 0.0,
            "policy_name": "META-A",
            "decision": "REJECT_TO_HOLD",
            "rejection_reason": "SOURCE_ALREADY_HOLD",
        }
        for ticker in ("NVDA", "MSFT")
    ]
    _write_rows(path, rows)

    text = render_latest_meta(ticker="nvda", path=path)

    assert "NVDA" in text
    assert "MSFT" not in text


def test_render_meta_status_counts_records(tmp_path):
    path = tmp_path / "shadow.jsonl"
    rows = [
        {
            "run_id": "run-1",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": "NVDA",
            "policy_name": "META-A",
            "decision": "ALLOW_SHADOW",
            "opportunity_id": "analysis:run-1:0:NVDA:decision_log",
        },
        {
            "run_id": "run-1",
            "as_of": "2026-09-24T18:00:00+00:00",
            "ticker": "NVDA",
            "policy_name": "META-C",
            "decision": "REJECT_TO_HOLD",
            "opportunity_id": "analysis:run-1:0:NVDA:decision_log",
        },
    ]
    _write_rows(path, rows)

    text = render_meta_status(path=path)

    assert "Records: 2 · runs: 1 · tickers: 1" in text
    assert "Auto análisis: 2 records · 1 runs · 1 tickers" in text
    assert "Manual/pruebas: 0 records" in text
    assert "META-A: allow=1 · hold=0" in text
    assert "META-C: allow=0 · hold=1" in text
    assert "Capital effect: NO" in text
