from types import SimpleNamespace

import src.analysis.economic_meta_watcher as meta_watcher
from src.analysis.economic_meta_telegram import _latest_run, _preferred_rows
from src.analysis.economic_meta_watcher import _action, _portfolio_turnover


def _row(*, run_id: str, as_of: str, opportunity_id: str) -> dict:
    return {
        "run_id": run_id,
        "as_of": as_of,
        "opportunity_id": opportunity_id,
        "ticker": "NVDA",
        "policy_name": "META-A",
        "decision": "ALLOW_SHADOW",
    }


def test_newer_report_is_not_hidden_by_older_db_watcher_run():
    rows = [
        _row(
            run_id="db-old",
            as_of="2026-09-25T18:16:00+00:00",
            opportunity_id="analysis:meta-db-input-v3:db-old:1:NVDA:decision_log",
        ),
        _row(
            run_id="analysis-report-20260925T201200Z",
            as_of="2026-09-25T20:12:00+00:00",
            opportunity_id="analysis:report:20260925T201200Z:NVDA",
        ),
    ]

    preferred, mode = _preferred_rows(rows)
    run_id, selected = _latest_run(preferred)

    assert mode == "AUTO_ANALYSIS"
    assert run_id == "analysis-report-20260925T201200Z"
    assert len(selected) == 1


def test_db_wins_timestamp_tie_because_it_has_richer_pit_evidence():
    rows = [
        _row(
            run_id="analysis-report-20260925T201200Z",
            as_of="2026-09-25T20:12:00+00:00",
            opportunity_id="analysis:report:20260925T201200Z:NVDA",
        ),
        _row(
            run_id="db-same-time",
            as_of="2026-09-25T20:12:00+00:00",
            opportunity_id="analysis:meta-db-input-v3:db-same-time:1:NVDA:decision_log",
        ),
    ]

    preferred, mode = _preferred_rows(rows)
    run_id, selected = _latest_run(preferred)

    assert mode == "AUTO_ANALYSIS_DB"
    assert run_id == "db-same-time"
    assert len(selected) == 1


def test_db_action_normalization_matches_legacy_report_semantics():
    assert _action("SELL_PARTIAL") == "REDUCE"
    assert _action("REDUCE") == "REDUCE"
    assert _action("SELL_FULL") == "SELL"
    assert _action("BUY_PARTIAL") == "BUY"
    assert _action("HOLD") == "HOLD"


def test_db_turnover_ignores_hold_deltas_and_does_not_add_both_sides():
    rows = [
        {"action": "BUY", "delta_weight": 0.15},
        {"action": "BUY_PARTIAL", "delta_weight": 0.10},
        {"action": "SELL_PARTIAL", "delta_weight": -0.08},
        # This is the regression: HOLD target deltas must not make the whole run
        # look like high turnover.
        {"action": "HOLD", "delta_weight": 0.30},
    ]

    assert _portfolio_turnover(rows) == 0.25


def test_db_turnover_uses_larger_operable_side_like_report_ingest():
    rows = [
        {"action": "BUY", "delta_weight": 0.20},
        {"action": "SELL", "delta_weight": -0.30},
    ]

    # Legacy report semantics use max(buy_delta, sell_delta), not their sum.
    assert _portfolio_turnover(rows) == 0.30


def test_legacy_null_owner_is_available_only_for_configured_single_user(monkeypatch):
    cfg = SimpleNamespace(
        multiuser_enabled=False,
        scraper=SimpleNamespace(telegram_chat_id="123456"),
    )
    monkeypatch.setattr(meta_watcher, "get_config", lambda: cfg)

    assert meta_watcher._legacy_single_owner_chat_id() == 123456

    cfg.multiuser_enabled = True
    assert meta_watcher._legacy_single_owner_chat_id() is None


def test_legacy_null_owner_fails_closed_without_numeric_configured_owner(monkeypatch):
    cfg = SimpleNamespace(
        multiuser_enabled=False,
        scraper=SimpleNamespace(telegram_chat_id="not-a-chat-id"),
    )
    monkeypatch.setattr(meta_watcher, "get_config", lambda: cfg)

    assert meta_watcher._legacy_single_owner_chat_id() is None
