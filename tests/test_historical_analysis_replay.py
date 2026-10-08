from datetime import date, datetime, timezone

from src.analysis.historical_analysis_replay import (
    _action_direction,
    macro_input_for_ticker,
    select_analysis_vintage,
    sentiment_input_for_ticker,
)


UTC = timezone.utc


def _layer(macro=0.2, sentiment=0.1):
    return {
        "macro": {"raw": macro, "reason": ""},
        "sentiment": {"raw": sentiment, "reason": ""},
    }


def test_vintage_uses_argentina_session_date_and_later_joint_cutoff():
    rows = [
        {
            "decided_at": datetime(2026, 7, 21, 20, 13, tzinfo=UTC),
            "decision_date": date(2026, 7, 21),
            "ticker": "ASML",
            "run_id": "run-a",
            "owner_chat_id": None,
            "source": "execution_plan",
            "layers": _layer(),
        },
        {
            "decided_at": datetime(2026, 7, 21, 20, 13, 1, tzinfo=UTC),
            "decision_date": date(2026, 7, 21),
            "ticker": "IBM",
            "run_id": "run-a",
            "owner_chat_id": None,
            "source": "execution_plan",
            "layers": _layer(),
        },
    ]
    snapshot = datetime(2026, 7, 22, 2, 3, tzinfo=UTC)
    selected = select_analysis_vintage(
        rows,
        observed_date=date(2026, 7, 21),
        snapshot_scraped_at=snapshot,
        owner_chat_id=123,
    )
    assert selected is not None
    assert selected["run_id"] == "run-a"
    assert selected["cutoff"] == snapshot
    assert selected["owner_scope"] == "LEGACY_UNSCOPED"
    assert selected["analysis_vintage_age_seconds"] > 0


def test_macro_can_use_owner_exact_supplement_for_legacy_run_same_rule_group():
    cutoff = datetime(2026, 7, 21, 23, 0, tzinfo=UTC)
    vintage = {
        "run_id": "legacy",
        "observed_date": date(2026, 7, 21),
        "cutoff": cutoff,
        "owner_scope": "LEGACY_UNSCOPED",
        "owner_chat_id": 123,
        "rows": [],
    }
    rows = [
        {
            "decided_at": datetime(2026, 7, 21, 21, 0, tzinfo=UTC),
            "decision_date": date(2026, 7, 21),
            "ticker": "AMD",
            "run_id": "owner-movement",
            "owner_chat_id": 123,
            "source": "broker_movement",
            "layers": _layer(macro=0.37),
        },
        {
            "decided_at": datetime(2026, 7, 21, 22, 0, tzinfo=UTC),
            "decision_date": date(2026, 7, 21),
            "ticker": "NVDA",
            "run_id": "other-owner",
            "owner_chat_id": 999,
            "source": "broker_movement",
            "layers": _layer(macro=-0.9),
        },
    ]
    resolved = macro_input_for_ticker("MU", vintage=vintage, decision_rows=rows)
    assert resolved is not None
    assert resolved["score"] == 0.37
    assert resolved["source_ticker"] == "AMD"
    assert resolved["resolution"] == "SAME_MACRO_RULE_GROUP"


def test_sentiment_prefers_recorded_layer_and_never_reads_future_bucket():
    cutoff = datetime(2026, 8, 20, 20, 0, tzinfo=UTC)
    vintage = {
        "cutoff": cutoff,
        "rows": [
            {
                "ticker": "YPFD",
                "run_id": "recorded",
                "layers": _layer(sentiment=-0.25),
            }
        ],
    }
    sentiment = sentiment_input_for_ticker(
        "YPFD",
        vintage=vintage,
        sentiment_rows=[
            {
                "bucket_ts": datetime(2026, 8, 20, 21, 0, tzinfo=UTC),
                "ticker": "YPFD",
                "score": 0.99,
                "confidence": 1.0,
                "event_count": 1,
                "sources": {"_policy": "event_time_finbert_entity_v1"},
            }
        ],
    )
    assert sentiment == {
        "active": True,
        "score": -0.25,
        "source": "RECORDED_DECISION_LAYER",
        "source_run_id": "recorded",
    }


def test_planner_action_direction_contract():
    assert _action_direction("BUY_REBALANCE") == "BUY"
    assert _action_direction("SELL_PARTIAL") == "SELL"
    assert _action_direction("HOLD") == "HOLD"
