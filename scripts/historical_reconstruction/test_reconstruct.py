from datetime import datetime, timezone

from reconstruct import classify_episode, reconstruct_episodes, rows_for_confidence

UTC = timezone.utc
NOW = datetime(2026, 9, 1, 20, tzinfo=UTC)


def row(**overrides):
    base = {
        "plan_id": "plan-1",
        "plan_owner_chat_id": 123,
        "plan_run_id": "run-1",
        "created_at": NOW,
        "plan_updated_at": NOW,
        "plan_source": "execution_plan",
        "feasible": True,
        "intent_id": 10,
        "decision_log_id": 20,
        "ticker": "NVDA",
        "side": "BUY",
        "is_executable": True,
        "was_blocked": False,
        "intent_created_at": NOW,
        "intent_updated_at": NOW,
        "decision_exists": True,
        "decision_owner_chat_id": 123,
        "decision_run_id": "run-1",
        "decision_ticker": "NVDA",
        "decision_source": "execution_plan",
        "superseded_by_id": None,
    }
    base.update(overrides)
    return base


def test_explicit_coherent_plan_is_medium_without_immutable_capture():
    result = classify_episode(row(), requested_owner=123, legacy_owner_verified=True)
    assert result["confidence"] == "MEDIUM"
    assert result["primary_eligible"] is True


def test_immutable_capture_with_coherent_explicit_lineage_is_high():
    result = classify_episode(
        row(),
        requested_owner=123,
        legacy_owner_verified=True,
        immutable_plan_ids={"plan-1"},
    )
    assert result["confidence"] == "HIGH"
    assert result["primary_eligible"] is True
    assert "IMMUTABLE_PLAN_CAPTURE" in result["reason_codes"]


def test_verified_null_owner_is_low_and_never_primary():
    result = classify_episode(
        row(plan_owner_chat_id=None, decision_owner_chat_id=None),
        requested_owner=123,
        legacy_owner_verified=True,
    )
    assert result["confidence"] == "LOW"
    assert result["primary_eligible"] is False
    assert "LEGACY_OWNER_INFERRED" in result["reason_codes"]


def test_null_owner_without_single_owner_proof_is_unrecoverable():
    result = classify_episode(
        row(plan_owner_chat_id=None),
        requested_owner=123,
        legacy_owner_verified=False,
    )
    assert result["confidence"] == "UNRECOVERABLE"
    assert result["primary_eligible"] is False


def test_cross_run_decision_link_is_unrecoverable():
    result = classify_episode(
        row(decision_run_id="another-run"),
        requested_owner=123,
        legacy_owner_verified=True,
    )
    assert result["confidence"] == "UNRECOVERABLE"
    assert result["reason_codes"] == ["MUTATED_CROSS_RUN_DECISION_LINK"]


def test_radar_or_optimizer_decision_cannot_enter_formal_sample():
    for source in ("radar", "optimizer"):
        result = classify_episode(
            row(decision_source=source),
            requested_owner=123,
            legacy_owner_verified=True,
        )
        assert result["confidence"] == "UNRECOVERABLE"
        assert result["reason_codes"] == ["CROSS_DOMAIN_DECISION_LINK"]


def test_reused_decision_link_across_plans_is_low():
    rows = [
        row(plan_id="plan-1", intent_id=10, decision_log_id=20),
        row(plan_id="plan-2", intent_id=11, decision_log_id=20),
    ]
    result = reconstruct_episodes(rows, requested_owner=123, legacy_owner_verified=True)
    assert result["reused_decision_links"] == 1
    assert result["confidence_counts"]["LOW"] == 2
    assert all(
        "DECISION_LINK_REUSED_ACROSS_PLANS" in e["reason_codes"]
        for e in result["episodes"]
    )


def test_blocked_and_non_executable_intents_are_inventory_not_episodes():
    rows = [
        row(intent_id=1),
        row(intent_id=2, was_blocked=True),
        row(intent_id=3, is_executable=False),
    ]
    result = reconstruct_episodes(rows, requested_owner=123, legacy_owner_verified=True)
    assert result["raw_intents"] == 3
    assert result["evaluable_intents"] == 1
    assert result["excluded_non_evaluable"] == 2


def test_only_high_and_medium_can_be_selected_for_primary_outcomes():
    rows = [
        row(plan_id="p1", intent_id=1),
        row(
            plan_id="p2",
            intent_id=2,
            plan_owner_chat_id=None,
            decision_owner_chat_id=None,
        ),
        row(plan_id="p3", intent_id=3, decision_run_id="wrong"),
    ]
    reconstructed = reconstruct_episodes(
        rows,
        requested_owner=123,
        legacy_owner_verified=True,
    )
    primary = rows_for_confidence(
        rows,
        reconstructed["episodes"],
        {"HIGH", "MEDIUM"},
    )
    assert [r["intent_id"] for r in primary] == [1]
