from src.analysis.plan_follow_reporting import (
    apply_plan_follow_overlay,
    summarize_plan_follow_operations,
)


def test_confirmed_attribution_overrides_legacy_match_evidence():
    plans = [{"id": 10, "same_amount_ars": 0}]
    links = {
        10: {
            "attribution_id": 4,
            "matched_amount_ars": 105_000,
            "follow_status": "FOLLOWED",
            "temporal_quality": "CONFIRMED_SEQUENCE",
            "eligible_for_viability": True,
            "executed_at": "2026-08-10T15:02:00+00:00",
            "executed_at_precision": "observed_after",
            "executed_at_source": "portfolio_snapshot",
        }
    }

    apply_plan_follow_overlay(plans, links)

    assert plans[0]["normalized_override_status"] == "FOLLOWED"
    assert plans[0]["same_amount_ars"] == 105_000
    assert plans[0]["match_evidence"] == "plan_execution_attribution"


def test_ambiguous_attribution_does_not_override_legacy_status():
    plans = [{"id": 10, "same_amount_ars": 0}]
    links = {
        10: {
            "attribution_id": 4,
            "follow_status": "OVERFOLLOWED",
            "temporal_quality": "AMBIGUOUS_SAME_DAY",
            "eligible_for_viability": False,
        }
    }

    apply_plan_follow_overlay(plans, links)

    assert "normalized_override_status" not in plans[0]
    assert plans[0]["normalized_temporal_quality"] == "AMBIGUOUS_SAME_DAY"


def test_operation_summary_is_deduplicated_and_excludes_ambiguous_rows():
    summary = summarize_plan_follow_operations(
        [
            {
                "eligible_for_viability": True,
                "follow_status": "FOLLOWED",
                "outcome_5d": 0.10,
                "executed_amount_ars": 80_000,
                "target_amount_ars": 100_000,
                "plan_link_count": 3,
                "movement_link_count": 1,
            },
            {
                "eligible_for_viability": False,
                "follow_status": "OVERFOLLOWED",
                "outcome_5d": -0.10,
                "executed_amount_ars": 120_000,
                "target_amount_ars": 100_000,
                "plan_link_count": 1,
                "movement_link_count": 1,
            },
        ]
    )

    assert summary["operations"] == 2
    assert summary["eligible"] == 1
    assert summary["ambiguous"] == 1
    assert summary["closed_5d"] == 1
    assert summary["win_rate_5d"] == 1.0
    assert summary["actual_pnl_5d_ars"] == 8_000
    assert summary["plan_links"] == 4
