from src.analysis.plan_follow_reporting import (
    apply_plan_follow_overlay,
    summarize_plan_follow_operations,
)
from src.analysis.decision_ledger import render_decision_ledger
from src.core.telegram_format import validate_telegram_html


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


def test_decision_ledger_renderer_is_compact_and_scope_explicit():
    data = {
        "days": 180,
        "summary": {
            "real_total": 162,
            "real_closed_5d": 160,
            "real_win_rate_5d": 0.481,
            "real_pnl_5d_ars": 25_756,
            "plans_closed_5d": 127,
            "bot_full_pnl_5d_ars": -217_890,
            "human_matched_pnl_5d_ars": -305_642,
            "human_vs_bot_5d_ars": -87_752,
            "radar_total": 108,
            "radar_closed_5d": 108,
            "radar_avg_5d": 0.009,
            "radar_operable_closed_5d": 52,
            "radar_operable_avg_5d": -0.018,
            "radar_blocked_closed_5d": 56,
            "radar_blocked_avg_5d": 0.033,
            "swap_total": 31,
            "swap_closed_5d": 31,
            "swap_avg_alpha_5d": 0.051,
            "swap_alpha_5d_ars": 107_125,
            "followed_normalized": {
                "closed_5d": 23,
                "eligible": 26,
                "win_rate_5d": 0.609,
                "avg_return_5d": 0.029,
                "actual_pnl_5d_ars": 88_619,
            },
        },
        "real_executions": [],
        "bot_vs_human": [],
        "radar": [],
        "pending_mark": [],
    }

    report = render_decision_ledger(data)

    assert len(report) < 4096
    assert "NORMALIZADO" in report
    assert "PLAN-LEVEL" in report
    assert "TEÓRICO" in report
    assert "Bruto = antes de costos" in report
    assert validate_telegram_html(report)[0] is True
