from datetime import datetime
from zoneinfo import ZoneInfo

from scripts.run_radar_metrics import render_radar_metrics
from src.core.telegram_format import validate_telegram_html


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def test_no_capture_is_explicitly_prospective_and_non_operational():
    report = render_radar_metrics({"status": "NO_CAPTURE", "cost_bps": 75})

    assert "Todavía no hay una cohorte prospectiva capturada" in report
    assert "16:50 ART" in report
    assert "No usa replay ni backfill" in report
    assert "no cambia ranking, plan ni órdenes" in report
    assert validate_telegram_html(report) == (True, [])


def test_radar_metrics_render_scores_and_mature_evidence_without_false_authority():
    empty_summary = {
        "cohorts": {
            "discovery_top_quintile": {"n": 0},
            "all_universe": {"n": 0},
        }
    }
    discovery_5r = {
        "cohorts": {
            "discovery_top_quintile": {
                "n": 12,
                "win_rate": 0.583,
                "mean_net_return": 0.031,
                "mean_excess_vs_universe": 0.018,
            }
        }
    }
    trigger_5r = {
        "cohorts": {
            "all_universe": {
                "n": 4,
                "mean_net_return": 0.024,
                "mean_excess_vs_qqq": 0.011,
            }
        }
    }
    payload = {
        "status": "OK",
        "cost_bps": 75,
        "run": {
            "captured_at": datetime(2026, 8, 20, 16, 50, tzinfo=ART_TZ),
            "captured_session": "2026-08-20",
            "scoring_version": "radar-v2+shadow-v3:abcdef0123456789",
            "universe_count": 249,
            "evaluated_count": 248,
            "eligible_count": 31,
            "selected_count": 6,
        },
        "version_stats": {"run_count": 3},
        "snapshot_count": 249,
        "complete_score_count": 240,
        "feature_quality_counts": {"GOOD": 30, "PARTIAL": 210, "INSUFFICIENT": 9},
        "readiness_counts": {"PRE_BREAKOUT": 20, "WATCH": 229},
        "top_shadow": [
            {
                "ticker": "AAA",
                "radar_eligible": True,
                "feature_quality_flag": "GOOD",
                "readiness_state": "PRE_BREAKOUT",
                "discovery_score": 43.2,
                "setup_score": 41.8,
                "composite_shadow_score": 85.0,
            }
        ],
        "outcomes": {
            5: {"discovery": discovery_5r, "trigger": trigger_5r},
            10: {"discovery": empty_summary, "trigger": empty_summary},
            20: {"discovery": empty_summary, "trigger": empty_summary},
            40: {"discovery": empty_summary, "trigger": empty_summary},
        },
    }

    report = render_radar_metrics(payload)

    assert "Universo: <b>249</b>" in report
    assert "Scores completos: <b>240/249</b> (96.4%)" in report
    assert "AAA</b> · total 85.0/100 · D 43.2/50 · S 41.8/50" in report
    assert "5r</b> · desc top20 n=12 · acierto 58.3% · neto +3.1%" in report
    assert "trigger n=4 · neto +2.4% · vs QQQ +1.1%" in report
    assert "Los scores no son probabilidades" in report
    assert len(report) < 3900
    assert validate_telegram_html(report) == (True, [])
