from scripts.run_performance import _ev_scope, render_performance_report


def _base_stats(dataset_stats):
    return {
        "total_trades": 10,
        "pending": 0,
        "lookback_days": 90,
        "win_rate": 0.6,
        "avg_win_5d": 0.04,
        "avg_loss_5d": -0.02,
        "ev": 0.016,
        "winners": 6,
        "losers": 4,
        "avg_return_5d": 0.016,
        "avg_return_10d": 0.02,
        "avg_return_20d": 0.03,
        "best_trade": 0.08,
        "worst_trade": -0.03,
        "dataset_stats": dataset_stats,
    }


def test_ev_scope_labels_optimizer_only_as_historical():
    title, note = _ev_scope(
        [
            {
                "source": "optimizer",
                "status": "THEORETICAL",
                "decision_type": "theoretical",
                "con_5d": 8,
            }
        ]
    )

    assert title == "EV histórico agregado"
    assert "ejecución real" in note


def test_render_performance_separates_historical_ev_from_execution_ev():
    report = render_performance_report(
        _base_stats(
            [
                {
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                    "decision": "BUY",
                    "n": 10,
                    "con_5d": 10,
                    "con_10d": 8,
                    "con_20d": 6,
                }
            ]
        )
    )

    assert "EV histórico agregado" in report
    assert "Todavía no mide performance de ejecución real" in report
    assert "histórico levemente favorable, seguir midiendo" in report
    assert "el sistema tiene edge real" not in report


def test_render_performance_mentions_execution_when_present():
    report = render_performance_report(
        _base_stats(
            [
                {
                    "source": "execution_plan",
                    "status": "EXECUTED",
                    "decision_type": "executable",
                    "decision": "BUY",
                    "n": 4,
                    "con_5d": 4,
                    "con_10d": 2,
                    "con_20d": 1,
                }
            ]
        )
    )

    assert "EV operativo 5D" in report
    assert "Execution Audit" in report


def test_render_performance_does_not_call_approved_plan_real_execution():
    report = render_performance_report(
        _base_stats(
            [
                {
                    "source": "execution_plan",
                    "status": "APPROVED",
                    "decision_type": "executable",
                    "decision": "BUY",
                    "n": 4,
                    "con_5d": 4,
                    "con_10d": 2,
                    "con_20d": 1,
                }
            ]
        )
    )

    assert "fills reales confirmados" not in report
    assert "planes aprobados" in report


def test_render_performance_calls_out_legacy_external_rows():
    report = render_performance_report(
        _base_stats(
            [
                {
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                    "decision": "BUY",
                    "n": 10,
                    "con_5d": 0,
                    "con_10d": 0,
                    "con_20d": 0,
                    "legacy_external": 10,
                }
            ]
        )
    )

    assert "Legacy externo omitido: 10" in report


def test_render_performance_explains_new_user_cold_start():
    report = render_performance_report(
        {
            "total_trades": 0,
            "pending": 0,
            "lookback_days": 90,
            "dataset_stats": [],
            "owner_chat_id": 123,
        }
    )

    assert "Este usuario todavia no acumulo decisiones propias" in report
    assert "Verificá que <b>run_analysis.py</b>" not in report


def test_render_performance_explains_pending_outcomes_without_self_referential_command():
    report = render_performance_report(
        {
            "total_trades": 0,
            "pending": 2,
            "lookback_days": 90,
            "dataset_stats": [
                {
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                    "decision": "BUY",
                    "n": 4,
                    "con_5d": 0,
                    "con_10d": 0,
                    "con_20d": 0,
                }
            ],
        }
    )

    assert "Hay <b>2</b> ejecuciones reales pendientes de outcome" in report
    assert "No es error: aun falta recorrido de mercado" in report
    assert "Corriendo <code>python scripts/run_performance.py</code>" not in report


def test_ev_label_is_judged_net_of_estimated_costs():
    from scripts.run_performance import _ev_label

    # 2.5% gross is only 1.75% after the 0.75% cost estimate: marginal, not positive.
    assert _ev_label(0.025).startswith("🟡 MARGINAL")
    assert _ev_label(0.005).startswith("❌ NEGATIVO")
