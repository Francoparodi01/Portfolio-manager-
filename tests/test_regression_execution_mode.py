import pandas as pd

from src.analysis.regression_audit import RegressionAuditConfig, apply_audit_mode_filter


def test_execution_mode_keeps_only_confirmed_broker_fills():
    frame = pd.DataFrame(
        [
            {
                "decision": "BUY",
                "source": "execution_plan",
                "decision_type": "executable",
                "status": "APPROVED",
                "is_executable": True,
            },
            {
                "decision": "BUY",
                "source": "execution_plan",
                "decision_type": "executable",
                "status": "EXECUTED",
                "is_executable": True,
            },
        ]
    )

    filtered, warnings = apply_audit_mode_filter(
        frame,
        RegressionAuditConfig(database_url="postgresql://unused", mode="execution"),
    )

    assert warnings == []
    assert filtered["status"].tolist() == ["EXECUTED"]


def test_execution_mode_warns_when_only_approved_plans_exist():
    frame = pd.DataFrame(
        [
            {
                "decision": "BUY",
                "source": "execution_plan",
                "decision_type": "executable",
                "status": "APPROVED",
                "is_executable": True,
            },
        ]
    )

    filtered, warnings = apply_audit_mode_filter(
        frame,
        RegressionAuditConfig(database_url="postgresql://unused", mode="execution"),
    )

    assert filtered.empty
    assert "APPROVED/executable" in warnings[0]


def test_optimizer_mode_does_not_mix_radar_theoretical_rows():
    frame = pd.DataFrame(
        [
            {
                "decision": "BUY",
                "source": "optimizer",
                "decision_type": "theoretical",
                "status": "THEORETICAL",
            },
            {
                "decision": "BUY",
                "source": "radar",
                "decision_type": "radar_comprable_ahora_swap_candidate",
                "status": "THEORETICAL",
            },
        ]
    )

    filtered, warnings = apply_audit_mode_filter(
        frame,
        RegressionAuditConfig(database_url="postgresql://unused", mode="optimizer"),
    )

    assert warnings == []
    assert filtered["source"].tolist() == ["optimizer"]
