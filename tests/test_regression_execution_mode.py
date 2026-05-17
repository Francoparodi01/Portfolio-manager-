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
