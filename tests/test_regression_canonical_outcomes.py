import pandas as pd

from src.analysis.regression_audit import RegressionAuditConfig, prepare_model_frame


def test_directional_regression_uses_canonical_outcome_without_double_inverting_sell():
    frame = pd.DataFrame(
        [
            {
                "decision": "SELL",
                "outcome_5d": 0.10,
                "final_score": -0.20,
            }
        ]
    )

    prepared, target_col, _actions, warnings = prepare_model_frame(
        frame,
        "5d",
        RegressionAuditConfig(database_url="postgresql://unused"),
    )

    assert warnings == []
    assert target_col == "directional_5d"
    assert prepared[target_col].tolist() == [0.10]
