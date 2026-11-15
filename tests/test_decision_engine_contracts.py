from src.analysis.decision_engine import make_decision


def test_make_decision_uses_layers():
    default = make_decision(
        ticker="NVDA",
        score=0.10,
        conviction=0.80,
        regime="NORMAL",
        current_weight=0.10,
        target_weight=0.15,
    )
    layered = make_decision(
        ticker="NVDA",
        score=0.10,
        conviction=0.80,
        regime="NORMAL",
        layers={
            "technical": {"raw_score": 0.80, "weight": 0.75},
            "macro": {"raw_score": -0.20, "weight": 0.25},
        },
        current_weight=0.10,
        target_weight=0.15,
    )

    assert default.score == 0.10
    assert layered.score == 0.55
    assert layered.score != default.score
