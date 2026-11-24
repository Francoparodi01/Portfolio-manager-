from src.analysis.opportunity_screener import (
    AsymmetryMetrics,
    CandidateStatus,
    EdgeMetrics,
    ScreenerMetrics,
    TradeType,
    _classify,
)


def _strong_inputs():
    return {
        "score": 0.19,
        "conviction": 0.70,
        "asym": AsymmetryMetrics(
            ticker="YPF",
            risk_reward=1.6,
            rr_valid=True,
        ),
        "edge": EdgeMetrics(
            raw=0.03,
            label="positivo",
            vs_ticker="MELI",
            vs_score=0.18,
            explanation="test",
        ),
        "screener": ScreenerMetrics(ticker="YPF", momentum_20d=0.02),
        "gate_state": "NORMAL",
        "tech_score_raw": 0.20,
    }


def test_ypf_lands_in_vigilancia_a_not_c():
    status, trade_type, *_ = _classify(
        **_strong_inputs(),
        competes_with=["MELI"],
        portfolio_scores={"MELI": 0.18},
    )

    assert status in {
        CandidateStatus.VIGILANCIA_A,
        CandidateStatus.COMPRA_HABILITADA,
        CandidateStatus.COMPRABLE_AHORA,
        CandidateStatus.SWAP_CANDIDATO,
    }
    assert status != CandidateStatus.VIGILANCIA_C
    assert trade_type in {
        TradeType.SWAP_CANDIDATE,
        TradeType.NEW_ENTRY,
        TradeType.WATCHLIST,
    }


def test_new_entry_vs_rebalance_classification():
    new_entry = _classify(
        **_strong_inputs(),
        competes_with=[],
        portfolio_scores={},
    )
    rebalance = _classify(
        **_strong_inputs(),
        competes_with=["MELI"],
        portfolio_scores={"MELI": 0.10},
    )

    assert new_entry[1] == TradeType.NEW_ENTRY
    assert rebalance[1] == TradeType.SWAP_CANDIDATE
