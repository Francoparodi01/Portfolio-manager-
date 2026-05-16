from types import SimpleNamespace

from scripts import run_analysis
from src.analysis.opportunity_screener import (
    CandidateStatus,
    OpportunityCandidate,
    OpportunityReport,
    TradeType,
    render_opportunity_report,
)
from src.analysis.synthesis import blend_scores
from src.analysis.technical import Signal


def _risk_position():
    return {
        "risk_level": "NORMAL",
        "warnings": [],
        "suggested_pct_adj": 0.10,
    }


def _macro_snapshot():
    return SimpleNamespace(
        wti=None,
        brent=None,
        dxy=None,
        vix=None,
        sp500=None,
        merval=None,
        tnx=None,
        ccl=None,
        mep=None,
        reservas=None,
        riesgo_pais=None,
    )


def test_blend_scores_preserves_source_metadata_without_changing_score():
    baseline = blend_scores(
        ticker="T",
        technical_signal="BUY",
        technical_strength=0.7,
        macro_score=0.1,
        risk_position=_risk_position(),
        sentiment_score=0.0,
    )
    audited = blend_scores(
        ticker="T",
        technical_signal="BUY",
        technical_strength=0.7,
        macro_score=0.1,
        risk_position=_risk_position(),
        sentiment_score=0.0,
        technical_candle_source_mode="mixed",
        technical_has_reconstructed_candles=True,
        technical_candle_sources=("COCOS", "internal_snapshot"),
        technical_candle_source_counts={"COCOS": 70, "internal_snapshot": 10},
    )

    assert audited.final_score == baseline.final_score
    assert audited.technical_candle_source_mode == "mixed"
    assert audited.technical_has_reconstructed_candles is True
    assert audited.technical_candle_sources == ("COCOS", "internal_snapshot")
    assert audited.technical_candle_source_counts == {"COCOS": 70, "internal_snapshot": 10}


def test_layers_payload_persists_technical_source_metadata():
    result = blend_scores(
        ticker="T",
        technical_signal="BUY",
        technical_strength=0.7,
        macro_score=0.1,
        risk_position=_risk_position(),
        sentiment_score=0.0,
        technical_candle_source_mode="reconstructed",
        technical_has_reconstructed_candles=True,
        technical_candle_sources=("internal_snapshot",),
        technical_candle_source_counts={"internal_snapshot": 80},
    )

    payload = run_analysis._layers_payload_for_decision(result)

    assert payload["technical_data_source_mode"] == "reconstructed"
    assert payload["technical_has_reconstructed_candles"] is True
    assert payload["technical_candle_sources"] == ["internal_snapshot"]
    assert payload["technical_candle_source_counts"] == {"internal_snapshot": 80}


def test_render_report_exposes_technical_source_quality():
    result = blend_scores(
        ticker="T",
        technical_signal="BUY",
        technical_strength=0.7,
        macro_score=0.1,
        risk_position=_risk_position(),
        sentiment_score=0.0,
        technical_candle_source_mode="mixed",
        technical_has_reconstructed_candles=True,
        technical_candle_sources=("COCOS", "internal_snapshot"),
        technical_candle_source_counts={"COCOS": 70, "internal_snapshot": 10},
    )

    output = run_analysis.render_report(
        results=[result],
        macro_snap=_macro_snapshot(),
        total_ars=100000.0,
        cash_ars=1000.0,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[{"ticker": "T", "market_value": 99000.0}],
        universe_results=[],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=None,
    )

    assert "Fuente técnica: <b>mixed (COCOS 70, internal_snapshot 10)</b>" in output


def test_render_opportunity_report_exposes_source_quality():
    candidate = OpportunityCandidate(
        ticker="T",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.WATCHLIST,
        final_score=0.15,
        conviction=0.6,
        technical_candle_source_mode="reconstructed",
        technical_has_reconstructed_candles=True,
        technical_candle_sources=("internal_snapshot",),
        technical_candle_source_counts={"internal_snapshot": 80},
        price_usd=100.0,
        action_concreta="Esperar confirmacion",
    )
    report = OpportunityReport(
        universe_size=1,
        screened_count=1,
        candidates=[candidate],
        en_vigilancia=[candidate],
    )

    output = render_opportunity_report(report)

    assert "Fuente técnica: <b>reconstructed (internal_snapshot 80)</b>" in output


def test_technical_signal_report_exposes_source_quality():
    signal = Signal(
        ticker="T",
        signal="BUY",
        strength=0.8,
        score_raw=4.0,
        reasons=["momentum"],
        price_usd=120.0,
        candle_source_mode="official",
        candle_sources=("COCOS",),
        candle_source_counts={"COCOS": 80},
    )

    output = signal.to_telegram()

    assert "Fuente técnica: <b>official (COCOS 80)</b>" in output
