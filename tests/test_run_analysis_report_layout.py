from types import SimpleNamespace

from scripts import run_analysis
from src.analysis.enums import DecisionType
from src.analysis.execution_planner import DecisionIntent, reconcile_funding


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


def _result(ticker: str, score: float):
    return SimpleNamespace(
        ticker=ticker,
        final_score=score,
        score=score,
        layers=[],
        technical_candle_source_mode="official",
        technical_candle_source_counts={"COCOS": 260},
    )


def _decision(ticker: str, score: float, amount: float) -> DecisionIntent:
    return DecisionIntent(
        ticker=ticker,
        action=DecisionType.BUY,
        reason_primary="Comprar",
        reason_secondary="setup de compra",
        current_weight=0.10,
        target_weight=0.20,
        delta_weight=0.10,
        score=score,
        conviction=0.80,
        theoretical_ars=amount,
    )


def test_report_leads_with_clear_no_buy_recommendation_when_unfunded():
    plan = reconcile_funding(
        decisions=[_decision("NVDA", 0.12, 80_000)],
        current_positions={},
        cash_before=3_000,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
    )

    output = run_analysis.render_report(
        results=[_result("NVDA", 0.12)],
        macro_snap=_macro_snapshot(),
        total_ars=1_000_000.0,
        cash_ars=3_000.0,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[{"ticker": "NVDA", "market_value": 100_000.0}],
        universe_results=[],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=plan,
    )

    assert "<b>DECISIÓN DE HOY</b>" in output
    assert "<b>MANTENER / NO COMPRAR HOY</b>" in output
    assert "Mejor señal interna: <b>NVDA</b>" in output
    assert "Próximo paso: esperar funding o evaluar swap financiado." in output
    assert "<b>VEREDICTO FINAL</b>" not in output


def test_report_expands_main_action_with_recommendation_context():
    plan = reconcile_funding(
        decisions=[_decision("NVDA", 0.12, 80_000)],
        current_positions={},
        cash_before=100_000,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
    )

    output = run_analysis.render_report(
        results=[_result("NVDA", 0.12)],
        macro_snap=_macro_snapshot(),
        total_ars=1_000_000.0,
        cash_ars=100_000.0,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[{"ticker": "NVDA", "market_value": 100_000.0}],
        universe_results=[],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=plan,
    )

    assert "<b>COMPRAR NVDA" in output
    assert "Recomendación: aumentar exposición" in output
    assert "Score: <code>+0.120</code>" in output
    assert "Motivo:" in output


def test_report_summarizes_official_history_once_for_holdings():
    output = run_analysis.render_report(
        results=[_result("NVDA", 0.12), _result("AMD", 0.05)],
        macro_snap=_macro_snapshot(),
        total_ars=1_000_000.0,
        cash_ars=50_000.0,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[
            {"ticker": "NVDA", "market_value": 500_000.0},
            {"ticker": "AMD", "market_value": 450_000.0},
        ],
        universe_results=[],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=None,
    )

    assert "Historia técnica: <b>official 2</b>" in output
    assert output.count("Fuente técnica:") == 0
