from types import SimpleNamespace

from scripts import run_analysis
from src.analysis.enums import DecisionType
from src.analysis.execution_planner import DecisionIntent, reconcile_funding
from src.analysis.opportunity_screener import (
    CandidateStatus,
    OpportunityCandidate,
    OpportunityReport,
    TradeType,
    _apply_cash_constraint,
    render_opportunity_report,
)


def _decision(ticker: str, amount: float) -> DecisionIntent:
    return DecisionIntent(
        ticker=ticker,
        action=DecisionType.BUY,
        reason_primary="test",
        reason_secondary=None,
        current_weight=0.10,
        target_weight=0.20,
        delta_weight=0.10,
        score=0.20,
        conviction=0.80,
        theoretical_ars=amount,
    )


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


def _result(ticker: str, *, score: float = 0.20, decision: str = "BUY"):
    return SimpleNamespace(
        ticker=ticker,
        final_score=score,
        score=score,
        decision=decision,
        conviction=0.80,
        layers=[],
        technical_candle_source_mode="official",
        technical_candle_source_counts={},
    )


def test_render_report_labels_unfunded_buy_as_watch():
    plan = reconcile_funding(
        decisions=[_decision("AAPL", 80_000)],
        current_positions={},
        cash_before=0,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    output = run_analysis.render_report(
        results=[_result("AAPL")],
        macro_snap=_macro_snapshot(),
        total_ars=1_000_000.0,
        cash_ars=0.0,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[{"ticker": "AAPL", "market_value": 100_000.0}],
        universe_results=[],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=plan,
    )

    assert "<b>AAPL</b> → <b>WATCH</b>" in output
    assert "<b>AAPL</b> → <b>BUY</b>" not in output
    assert "Compras: <b>$0 ARS</b>" in output


def test_compact_radar_does_not_call_it_buy_without_funding():
    plan = reconcile_funding(
        decisions=[],
        current_positions={},
        cash_before=3_027,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    output = run_analysis.render_report(
        results=[],
        macro_snap=_macro_snapshot(),
        total_ars=1_000_000.0,
        cash_ars=3_027.0,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[],
        universe_results=[_result("MU")],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=plan,
    )

    assert "Candidatos fuertes sin funding" in output
    assert "Sin cash libre: solo vía swap o venta financiadora." in output
    assert "<b>Compras fuertes</b>" not in output


def test_new_entry_without_cash_degrades_to_vigilancia():
    candidate = OpportunityCandidate(
        ticker="MU",
        status=CandidateStatus.COMPRABLE_AHORA,
        trade_type=TradeType.NEW_ENTRY,
        final_score=0.20,
        conviction=0.80,
        action_concreta="Comprar",
    )

    adjusted = _apply_cash_constraint(candidate, available_cash_ars=3_027)

    assert adjusted.status == CandidateStatus.VIGILANCIA_A
    assert adjusted.action_concreta == "Esperar funding o evaluar swap"
    assert "sin cash ejecutable" in adjusted.why_not_now


def test_swap_candidate_remains_actionable_without_cash():
    candidate = OpportunityCandidate(
        ticker="YPF",
        status=CandidateStatus.SWAP_CANDIDATO,
        trade_type=TradeType.SWAP_CANDIDATE,
        final_score=0.20,
        conviction=0.80,
        action_concreta="Reducir MELI y rotar a YPF",
    )

    adjusted = _apply_cash_constraint(candidate, available_cash_ars=0)

    assert adjusted.status == CandidateStatus.SWAP_CANDIDATO
    assert adjusted.action_concreta == "Reducir MELI y rotar a YPF"


def test_opportunity_report_marks_subminimum_cash_as_not_executable():
    report = OpportunityReport(
        universe_size=1,
        screened_count=0,
        available_cash_ars=3_027,
    )

    output = render_opportunity_report(report)

    assert "Cash libre: <b>$3.027 ARS</b>" in output
    assert "Sin cash ejecutable: nuevas entradas solo via funding o swap." in output
