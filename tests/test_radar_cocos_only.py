from types import SimpleNamespace

from scripts import run_analysis
from src.analysis.opportunity_screener import (
    CandidateStatus,
    run_opportunity_analysis,
)


def test_run_opportunity_marks_missing_cocos_history_as_externo(monkeypatch):
    import pandas as pd

    macro = SimpleNamespace(vix=None)
    frame = pd.DataFrame(
        {
            "Close": [100 + i for i in range(260)],
            "High": [101 + i for i in range(260)],
            "Low": [99 + i for i in range(260)],
            "Volume": [1_000_000 for _ in range(260)],
        }
    )

    def fail_legacy_fetch(*_args, **_kwargs):
        raise AssertionError("legacy fetch should not run in strict Cocos mode")

    monkeypatch.setattr("src.analysis.technical.fetch_history", fail_legacy_fetch)
    monkeypatch.setattr("src.analysis.technical.analyze_portfolio", fail_legacy_fetch)

    report = run_opportunity_analysis(
        universe=["AAPL"],
        portfolio_positions=[],
        macro_snap=macro,
        macro_regime={},
        history_frames={"SPY": frame, "QQQ": frame},
    )

    assert [candidate.ticker for candidate in report.externos] == ["AAPL"]
    assert report.externos[0].status == CandidateStatus.EXTERNO
    assert report.externos[0].why_not_now == "sin velas Cocos suficientes"


def test_render_report_mentions_external_universe_count():
    macro = SimpleNamespace(
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
    portfolio_risk = SimpleNamespace(positions=[])

    output = run_analysis.render_report(
        results=[],
        macro_snap=macro,
        total_ars=0.0,
        cash_ars=0.0,
        portfolio_risk=portfolio_risk,
        rebalance_report=None,
        positions=[],
        universe_results=[],
        external_universe_tickers=["AAPL", "MSFT"],
        ic_metrics={},
        execution_plan=None,
    )

    assert "2 tickers EXTERNO" in output
    assert "sin velas Cocos suficientes" in output
