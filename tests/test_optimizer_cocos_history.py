from __future__ import annotations

import pandas as pd

from src.analysis import optimizer


def _frame(prices: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"Close": prices})


def test_fetch_returns_prefers_cocos_frames(monkeypatch):
    def fail_fetch_history(*_args, **_kwargs):
        raise AssertionError("legacy fetch should not run when Cocos frame exists")

    monkeypatch.setattr("src.analysis.technical.fetch_history", fail_fetch_history)

    returns = optimizer._fetch_returns(
        ["NVDA", "AMD"],
        history_frames={
            "NVDA": _frame([100 + i for i in range(70)]),
            "AMD": _frame([50 + i for i in range(70)]),
        },
    )

    assert list(returns.columns) == ["NVDA", "AMD"]
    assert len(returns) == 69


def test_run_optimizer_passes_history_frames_to_return_loader(monkeypatch):
    captured = {}

    def fake_fetch_returns(tickers, history_frames=None):
        captured["tickers"] = tickers
        captured["history_frames"] = history_frames
        return pd.DataFrame(
            {
                "NVDA": [0.01, 0.02, -0.01],
                "AMD": [0.03, -0.01, 0.01],
            }
        )

    monkeypatch.setattr(optimizer, "_fetch_returns", fake_fetch_returns)
    monkeypatch.setattr(optimizer, "_select_method", lambda *_args: ("MAX_SHARPE", "test"))
    monkeypatch.setattr(optimizer, "_optimize_max_sharpe_np", lambda *_args: [0.5, 0.5])

    report = optimizer.run_optimizer(
        current_positions=[
            {"ticker": "NVDA", "market_value": 50},
            {"ticker": "AMD", "market_value": 50},
        ],
        portfolio_value_ars=100,
        cash_ars=0,
        macro_regime={"market": "neutral"},
        vix=20,
        synthesis_results=[
            type("R", (), {"ticker": "NVDA", "final_score": 0.1, "confidence": 0.8})(),
            type("R", (), {"ticker": "AMD", "final_score": 0.2, "confidence": 0.8})(),
        ],
        market_assets=[],
        history_frames={"NVDA": _frame([1, 2]), "AMD": _frame([1, 2])},
    )

    assert report is not None
    assert captured["tickers"] == ["AMD", "NVDA"]
    assert set(captured["history_frames"]) == {"NVDA", "AMD"}
