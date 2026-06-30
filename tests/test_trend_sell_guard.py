from src.analysis.enums import DecisionType
from src.analysis.execution_planner import AssetSignal, _sell_guard


def _signal(**overrides):
    values = {
        "ticker": "NVDA",
        "score": -0.03,
        "conviction": 0.7,
        "technical": -0.1,
        "macro": 0.1,
        "sentiment": 0.0,
        "technical_regime": "STRONG_UPTREND",
        "overbought_momentum": True,
    }
    values.update(overrides)
    return AssetSignal(**values)


def test_strong_uptrend_blocks_overbought_only_sell():
    action, _, reason = _sell_guard(
        score=-0.03,
        conv=0.7,
        w_cur=0.35,
        w_opt=0.20,
        delta=-0.15,
        signal=_signal(),
    )
    assert action == DecisionType.HOLD
    assert "no agregar, no vender" in reason


def test_strong_uptrend_allows_clearly_negative_or_structural_exit():
    negative_action, _, _ = _sell_guard(
        score=-0.081,
        conv=0.7,
        w_cur=0.35,
        w_opt=0.20,
        delta=-0.15,
        signal=_signal(score=-0.081),
    )
    structural_action, _, _ = _sell_guard(
        score=-0.03,
        conv=0.7,
        w_cur=0.35,
        w_opt=0.20,
        delta=-0.15,
        signal=_signal(structural_break_confirmed=True),
    )
    assert negative_action == DecisionType.SELL_PARTIAL
    assert structural_action == DecisionType.SELL_PARTIAL
