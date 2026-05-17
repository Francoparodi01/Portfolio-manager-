from src.analysis.enums import DecisionType
from src.analysis.execution_planner import (
    DecisionIntent,
    PositionSnapshot,
    reconcile_funding,
)


def _decision(
    ticker: str,
    action: DecisionType,
    theoretical_ars: float,
) -> DecisionIntent:
    return DecisionIntent(
        ticker=ticker,
        action=action,
        reason_primary="test",
        reason_secondary=None,
        current_weight=0.0,
        target_weight=0.0,
        delta_weight=0.0,
        score=0.2,
        conviction=0.8,
        theoretical_ars=theoretical_ars,
    )


def test_cash_never_negative():
    plan = reconcile_funding(
        decisions=[
            _decision("AAPL", DecisionType.BUY, 80_000),
            _decision("MSFT", DecisionType.BUY, 80_000),
            _decision("NVDA", DecisionType.BUY, 80_000),
        ],
        current_positions={},
        cash_before=100_000,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    assert plan.cash_after >= 0
    assert sum(order.amount_ars for order in plan.buy_orders) <= 100_000


def test_sell_proceeds_fund_buys():
    plan = reconcile_funding(
        decisions=[
            _decision("CVX", DecisionType.SELL_PARTIAL, 50_000),
            _decision("MU", DecisionType.BUY, 100_000),
        ],
        current_positions={
            "CVX": PositionSnapshot(
                ticker="CVX",
                quantity=10,
                price=5_000,
                market_value_ars=50_000,
                current_weight=0.05,
            )
        },
        cash_before=0,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    assert plan.net_sell_ars > 0
    assert plan.gross_buy_ars <= plan.net_sell_ars
    assert plan.cash_after >= 0


def test_unfunded_buy_is_watch_not_buy():
    plan = reconcile_funding(
        decisions=[
            _decision("AAPL", DecisionType.BUY, 80_000),
        ],
        current_positions={},
        cash_before=0,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    assert plan.buy_orders == []
    assert plan.pending_buys == ["AAPL"]
    assert plan.decisions[0].action == DecisionType.WATCH
    assert "sin cash disponible" in (plan.decisions[0].reason_secondary or "").lower()
    assert "sin cash disponible" in plan.blocked_orders[0].reason.lower()
    assert "mantener o evaluar swaps" in plan.verdict().lower()
