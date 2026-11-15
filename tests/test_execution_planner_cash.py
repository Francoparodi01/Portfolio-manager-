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
    *,
    current_weight: float = 0.0,
    target_weight: float = 0.0,
    score: float = 0.2,
) -> DecisionIntent:
    return DecisionIntent(
        ticker=ticker,
        action=action,
        reason_primary="test",
        reason_secondary=None,
        current_weight=current_weight,
        target_weight=target_weight,
        delta_weight=target_weight - current_weight,
        score=score,
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


def test_orphan_sell_is_trimmed_to_independent_concentration_reduction():
    sell = _decision(
        "AMD",
        DecisionType.SELL_PARTIAL,
        244_000,
        current_weight=0.294,
        target_weight=0.05,
        score=0.033,
    )
    blocked_buy = _decision(
        "HMY",
        DecisionType.BLOCKED,
        200_000,
        current_weight=0.10,
        target_weight=0.25,
        score=-0.139,
    )

    plan = reconcile_funding(
        decisions=[sell, blocked_buy],
        current_positions={
            "AMD": PositionSnapshot(
                ticker="AMD",
                quantity=6,
                price=50_000,
                market_value_ars=294_000,
                current_weight=0.294,
            )
        },
        cash_before=0,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    assert len(plan.sell_orders) == 1
    assert plan.sell_orders[0].ticker == "AMD"
    assert plan.sell_orders[0].amount_ars == 50_000
    assert sell.theoretical_ars == 44_000
    assert sell.target_weight == 0.244
    assert sell.action == DecisionType.SELL_PARTIAL
    assert "financiación bloqueada" in (sell.reason_secondary or "")
    assert sell.sell_cause == "concentration+funding"
    assert sell.funding_for == {"HMY": 200_000}
    assert blocked_buy.funded_by == {"AMD": 200_000}


def test_pure_funding_sell_is_cancelled_when_destination_is_blocked():
    sell = _decision(
        "AMD",
        DecisionType.SELL_PARTIAL,
        150_000,
        current_weight=0.20,
        target_weight=0.05,
        score=0.02,
    )

    plan = reconcile_funding(
        decisions=[sell],
        current_positions={},
        cash_before=0,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    assert plan.sell_orders == []
    assert sell.action == DecisionType.WATCH
    assert sell.target_weight == sell.current_weight
    assert "venta condicional cancelada" in sell.reason_primary.lower()


def test_conditional_sell_survives_only_to_fund_executable_core_buy():
    sell = _decision(
        "AMD",
        DecisionType.SELL_PARTIAL,
        244_000,
        current_weight=0.294,
        target_weight=0.05,
        score=0.033,
    )
    buy = _decision(
        "HMY",
        DecisionType.BUY,
        100_000,
        current_weight=0.10,
        target_weight=0.20,
        score=0.12,
    )

    plan = reconcile_funding(
        decisions=[sell, buy],
        current_positions={
            "AMD": PositionSnapshot(
                ticker="AMD",
                quantity=6,
                price=50_000,
                market_value_ars=294_000,
                current_weight=0.294,
            ),
            "HMY": PositionSnapshot(
                ticker="HMY",
                quantity=2,
                price=50_000,
                market_value_ars=100_000,
                current_weight=0.10,
            ),
        },
        cash_before=0,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        min_trade_ars=1,
    )

    assert len(plan.sell_orders) == 1
    assert 100_000 < plan.sell_orders[0].amount_ars < 244_000
    assert len(plan.buy_orders) == 1
    assert plan.buy_orders[0].amount_ars == 100_000
    assert plan.buy_orders[0].funded_by == ["AMD"]
    assert sell.funding_for == {"HMY": 100_000}
    assert buy.funded_by == {"AMD": 100_000}
    assert plan.cash_after >= 0
