from datetime import date, datetime

from src.analysis.enums import DecisionType
from src.analysis.execution_planner import (
    DecisionIntent,
    PositionSnapshot,
    reconcile_funding,
)
from src.analysis.manual_market_events import (
    ART_TZ,
    BLOCK_NEW_BUYS,
    ManualMarketEvent,
    active_event_risk_by_ticker,
    default_active_window,
    render_manual_market_events_html,
)
from src.collector.live_portfolio import PortfolioMoveAlert, render_live_portfolio_alert
from src.collector.broker_movements import BrokerMovement
from src.scheduler.runner import _render_new_movements_notice
from scripts.run_analysis import _render_manual_event_position_exposure


def _mu_event() -> ManualMarketEvent:
    active_from, active_until = default_active_window(date(2026, 6, 24), "after_close")
    return ManualMarketEvent(
        id=1,
        event_date=date(2026, 6, 24),
        event_time_hint="after_close",
        ticker="MU",
        title="Micron earnings",
        impact_scope=("semiconductors", "AI", "memory"),
        related_tickers=("AMD", "NVDA", "TSM", "AVGO", "SMH"),
        severity="high",
        active_from=active_from,
        active_until=active_until,
        action_policy=BLOCK_NEW_BUYS,
        notes="Evento binario; evitar compras semis hasta guidance.",
    )


def test_manual_event_maps_block_new_buys_to_related_tickers():
    event = _mu_event()

    blocklist = active_event_risk_by_ticker([event])

    assert blocklist["MU"].startswith("EVENT_RISK")
    assert blocklist["AMD"].startswith("EVENT_RISK")
    assert blocklist["NVDA"].startswith("EVENT_RISK")
    assert "block_new_buys" in blocklist["TSM"]

    rendered = "\n".join(render_manual_market_events_html([event]))
    assert "Micron earnings" in rendered
    assert "bloquea compras nuevas" in rendered
    assert "AMD" in rendered


def test_reconcile_funding_blocks_core_buy_when_event_active():
    event = _mu_event()
    blocklist = active_event_risk_by_ticker([event])
    decisions = [
        DecisionIntent(
            ticker="AMD",
            action=DecisionType.BUY,
            reason_primary="Aumentar posición por score positivo",
            reason_secondary="score +0.120",
            current_weight=0.0,
            target_weight=0.05,
            delta_weight=0.05,
            score=0.12,
            conviction=0.7,
            theoretical_ars=80_000,
        )
    ]

    plan = reconcile_funding(
        decisions=decisions,
        current_positions={},
        cash_before=200_000,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        blocked_buy_tickers=blocklist,
    )

    assert plan.buy_orders == []
    assert len(plan.blocked_orders) == 1
    assert plan.blocked_orders[0].ticker == "AMD"
    assert plan.blocked_orders[0].action == DecisionType.BLOCKED
    assert "EVENT_RISK" in plan.blocked_orders[0].reason


def test_reconcile_funding_blocks_external_radar_buy_but_keeps_sell():
    event = _mu_event()
    blocklist = active_event_risk_by_ticker([event])
    decisions = [
        DecisionIntent(
            ticker="NVDA",
            action=DecisionType.SELL_PARTIAL,
            reason_primary="Reducir exposición por score negativo",
            reason_secondary="score -0.090",
            current_weight=0.30,
            target_weight=0.25,
            delta_weight=-0.05,
            score=-0.09,
            conviction=0.6,
            theoretical_ars=50_000,
        )
    ]
    positions = {
        "NVDA": PositionSnapshot(
            ticker="NVDA",
            quantity=10,
            price=13_000,
            market_value_ars=130_000,
            current_weight=0.30,
        )
    }

    plan = reconcile_funding(
        decisions=decisions,
        current_positions=positions,
        cash_before=200_000,
        portfolio_value_ars=1_000_000,
        gate="NORMAL",
        external_buys=[
            {
                "ticker": "AMD",
                "amount_ars": 80_000,
                "score": 0.14,
                "reason": "Radar externo fuerte",
                "reference_price": 80_000,
            }
        ],
        blocked_buy_tickers=blocklist,
    )

    assert [order.ticker for order in plan.sell_orders] == ["NVDA"]
    assert plan.buy_orders == []
    assert any(order.ticker == "AMD" and order.action == DecisionType.BLOCKED for order in plan.blocked_orders)


def test_analysis_report_flags_existing_position_exposed_to_manual_event():
    event = _mu_event()

    lines = _render_manual_event_position_exposure(
        [event],
        [{"ticker": "MU", "market_value": 770_000}],
        1_000_000,
    )
    rendered = "\n".join(lines)

    assert "Exposición actual bajo catalyst" in rendered
    assert "MU" in rendered
    assert "Micron earnings" in rendered
    assert "concentración alta" in rendered


def test_live_portfolio_alert_mentions_manual_event_risk():
    event = _mu_event()
    risk_by_ticker = active_event_risk_by_ticker([event])

    rendered = render_live_portfolio_alert(
        [
            PortfolioMoveAlert(
                ticker="MU",
                level="MAJOR",
                direction="DOWN",
                change_pct_1d=-0.0412,
                weight_live=0.77,
                market_value=944_325,
            )
        ],
        {
            "total_value_ars": 1_218_000,
            "invested_ars": 1_218_000,
            "cash_ars": 0,
            "manual_event_risk_by_ticker": risk_by_ticker,
        },
    )

    assert "EVENT_RISK activo" in rendered
    assert "Micron earnings" in rendered


def test_new_buy_movement_notice_marks_manual_event_risk():
    event = _mu_event()
    risk_by_ticker = active_event_risk_by_ticker([event])

    rendered = _render_new_movements_notice(
        [
            BrokerMovement(
                external_movement_id="m1",
                executed_at=datetime(2026, 6, 24, 12, 31, tzinfo=ART_TZ),
                movement_type="BUY",
                currency="ARS",
                amount=942_975,
                quantity=3,
                price=314_325,
                ticker="MU",
                executed_at_precision="exact",
            )
        ],
        portfolio_refreshed=True,
        manual_event_risk_by_ticker=risk_by_ticker,
    )

    assert "BUY contra EVENT_RISK activo" in rendered
    assert "Micron earnings" in rendered
