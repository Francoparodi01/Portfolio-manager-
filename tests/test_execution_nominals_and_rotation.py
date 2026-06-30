import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.analysis.enums import DecisionType
from src.analysis.execution_planner import (
    DecisionIntent,
    OrderIntent,
    OrderSide,
    PositionSnapshot,
    reconcile_funding,
)
from src.analysis.opportunity_screener import (
    CandidateStatus,
    OpportunityCandidate,
    OpportunityReport,
    TradeType,
)
from src.analysis.validators import validate_execution_plan
from src.collector.db import PortfolioDatabase
from scripts import run_analysis
from scripts.run_analysis import _radar_buys_for_execution


def _sell_decision() -> DecisionIntent:
    return DecisionIntent(
        ticker="TSM",
        action=DecisionType.SELL_PARTIAL,
        reason_primary="Reducir exposicion: 33.3% -> 25.0%",
        reason_secondary="rebalanceo por concentracion",
        current_weight=0.333,
        target_weight=0.25,
        delta_weight=-0.083,
        score=-0.10,
        conviction=0.60,
        theoretical_ars=140_489,
    )


def test_sell_and_radar_buy_are_chained_in_whole_nominals():
    sell = _sell_decision()
    plan = reconcile_funding(
        decisions=[sell],
        current_positions={
            "TSM": PositionSnapshot(
                ticker="TSM",
                quantity=7,
                price=80_450,
                market_value_ars=563_150,
                current_weight=0.333,
            )
        },
        cash_before=46_084,
        portfolio_value_ars=1_690_600,
        gate="NORMAL",
        min_trade_ars=25_000,
        external_buys=[
            {
                "ticker": "SNOW",
                "amount_ars": 100_000,
                "score": 0.20,
                "reference_price": 25_000,
                "reason": "Radar elegible",
            }
        ],
    )

    assert len(plan.sell_orders) == 1
    assert plan.sell_orders[0].quantity_est == 2
    assert plan.sell_orders[0].amount_ars == 160_900
    assert plan.sell_orders[0].amount_ars == (
        plan.sell_orders[0].quantity_est * plan.sell_orders[0].reference_price
    )
    assert len(plan.buy_orders) == 1
    assert plan.buy_orders[0].ticker == "SNOW"
    assert plan.buy_orders[0].quantity_est == 4
    assert plan.buy_orders[0].amount_ars == 100_000
    assert plan.buy_orders[0].funded_by == ["TSM"]
    assert plan.cash_after >= 0
    validate_execution_plan(plan)


def test_radar_funding_candidate_survives_cash_only_downgrade():
    candidate = OpportunityCandidate(
        ticker="SNOW",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.NEW_ENTRY,
        final_score=0.20,
        conviction=0.70,
        sizing_suggested=0.05,
        price_usd=25_000,
        cash_funding_required=True,
        action_concreta="Esperar funding o evaluar swap",
    )
    report = OpportunityReport(candidates=[candidate], available_cash_ars=0)

    buys = _radar_buys_for_execution(report, 2_000_000)

    assert buys == [
        {
            "ticker": "SNOW",
            "amount_ars": 100_000,
            "score": 0.20,
            "reference_price": 25_000.0,
            "reason": "Radar elegible: Esperar funding o evaluar swap",
        }
    ]


def test_non_funding_watch_candidate_is_not_promoted_to_buy():
    candidate = OpportunityCandidate(
        ticker="MU",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.NEW_ENTRY,
        final_score=0.20,
        conviction=0.70,
        sizing_suggested=0.05,
        price_usd=25_000,
        cash_funding_required=False,
    )
    report = OpportunityReport(candidates=[candidate], available_cash_ars=0)

    assert _radar_buys_for_execution(report, 2_000_000) == []


def test_analysis_radar_uses_cash_remaining_after_core_plan():
    buy = DecisionIntent(
        ticker="NVDA",
        action=DecisionType.BUY,
        reason_primary="Aumentar posicion",
        reason_secondary="score positivo",
        current_weight=0.279,
        target_weight=0.35,
        delta_weight=0.071,
        score=0.09,
        conviction=0.80,
        theoretical_ars=120_202,
    )
    plan = reconcile_funding(
        decisions=[buy],
        current_positions={
            "NVDA": PositionSnapshot(
                ticker="NVDA",
                quantity=35,
                price=13_470,
                market_value_ars=471_450,
                current_weight=0.279,
            )
        },
        cash_before=46_084,
        portfolio_value_ars=1_690_600,
        gate="NORMAL",
    )
    candidate = OpportunityCandidate(
        ticker="UPST",
        status=CandidateStatus.COMPRABLE_AHORA,
        trade_type=TradeType.NEW_ENTRY,
        final_score=0.30,
        conviction=0.70,
        sizing_suggested=0.10,
        price_usd=10_000,
    )
    opportunity_report = OpportunityReport(
        candidates=[candidate],
        comprable_ahora=[candidate],
        available_cash_ars=46_084,
    )
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

    output = run_analysis.render_report(
        results=[],
        macro_snap=macro,
        total_ars=1_690_600,
        cash_ars=46_084,
        portfolio_risk=SimpleNamespace(positions=[]),
        rebalance_report=None,
        positions=[{"ticker": "NVDA", "market_value": 471_450}],
        universe_results=[],
        external_universe_tickers=[],
        ic_metrics={},
        execution_plan=plan,
        opportunity_report=opportunity_report,
    )

    assert plan.cash_after < 25_000
    assert "Sin cash ejecutable: compras nuevas solo via funding o swap." in output
    assert "ejecutable $46.084 ARS" not in output


def test_external_radar_order_persists_reference_price(monkeypatch):
    class _Connection:
        def __init__(self):
            self.insert_args = None

        async def fetchval(self, _statement, *_args):
            return None

        async def fetchrow(self, statement, *args):
            assert "INSERT INTO decision_log" in statement
            self.insert_args = args
            return {"id": 463}

        async def close(self):
            return None

    conn = _Connection()

    async def _connect(_url):
        return conn

    async def _ensure_scope(_conn):
        return None

    import asyncpg

    monkeypatch.setattr(asyncpg, "connect", _connect)
    monkeypatch.setattr(run_analysis, "ensure_decision_audit_scope_columns", _ensure_scope)

    order = OrderIntent(
        ticker="UPST",
        side=OrderSide.BUY,
        action=DecisionType.BUY,
        amount_ars=40_160,
        theoretical_ars=111_342,
        quantity_est=4,
        reference_price=10_040,
        reason="Radar elegible",
        priority=3,
    )
    plan = SimpleNamespace(
        decisions=[],
        sell_orders=[],
        buy_orders=[order],
        blocked_orders=[],
        pending_buys=[],
        gate="NORMAL",
    )
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))

    saved = asyncio.run(
        run_analysis._save_execution_plan_events(
            cfg=cfg,
            execution_plan=plan,
            results=[],
            macro_snap=SimpleNamespace(vix=17.0),
            macro_regime={"market": "neutral"},
            total_ars=1_694_700,
            positions=[],
            owner_chat_id=1,
        )
    )

    assert saved == [463]
    assert conn.insert_args is not None
    assert conn.insert_args[7] == 10_040


def test_decision_price_warning_excludes_blocked_rows(monkeypatch):
    from src.scheduler import runner

    sent = []

    class _Connection:
        async def fetchrow(self, statement):
            assert "decision_type = 'executable'" in statement
            assert "status != 'BLOCKED'" in statement
            # Three executable rows: two priced and one missing. Two additional
            # BLOCKED rows without price must not enter this health check.
            return {"total": 3, "missing_price": 1}

    class _Acquire:
        async def __aenter__(self):
            return _Connection()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    class _Database:
        def __init__(self, _url):
            pass

        async def connect(self):
            return None

        async def get_pool(self):
            return _Pool()

        async def close(self):
            return None

    class _Notifier:
        def __init__(self, *_args):
            pass

        def send_raw(self, message):
            sent.append(message)

    cfg = SimpleNamespace(
        database=SimpleNamespace(url="postgresql://unused"),
        scraper=SimpleNamespace(telegram_bot_token="token", telegram_chat_id="chat"),
    )
    monkeypatch.setattr(runner, "_is_business_day", lambda: True)
    monkeypatch.setattr(runner, "get_config", lambda: cfg)
    monkeypatch.setattr(runner, "PortfolioDatabase", _Database)
    monkeypatch.setattr(runner, "TelegramNotifier", _Notifier)

    asyncio.run(runner.run_verify_decision_prices())

    assert sent == [
        "ADVERTENCIA: decision_price_status: 1/3 decisiones de hoy sin price_at_decision"
    ]


def test_fill_reconciliation_backfills_missing_decision_price():
    class _Connection:
        def __init__(self):
            self.execute_calls = []

        async def fetch(self, statement):
            if "FROM broker_fills" in statement:
                return [
                    {
                        "id": 1,
                        "source": "cocos_movements",
                        "external_fill_id": "fill-upst",
                        "executed_at": datetime(2026, 6, 23, 14, 35, tzinfo=timezone.utc),
                        "executed_at_precision": "timestamp",
                        "executed_at_source": "broker",
                        "ticker": "UPST",
                        "side": "BUY",
                        "quantity": 4,
                        "avg_fill_price": 10_120,
                        "gross_amount_ars": 40_480,
                        "fees_ars": 100,
                        "raw_payload": {},
                    }
                ]
            if "FROM decision_log" in statement:
                return [
                    {
                        "id": 463,
                        "ticker": "UPST",
                        "decision": "BUY",
                        "decided_at": datetime(2026, 6, 22, 20, 12, tzinfo=timezone.utc),
                        "status": "APPROVED",
                        "theoretical_amount_ars": 111_342,
                    }
                ]
            return []

        async def execute(self, statement, *args):
            self.execute_calls.append((statement, args))

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    conn = _Connection()
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)
    db._execution_timestamp_meta_ready = True

    reconciled = asyncio.run(db.reconcile_broker_fills())

    assert reconciled == 1
    decision_update = next(
        (call for call in conn.execute_calls if "UPDATE decision_log" in call[0]),
        None,
    )
    assert decision_update is not None
    statement, args = decision_update
    assert "price_at_decision = COALESCE(price_at_decision, $4)" in statement
    assert args[3] == 10_120.0
