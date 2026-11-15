import unittest
from datetime import datetime,timedelta
from src.backtesting.ledger import Ledger,D

T=datetime.fromisoformat('2026-08-13T11:00:00-03:00')
def book(points):
    rows={'intraday_points':[dict(ticker='AMD',asset_type='CEDEAR',currency='ARS',ts=(T+timedelta(seconds=s)).isoformat(),last_price=p) for s,p in points],
          'daily_candles':[dict(ticker='AMD',asset_type='CEDEAR',currency='ARS',scraped_at='2026-08-12T22:00:00+00:00',ts='2026-08-12T13:30:00+00:00',source='TRADINGVIEW_BYMA',close_price=100)]}
    return Ledger(rows,['AMD'])
def plan(side,q): return dict(feasible=True,buy_orders=[dict(ticker='AMD',quantity_est=q,reason='test')] if side=='BUY' else [],sell_orders=[dict(ticker='AMD',quantity_est=q,reason='test')] if side=='SELL' else [])

class TestLedger(unittest.TestCase):
    def test_latency_and_no_future_marks(self):
        b=book([(-1,100),(30,999),(61,105),(120,110)])
        self.assertEqual(b.mark('AMD',T)[0],D(100))
        f=b.execute(plan('BUY',10),T,T+timedelta(hours=2))
        self.assertEqual(f[0]['quote_price'],105)
        self.assertEqual(f[0]['filled_at'],(T+timedelta(seconds=61)).isoformat())
        self.assertAlmostEqual(float(b.cash),3000000-10*105*1.0015*1.006)
        b.valuation(T+timedelta(minutes=5))

    def test_expires_without_quote_and_at_next_cut(self):
        b=book([(7200,100)])
        self.assertEqual(b.execute(plan('BUY',10),T,T+timedelta(hours=2)),[])
        self.assertEqual(b.cash,D(3000000))

    def test_no_negative_cash_or_short(self):
        b=book([(61,100),(121,100)])
        b.execute(plan('SELL',10),T,T+timedelta(seconds=100))
        self.assertEqual(b.qty['AMD'],0)
        b.execute(plan('BUY',100000),T,T+timedelta(seconds=100))
        self.assertGreaterEqual(b.cash,0)
        self.assertLess(b.qty['AMD'],30000)
        b.execute(plan('SELL',100000),T+timedelta(seconds=60),T+timedelta(seconds=180))
        self.assertEqual(b.qty['AMD'],0)
        self.assertEqual(b.basis['AMD'],0)
        self.assertLess(b.realized['AMD'],0)
        b.valuation(T+timedelta(minutes=3))

    def test_infeasible_plan_is_not_executed(self):
        b=book([(61,100)]); p=plan('BUY',10);p['feasible']=False
        self.assertEqual(b.execute(p,T,T+timedelta(hours=2)),[])

    def test_later_sale_does_not_finance_an_earlier_buy(self):
        rows={'intraday_points':[
            dict(ticker='AMD',asset_type='CEDEAR',currency='ARS',ts=(T+timedelta(seconds=121)).isoformat(),last_price=100),
            dict(ticker='BBB',asset_type='CEDEAR',currency='ARS',ts=(T+timedelta(seconds=61)).isoformat(),last_price=100)],'daily_candles':[]}
        b=Ledger(rows,['AMD','BBB']);b.cash=D(0);b.qty['AMD']=10;b.basis['AMD']=D(1000)
        p=dict(feasible=True,sell_orders=[dict(ticker='AMD',quantity_est=2,reason='test')],buy_orders=[dict(ticker='BBB',quantity_est=1,reason='test')])
        f=b.execute(p,T,T+timedelta(hours=2))
        self.assertEqual([x['side'] for x in f],['SELL'])
        self.assertEqual(b.qty['BBB'],0)
        self.assertIn('NO_CASH_OR_POSITION_AT_FILL',[o['status'] for o in b.orders])

    def test_local_equity_keeps_its_identity(self):
        rows={'intraday_points':[dict(ticker='TEST',asset_type='ACCION',currency='ARS',ts=(T-timedelta(seconds=1)).isoformat(),last_price=100)],'daily_candles':[]}
        b=Ledger(rows,['TEST']);b.qty['TEST']=2;b.basis['TEST']=D(200)
        positions,nav,cash=b.snapshot(T)
        self.assertEqual(positions[0]['asset_type'],'ACCION')
        self.assertEqual(positions[0]['currency'],'ARS')

    def test_ambiguous_identity_cannot_fill(self):
        rows={'intraday_points':[dict(ticker='TEST',asset_type=kind,currency='ARS',ts=(T+timedelta(seconds=61)).isoformat(),last_price=100) for kind in ['ACCION','CEDEAR']],'daily_candles':[]}
        b=Ledger(rows,['TEST']);p=dict(feasible=True,buy_orders=[dict(ticker='TEST',quantity_est=1,reason='test')],sell_orders=[])
        self.assertEqual(b.execute(p,T,T+timedelta(hours=2)),[])
        self.assertEqual(b.orders[0]['status'],'AMBIGUOUS_INSTRUMENT_IDENTITY')

    def test_closed_position_does_not_require_a_future_mark(self):
        b=book([(61,100),(121,110)])
        b.execute(plan('BUY',10),T,T+timedelta(seconds=100))
        b.execute(plan('SELL',10),T+timedelta(seconds=60),T+timedelta(minutes=3))
        v=b.valuation(T+timedelta(days=10))
        self.assertEqual(v['assets'][0]['mark']['basis'],'CLOSED_POSITION_NO_MARK_REQUIRED')
        self.assertIsNone(v['assets'][0]['price'])

class TestCashStart(unittest.TestCase):
    def setUp(self):
        import asyncio
        from types import SimpleNamespace
        from scripts import run_analysis as native
        from src.backtesting.bridge import install_cash_bridge
        from src.analysis.opportunity_screener import CandidateStatus, TradeType

        self.asyncio = asyncio
        self.native = native
        self.original_main = native.main
        self.candidate = SimpleNamespace(
            ticker='AMD', trade_type=TradeType.NEW_ENTRY,
            status=CandidateStatus.COMPRABLE_AHORA, sizing_suggested=0.10,
            price_usd=10000.0, final_score=0.20, data_quality={},
            action_concreta='Entrada elegible', cash_funding_required=False)
        self.candidates = [self.candidate]
        self.vix = 18.0
        self.event_blocks = {}
        self.corporate_blocks = {}
        self.positions = []
        self.cash = 3000000.0
        self.existing_plan = None
        self.run_radar = True
        self.engine = SimpleNamespace(
            run_opportunity_analysis=lambda **kw: SimpleNamespace(candidates=self.candidates),
            active_event_risk_by_ticker=lambda events: dict(self.event_blocks),
            _radar_buys_for_execution=native._radar_buys_for_execution,
            reconcile_funding=native.reconcile_funding,
            validate_execution_plan=native.validate_execution_plan,
            _ensure_corporate_action_blocks_in_plan=native._ensure_corporate_action_blocks_in_plan)

        async def main(**kwargs):
            self.engine.active_event_risk_by_ticker([])
            if self.run_radar:
                self.engine.run_opportunity_analysis(
                    macro_snap=SimpleNamespace(vix=self.vix), macro_regime={'market':'neutral'})
            result = self.engine._ensure_corporate_action_blocks_in_plan(
                self.existing_plan, blocked_by_ticker=self.corporate_blocks,
                positions=self.positions, cash_ars=self.cash)
            return {'execution_plan':result}

        self.engine.main = main
        install_cash_bridge(self.engine)

    def run_cut(self, **overrides):
        args=dict(no_persist=True, no_telegram=True, run_intent='exploratory')
        args.update(overrides)
        return self.asyncio.run(self.engine.main(**args))

    def test_cash_only_plan_fills_with_real_native_planner_and_ledger(self):
        from dataclasses import asdict
        result=self.run_cut()
        p=result['execution_plan']
        self.assertTrue(result['backtest_cash_start']['attempted'])
        self.assertEqual(len(p.buy_orders),1)
        self.native.validate_execution_plan(p)
        b=book([(61,10000)])
        fills=b.execute(asdict(p),T,T+timedelta(hours=2))
        self.assertEqual(len(fills),1)
        self.assertGreater(b.qty['AMD'],0)
        self.assertGreaterEqual(b.cash,0)
        self.assertLess(b.cash,D(3000000))
        self.assertIs(self.native.main,self.original_main)

    def test_no_candidates_or_insufficient_score_does_not_force_buy(self):
        self.candidate.final_score=0.01
        self.assertEqual(self.run_cut()['execution_plan'].buy_orders,[])
        self.candidates=[]
        self.assertEqual(self.run_cut()['execution_plan'].buy_orders,[])

    def test_non_actionable_candidate_is_not_promoted(self):
        self.candidate.status='VIGILANCIA'
        self.assertEqual(self.run_cut()['execution_plan'].buy_orders,[])

    def test_risk_gate_blocks_cash_entry(self):
        self.vix=50
        p=self.run_cut()['execution_plan']
        self.assertEqual(p.gate,'BLOCKED')
        self.assertEqual(p.buy_orders,[])

    def test_manual_and_corporate_blocks_are_retained(self):
        self.event_blocks={'AMD':'Evento activo'}
        p=self.run_cut()['execution_plan']
        self.assertEqual(p.buy_orders,[])
        self.assertTrue(p.blocked_orders)
        self.event_blocks={}
        self.corporate_blocks={'AMD':'Precio no comparable'}
        p=self.run_cut()['execution_plan']
        self.assertEqual(p.buy_orders,[])
        self.assertIn('BLOCKED_CORPORATE_ACTION',[x.block_code for x in p.blocked_orders])

    def test_holdings_or_existing_plan_remain_native(self):
        p=self.run_cut()['execution_plan']
        self.existing_plan=p
        self.assertIs(self.run_cut()['execution_plan'],p)
        self.existing_plan=None
        self.positions=[{'ticker':'NVDA','quantity':1}]
        self.assertIsNone(self.run_cut()['execution_plan'])

    def test_cash_and_disabled_analysis_do_not_create_orders(self):
        self.cash=0
        self.assertIsNone(self.run_cut()['execution_plan'])
        self.cash=3000000
        self.assertIsNone(self.run_cut(no_optimizer=True)['execution_plan'])
        self.assertIsNone(self.run_cut(skip_radar=True)['execution_plan'])

    def test_no_stale_candidates_between_cuts(self):
        self.assertTrue(self.run_cut()['execution_plan'].buy_orders)
        self.run_radar=False
        self.assertIsNone(self.run_cut()['execution_plan'])

    def test_operational_invocation_is_rejected(self):
        for override in [dict(no_persist=False),dict(no_telegram=False),dict(run_intent='formal_plan')]:
            with self.subTest(override=override),self.assertRaisesRegex(RuntimeError,'ISOLATED_BACKTEST'):
                self.run_cut(**override)


if __name__=='__main__': unittest.main()
