"""Backtest-only policy boundaries; no broker, database or market access."""
import inspect
import unittest
import json
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

from src.backtesting.spec import parse_spec, rotation_overrides, spec_hash
from src.backtesting.rotation import install_rotation_policy
from src.analysis.enums import DecisionType
from src.analysis.execution_planner import derive_decision_intents, reconcile_funding


class RotationTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime.fromisoformat('2026-07-02T11:00:00-03:00')
        self.spec=dict(sessions=['2026-07-01','2026-07-02','2026-07-03','2026-07-06'],
                       cutoffs=['2026-07-01T11:00:00-03:00'])
        self.book=NS(fills=[])
        self.derive_args=[]
        def derive(*args,**kwargs):
            bound=inspect.signature(derive_decision_intents).bind(*args,**kwargs)
            bound.apply_defaults();self.derive_args.append(bound.arguments)
            return []
        def funding(*args,**kwargs):
            return inspect.signature(reconcile_funding).bind(*args,**kwargs).arguments
        self.engine=NS(derive_decision_intents=derive,reconcile_funding=funding)

    def install(self,**overrides):
        self.spec['rotation_overrides']=overrides
        return install_rotation_policy(self.engine,self.spec,self.book,lambda:self.now)

    def decision(self,action=DecisionType.SELL_PARTIAL,**fields):
        return NS(ticker='AAA',action=action,reason_primary='Rebalanceo discrecional',
                  reason_secondary='',sell_cause=None,block_code=None,data_quality={},**fields)

    def fund(self,decisions=(),**kwargs):
        return self.engine.reconcile_funding(decisions=list(decisions),current_positions={},
                    cash_before=3000000,portfolio_value_ars=3000000,gate='OPEN',**kwargs)

    def last_fill(self,side):
        self.book.fills.append(dict(ticker='AAA',side=side,filled_at='2026-07-01T14:02:00+00:00'))

    def test_control_does_not_install_wrappers(self):
        before=(self.engine.derive_decision_intents,self.engine.reconcile_funding)
        self.assertIsNone(install_rotation_policy(self.engine,self.spec,self.book,lambda:self.now))
        self.assertEqual(before,(self.engine.derive_decision_intents,self.engine.reconcile_funding))

    def test_min_hold_discretionary_wait_and_boundary(self):
        self.install(min_hold_sessions=3);self.last_fill('BUY')
        d=self.decision()
        self.assertEqual(self.fund([d])['decisions'][0].action,DecisionType.HOLD)
        self.assertEqual(d.action,DecisionType.SELL_PARTIAL)
        self.now=datetime.fromisoformat('2026-07-06T11:00:00-03:00')
        self.assertIs(self.fund([d])['decisions'][0],d)

    def test_min_hold_preserves_stop_blocked_corporate_and_invalid_data(self):
        self.install(min_hold_sessions=10);self.last_fill('BUY')
        self.engine.derive_decision_intents(None,{'AAA':NS(stop_triggered=True)}, {},3000000,'OPEN')
        stop=self.decision()
        self.assertIs(self.fund([stop])['decisions'][0],stop)
        self.engine.derive_decision_intents(None,{}, {},3000000,'OPEN')
        blocked=self.decision(DecisionType.BLOCKED)
        self.assertIs(self.fund([blocked])['decisions'][0],blocked)
        corporate=self.decision()
        self.assertIs(self.fund([corporate],blocked_trade_tickers={'AAA':'corporate action'})['decisions'][0],corporate)
        invalid=self.decision();invalid.data_quality={'is_operable':False}
        self.assertIs(self.fund([invalid])['decisions'][0],invalid)

    def test_cooldown_applies_to_planner_and_radar_buys(self):
        self.install(redeploy_cooldown_sessions=3);self.last_fill('SELL')
        buy=self.decision(DecisionType.BUY)
        output=self.fund([buy],external_buys=[dict(ticker='AAA',reason='Radar elegible: Abrir posición')])
        self.assertEqual(output['decisions'][0].action,DecisionType.WATCH)
        self.assertEqual(output['external_buys'],[])

    def test_tier_filter_and_startup_are_independent(self):
        self.install(radar_buy_tiers='SOLO_ABRIR_POSICION',delta_band_mult=3)
        candidates=[dict(ticker=t,reason='Radar elegible: '+r) for t,r in
                    [('AAA','Abrir posición (parcial)'),('BBB','Esperar funding o evaluar swap'),
                     ('CCC','Compra habilitada (no prioritaria)')]]
        self.assertEqual(len(self.fund(external_buys=candidates)['external_buys']),1)
        self.engine.derive_decision_intents(None,{}, {},3000000,'OPEN')
        self.assertAlmostEqual(self.derive_args[-1]['min_weight_delta'],.045)
        self.now=datetime.fromisoformat(self.spec['cutoffs'][0])
        self.assertEqual(self.fund(external_buys=candidates)['external_buys'],candidates)
        self.engine.derive_decision_intents(None,{}, {},3000000,'OPEN')
        self.assertAlmostEqual(self.derive_args[-1]['min_weight_delta'],.015)

    def test_no_funding_tier_keeps_nonpriority(self):
        self.install(radar_buy_tiers='SIN_ESPERAR_FUNDING')
        result=self.fund(external_buys=[dict(ticker='AAA',reason='Radar elegible: Compra habilitada (no prioritaria)')])
        self.assertEqual(len(result['external_buys']),1)

    def test_spec_only_hash_and_default_fee_contract(self):
        now=datetime.fromisoformat('2026-10-29T18:00:00-03:00')
        text='desde=01/07/2026 hasta=03/07/2026 capital=3m'
        with patch.dict('os.environ',{'MIN_HOLD_SESSIONS':'10','RADAR_BUY_TIERS':'SOLO_ABRIR_POSICION'}):
            control=parse_spec(text,now)
            variant=parse_spec(text+' min_hold_sessions=3',now)
        self.assertNotIn('rotation_overrides',control)
        self.assertEqual(control['fee_rate'],'0.006')
        self.assertEqual(control['slippage_rate'],'0.0015')
        self.assertEqual(variant['rotation_overrides'],{'min_hold_sessions':3})
        self.assertNotEqual(spec_hash(control),spec_hash(variant))
        self.assertEqual(spec_hash(control),spec_hash(dict(reversed(list(control.items())))))
        with self.assertRaises(ValueError):
            rotation_overrides({'rotation_overrides':{'min_hold_sessions':True}})

    def test_manifest_contains_exact_spec_hash_and_overrides(self):
        from src.backtesting.report import export
        spec=dict(schema='backtest-v1',methodology='RESEARCH_TEST',rotation_overrides={'min_hold_sessions':3})
        result=dict(spec=spec,source_rows={})
        result.update({key:[] for key in ('fills','orders','daily','assets','decisions','horizons','analyses','cuts')})
        with tempfile.TemporaryDirectory() as directory, patch('src.backtesting.report.pdf'):
            export(Path(directory),result)
            manifest=json.loads((Path(directory)/'exports/manifest.json').read_text())
            self.assertEqual(manifest['spec_sha256'],spec_hash(spec))
            self.assertEqual(manifest['rotation_overrides'],{'min_hold_sessions':3})


if __name__=='__main__':
    unittest.main()
