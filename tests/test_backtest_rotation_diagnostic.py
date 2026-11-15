"""Arithmetic and evidence-boundary tests for the read-only diagnostic."""
import importlib.util
import unittest
from decimal import Decimal as D
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    'rotation_diagnostic', ROOT/'scripts/backtest_rotation_diagnostic.py')
diagnostic = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostic)


def fill(i, side, qty, cost, session, ticker='AAA'):
    return dict(fill_id=i, ticker=ticker, side=side, qty=D(qty), cost=D(cost),
                quote=D('100'), session=session)


class DiagnosticTests(unittest.TestCase):
    def test_fifo_partial_closes_allocate_cost_once(self):
        fills = [fill(0,'BUY',10,10,0), fill(1,'BUY',5,10,1),
                 fill(2,'SELL',12,24,3), fill(3,'SELL',3,6,5)]
        pairs = diagnostic.pair_fifo(fills,'BUY')
        self.assertEqual([p['quantity'] for p in pairs], [D(10),D(2),D(3)])
        self.assertEqual([p['cost'] for p in pairs], [D(30),D(8),D(12)])
        self.assertEqual(sum(p['cost'] for p in pairs), D(50))
        self.assertEqual([p['sessions'] for p in pairs], [3,2,4])

    def test_fifo_reverse_does_not_invent_or_cross_tickers(self):
        fills = [fill(0,'BUY',1,1,0), fill(1,'SELL',3,3,2),
                 fill(2,'BUY',2,2,4,'BBB'), fill(3,'BUY',5,5,5)]
        pairs = diagnostic.pair_fifo(fills,'SELL')
        self.assertEqual(len(pairs),1)
        self.assertEqual(pairs[0]['quantity'], D(3))
        self.assertEqual(pairs[0]['cost'], D(6))

    def test_recent_event_and_fifo_measure_different_questions(self):
        fills = [fill(0,'BUY',10,10,0), fill(1,'BUY',1,1,20),
                 fill(2,'SELL',1,1,22), fill(3,'SELL',1,1,23)]
        edges, costs = diagnostic.recent_opposite_events(fills,'SELL',3)
        self.assertEqual(len(edges),2)
        self.assertEqual(costs,D(3))  # Shared entry is included once.
        self.assertEqual(diagnostic.pair_fifo(fills,'BUY')[0]['sessions'],22)
        self.assertEqual(len(diagnostic.recent_opposite_events(fills,'SELL',2)[0]),1)

    def test_t10_uses_session_index_and_preserves_missingness(self):
        sessions = ['2026-08-03','2026-08-04','2026-08-05','2026-08-06',
                    '2026-08-07','2026-08-10','2026-08-11','2026-08-12',
                    '2026-08-13','2026-08-14','2026-08-18']
        entry=fill(0,'BUY',1,1,0)
        daily={sessions[10]: {'assets':[dict(ticker='AAA',price='110',
                    mark=dict(basis='RECORDED_INTRADAY_LAST',ts='2026-08-18T19:00:00+00:00'))]}}
        self.assertEqual(diagnostic.horizon_10(entry,sessions,daily)[0],D(10))
        self.assertIsNone(diagnostic.horizon_10(entry,sessions,{})[0])
        daily[sessions[10]]['assets'][0]['mark']['ts']='2026-08-17T19:00:00+00:00'
        self.assertEqual(diagnostic.horizon_10(entry,sessions,daily)[1], 'STALE_OR_NON_INTRADAY_T10_MARK')
        self.assertEqual(diagnostic.horizon_10(entry,sessions[:10],daily)[1], 'IMMATURE_AT_WINDOW_END')

    def test_timestamp_join_and_startup_day_are_distinct(self):
        self.assertEqual(diagnostic.instant('2026-08-03T11:00:00-03:00'),
                         diagnostic.instant('2026-08-03T14:00:00+00:00'))
        f=dict(side='BUY',reason='Radar elegible: Abrir posición',day='2026-08-03')
        self.assertEqual(diagnostic.origin(f,'2026-08-03'),'radar-primer-dia')
        self.assertEqual(diagnostic.origin(f,'2026-07-01'),'radar-plan')

    def test_known_run_rotation_and_additive_sections(self):
        root=ROOT/'outputs/backtests'
        if not (root/diagnostic.RUNS['C']).exists():
            self.skipTest('Frozen research run C is not installed')
        s, rows=diagnostic.analyze_run(root,'C',diagnostic.RUNS['C'])
        self.assertEqual(s['turnover'],D('6.260490004966666673333333333'))
        self.assertEqual(s['fills'],121)
        for section in ('origin','exact_reason','asset_concentration','week_concentration'):
            subset=[r for r in rows if r['section']==section]
            self.assertEqual(sum(r['gross_ars'] for r in subset),s['gross_ars'])
            self.assertEqual(sum(r['costs_ars'] for r in subset),s['costs_ars'])
            self.assertEqual(sum(r['fills'] for r in subset),121)
        self.assertEqual(len([r for r in rows if r['section']=='summary']),1)
        self.assertEqual(diagnostic.gate([s])['status'],'STOP_BEFORE_VARIANTS')


if __name__=='__main__':
    unittest.main()
