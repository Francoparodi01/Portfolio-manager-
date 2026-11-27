from datetime import date, datetime, time, timedelta
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.backtesting import store
from src.backtesting.benchmarks import reference_curves
from src.backtesting.report import brief, export, summarize
from src.backtesting.spec import ART

DAYS = [d for d in (date(2026, 7, 1)+timedelta(n) for n in range(14)) if d.weekday() < 5]


def make_spec(**extra):
    return dict(start=str(DAYS[0]), end=str(DAYS[-1]), asof=datetime.combine(DAYS[-1], time(17), ART).isoformat(),
                cutoffs=[datetime.combine(d, time(h), ART).isoformat() for d in DAYS for h in (11, 16)],
                initial_cash='3000000', fee_rate='0.006', slippage_rate='0.0015', latency_seconds=60, tickers=[], **extra)


def capture(prices=None, ticker='SPY'):
    rows = []
    for k, d in enumerate(DAYS):
        price = (prices or {}).get(k, 20000+100*k)
        for minute in (11*60+5, 16*60+5, 16*60+50):
            rows.append(dict(ts=datetime.combine(d, time(minute//60, minute % 60), ART).isoformat(), ticker=ticker,
                             asset_type='CEDEAR', currency='ARS', last_price=price, change_pct_1d=0))
    return [dict(name='intraday_points', rows=rows, row_count=len(rows)), dict(name='daily_candles', rows=[], row_count=0),
            dict(name='corporate_full', rows=[], row_count=0)]


class ReferenceCurveTests(unittest.TestCase):
    def test_buy_and_hold_uses_ledger_costs_and_first_quote(self):
        bot = [dict(asof=datetime.combine(d, time(17), ART).isoformat(), nav_ars=3000000-1000*k) for k, d in enumerate(DAYS)]
        b = reference_curves(make_spec(), capture(), bot)
        self.assertEqual(b['status'], 'COMPLETE')
        fill = 20000*1.0015
        qty = int(3000000/(fill*1.006))
        self.assertEqual(b['entry']['quantity'], qty)
        self.assertEqual(b['entry']['cutoffs_waited'], 0)
        cash = 3000000-qty*fill*1.006
        last = 20000+100*(len(DAYS)-1)
        self.assertAlmostEqual(b['daily'][-1]['reference_nav_ars'], cash+qty*last, places=4)
        self.assertEqual(len(b['daily']), len(DAYS))
        self.assertTrue(all(d['cash_nav_ars'] == 3000000 for d in b['daily']))
        self.assertAlmostEqual(b['excess_vs_reference_pp'], b['bot']['return_pct']-b['reference']['return_pct'])
        self.assertEqual(b['bot_days_missing'], 0)
        self.assertLess(b['reference_after_exit_nav_ars'], b['reference']['final_nav_ars'])

    def test_missing_reference_is_not_evaluable_not_zero(self):
        b = reference_curves(make_spec(), capture(ticker='QQQ'), [])
        self.assertEqual(b['status'], 'NOT_EVALUABLE')
        self.assertNotIn('reference', b)


class ReportIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, BACKTEST_ROOT=self.tmp.name)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def fixture(self, observations):
        from src.backtesting.spec import parse_spec
        spec = dict(parse_spec('ruedas=1', datetime(2026, 7, 20, 14, tzinfo=ART)), **make_spec())
        job = store.enqueue(1, 'ref', spec, notify=False)
        path = store.directory(1, job['id'])
        (path/'replay').mkdir()
        (path/'input').mkdir()
        store.write(path/'isolation.json', dict(external_io_attempts=[], no_persist=True))
        store.write(path/'input'/'research_data_capture.json', dict(observations=observations))
        daily = [dict(asof=datetime.combine(d, time(17), ART).isoformat(), nav_ars=3000000, cash_ars=3000000, return_pct=0, assets=[]) for d in DAYS]
        for i, c in enumerate(spec['cutoffs']):
            store.write(path/'replay'/f'cut_{i+1:03d}.json', dict(main_completed=True, cutoff_art=c, universe_membership=dict(count=80),
                        candle_reads=[dict(rows=120)], valuation_after_interval=daily[i//2]))
        store.write(path/'replay'/'daily.json', daily)
        return path

    def test_report_carries_references_to_pdf_csv_and_telegram(self):
        path = self.fixture(capture())
        result = summarize(path, 0)
        self.assertEqual(result['benchmarks']['status'], 'COMPLETE')
        self.assertIn('SPY comprado y mantenido', brief(result))
        out = export(path, result)
        self.assertGreater((out/'informe.pdf').stat().st_size, 1000)
        header = (out/'referencias_diarias.csv').read_text(encoding='utf-8-sig').splitlines()
        self.assertEqual(len(header), len(DAYS)+1)
        self.assertIn('reference_nav_ars', header[0])

    def test_report_survives_missing_reference(self):
        path = self.fixture(capture(ticker='QQQ'))
        result = summarize(path, 0)
        self.assertEqual(result['status'], 'COMPLETE')
        self.assertTrue(any('Referencias SPY' in a for a in result['alerts']))
        self.assertNotIn('comprado y mantenido', brief(result))
        self.assertTrue((export(path, result)/'informe.pdf').exists())
