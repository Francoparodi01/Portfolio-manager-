import importlib.util
import csv
import json
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

root=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('rotation_sweep',root/'scripts/backtest_rotation_sweep.py')
sweep=importlib.util.module_from_spec(spec)
spec.loader.exec_module(sweep)


class MetricsTests(unittest.TestCase):
    def test_exports_never_overlap_native_replays(self):
        events=[]
        def job(base,plan,config,window,stage,*,phase):
            events.append((phase,config))
            return config
        jobs=[('A','W1','OFAT'),('B','W1','OFAT')]
        with patch.object(sweep,'run_job',side_effect=job):
            self.assertEqual(sweep.batch(None,None,jobs),['A','B'])
        self.assertEqual([phase for phase,_ in events],['replay','replay','measure','measure'])

    def test_split_quality_flags_exposure_without_rewriting_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            self.assertEqual(sweep.known_split_quality(directory)['daily_drawdown_quality'],
                             'UNKNOWN_MISSING_EXPORTS')
            exports=directory/'exports';exports.mkdir()
            with (exports/'cartera_diaria.csv').open('w',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=['asof','assets']);writer.writeheader()
                for day,qty in [('2026-08-03',6),('2026-08-04',60)]:
                    writer.writerow(dict(asof=day,assets=json.dumps([dict(ticker='YPFD',quantity=qty)])))
            (exports/'operaciones.csv').write_text('filled_at,ticker\n2026-08-03T14:00:00+00:00,AAA\n')
            result=sweep.known_split_quality(directory)
            self.assertTrue(result['ypfd_split_mismatch_exposure'])
            self.assertEqual(result['ypfd_quantity_aug3'],6)
            self.assertEqual(result['ypfd_quantity_aug4'],60)
            self.assertEqual(result['aug3_fills'],1)
            self.assertEqual(result['aug3_ypfd_fills'],0)
            self.assertEqual(result['daily_drawdown_quality'],'NOT_EVALUABLE_YPFD_SPLIT_MISMATCH')
            (exports/'cartera_diaria.csv').write_text('asof,assets\n2026-08-03,[]\n')
            result=sweep.known_split_quality(directory)
            self.assertFalse(result['ypfd_split_mismatch_exposure'])
            self.assertEqual(result['daily_drawdown_quality'],'UNVERIFIED_HISTORICAL_PRICES')

    def test_fifo_cohorts_reconcile_costs_and_partial_sales(self):
        result=dict(spec=dict(sessions=['2026-07-01','2026-07-02','2026-07-03'],
                              cutoffs=['2026-07-01T11:00:00-03:00']),
                    fills=[dict(ticker='AAA',side='BUY',quantity=2,gross_ars=200,fee_ars=2,
                                cutoff='2026-07-01T14:00:00+00:00',filled_at='2026-07-01T14:01:00+00:00'),
                           dict(ticker='AAA',side='BUY',quantity=1,gross_ars=110,fee_ars=1,
                                cutoff='2026-07-02T14:00:00+00:00',filled_at='2026-07-02T14:01:00+00:00'),
                           dict(ticker='AAA',side='SELL',quantity=1,gross_ars=120,fee_ars=1,
                                cutoff='2026-07-03T14:00:00+00:00',filled_at='2026-07-03T14:01:00+00:00')],
                    assets=[dict(ticker='AAA',price=125)],research_net_pnl_ars=56)
        pnl,hold,pairs=sweep.attribution(result,[])
        self.assertEqual(pnl,dict(startup=D(42),ongoing=D(14)))
        self.assertEqual(hold,D(2));self.assertEqual(pairs,1)

    def test_split_preserves_entry_basis_and_cohort(self):
        result=dict(spec=dict(sessions=['2026-07-01','2026-07-02'],cutoffs=['2026-07-01T11:00:00-03:00']),
                    fills=[dict(ticker='AAA',side='BUY',quantity=2,gross_ars=200,fee_ars=2,
                                cutoff='2026-07-01T14:00:00+00:00',filled_at='2026-07-01T14:01:00+00:00')],
                    assets=[dict(ticker='AAA',price=60)],research_net_pnl_ars=38)
        events=[dict(ticker='AAA',quantity_before=2,quantity_after=4,asof='2026-07-02T14:00:00+00:00')]
        pnl,hold,pairs=sweep.attribution(result,events)
        self.assertEqual(pnl['startup'],D(38));self.assertIsNone(hold)

    def test_missing_windows_cannot_win_even_with_good_observed_results(self):
        row=dict(window='W1',delta_vs_control_pp=3,delta_without_top3_pp=2,
                 drawdown_degradation_pp=0,turnover_reduction_pct=50)
        self.assertEqual(sweep.verdict([row,dict(row,window='W2')]),'NOT_EVALUABLE_MISSING_WINDOWS')
        five=[dict(row,window='W'+str(i)) for i in range(1,6)]
        self.assertEqual(sweep.verdict(five),'PASS_RESEARCH_ONLY')
        five[-1]['delta_vs_control_pp']=0
        self.assertEqual(sweep.verdict(five),'FAIL')


if __name__=='__main__':unittest.main()
