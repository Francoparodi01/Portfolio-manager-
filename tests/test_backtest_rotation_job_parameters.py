import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_SPEC = importlib.util.spec_from_file_location(
    'rotation_job_parameters', ROOT / 'scripts/backtest_rotation_sweep.py'
)
sweep = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(sweep)


class JobParameterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.base = self.root / 'research'
        self.source = self.root / 'outputs/backtests/capture'
        (self.source / 'input').mkdir(parents=True)
        self.source_job = self.source / 'job.json'
        self.source_job.write_text(json.dumps({'spec': {
            'sessions': ['2026-07-01', '2026-07-02', '2026-07-03'],
            'initial_cash': '6000000', 'fee_rate': '0.006',
        }}), encoding='utf-8')
        self.plan = {
            '_repo': str(self.root), 'capture_run': 'capture',
            'windows': {'W1': ['2026-07-01', '2026-07-03']},
            'hours_art': ['11:00', '13:00', '16:00'],
            'git_commit': 'commit', 'capture_sha256': 'capture-hash',
            'image': 'frozen-image', '_prereg_sha256': 'prereg-hash',
        }
        self.config = {'id': 'control', 'overrides': {}}

    def prepare(self, **kwargs):
        return sweep.prepare_job(
            self.base, self.plan, self.config, 'W1', 'ENSEMBLE', **kwargs
        )[0]

    def test_default_job_stays_identical_and_capitals_do_not_collide(self):
        original = self.source_job.read_bytes()
        default = self.prepare()
        before = (default / 'job.json').read_bytes()
        self.assertEqual(default.name, 'control__W1__slip_0.0015')
        same = self.prepare(initial_cash='3000000.00')
        self.assertEqual(same, default)
        self.assertEqual((same / 'job.json').read_bytes(), before)
        paths = {default}
        for capital in ['3060000', '2940000']:
            directory = self.prepare(initial_cash=capital)
            paths.add(directory)
            spec = sweep.read(directory / 'job.json')['spec']
            self.assertEqual(spec['initial_cash'], capital)
            self.assertEqual(spec['fee_rate'], '0.006')
            self.assertNotIn('rotation_overrides', spec)
        self.assertEqual(len(paths), 3)
        self.assertEqual((default / 'job.json').read_bytes(), before)
        self.assertEqual(self.source_job.read_bytes(), original)

    def test_window_dates_and_session_cutoffs_remain_parameterized(self):
        self.plan['windows']['W1'] = ['2026-07-02', '2026-07-03']
        directory = self.prepare(initial_cash='3060000')
        spec = sweep.read(directory / 'job.json')['spec']
        self.assertEqual(spec['start'], '2026-07-02')
        self.assertEqual(spec['end'], '2026-07-03')
        self.assertEqual(spec['sessions'], ['2026-07-02', '2026-07-03'])
        self.assertEqual(len(spec['cutoffs']), 6)
        self.assertEqual(spec['cutoffs'][0], '2026-07-02T11:00:00-03:00')

    def test_invalid_capital_does_not_create_run_artifacts(self):
        for capital in ['NaN', 'Infinity', '-1', '0', '999', '1000000001', 'bad']:
            with self.subTest(capital=capital):
                with self.assertRaisesRegex(ValueError, 'INVALID_INITIAL_CASH'):
                    self.prepare(initial_cash=capital)
        self.assertFalse((self.base / 'runs').exists())

    def test_run_job_uses_matching_capital_cache(self):
        directory = self.prepare(initial_cash='3060000')
        sweep.write(directory / 'research_complete.json', {'status': 'COMPLETE'})
        sweep.write(directory / 'metrics.json', {'capital_ars': '3060000'})
        with patch.object(sweep.subprocess, 'call') as docker:
            result = sweep.run_job(
                self.base, self.plan, self.config, 'W1', 'ENSEMBLE',
                initial_cash='3060000',
            )
        self.assertEqual(result, {'capital_ars': '3060000'})
        docker.assert_not_called()


if __name__ == '__main__':
    unittest.main()
