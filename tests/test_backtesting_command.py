import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from src.backtesting import store
from src.backtesting.spec import ART, describe, parse_spec, spec_hash
from src.backtesting.report import summarize, export
from src.backtesting.telegram import handle, callback

NOW=datetime(2026,10,27,14,tzinfo=ART)


class SpecTests(unittest.TestCase):
    def test_defaults_use_completed_sessions_and_cash_start(self):
        s=parse_spec('',NOW)
        self.assertEqual(s['initial_cash'],'3000000')
        self.assertEqual(s['end'],'2026-10-26')
        self.assertEqual(len(s['cutoffs']),60)
        self.assertEqual(s['variant'],'FULL_CASH_START')
        self.assertEqual(s['cash_start_policy'],'ELIGIBLE_RADAR_V1')

    def test_old_bot_requests_receive_cash_start_without_mutating_input(self):
        from src.backtesting.bridge import prepare_spec
        old = dict(startup='nativo', variant='NATIVE_FULL')
        self.assertEqual(prepare_spec(old)['variant'], 'FULL_CASH_START')
        self.assertEqual(old['variant'], 'NATIVE_FULL')
        self.assertEqual(parse_spec('arranque=radar',NOW)['cash_start_policy'], 'ELIGIBLE_RADAR_V1')

    def test_all_params(self):
        s=parse_spec('desde=01/08/2026 hasta=26/10/2026 capital=3.000.000 tickers=AMD,NVDA,PYPL,AXP,MU horas=11:30,13,16 medir=riesgo,operaciones horizontes=5,10,20 comision=0.600 deslizamiento=0,15',NOW)
        self.assertEqual(s['fee_rate'],'0.006')
        self.assertEqual(s['slippage_rate'],'0.0015')
        self.assertEqual(s['tickers'],['AMD','AXP','MU','NVDA','PYPL'])
        self.assertEqual(s['hours'],['11:30','13:00','16:00'])
        self.assertEqual(s['metrics'],['riesgo','operaciones'])

    def test_reject_invalid_inputs(self):
        for text in ['hasta=2030-01-01','ruedas=121','capital=NaN','capital=-1','horas=11:10:20','horas=17','medir=inventado','comision=-1','desde=01/08/2026 ruedas=5','tickers=AMD;DROP','ruedas=3 ruedas=4']:
            with self.subTest(text=text),self.assertRaises(ValueError):parse_spec(text,NOW)

    def test_missing_horizons_are_not_silently_dropped(self):
        s=parse_spec('ruedas=1',NOW)
        self.assertEqual(s['horizons'],[5,10,20])

    def test_rotation_options_are_explicit_and_hashed(self):
        control=parse_spec('ruedas=1',NOW)
        self.assertNotIn('rotation_overrides',control)
        self.assertNotIn('Filtros del backtest',describe(control))
        text='delta_band_mult=3 radar_buy_tiers=solo_abrir_posicion redeploy_cooldown_sessions=3 min_hold_sessions=10'
        variant=parse_spec('ruedas=1 '+text,NOW)
        self.assertEqual(variant['rotation_overrides'],dict(delta_band_mult=3,
            radar_buy_tiers='SOLO_ABRIR_POSICION',redeploy_cooldown_sessions=3,min_hold_sessions=10))
        self.assertNotEqual(spec_hash(control),spec_hash(variant))
        self.assertIn('delta_band_mult=3',describe(variant))
        self.assertEqual({k:v for k,v in variant.items() if k!='rotation_overrides'},control)

    def test_invalid_rotation_options_are_rejected(self):
        for text in ['delta_band_mult=4','delta_band_mult=3.5','delta_band_mult=true',
                     'delta_band_mult=2 delta_band_mult=3','min_hold_sessions=2',
                     'redeploy_cooldown_sessions=-1','radar_buy_tiers=UNKNOWN']:
            with self.subTest(text=text),self.assertRaises(ValueError):
                parse_spec('ruedas=1 '+text,NOW)


class QueueAndReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.env=patch.dict(os.environ,BACKTEST_ROOT=self.tmp.name)
        self.env.start()
        self.spec=parse_spec('ruedas=1',NOW)
    def tearDown(self):self.env.stop();self.tmp.cleanup()
    def test_owner_scope_and_idempotency(self):
        a=store.enqueue(123,'update1',self.spec,notify=False)
        self.assertEqual(a,store.enqueue(123,'update1',self.spec,notify=False))
        with self.assertRaises(ValueError):store.enqueue(123,'update2',self.spec)
        with self.assertRaises(ValueError):store.directory(456,a['id'])
        with self.assertRaises(ValueError):store.directory(123,'../../job')
        self.assertEqual(store.cancel(123,a['id'])['status'],'CANCELLED')
        self.assertEqual(store.enqueue(123,'update2',self.spec)['status'],'QUEUED')
    def test_absent_or_unsafe_evidence_is_not_zero_return(self):
        a=store.enqueue(123,'report',self.spec,notify=False)
        path=store.directory(123,a['id'])
        result=summarize(path,1)
        self.assertEqual(result['status'],'NOT_EVALUABLE')
        self.assertIsNone(result['return_pct'])
        self.assertIsNone(result['win_rate_pct'])
    def fixture(self):
        a=store.enqueue(123,'report',self.spec,notify=False)
        path=store.directory(123,a['id']);(path/'replay').mkdir()
        store.write(path/'isolation.json',dict(external_io_attempts=[],no_persist=True))
        valuation=dict(asof=self.spec['asof'],nav_ars=3030000,cash_ars=3030000,return_pct=1,assets=[])
        for i,c in enumerate(self.spec['cutoffs']):
            store.write(path/'replay'/f'cut_{i+1:03d}.json',dict(main_completed=True,cutoff_art=c,
                universe_membership=dict(count=80),candle_reads=[dict(rows=120)],valuation_after_interval=valuation))
        store.write(path/'replay'/'daily.json',[valuation])
        return path
    def test_cycles_count_partial_sales_once_and_missing_horizons_null(self):
        path=self.fixture()
        fills=[dict(ticker='AMD',side='SELL',realized_pnl=p,position_after=q,filled_at=self.spec['asof'],fee_ars=0,slippage_ars=0) for p,q in [(20,3),(-5,0)]]
        store.write(path/'replay'/'fills.json',fills)
        result=summarize(path,0)
        self.assertEqual(result['completed_cycles'],1)
        self.assertEqual(result['profitable_cycles'],1)
        self.assertEqual(result['cycles'][0]['pnl_ars'],15)
        self.assertTrue(all(x['return_pct'] is None for x in result['horizons']))
        self.assertAlmostEqual(result['return_pct'],1)
    def test_partial_cannot_be_labelled_full_period(self):
        path=self.fixture()
        (path/'replay'/'cut_003.json').unlink()
        self.assertEqual(summarize(path,1)['status'],'PARTIAL')
        store.write(path/'isolation.json',dict(external_io_attempts=['network'],no_persist=True))
        self.assertEqual(summarize(path,0)['status'],'NOT_EVALUABLE')
    def test_exports_inspectable_pdf_and_csv(self):
        path=self.fixture();out=export(path,summarize(path,0))
        self.assertGreater((out/'informe.pdf').stat().st_size,1000)
        self.assertTrue((out/'datos.zip').exists())
        self.assertTrue((out/'decisiones.csv').exists())

    def test_report_retains_cash_start_policy_and_cut_evidence(self):
        path=self.fixture()
        trace=path/'replay'/'cut_001.json'
        raw=json.loads(trace.read_text())
        raw['main_return']={'backtest_cash_start':dict(attempted=True,candidates=2,buy_orders=1,gate='NORMAL')}
        store.write(trace,raw)
        result=summarize(path,0)
        self.assertEqual(result['cuts'][0]['cash_start_buy_orders'],1)
        self.assertTrue(any('SOLO en el backtest' in x for x in result['alerts']))


class TelegramTests(QueueAndReportTests):
    def test_rotation_command_enqueues_exact_spec_and_reports_options(self):
        store.write(store.root()/'worker.json',dict(updated_at=store.stamp()))
        bot=SimpleNamespace(send_message=AsyncMock(),send_document=AsyncMock())
        update=SimpleNamespace(effective_chat=SimpleNamespace(id=123),update_id=102)
        context=SimpleNamespace(bot=bot,args=['ruedas=1','delta_band_mult=3'])
        asyncio.run(handle(update,context))
        jobs=store.jobs(123)
        self.assertEqual(len(jobs),1)
        self.assertEqual(jobs[0]['spec']['rotation_overrides'],{'delta_band_mult':3})
        self.assertIn('delta_band_mult=3',bot.send_message.call_args.kwargs['text'])
        self.assertEqual(bot.send_document.call_count,0)

    def test_command_enqueues_and_status_owner_scoped(self):
        store.write(store.root()/'worker.json',dict(updated_at=store.stamp()))
        bot=SimpleNamespace(send_message=AsyncMock(),send_document=AsyncMock())
        update=SimpleNamespace(effective_chat=SimpleNamespace(id=123),update_id=100)
        context=SimpleNamespace(bot=bot,args=['ruedas=1'])
        asyncio.run(handle(update,context))
        self.assertEqual(len(store.jobs(123)),1)
        job=store.jobs(123)[0]
        self.assertIn('Corrida en cola',bot.send_message.call_args.kwargs['text'])
        other=SimpleNamespace(effective_chat=SimpleNamespace(id=456),update_id=101)
        asyncio.run(handle(other,context,['resultado',job['id']]))
        self.assertEqual(bot.send_message.call_args.kwargs['text'],'Corrida no encontrada.')
        self.assertEqual(bot.send_document.call_count,0)
    def test_callback_ack_and_cancel(self):
        a=store.enqueue(123,'callback',self.spec,notify=False)
        bot=SimpleNamespace(send_message=AsyncMock())
        query=SimpleNamespace(data='bt:cancelar:'+a['id'],answer=AsyncMock())
        update=SimpleNamespace(effective_chat=SimpleNamespace(id=123),update_id=1,callback_query=query)
        asyncio.run(callback(update,SimpleNamespace(bot=bot)))
        query.answer.assert_awaited_once()
        self.assertEqual(store.jobs(123)[0]['status'],'CANCELLED')


if __name__=='__main__':unittest.main()
