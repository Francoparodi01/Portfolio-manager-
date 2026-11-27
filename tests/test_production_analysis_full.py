"""Behavior checks runnable with unittest in either production image."""
import asyncio
from datetime import datetime
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from src.core.analysis_profile import ANALYSIS_FULL_PROFILE, full_analysis_args


class FullContractTests(unittest.TestCase):
    def test_full_profile_keeps_radar_and_persistence(self):
        args = full_analysis_args(1234)
        self.assertIn('--no-telegram', args)
        self.assertEqual(args[args.index('--owner-chat-id') + 1], '1234')
        self.assertEqual(args[args.index('--run-intent') + 1], 'formal_plan')
        for disabled in ['--skip-radar', '--no-persist', '--no-optimizer', '--no-sentiment']:
            self.assertNotIn(disabled, args)

    def test_exploration_remains_non_persistent(self):
        args = full_analysis_args(1234, persist=False, notify=True)
        self.assertIn('--no-persist', args)
        self.assertIn('exploratory', args)
        self.assertNotIn('--no-telegram', args)
        self.assertNotIn('--skip-radar', args)

    def test_owner_is_required(self):
        for owner in (0, -1, 'unknown'):
            with self.assertRaises((ValueError, TypeError)):
                full_analysis_args(owner)

    def test_market_guard_is_preserved(self):
        from scripts.run_analysis import _analysis_run_policy
        tz = ZoneInfo('America/Argentina/Buenos_Aires')
        self.assertEqual(_analysis_run_policy(False, 'formal_plan', datetime(2026, 10, 27, 13, tzinfo=tz)), (False, 'formal_plan', False))
        self.assertEqual(_analysis_run_policy(False, 'formal_plan', datetime(2026, 10, 27, 17, 12, tzinfo=tz)), (True, 'exploratory', True))
        self.assertEqual(_analysis_run_policy(False, 'formal_plan', datetime(2026, 10, 25, 13, tzinfo=tz)), (True, 'exploratory', True))


class FullPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_driver_receives_normalized_database_url(self):
        from scripts import run_analysis
        class ReachedDatabase(Exception):
            pass
        cfg = SimpleNamespace(database=SimpleNamespace(url='postgresql+asyncpg://local/test'))
        with patch('asyncpg.connect', new_callable=AsyncMock, side_effect=ReachedDatabase) as connect:
            with self.assertRaises(ReachedDatabase):
                await run_analysis._save_execution_plan_events(
                    cfg=cfg, execution_plan=SimpleNamespace(), results=[],
                    macro_snap=SimpleNamespace(), macro_regime='NEUTRAL', total_ars=1000,
                    owner_chat_id=1234, run_id='00000000-0000-0000-0000-000000000001',
                    analysis_profile=ANALYSIS_FULL_PROFILE,
                )
            connect.assert_awaited_once_with('postgresql://local/test')


@unittest.skipIf(os.environ.get('TEST_COMPONENT') == 'scheduler', 'Telegram image only')
class FullTelegramTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_analysis_delegates_once_to_full(self):
        from scripts import telegram_bot as bot
        with patch.object(bot, 'action_analysis_full', new_callable=AsyncMock) as full:
            context = object()
            await bot.action_analysis(context, 1234)
            full.assert_awaited_once_with(context, 1234)

    async def test_full_refreshes_and_records_owner_scoped_plan(self):
        from scripts import telegram_bot as bot
        with patch.object(bot, 'sync_operational_state', new_callable=AsyncMock, return_value='') as sync, \
             patch.object(bot, 'run_first_existing_script', new_callable=AsyncMock, return_value='full-report') as run, \
             patch.object(bot, '_load_cached_report', new_callable=AsyncMock) as load, \
             patch.object(bot, '_save_cached_report', new_callable=AsyncMock) as save, \
             patch.object(bot, 'send_text', new_callable=AsyncMock) as send:
            context = object()
            await bot.action_analysis_full(context, 1234)
            sync.assert_awaited_once_with(full=True, owner_chat_id=1234)
            run.assert_awaited_once_with([full_analysis_args(1234)], timeout=900)
            load.assert_not_awaited()
            save.assert_awaited_once_with('analysis', 1234, 'full-report')
            send.assert_awaited_once_with(context, 1234, 'full-report')

    async def test_full_is_visible_and_help_describes_measurement(self):
        from scripts import telegram_bot as bot
        self.assertIn('analisis_full', dict(bot.BOT_COMMAND_SPECS))
        self.assertIn('guarda decisiones válidas en rueda', bot.help_text())


@unittest.skipIf(os.environ.get('TEST_COMPONENT') == 'telegram', 'Scheduler image only')
class FullSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def run_case(self, formal, market=True, refresh_ok=True, snapshot_age_hours=None):
        from src.scheduler import runner
        cfg = SimpleNamespace(scraper=SimpleNamespace(telegram_bot_token='', telegram_chat_id='1234'), database=SimpleNamespace(url='unused'))
        process = SimpleNamespace(returncode=0, communicate=AsyncMock(return_value=(b'full-report ' * 12, b'')))
        with patch.object(runner, 'get_config', return_value=cfg), \
             patch.object(runner, '_is_business_day', return_value=True), \
             patch.object(runner, '_is_market_window', return_value=market), \
             patch.object(runner, 'request_portfolio_refresh', new_callable=AsyncMock, return_value={'ok': refresh_ok}) as refresh, \
             patch.object(runner.asyncio, 'create_subprocess_exec', new_callable=AsyncMock, return_value=process) as launch, \
             patch.object(runner, 'save_report_artifact', new_callable=AsyncMock) as save, \
             patch.object(runner, 'run_verify_decision_prices', new_callable=AsyncMock), \
             patch.object(runner, '_latest_snapshot_age_hours', new_callable=AsyncMock, return_value=snapshot_age_hours), \
             patch.object(runner.asyncio, 'sleep', new_callable=AsyncMock):
            await runner.run_daily_analysis(formal=formal, notify=False)
            return refresh, launch, save

    async def test_intraday_uses_same_full_command_and_measurement(self):
        refresh, launch, save = await self.run_case(True)
        self.assertEqual(list(launch.call_args.args)[1:], full_analysis_args(1234))
        self.assertTrue(refresh.call_args.kwargs['include_market'])
        self.assertEqual(save.call_args.kwargs['metadata']['analysis_profile'], ANALYSIS_FULL_PROFILE)
        self.assertEqual(save.call_args.kwargs['metadata']['job'], 'analysis_full_measurement')
        self.assertEqual(launch.await_count, 1)

    async def test_no_formal_measurement_outside_session(self):
        refresh, launch, save = await self.run_case(True, market=False)
        refresh.assert_not_awaited(); launch.assert_not_awaited(); save.assert_not_awaited()

    async def test_failed_refresh_without_recent_snapshot_does_not_create_measurement(self):
        # Refresh is retried once; with no snapshot newer than the fallback window, no measurement.
        refresh, launch, save = await self.run_case(True, refresh_ok=False, snapshot_age_hours=None)
        self.assertEqual(refresh.await_count, 2); launch.assert_not_awaited(); save.assert_not_awaited()

    async def test_failed_refresh_with_recent_snapshot_still_measures(self):
        refresh, launch, save = await self.run_case(True, refresh_ok=False, snapshot_age_hours=2.0)
        self.assertEqual(refresh.await_count, 2)
        self.assertEqual(list(launch.call_args.args)[1:], full_analysis_args(1234))

    async def test_closing_report_remains_exploratory_with_full_radar(self):
        refresh, launch, save = await self.run_case(False)
        refresh.assert_not_awaited()
        self.assertEqual(list(launch.call_args_list[0].args)[1:], full_analysis_args(1234, persist=False))


if __name__ == '__main__':
    unittest.main(verbosity=2)
