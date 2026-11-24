import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src.backtesting import capture


T = datetime(2026, 7, 1, 14, tzinfo=timezone.utc)


def quote(n=0, price=100):
    return dict(ts=T + timedelta(seconds=n), ticker='AMD', asset_type='CEDEAR',
                currency='ARS', last_price=price, change_pct_1d=None)


class CursorConnection:
    def __init__(self, factory):
        self.factory = factory
        self.calls = []
        self.closed = False
        self.in_transaction = False

    def cursor(self, sql, *args, prefetch):
        self.calls.append((sql, args, prefetch))
        async def rows():
            for row in self.factory(sql):
                yield row
        return rows()

    def transaction(self, **kwargs):
        assert kwargs == dict(readonly=True, isolation='repeatable_read')
        return self

    async def __aenter__(self):
        self.in_transaction = True

    async def __aexit__(self, *args):
        self.in_transaction = False

    async def fetchval(self, sql):
        assert self.in_transaction
        return 'on'

    async def close(self):
        self.closed = True


class NullSink:
    def write(self, data):
        return len(data)


class CaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_more_than_old_intraday_ceiling_is_not_truncated(self):
        conn = CursorConnection(lambda _: (quote(n) for n in range(600001)))
        writer = capture.CaptureWriter(NullSink())
        count = await capture.capture_observation(conn, writer, 'intraday_points',
                                                  'SELECT ordered_quotes', [], None)
        self.assertEqual(count, 600001)
        self.assertNotIn('LIMIT', conn.calls[0][0])
        self.assertEqual(conn.calls[0][2], capture.CURSOR_PREFETCH)

    async def test_conflicts_across_cursor_batch_boundary_are_rejected(self):
        rows = [quote(n) for n in range(capture.CURSOR_PREFETCH)]
        rows.append(quote(capture.CURSOR_PREFETCH - 1, 101))
        conn = CursorConnection(lambda _: iter(rows))
        with self.assertRaisesRegex(ValueError, 'CONFLICTING_RECORDED_QUOTES'):
            await capture.capture_observation(conn, capture.CaptureWriter(io.BytesIO()),
                                               'intraday_points', 'SELECT quotes', [], None)

    async def test_identical_duplicates_and_distinct_instants_are_preserved(self):
        original = [quote(), quote(), quote(1, 101)]
        sink = io.BytesIO()
        n = await capture.capture_observation(CursorConnection(lambda _: iter(original)),
                    capture.CaptureWriter(sink), 'intraday_points', 'SELECT quotes', [], None)
        doc = json.loads(sink.getvalue())
        self.assertEqual(n, 3)
        self.assertEqual(doc['rows'], json.loads(json.dumps(original, default=str)))

    async def test_other_dataset_row_limits_remain_enforced(self):
        conn = CursorConnection(lambda _: iter([{'id': 1}, {'id': 2}]))
        with self.assertRaisesRegex(ValueError, 'DATA_LIMIT:events'):
            await capture.capture_observation(conn, capture.CaptureWriter(io.BytesIO()),
                                              'events', 'SELECT events', [], 1)
        self.assertTrue(conn.calls[0][0].endswith('LIMIT 2'))

    def test_byte_budget_uses_encoded_size(self):
        sink = io.BytesIO();writer = capture.CaptureWriter(sink, max_bytes=3)
        writer.write('á')
        with self.assertRaisesRegex(ValueError, 'CAPTURE_SIZE_LIMIT'):
            writer.write('é')
        self.assertEqual(sink.getvalue(), 'á'.encode('utf-8'))

    async def run_capture(self, factory, dest):
        conn = CursorConnection(factory)
        spec = dict(start='2026-07-01', asof='2026-09-25T17:00:00-03:00', tickers=[])
        with patch.object(capture.asyncpg, 'connect', new=AsyncMock(return_value=conn)):
            result = await capture.capture_database('postgresql://test', spec, dest)
        return result, conn

    async def test_publish_complete_compatible_json_and_close_connection(self):
        def source(sql):
            if 'FROM market_candles' in sql:return iter([{'ticker': 'AMD', 'close_price': 100}])
            if 'FROM market_prices' in sql:return iter([quote(), quote(1)])
            return iter([])
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            result, conn = await self.run_capture(source, dest)
            doc = json.loads((dest/'research_data_capture.json').read_text())
            self.assertEqual(result['intraday_points'], 2)
            self.assertEqual({r['name']: r['row_count'] for r in doc['observations']}, result)
            self.assertTrue(doc['transaction_read_only'])
            self.assertTrue(conn.closed)
            self.assertFalse((dest/'research_data_capture.json.tmp').exists())

    async def test_failed_capture_preserves_previous_file_and_cleans_temporary(self):
        def source(sql):
            if 'FROM market_candles' in sql:return iter([{'ticker': 'AMD'}])
            if 'FROM market_prices' in sql:return iter([quote(), quote(price=101)])
            return iter([])
        conn = CursorConnection(source)
        with tempfile.TemporaryDirectory() as tmp:
            dest=Path(tmp);target=dest/'research_data_capture.json';target.write_text('previous')
            with patch.object(capture.asyncpg, 'connect', new=AsyncMock(return_value=conn)):
                with self.assertRaisesRegex(ValueError, 'CONFLICTING_RECORDED_QUOTES'):
                    await capture.capture_database('postgresql://test',dict(start='2026-07-01',asof='2026-09-25T17:00:00-03:00',tickers=[]),dest)
            self.assertEqual(target.read_text(), 'previous')
            self.assertFalse((dest/'research_data_capture.json.tmp').exists())
            self.assertTrue(conn.closed)

    async def test_empty_market_data_is_not_published(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest=Path(tmp)
            with self.assertRaisesRegex(ValueError, 'No hay velas'):
                await self.run_capture(lambda _: iter([]),dest)
            self.assertFalse((dest/'research_data_capture.json').exists())

    async def test_output_open_failure_still_closes_database_connection(self):
        conn = CursorConnection(lambda _: iter([]))
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(capture.asyncpg, 'connect', new=AsyncMock(return_value=conn)):
                with patch.object(Path, 'open', side_effect=PermissionError('read only')):
                    with self.assertRaises(PermissionError):
                        await capture.capture_database('postgresql://test',dict(start='2026-07-01',asof='2026-09-25T17:00:00-03:00',tickers=[]),Path(tmp))
            self.assertTrue(conn.closed)


if __name__ == '__main__':
    unittest.main()
