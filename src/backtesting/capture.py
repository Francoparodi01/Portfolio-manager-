"""Bounded, read-only capture. User parameters are bound SQL values, never SQL."""
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import quote

import asyncpg
import requests

from src.backtesting.spec import ART


CAPTURE_MAX_BYTES = 512 * 1024 * 1024
CURSOR_PREFETCH = 5000


class CaptureWriter:
    """Stream the existing JSON format without retaining the full query in RAM."""

    def __init__(self, stream, max_bytes=CAPTURE_MAX_BYTES):
        self.stream = stream
        self.max_bytes = max_bytes
        self.size = 0

    def write(self, text):
        data = text.encode('utf-8')
        if self.size + len(data) > self.max_bytes:
            raise ValueError('CAPTURE_SIZE_LIMIT: la captura supera el presupuesto de almacenamiento del backtest; no se recortaron datos.')
        self.stream.write(data)
        self.size += len(data)

    def value(self, value):
        self.write(json.dumps(value, default=str, ensure_ascii=False, separators=(',', ':')))


async def capture_observation(conn, writer, name, sql, args, limit):
    writer.write('{"name":')
    writer.value(name)
    writer.write(',"query_sha256":')
    writer.value(hashlib.sha256(sql.encode()).hexdigest())
    writer.write(',"rows":[')
    count = 0
    previous_key = previous_price = None
    # The cursor belongs to capture_database's single repeatable-read transaction.
    query = sql if limit is None else sql + f' LIMIT {limit + 1}'
    async for record in conn.cursor(query, *args, prefetch=CURSOR_PREFETCH):
        row = dict(record)
        if limit is not None and count >= limit:
            raise ValueError(f'DATA_LIMIT:{name}: acortá el período o elegí menos activos')
        if name == 'intraday_points':
            # ORDER BY ts,ticker,asset_type makes equal identities adjacent,
            # including across cursor batches. Currency is fixed by the query.
            key = (row['ts'], row['ticker'], row['asset_type'], row['currency'])
            if key == previous_key and row['last_price'] != previous_price:
                raise ValueError('CONFLICTING_RECORDED_QUOTES')
            previous_key, previous_price = key, row['last_price']
        if count:
            writer.write(',')
        writer.value(row)
        count += 1
    writer.write('],"row_count":')
    writer.value(count)
    writer.write('}')
    return count


async def capture_database(dsn: str, spec: dict, dest: Path):
    start = datetime.fromisoformat(spec['start']).replace(tzinfo=ART)
    end = datetime.fromisoformat(spec['asof'])
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / 'research_data_capture.json'
    temporary = dest / 'research_data_capture.json.tmp'
    conn = await asyncpg.connect(dsn.replace('postgresql+asyncpg://', 'postgresql://'), timeout=20,
                               server_settings={'default_transaction_read_only': 'on', 'statement_timeout': '60000',
                                                'application_name': 'quantia_backtest_readonly'})
    counts = {}
    stream = None
    async def collect(name, sql, args, limit):
        if counts:
            writer.write(',')
        counts[name] = await capture_observation(conn, writer, name, sql, args, limit)
    try:
        stream = temporary.open('wb')
        writer = CaptureWriter(stream)
        writer.write('{"transaction_read_only":true,"source":"CURRENT_DATABASE_CAPTURE_NOT_ORIGINAL_VINTAGE","observations":[')
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            assert await conn.fetchval("SELECT current_setting('transaction_read_only')") == 'on'
            await collect('daily_candles', '''SELECT ts,ticker,long_ticker,asset_type,currency,venue,interval,
                open_price,high_price,low_price,close_price,volume,source,scraped_at,bar_start,bar_end,
                available_at,is_closed,volume_unit,calendar,calendar_validation,adjustment_policy,depositary_ratio
                FROM market_candles WHERE ts >= $1 AND ts <= $2 AND currency='ARS'
                AND (cardinality($3::text[])=0 OR ticker=ANY($3)) ORDER BY ticker,source,ts''',
                [start-timedelta(days=400),end,spec['tickers']], 350000)
            await collect('intraday_points', '''SELECT ts,ticker,asset_type,currency,last_price,change_pct_1d
                FROM market_prices WHERE ts >= $1 AND ts <= $2 AND last_price > 0 AND currency='ARS'
                ORDER BY ts,ticker,asset_type''', [start-timedelta(days=7),end], None)
            await collect('sentiment_aggregates', '''SELECT id,bucket_ts,ticker,asset_scope,score,confidence,
                event_count,high_impact_count,sources,updated_at FROM sentiment_aggregated
                WHERE bucket_ts >= $1 AND bucket_ts <= $2 ORDER BY bucket_ts,ticker''',
                [start-timedelta(days=1),end],50000)
            await collect('corporate_full', '''SELECT to_jsonb(e) AS event,to_jsonb(i) AS effect
                FROM corporate_events e JOIN corporate_event_instrument_effects i ON i.event_id=e.id
                WHERE e.effective_at >= $1 AND e.effective_at <= $2 ORDER BY e.effective_at,i.ticker''',
                [start-timedelta(days=730),end+timedelta(days=7)],5000)
            await collect('quality_flags', '''SELECT * FROM price_quality_flags
                WHERE observed_at <= $1 ORDER BY observed_at,id''',[end],10000)
            await collect('manual_events', '''SELECT * FROM manual_market_events
                WHERE active_from <= $1 AND active_until >= $2 ORDER BY active_from,id''',[end,start],5000)
            await collect('issuer_events', '''SELECT * FROM issuer_event_observations
                WHERE event_date >= $1 AND event_date <= $2 ORDER BY ticker,event_date,id''',
                [(start-timedelta(days=1)).date(),(end+timedelta(days=30)).date()],10000)
        if not counts['daily_candles'] or not counts['intraday_points']:
            raise ValueError('No hay velas o cotizaciones históricas para ese período.')
        writer.write('],"captured_at":')
        writer.value(datetime.now(timezone.utc).isoformat())
        writer.write('}')
        if stream is not None:
            stream.close()
        os.replace(temporary, target)
        return counts
    finally:
        try:
            if stream is not None:
                stream.close()
        finally:
            try:
                temporary.unlink(missing_ok=True)
            finally:
                await conn.close()


def capture_macro(spec: dict, dest: Path):
    from src.analysis.macro import MACRO_TICKERS
    start=datetime.fromisoformat(spec['start']).replace(tzinfo=ART)-timedelta(days=180)
    end=datetime.fromisoformat(spec['asof'])+timedelta(days=1)
    payload={'captured_at':datetime.now(timezone.utc).isoformat(),'series':{},'argentina':{},'errors':[]}
    for field,ticker in MACRO_TICKERS.items():
        url=f'https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker,safe="")}?period1={int(start.timestamp())}&period2={int(end.timestamp())}&interval=1d'
        try:
            response=requests.get(url,headers={'User-Agent':'Mozilla/5.0 Quantia historical research'},timeout=25)
            response.raise_for_status(); result=response.json()['chart']['result'][0]
            points=[dict(ts=datetime.fromtimestamp(t,timezone.utc).isoformat(),close=float(c))
                    for t,c in zip(result['timestamp'],result['indicators']['quote'][0]['close']) if c is not None]
            payload['series'][field]=dict(source=url,timezone=result['meta']['exchangeTimezoneName'],points=points)
        except Exception as exc:
            payload['errors'].append({'field':field,'error':type(exc).__name__})
    for field,endpoint,value in [('ccl','cotizaciones/dolares/contadoconliqui','venta'),('riesgo_pais','finanzas/indices/riesgo-pais','valor')]:
        url='https://api.argentinadatos.com/v1/'+endpoint
        try:
            response=requests.get(url,timeout=25);response.raise_for_status()
            points=response.json()
            if not isinstance(points,list):raise ValueError('Invalid series')
            payload['argentina'][field]=dict(source=url,value=value,points=points)
        except Exception as exc:
            payload['errors'].append({'field':field,'error':type(exc).__name__})
    (dest/'macro.json').write_text(json.dumps(payload,ensure_ascii=False),encoding='utf8')
    if not {'vix','sp500'}.issubset(payload['series']):
        raise ValueError('MACRO_UNAVAILABLE: no se recuperaron series históricas de VIX y S&P 500.')
    return list(payload['series'])


if __name__ == '__main__':
    import os
    import sys
    root=Path(sys.argv[1])
    spec=json.loads((root/'job.json').read_text())['spec']
    try:
        counts=asyncio.run(capture_database(os.environ['DATABASE_URL'],spec,root/'input'))
        capture_macro(spec,root/'input')
        print(json.dumps(dict(status='CAPTURED',rows=counts)),flush=True)
    except Exception as exc:
        message=str(exc) if isinstance(exc,ValueError) else 'No se pudo consultar una fuente histórica. Reintentá o acortá el período.'
        (root/'capture_error.json').write_text(json.dumps(dict(code=type(exc).__name__,message=message)),encoding='utf8')
        print(type(exc).__name__,flush=True)
        raise SystemExit(1)
