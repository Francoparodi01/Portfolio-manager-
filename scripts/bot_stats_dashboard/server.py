"""Owner-scoped local research dashboard. All DB connections are read-only."""
from __future__ import annotations
import argparse
import asyncio
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
import asyncpg
from dotenv import load_dotenv
from metrics import compute, number

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def owner_chat_id():
    return os.environ.get('OWNER_CHAT_ID') or os.environ.get('TELEGRAM_CHAT_ID')


async def verify_legacy_single_owner(conn, owner_id):
    """Allow NULL-owner legacy rows only when this is the sole explicit owner.

    This is stricter than treating NULL as a wildcard: the requested owner must
    exist and no operational source, including execution_plans, may name another.
    """
    requested_owner_exists = await conn.fetchval("""SELECT EXISTS (
        SELECT 1 FROM portfolio_snapshots WHERE owner_chat_id=$1
        UNION ALL SELECT 1 FROM decision_log WHERE owner_chat_id=$1
        UNION ALL SELECT 1 FROM broker_fills WHERE owner_chat_id=$1
        UNION ALL SELECT 1 FROM execution_plans WHERE owner_chat_id=$1
    )""", owner_id)
    other_owner_exists = await conn.fetchval("""SELECT EXISTS (
        SELECT 1 FROM portfolio_snapshots WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        UNION ALL SELECT 1 FROM decision_log WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        UNION ALL SELECT 1 FROM broker_fills WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        UNION ALL SELECT 1 FROM execution_plans WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
    )""", owner_id)
    return bool(requested_owner_exists and not other_owner_exists)


async def load(owner_id, days):
    if not owner_id or not 30 <= days <= 730:
        raise ValueError('Owner requerido; ventana entre 30 y 730 dias')
    dsn = os.environ['DATABASE_URL'].replace('postgresql+asyncpg://', 'postgresql://')
    conn = await asyncpg.connect(dsn, timeout=15, server_settings={
        'default_transaction_read_only': 'on', 'statement_timeout': '15000'})
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            db_time = await conn.fetchval('SELECT NOW()')
            readonly = await conn.fetchval("SELECT current_setting('transaction_read_only')")
            if readonly != 'on':
                raise RuntimeError('Read-only required')
            start = db_time-timedelta(days=days)
            legacy_owner_inferred = await verify_legacy_single_owner(conn, owner_id)
            # Separate plan count: an inner join hides plans without intents.
            plans = dict(await conn.fetchrow("""
                SELECT count(*) AS all_owner_plans,
                       count(*) FILTER (WHERE source='execution_plan') AS all_bot_plans,
                       count(*) FILTER (WHERE source='execution_plan' AND created_at >= $2) AS window_bot_plans,
                       count(*) FILTER (WHERE owner_chat_id=$1) AS explicit_owner_plans,
                       count(*) FILTER (WHERE owner_chat_id IS NULL) AS legacy_null_plans,
                       min(created_at) AS first_plan, max(created_at) AS last_plan
                FROM execution_plans
                WHERE (owner_chat_id=$1 OR ($4::boolean AND owner_chat_id IS NULL))
                  AND created_at <= $3
            """, owner_id, start, db_time, legacy_owner_inferred))
            rows = await conn.fetch("""
                SELECT p.id AS plan_id,p.run_id,p.created_at,p.source,p.feasible,
                       i.id AS intent_id,i.ticker,i.side,i.is_executable,i.was_blocked
                FROM execution_plans p JOIN order_intents i ON i.execution_plan_id=p.id
                WHERE (p.owner_chat_id=$1 OR ($4::boolean AND p.owner_chat_id IS NULL))
                  AND p.created_at >= $2 AND p.created_at <= $3
                ORDER BY p.created_at,i.id LIMIT 25001
            """, owner_id, start, db_time, legacy_owner_inferred)
            # Independent SQL denominator, without calling the Python selector.
            counts = dict(await conn.fetchrow("""
                SELECT count(*) AS raw_intents,
                  count(*) FILTER (WHERE p.source='execution_plan' AND p.feasible
                    AND i.is_executable AND i.was_blocked IS FALSE AND i.side IN ('BUY','SELL') AND trim(i.ticker)<>'') AS eligible_intents,
                  count(DISTINCT ((p.created_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,upper(trim(i.ticker)),i.side))
                    FILTER (WHERE p.source='execution_plan' AND p.feasible AND i.is_executable
                    AND i.was_blocked IS FALSE AND i.side IN ('BUY','SELL') AND trim(i.ticker)<>'') AS unique_signals
                FROM execution_plans p JOIN order_intents i ON i.execution_plan_id=p.id
                WHERE (p.owner_chat_id=$1 OR ($4::boolean AND p.owner_chat_id IS NULL))
                  AND p.created_at >= $2 AND p.created_at <= $3
            """, owner_id, start, db_time, legacy_owner_inferred))
            tickers = sorted({str(r['ticker']).strip().upper() for r in rows if r['source'] == 'execution_plan'})
            # Market facts have no owner: restrict to this owner's bot tickers.
            candles = await conn.fetch("""
                SELECT m.ticker,m.long_ticker,m.source,m.currency,m.venue,m.interval,
                       m.ts,m.open_price,m.close_price,m.scraped_at
                FROM market_candles m
                WHERE m.ticker=ANY($1::text[]) AND m.interval='1d' AND m.currency='ARS' AND m.venue='BYMA'
                  AND m.ts >= $2 AND m.ts <= $3 AND m.scraped_at <= $3
                ORDER BY m.ticker,m.ts LIMIT 250001
            """, tickers, start-timedelta(days=7), db_time) if tickers else []
            registry_available = bool(await conn.fetchval("SELECT to_regclass('public.corporate_events') IS NOT NULL AND to_regclass('public.corporate_event_instrument_effects') IS NOT NULL"))
            events = await conn.fetch("""
                SELECT e.event_type,e.lifecycle_status,e.effective_at,f.ticker,f.price_factor
                FROM corporate_events e JOIN corporate_event_instrument_effects f ON f.event_id=e.id
                WHERE f.ticker=ANY($1::text[]) AND f.is_active
                  AND (f.venue IS NULL OR f.venue='BYMA') AND (f.currency IS NULL OR f.currency='ARS')
                  AND e.effective_at >= $2 AND e.effective_at <= $3 LIMIT 10001
            """, tickers, start, db_time) if registry_available and tickers else []
            # Context only, not inputs to bot returns or account PnL.
            decisions = [dict(r) for r in await conn.fetch("""
                SELECT COALESCE(source,layers->>'source') AS source,status,count(*) AS n
                FROM decision_log
                WHERE (owner_chat_id=$1 OR ($4::boolean AND owner_chat_id IS NULL))
                  AND COALESCE(source,layers->>'source','') <> 'execution_plan'
                  AND decided_at >= $2 AND decided_at <= $3
                GROUP BY 1,status ORDER BY 1,status
            """, owner_id, start, db_time, legacy_owner_inferred)]
            account = dict(await conn.fetchrow("""
                SELECT (SELECT count(*) FROM portfolio_snapshots
                          WHERE (owner_chat_id=$1 OR ($4::boolean AND owner_chat_id IS NULL))
                          AND scraped_at >= $2 AND scraped_at <= $3) AS snapshots,
                       (SELECT count(*) FROM broker_fills
                          WHERE (owner_chat_id=$1 OR ($4::boolean AND owner_chat_id IS NULL))
                          AND executed_at >= $2 AND executed_at <= $3) AS raw_fills
            """, owner_id, start, db_time, legacy_owner_inferred))
        if len(rows)>25000 or len(candles)>250000 or len(events)>10000:
            raise ValueError('Extraccion excede el limite; reducir la ventana')
        return dict(rows=[dict(r) for r in rows], candles=[dict(r) for r in candles],
                    events=[dict(r) for r in events], events_available=registry_available,
                    as_of=db_time, start=start, plans=plans, counts=counts,
                    decisions=decisions, account=account, readonly=readonly,
                    legacy_owner_inferred=legacy_owner_inferred)
    finally:
        await conn.close()


async def stats(owner_id, days=180, cost=75):
    data = await load(owner_id, days)
    result = compute(data['rows'], data['candles'], as_of=data['as_of'], cost_bps=cost,
                     events=data['events'], events_available=data['events_available'])
    q, c = result['quality'], data['counts']
    reconciled = (q['raw_intents'] == c['raw_intents'] and q['unique_signals'] == c['unique_signals']
                  and q['same_day_duplicates'] == c['eligible_intents']-c['unique_signals'])
    if not reconciled:
        raise RuntimeError('Raw count reconciliation failed')
    result.update(as_of=data['as_of'].isoformat(), window_start=data['start'].isoformat(), days=days,
                  cost_bps=cost, raw_candles=len(data['candles']), plans=data['plans'],
                  raw_intents=len(data['rows']), sql_counts=c, counts_reconciled=reconciled,
                  readonly=data['readonly'], decision_sources=data['decisions'], account=data['account'],
                  legacy_owner_inferred=data['legacy_owner_inferred'],
                  owner_scope=('EXPLICIT_PLUS_VERIFIED_LEGACY_NULL' if data['legacy_owner_inferred'] else 'EXPLICIT_ONLY'),
                  corporate_events=len(data['events']), corporate_registry_available=data['events_available'],
                  account_realized_pnl=None, account_pnl_status='No medido: requiere conciliacion de lotes, costos y flujos; los fills no son PnL.')
    return result


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get('Host') not in (f'127.0.0.1:{self.server.server_port}', f'localhost:{self.server.server_port}'):
            self.send_error(403); return
        uri = urlsplit(self.path)
        if uri.path in ('/', '/index.html'):
            body, kind = (HERE/'index.html').read_bytes(), 'text/html; charset=utf-8'
        elif uri.path == '/api/stats':
            try:
                q = parse_qs(uri.query)
                days, cost = int(q.get('days', ['180'])[0]), number(q.get('cost_bps', ['75'])[0])
                if not 30 <= days <= 730 or cost is None or not 0 <= cost <= 400:
                    raise ValueError('Ventana 30-730 dias; costo 0-400 pb finitos')
                result = asyncio.run(stats(int(owner_chat_id()), days, cost))
                body, kind = json.dumps(result, ensure_ascii=False, default=str, allow_nan=False).encode(), 'application/json; charset=utf-8'
            except (KeyError, ValueError):
                self.send_error(400, 'Configuracion o parametros invalidos'); return
            except Exception as exc:
                self.log_message('Read failed: %s', type(exc).__name__)
                self.send_error(503, 'No se pudo verificar la base de datos'); return
        else:
            self.send_error(404); return
        self.send_response(200)
        self.send_header('Content-Type', kind)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, default=ROOT/'.env')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    load_dotenv(args.env_file, override=False)
    if not os.environ.get('DATABASE_URL') or not owner_chat_id():
        raise SystemExit('Se necesitan DATABASE_URL y OWNER_CHAT_ID o TELEGRAM_CHAT_ID')
    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    print(f'Dashboard local: http://127.0.0.1:{args.port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
