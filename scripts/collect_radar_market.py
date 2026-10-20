"""Freeze fresh inputs using existing collectors. SELECT-only DB; no alerts/orders.

Outputs contain private owner context: keep them outside version control.
The directory must not exist, so a subsequent collection cannot rewrite evidence.
"""
from __future__ import annotations
import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import shutil
from unittest.mock import patch
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import asyncpg
from dotenv import dotenv_values
from scripts.backfill_tradingview_byma import fetch_tv_candles
from src.collector.db import PortfolioDatabase
from src.collector.data.normalizer import is_market_ticker_candidate
from src.analysis.signal_aggregator import load_sentiment_contexts


def write_json(path, value):
    with Path(path).open('x', encoding='utf8') as f:
        json.dump(value, f, default=str, ensure_ascii=False, indent=2, allow_nan=False)


def connection_config(env_file, host=None, port=5432):
    config = dict(os.environ)
    if env_file:
        config.update(dotenv_values(env_file))
    owner = int(config.get('TELEGRAM_CHAT_ID') or 0)
    if owner <= 0:
        raise ValueError('Explicit configured owner required')
    dsn = config['DATABASE_URL'].replace('postgresql+asyncpg://', 'postgresql://', 1)
    if host:
        p = urlsplit(dsn)
        dsn = urlunsplit((p.scheme, p.netloc.rsplit('@', 1)[0] + f'@{host}:{port}', p.path, p.query, p.fragment))
    return dsn, owner


def collect_macro(output):
    # Observe the existing collector responses without introducing a second formula.
    from src.analysis import macro
    import requests
    original_download, original_get = macro.yf.download, requests.get
    calls = []

    def download(*args, **kwargs):
        frame = original_download(*args, **kwargs)
        frame.to_json(output / 'macro-yahoo.json', orient='split', date_format='iso')
        calls.append({'source': 'yfinance', 'symbols': args[0], 'received_at': datetime.now(timezone.utc).isoformat()})
        return frame

    def get(url, *args, **kwargs):
        result = original_get(url, *args, **kwargs)
        calls.append({'url': url, 'status': result.status_code, 'body': result.text,
                      'received_at': datetime.now(timezone.utc).isoformat()})
        return result

    with patch.object(macro.yf, 'download', download), patch.object(macro.requests, 'get', get):
        snapshot = macro.fetch_macro()
    write_json(output / 'macro.json', snapshot.to_dict())
    write_json(output / 'macro-sources.json', calls)


async def collect(args):
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / 'candles').mkdir()
    dsn, owner = connection_config(args.env_file, args.host, args.port)
    started = datetime.now(timezone.utc)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, command_timeout=30,
                                    server_settings={'default_transaction_read_only': 'on'})
    try:
        db = PortfolioDatabase(dsn)
        db._pool = pool  # Deliberately do not call connect(), which performs DDL.
        snapshot = await db.get_latest_snapshot(owner_chat_id=owner)
        if not snapshot or datetime.fromisoformat(snapshot['scraped_at']) > started:
            raise ValueError('Missing or future owner snapshot')
        assets = await db.get_cocos_universe_assets(fresh_only=False)
        history = await db.get_portfolio_history(limit=180, owner_chat_id=owner)
        universe = {a['ticker']: a['asset_type'] for a in assets if is_market_ticker_candidate(a['ticker'])}
        universe.update({p['ticker']: p['asset_type'] for p in snapshot['positions'] if is_market_ticker_candidate(p['ticker'])})
        universe.update({'SPY': 'CEDEAR', 'QQQ': 'CEDEAR'})
        async with pool.acquire() as conn:
            sentiments = await load_sentiment_contexts(conn, list(universe), as_of=started)
            from src.analysis.manual_market_events import active_event_risk_by_ticker, manual_market_event_from_row
            event_rows = None
            if await conn.fetchval("SELECT to_regclass('public.manual_market_events')"):
                event_rows = await conn.fetch("""SELECT * FROM manual_market_events
                    WHERE is_active AND active_from <= $1 AND active_until >= $1""", started)
            event_risks = (active_event_risk_by_ticker(map(manual_market_event_from_row, event_rows))
                           if event_rows is not None else None)
        write_json(args.output / 'context-private.json', dict(owner_chat_id=owner, snapshot=snapshot,
                   universe=assets, asset_types=universe, portfolio_history=history,
                   manual_event_risks=event_risks,
                   sentiment={k: asdict(v) for k, v in sentiments.items()}, as_of=started.isoformat()))
    finally:
        await pool.close()
    semaphore = asyncio.Semaphore(4)
    results = []

    async def one(ticker, asset_type):
        evidence = {}
        async with semaphore:
            try:
                candles = await fetch_tv_candles(ticker, asset_type, bars=args.bars, evidence=evidence)
                result = {'ticker': ticker, 'rows': len(candles), 'status': 'OK' if candles else 'EMPTY'}
                write_json(args.output / 'candles' / f'{ticker}.json',
                           {'evidence': evidence, 'candles': [c.to_dict() for c in candles]})
            except Exception as exc:
                result = {'ticker': ticker, 'status': 'ERROR', 'error_type': type(exc).__name__}
                write_json(args.output / 'candles' / f'{ticker}.json', {'evidence': evidence, 'candles': [], 'error': result})
            results.append(result)
            if len(results) % 25 == 0:
                print(f'Collected {len(results)}/{len(universe)}', flush=True)
            await asyncio.sleep(.3)

    await asyncio.gather(*(one(t, a) for t, a in sorted(universe.items())))
    await asyncio.to_thread(collect_macro, args.output)
    close_session = getattr(args, 'close_session', None)
    if close_session:
        from src.collector.radar_close_evidence import collect_close_bundle
        await collect_close_bundle(args.output / 'byma-close',
            [r['ticker'] for r in results if r['status']=='OK'], close_session)
    files = {p.relative_to(args.output).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(args.output.rglob('*')) if p.is_file()}
    write_json(args.output / 'manifest.json', {'started_at': started.isoformat(),
               'finished_at': datetime.now(timezone.utc).isoformat(), 'results': results, 'sha256': files,
               'market_session': close_session.isoformat() if close_session else None})
    print(json.dumps({'requested': len(universe), 'ok': sum(r['status']=='OK' for r in results),
                      'errors': [r['ticker'] for r in results if r['status']!='OK']}))


async def retry_collection(args):
    """Retry failed symbols in a NEW bundle; preserve original failed responses."""
    parent = args.retry_inputs
    previous = json.loads((parent/'manifest.json').read_text(encoding='utf8'))
    if previous.get('market_session'):
        raise ValueError('Daily evidence must be recollected as a new complete bundle')
    context = json.loads((parent/'context-private.json').read_text(encoding='utf8'))
    _, owner = connection_config(args.env_file, args.host, args.port)
    if owner != context['owner_chat_id']: raise ValueError('Owner mismatch')
    args.output.mkdir(parents=True, exist_ok=False)
    failed = {r['ticker'] for r in previous['results'] if r['status']!='OK'}
    results = [r for r in previous['results'] if r['status']=='OK']
    for name, digest in previous['sha256'].items():
        src = parent/name.replace('\\','/')
        if hashlib.sha256(src.read_bytes()).hexdigest()!=digest: raise ValueError('Changed parent input')
        if name.startswith('macro'): continue
        target = args.output/name.replace('\\','/')
        if src.stem in failed: target = args.output/'previous-failures'/src.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src,target)
    for ticker in sorted(failed):
        evidence = {}
        try:
            candles = await fetch_tv_candles(ticker,context['asset_types'][ticker],bars=args.bars,evidence=evidence)
            result=dict(ticker=ticker,rows=len(candles),status='OK' if candles else 'EMPTY')
        except Exception as exc:
            candles=[]; result=dict(ticker=ticker,status='ERROR',error_type=type(exc).__name__)
        write_json(args.output/'candles'/f'{ticker}.json',dict(evidence=evidence,candles=[c.to_dict() for c in candles]))
        results.append(result)
        print(json.dumps(result),flush=True)
        await asyncio.sleep(.5)
    await asyncio.to_thread(collect_macro,args.output)
    files={p.relative_to(args.output).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
           for p in sorted(args.output.rglob('*')) if p.is_file()}
    write_json(args.output/'manifest.json',dict(started_at=previous['started_at'],
        finished_at=datetime.now(timezone.utc).isoformat(),results=results,sha256=files,
        parent_manifest_sha256=hashlib.sha256((parent/'manifest.json').read_bytes()).hexdigest()))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env-file', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--host')
    p.add_argument('--port', type=int, default=5432)
    p.add_argument('--bars', type=int, default=260)
    p.add_argument('--retry-inputs', type=Path, help='Retry failed symbols from a frozen bundle; never overwrite it')
    args=p.parse_args()
    asyncio.run(retry_collection(args) if args.retry_inputs else collect(args))
