"""Replay frozen current-market inputs through the existing corrected Radar.

Default: files only. --publish appends one atomic capture to existing Radar tables,
without DDL, decision_log, market-history updates, alerts, optimizer or orders.
"""
from __future__ import annotations
import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sys
from uuid import uuid5, NAMESPACE_URL
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.collect_radar_market import connection_config, write_json
from src.collector.cocos_history import candles_to_frame
from src.analysis.technical import analyze_portfolio_from_frames
from src.analysis.risk import build_portfolio_risk_report
from src.analysis.macro import MacroSnapshot, get_macro_regime, score_macro_for_ticker
from src.analysis.synthesis import blend_scores
from src.analysis.signal_aggregator import SentimentContext
from src.analysis.opportunity_screener import run_opportunity_analysis
from src.analysis.radar_discovery import build_discovery_observations, RadarDiscoveryStore, SCORING_FINGERPRINT_FILES
from src.monitor.opportunities import present_top_opportunities
from src.core.top10_telegram import render_top10

ART = ZoneInfo('America/Argentina/Buenos_Aires')
ROOT = Path(__file__).resolve().parents[1]
COMAFI_URL = 'https://www.comafi.com.ar/custodiaglobal/Multimedios/otros/14779.xlsx?v=192'


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def read_ratios(path):
    """Exact ticker + ISIN mapping, no aliases or ADR/common-share assumptions."""
    ns = {'x': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    with zipfile.ZipFile(path) as z:
        strings = [''.join(n.itertext()) for n in ET.fromstring(z.read('xl/sharedStrings.xml')).findall('x:si', ns)]
        rows = []
        for row in ET.fromstring(z.read('xl/worksheets/sheet1.xml')).findall('.//x:row', ns):
            cells = {}
            for cell in row.findall('x:c', ns):
                value = cell.find('x:v', ns)
                if value is None: continue
                cells[''.join(c for c in cell.attrib['r'] if c.isalpha())] = strings[int(value.text)] if cell.get('t')=='s' else value.text
            rows.append((row.get('r'), cells))
    declared_date = next((c.get('H') for _, c in rows if c.get('H') and c.get('F','').startswith('LISTA TOTAL')), None)
    result = {}
    for row, c in rows:
        if c.get('C') and ':' in c.get('H','') and c.get('E','').startswith('AR'):
            t = c['C'].strip().upper()
            value = dict(ratio=c['H'], isin=c['E'], underlying_isin=c.get('F'),
                         underlying_type=c.get('K'), as_of=declared_date, source=COMAFI_URL,
                         sheet='LISTA TOTAL DE CEDEARS', row=int(row))
            if t in result and result[t] != value:
                raise ValueError('Ambiguous depositary ticker: '+t)
            result[t] = value
    return result


def valid_ccl(calls, cutoff):
    for call in calls:
        if call.get('url','').endswith('/contadoconliqui') and call.get('status')==200:
            body = json.loads(call['body'])
            stamp = datetime.fromisoformat(body['fechaActualizacion'].replace('Z','+00:00'))
            received = datetime.fromisoformat(call['received_at'])
            if stamp <= received <= cutoff and (cutoff-stamp).total_seconds() <= 86400 and body.get('venta',0)>0:
                return dict(value=body['venta'], bid=body.get('compra'), currency='ARS/USD',
                            as_of=stamp.isoformat(), retrieved_at=received.isoformat(), source=call['url'],
                            basis='REFERENCE_CCL_ASK_NOT_INSTRUMENT_IMPLIED', quality='VERIFIED_SOURCE_TIMESTAMP')
    return dict(value=None, quality='NOT_VERIFIED', basis='REFERENCE_CCL_ASK_NOT_INSTRUMENT_IMPLIED')


def market_evidence(payload, ratio, cutoff, broker_quote=None):
    meta = payload['evidence'].get('symbol_metadata', {})
    rows = payload['candles']
    if not rows: raise ValueError('NO_CANDLES')
    if meta.get('exchange') != 'BYMA' or meta.get('currency_code') != 'ARS':
        raise ValueError('UNVERIFIED_MARKET_OR_CURRENCY')
    if meta.get('name') != rows[-1]['ticker']:
        raise ValueError('UNVERIFIED_SYMBOL_ALIAS')
    dates = [datetime.fromisoformat(c['ts']) for c in rows]
    if len(set(dates)) != len(dates) or dates != sorted(dates) or any(t>cutoff for t in dates):
        raise ValueError('INVALID_BAR_TIMESTAMPS')
    received = datetime.fromisoformat(payload['evidence']['received_at'])
    if received > cutoff: raise ValueError('FUTURE_RETRIEVAL')
    # Exchange schedule is versioned. Today's bar is not an exchange-confirmed
    # close merely because the clock passed the close time.
    session = dates[-1].astimezone(ART).date()
    local = received.astimezone(ART)
    delayed = meta.get('delay')
    regular_end_hour = 18 if session.isoformat() >= '2026-11-02' else 17
    end = datetime(session.year, session.month, session.day, regular_end_hour, 15, tzinfo=ART)
    after_close = delayed is not None and (local-end).total_seconds() >= float(delayed)
    basis = 'POST_SESSION_REPORTED_CLOSE_UNCONFIRMED' if after_close else 'INTRADAY_PROVISIONAL'
    corroborated = False
    if after_close and broker_quote:
        bt = datetime.fromisoformat(str(broker_quote['ts']))
        corroborated = (broker_quote.get('currency')=='ARS' and bt.astimezone(ART).date()==session
                        and end <= bt <= cutoff and float(broker_quote.get('last_price') or 0)==float(rows[-1]['close_price']))
    # Feed corroboration is explicit; it is not an official settlement assertion.
    if corroborated: basis = 'POST_SESSION_CLOSE_CORROBORATED_COCOS_TV'
    ratio_ok = ratio and meta.get('isin')==ratio['isin']
    previous = rows[-2] if len(rows)>1 else None
    return dict(currency='ARS', source='TRADINGVIEW_BYMA', venue='BYMA', provider_symbol=meta.get('full_name'),
                source_delay_seconds=delayed, retrieved_at=received.isoformat(),
                price_updated_at=None, bar_start=rows[-1]['ts'], market_session=session.isoformat(),
                price_basis=basis, previous_reported_close=(dict(price=previous['close_price'], bar_start=previous['ts']) if previous else None),
                confirmed_close=(dict(price=rows[-1]['close_price'], session=session.isoformat(),
                                      confirmation='COCOS_TV_AGREEMENT_NOT_OFFICIAL_SETTLEMENT') if corroborated else None),
                volume=rows[-1].get('volume'), volume_unit=None,
                ratio=ratio if ratio_ok else None,
                missing_data=([] if ratio_ok or rows[-1]['asset_type']!='CEDEAR' else ['ratio CEDEAR/ADR no verificado por ISIN'])
                     + ['hora exacta del último negocio no provista', 'unidad de volumen no declarada por proveedor']
                     + ([] if corroborated else ['cierre sin corroboración independiente']))


def build(args):
    context = read_json(args.inputs/'context-private.json')
    manifest = read_json(args.inputs/'manifest.json')
    for name, digest in manifest['sha256'].items():
        if sha256((args.inputs/name.replace('\\','/')).read_bytes()).hexdigest()!=digest:
            raise ValueError('Input digest mismatch: '+name)
    cutoff = datetime.fromisoformat(manifest['finished_at'])
    yahoo = read_json(args.inputs/'macro-yahoo.json')
    macro_dates = [datetime.fromisoformat(x.replace('Z','+00:00')) for x in yahoo['index']]
    if not macro_dates or any(d.replace(tzinfo=timezone.utc) > cutoff for d in macro_dates):
        raise ValueError('Missing or future macro bar dates')
    ratio_map = read_ratios(args.ratios)
    macro_data = read_json(args.inputs/'macro.json')
    macro_data['fetched_at'] = datetime.fromisoformat(macro_data['fetched_at'])
    macro = MacroSnapshot(**{k:v for k,v in macro_data.items() if k in MacroSnapshot.__dataclass_fields__})
    for key,value in macro_data.items():
        if key not in MacroSnapshot.__dataclass_fields__:
            setattr(macro,key,value)
    ccl = valid_ccl(read_json(args.inputs/'macro-sources.json'), cutoff)
    macro.ccl = ccl.get('value')
    snapshot = context['snapshot']
    positions = snapshot['positions']
    assets = context['asset_types']
    quotes = {q['ticker']:q for q in context['universe']}
    frames, provenance, rejected = {}, {}, {}
    for ticker in assets:
        try:
            payload = read_json(args.inputs/'candles'/f'{ticker}.json')
            evidence = market_evidence(payload, ratio_map.get(ticker), cutoff, quotes.get(ticker))
            frame = candles_to_frame(payload['candles'])
            # Current ratio is display evidence ONLY, not a historical ratio backfill.
            frame.attrs['cutoff'] = cutoff
            frame.attrs['series_identity']['adjustment_policy'] = 'provider_requested_splits'
            frames[ticker], provenance[ticker] = frame, evidence
        except (ValueError, KeyError, FileNotFoundError) as exc:
            rejected[ticker] = str(exc)
    sentiments = {k:SentimentContext(**v) for k,v in context['sentiment'].items()}
    cash = float(snapshot['cash_ars'])
    total = sum(float(p['market_value']) for p in positions)+cash
    risk = build_portfolio_risk_report(positions, {t:f.Close for t,f in frames.items()}, total,
                                       cash, context['portfolio_history'], macro.vix)
    risks = {p['ticker']:p for p in risk.positions}
    held = {p['ticker'] for p in positions}
    technical = analyze_portfolio_from_frames({t:f for t,f in frames.items() if t in held})
    portfolio_scores = {}
    # Same CORE synthesis inputs/formula as run_analysis; never decision_log scores.
    for tech in technical:
        sent = sentiments.get(tech.ticker)
        if sent is None or not sent.active: sent = sentiments.get('MACRO')
        active = sent is not None and sent.active
        score = max(-1.,min(1.,sent.score*max(sent.confidence,.2))) if active else 0.
        result = blend_scores(ticker=tech.ticker, technical_signal=tech.signal,
                    technical_strength=tech.strength, technical_score_raw=tech.score_raw,
                    macro_score=score_macro_for_ticker(tech.ticker,macro)[0], risk_position=risks[tech.ticker],
                    sentiment_score=score, skip_sentiment=not active,
                    technical_candle_source_mode=tech.candle_source_mode,
                    technical_has_reconstructed_candles=tech.has_reconstructed_candles,
                    technical_candle_sources=tech.candle_sources,
                    technical_candle_source_counts=tech.candle_source_counts)
        portfolio_scores[tech.ticker]=result.final_score
    if held-set(portfolio_scores):
        raise ValueError('Cannot replace missing fresh portfolio scores with zero')
    report = run_opportunity_analysis(universe=sorted(assets), portfolio_positions=positions,
                    macro_snap=macro, macro_regime=get_macro_regime(macro),
                    portfolio_scores=portfolio_scores, history_frames=frames, asset_types=assets,
                    available_cash_ars=cash, sentiment_contexts=sentiments, shadow_contexts={},
                    exclude_portfolio=False, capture_discovery=True, max_candidates=10, min_score=.10, min_rr=0.)
    report.generated_at=cutoff
    observations = build_discovery_observations(report,universe=sorted(assets), history_frames=frames,
                    asset_types=assets, portfolio_tickers=held, selected_tickers=[], min_score=.10,min_rr=0.)
    for obs in observations:
        extra = provenance.get(obs.ticker)
        if extra:
            obs.metadata['top10_evidence'].update(extra, ccl=ccl)
            obs.metadata['top10_evidence']['missing_data'] += ['metadatos históricos PIT incompletos', 'contexto shadow previo omitido; no se reutilizan scores']
            tech = report.discovery_technical_signals.get(obs.ticker)
            if tech:
                obs.metadata['top10_evidence'].update(technical_signal=tech.signal,
                    indicator_values=tech.data_quality.get('indicator_values'),
                    indicator_quality=tech.data_quality, indicators_as_of=extra['bar_start'])
                obs.metadata['top10_evidence']['missing_data'] += list(tech.data_quality.get('missing_features',[]))
    inputs_hash=sha256(json.dumps(manifest['sha256'],sort_keys=True).encode()+args.ratios.read_bytes()).hexdigest()
    code_hash=sha256(b''.join(p.encode()+b'\0'+(ROOT/p).read_text(encoding='utf8').replace('\r\n','\n').encode()
               for p in (*SCORING_FINGERPRINT_FILES, 'src/analysis/contextual_contracts.py', 'scripts/build_radar_market_snapshot.py'))).hexdigest()
    version=f'radar-market-v1:{code_hash[:16]}:{inputs_hash[:16]}'
    run=dict(run_id=str(uuid5(NAMESPACE_URL,version)), captured_at=cutoff.isoformat(),
             captured_session=cutoff.astimezone(ART).date().isoformat(), scoring_version=version,
             universe_count=len(assets)-len({'SPY','QQQ'}&set(assets)), eligible_count=sum(o.radar_eligible for o in observations),
             parameters=dict(top10_publication='v1', input_sha256=inputs_hash, engine_sha256=code_hash,
                input_manifest=manifest['sha256'], snapshot_at=snapshot['scraped_at'], fresh_portfolio_scores=True,
                omitted_contexts=['previous_shadow_scores'], full_operational_ranking_impact='NOT_VERIFIABLE',
                thresholds=dict(min_score=.10,min_rr=0.), collected=len(frames), rejected=rejected,
                ccl=ccl, ratio_file_sha256=sha256(args.ratios.read_bytes()).hexdigest()))
    run['parameters']['macro_provenance'] = dict(source='yfinance + Argentina APIs',
              requested_at=macro_data['fetched_at'].isoformat(), latest_bar_date=max(macro_dates).date().isoformat(),
              timestamp_precision='daily bar date; not last trade time', missing_fields=[k for k,v in macro_data.items() if v is None])
    candidates=[asdict(o) for o in observations if o.radar_eligible and o.screener_passed and not o.in_portfolio
                and o.rank_position is not None and o.ticker not in {'SPY','QQQ'}]
    payload=present_top_opportunities(run,candidates,as_of=cutoff)
    args.output.mkdir(parents=True, exist_ok=False)
    write_json(args.output/'run.json',run)
    write_json(args.output/'observations-private.json',[asdict(o) for o in observations])
    write_json(args.output/'top10.json',payload)
    write_json(args.output/'portfolio-scores-private.json',portfolio_scores)
    (args.output/'telegram-preview.html').write_text('\n\n'.join(render_top10(payload)),encoding='utf8')
    print(json.dumps(dict(count=payload['count'],run_id=run['run_id'],collected=len(frames),rejected=rejected)))
    return context,run,observations


async def publish(args,context,run,observations):
    import asyncpg
    dsn,owner=connection_config(args.env_file,args.host,args.port)
    if owner!=context['owner_chat_id']: raise ValueError('Owner mismatch')
    pool=await asyncpg.create_pool(dsn,min_size=1,max_size=1,command_timeout=60)
    try:
        async with pool.acquire() as conn:
            for table in ('radar_discovery_runs','radar_discovery_snapshots'):
                if not await conn.fetchval('SELECT to_regclass($1)', 'public.'+table):
                    raise ValueError('Schema missing; migrations forbidden')
        result=await RadarDiscoveryStore(pool).save_snapshot(run_id=run['run_id'], owner_chat_id=owner,
                    captured_at=datetime.fromisoformat(run['captured_at']),scoring_version=run['scoring_version'],
                    observations=observations,parameters=run['parameters'],initialize_schema=False)
        write_json(args.output/'publication.json',result)
        print(json.dumps(result))
    finally: await pool.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inputs',type=Path,required=True)
    p.add_argument('--ratios',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--publish',action='store_true')
    p.add_argument('--env-file',type=Path)
    p.add_argument('--host')
    p.add_argument('--port',type=int,default=5432)
    args=p.parse_args()
    context,run,observations=build(args)
    if args.publish: asyncio.run(publish(args,context,run,observations))
