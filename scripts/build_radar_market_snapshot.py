"""Replay frozen current-market inputs through the existing corrected Radar.

Default: files only. --publish appends one atomic capture to existing Radar tables,
without DDL, decision_log, market-history updates, alerts, optimizer or orders.
"""
from __future__ import annotations
import argparse
import asyncio
from dataclasses import asdict
from datetime import date, datetime, timezone
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
from src.collector.radar_close_evidence import assess_close, validate_candles, session_end, latest_due_session

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


def validate_owner_context(snapshot, session, cutoff):
    """A next-morning replay may use today's portfolio and yesterday's close."""
    snapshot_stamp = datetime.fromisoformat(snapshot['scraped_at'])
    if (snapshot_stamp > cutoff or snapshot_stamp.astimezone(ART).date() < session
            or latest_due_session(cutoff) != session):
        raise ValueError('Stale/future owner context or market session')


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
    # Legacy Cocos rows have scrape timestamps, not a dated official close.
    # Matching a last trade after 17:15 is insufficient to certify settlement.
    corroborated = False
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
    requested_session = manifest.get('market_session')
    daily = requested_session is not None
    session = date.fromisoformat(requested_session) if daily else cutoff.astimezone(ART).date()
    ratio_receipt = None
    if daily:
        ratio_receipt = read_json(args.ratios.parent/'ratios-source.json')
        if ratio_receipt['sha256'] != sha256(args.ratios.read_bytes()).hexdigest():
            raise ValueError('Ratio file digest mismatch')
        cutoff = max(cutoff, datetime.fromisoformat(ratio_receipt['retrieved_at']))
    if daily and cutoff < session_end(session):
        raise ValueError('Cannot calculate before session close')
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
    if daily:
        validate_owner_context(snapshot, session, cutoff)
    positions = snapshot['positions']
    assets = context['asset_types']
    quotes = {q['ticker']:q for q in context['universe']}
    frames, provenance, rejected = {}, {}, {}
    for ticker in assets:
        try:
            payload = read_json(args.inputs/'candles'/f'{ticker}.json')
            if daily:
                validate_candles(payload, session)
            evidence = market_evidence(payload, ratio_map.get(ticker), cutoff, quotes.get(ticker))
            if daily:
                source_path = args.inputs/'byma-close'/f'{ticker}.json'
                corroboration = assess_close(payload, read_json(source_path) if source_path.exists() else None,
                                             session, cutoff)
                evidence.update({k:v for k,v in corroboration.items() if k!='missing_data'})
                evidence['missing_data'] += corroboration['missing_data']
                if payload['candles'][-1]['asset_type'] == 'CEDEAR':
                    ratio = evidence.get('ratio')
                    try:
                        ratio_date = datetime.strptime((ratio or {}).get('as_of', ''), '%d.%m.%Y').date()
                        if ratio_date > session:
                            raise ValueError('future ratio')
                    except (ValueError, TypeError):
                        evidence['ratio'] = None
                        evidence['missing_data'].append('fecha de ratio no verificada para la rueda')
                if corroboration['confirmed_close']:
                    evidence['price_basis'] = 'POST_SESSION_CLOSE_CORROBORATED_BYMADATA_TV'
                    evidence['missing_data'].remove('cierre sin corroboración independiente')
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
    if daily:
        from scripts.run_opportunity import _apply_manual_event_risk_to_report
        _apply_manual_event_risk_to_report(report, context.get('manual_event_risks') or {})
    observations = build_discovery_observations(report,universe=sorted(assets), history_frames=frames,
                    asset_types=assets, portfolio_tickers=held, selected_tickers=[], min_score=.10,min_rr=0.)
    for obs in observations:
        extra = provenance.get(obs.ticker)
        if extra:
            obs.metadata['top10_evidence'].update(extra, ccl=ccl)
            obs.metadata['top10_evidence']['missing_data'] += ['metadatos históricos PIT incompletos', 'contexto shadow previo omitido; no se reutilizan scores']
            if daily and context.get('manual_event_risks') is None:
                obs.metadata['top10_evidence']['missing_data'].append('eventos manuales no verificados')
            tech = report.discovery_technical_signals.get(obs.ticker)
            if tech:
                obs.metadata['top10_evidence'].update(technical_signal=tech.signal,
                    indicator_values=tech.data_quality.get('indicator_values'),
                    indicator_quality=tech.data_quality, indicators_as_of=extra['bar_start'])
                obs.metadata['top10_evidence']['missing_data'] += list(tech.data_quality.get('missing_features',[]))
    inputs_hash=sha256(json.dumps(manifest['sha256'],sort_keys=True).encode()+args.ratios.read_bytes()
        +(json.dumps(ratio_receipt,sort_keys=True).encode() if ratio_receipt else b'')).hexdigest()
    code_hash=sha256(b''.join(p.encode()+b'\0'+(ROOT/p).read_text(encoding='utf8').replace('\r\n','\n').encode()
               for p in (*SCORING_FINGERPRINT_FILES, 'src/analysis/contextual_contracts.py',
                         'src/collector/radar_close_evidence.py', 'config/radar_market_policy.json',
                         'config/market_holidays_ar.json', 'scripts/build_radar_market_snapshot.py'))).hexdigest()
    version=f'radar-market-v1:{code_hash[:16]}:{inputs_hash[:16]}'
    run=dict(run_id=str(uuid5(NAMESPACE_URL,version)), captured_at=cutoff.isoformat(),
             captured_session=session.isoformat(), scoring_version=version,
             universe_count=len(assets)-len({'SPY','QQQ'}&set(assets)), eligible_count=sum(o.radar_eligible for o in observations),
             parameters=dict(top10_publication='v1', input_sha256=inputs_hash, engine_sha256=code_hash,
                input_manifest=manifest['sha256'], snapshot_at=snapshot['scraped_at'], fresh_portfolio_scores=True,
                omitted_contexts=['previous_shadow_scores'], full_operational_ranking_impact='NOT_VERIFIABLE',
                thresholds=dict(min_score=.10,min_rr=0.), collected=len(frames), rejected=rejected,
                ccl=ccl, ratio_file_sha256=sha256(args.ratios.read_bytes()).hexdigest()))
    if daily:
        run['parameters'].update(daily_job='top10-daily-v1', market_session=session.isoformat(),
            ratio_source_receipt=ratio_receipt,
            independent_close_count=sum(bool(p.get('independent_close')) for p in provenance.values()),
            corroborated_close_count=sum(bool(p.get('confirmed_close')) for p in provenance.values()),
            close_conflicts=sum(p.get('close_comparison')=='DIFFERENT' for p in provenance.values()))
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
                    observations=observations,parameters=run['parameters'],initialize_schema=False,
                    evidence_session=date.fromisoformat(run['captured_session']))
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
