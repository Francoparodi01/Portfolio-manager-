"""I/O-only adapter for frozen /analisis; run in read-only, network-none Docker.

Native scores, guards, optimizer, funding and renderer are retained. Cash-only
entry planning is completed by the backtest adapter. Command: /analisis_full. Explicit
scenario restriction: daily technical bars must precede the cutoff session;
their native ts/scraped_at filters also remain enforced. Macro uses recovered
previous closed daily observations, never a future daily close.
"""
import os
os.environ.clear()
os.environ.update(PATH='/usr/local/bin:/usr/bin:/bin', HOME='/tmp', MPLCONFIGDIR='/tmp/mpl', PYTHONDONTWRITEBYTECODE='1')
import sys, json, asyncio, io, math, dataclasses, enum, contextlib, hashlib, traceback
from pathlib import Path
from datetime import datetime as RealDatetime, date as RealDate, timezone, timedelta
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from collections import Counter
from src.backtesting.ledger import Ledger, dt
from src.backtesting.store import write
from src.backtesting.bridge import install_cash_bridge, prepare_spec
import pandas as pd
from scripts import run_analysis as engine
from src.analysis import macro, opportunity_screener as opportunity

ROOT = Path(sys.argv[1]).resolve()
EVIDENCE = ROOT/'input'
SCHEDULE = prepare_spec(json.loads((ROOT/'job.json').read_text())['spec'])
CAPTURE = json.loads((EVIDENCE/'research_data_capture.json').read_text())
ROWS = {x['name']: x['rows'] for x in CAPTURE['observations']}
TICKERS = SCHEDULE['tickers'] or sorted({r['ticker'] for r in ROWS['intraday_points']})
if not TICKERS: raise ValueError('EMPTY_UNIVERSE')
ROWS['universe_quote_snapshots']=ROWS['intraday_points']
members={}
for r in ROWS['intraday_points']:
    key=(r['ts'][:10],r['ticker'])
    if key not in members or r['ts']<members[key]['first_positive_ts']:
        members[key]=dict(market_date=key[0],ticker=key[1],first_positive_ts=r['ts'])
ROWS['universe_daily_membership']=list(members.values())
ROWS['corporate_effects']=[]
for row in ROWS['corporate_full']:
    e=json.loads(row['event']) if isinstance(row['event'],str) else row['event']
    i=json.loads(row['effect']) if isinstance(row['effect'],str) else row['effect']
    if e['lifecycle_status'] not in ['ANNOUNCED','CONFIRMED','EFFECTIVE']:continue
    ROWS['corporate_effects'].append(dict(**i,effective_at=e['effective_at'],event_created_at=e['created_at'],
        event_updated_at=e['updated_at'],effect_created_at=i['created_at'],effect_updated_at=i['updated_at']))
BOOK = Ledger(ROWS,TICKERS,SCHEDULE['initial_cash'],SCHEDULE['fee_rate'],SCHEDULE['slippage_rate'],SCHEDULE['latency_seconds'])
ART = ZoneInfo('America/Argentina/Buenos_Aires')
NOW = None
TRACE = {}

def dt(x):
    if x is None: return None
    if isinstance(x, RealDatetime): return x
    return RealDatetime.fromisoformat(str(x).replace('Z', '+00:00'))

def clean(x):
    if x is None or isinstance(x, (str, bool, int)): return x
    if isinstance(x, float): return x if math.isfinite(x) else None
    if isinstance(x, enum.Enum): return clean(x.value)
    if isinstance(x, (RealDatetime, RealDate)): return x.isoformat()
    if dataclasses.is_dataclass(x): return clean(dataclasses.asdict(x))
    if isinstance(x, dict): return {str(k): clean(v) for k,v in x.items()}
    if isinstance(x, (list, tuple, set)): return [clean(v) for v in x]
    if hasattr(x, 'item'): return clean(x.item())
    if hasattr(x, '__dict__'): return clean(vars(x))
    return str(x)

class DateMeta(type):
    def __instancecheck__(cls, obj): return isinstance(obj, RealDatetime)
class FrozenDatetime(RealDatetime, metaclass=DateMeta):
    @classmethod
    def now(cls, tz=None): return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)
    @classmethod
    def utcnow(cls): return NOW.astimezone(timezone.utc).replace(tzinfo=None)

def freeze():
    for name, mod in list(sys.modules.items()):
        if mod is not None and (name.startswith('src.') or name == 'scripts.run_analysis'):
            if getattr(mod, 'datetime', None) is RealDatetime:
                mod.datetime = FrozenDatetime

MACRO_DATA=json.loads((EVIDENCE/'macro.json').read_text())

def recovered_macro():
    snap=macro.MacroSnapshot(fetched_at=NOW)
    lineage={}
    for field,series in MACRO_DATA['series'].items():
        tz=ZoneInfo(series['timezone'])
        eligible=[r for r in series['points'] if dt(r['ts']).astimezone(tz).date()<NOW.astimezone(tz).date()]
        if len(eligible)<2:continue
        values=[r['close'] for r in eligible]
        setattr(snap,field,values[-1]);setattr(snap,field+'_chg',(values[-1]/values[-2]-1)*100)
        if hasattr(snap,field+'_trend'):setattr(snap,field+'_trend',macro._trend_slope(pd.Series(values),20))
        lineage[field]=dict(status='RECONSTRUCTED_PREVIOUS_CLOSED_DAILY_NOT_ORIGINAL_VINTAGE',
                            latest_label=eligible[-1]['ts'],source=series['source'])
    for field,series in MACRO_DATA['argentina'].items():
        eligible=[r for r in series['points'] if r.get('fecha','9999')<NOW.astimezone(ART).date().isoformat()]
        if eligible:
            row=max(eligible,key=lambda r:r['fecha']);setattr(snap,field,row[series['value']])
            lineage[field]=dict(status='PREVIOUS_DATE_PUBLICATION_TIME_UNKNOWN',date=row['fecha'],source=series['source'])
    TRACE['macro_lineage']=lineage
    TRACE['macro_missing_native_none']=[k for k,v in snap.to_dict().items() if v is None]
    if snap.vix is None or snap.sp500 is None:raise ValueError('MACRO_HISTORY_MISSING_AT_CUTOFF')
    return snap

class LocalConnection:
    async def __aenter__(self): return self
    async def __aexit__(self,*args): pass
    async def fetch(self, query, lookup, asof, hours, policy, scorer, retrieval):
        assert 'FROM sentiment_aggregated' in query
        assert asof == NOW
        eligible = []
        for row in ROWS['sentiment_aggregates']:
            src = json.loads(row['sources']) if isinstance(row['sources'],str) else row['sources']
            bucket = dt(row['bucket_ts'])
            updated = dt(row.get('updated_at'))
            if row['ticker'] not in lookup or not NOW-timedelta(hours=hours) <= bucket <= NOW: continue
            if updated is None or updated > NOW: continue
            if (src.get('_policy'),src.get('_scorer'),src.get('_ticker_retrieval_policy')) != (policy,scorer,retrieval): continue
            eligible.append(dict(row, sources=src, bucket_ts=bucket))
        selected = {}
        for row in sorted(eligible,key=lambda r:r['bucket_ts']): selected[row['ticker']] = row
        TRACE.setdefault('sentiment_reads',[]).append(dict(lookup=lookup, ids=[r['id'] for r in selected.values()],
              native_policy=policy, native_scorer=scorer, retrieval_policy=retrieval,
              additional_causality_filter='updated_at <= cutoff'))
        return list(selected.values())

class LocalPool:
    def acquire(self): return LocalConnection()

class LocalDatabase:
    def __init__(self,*args,**kwargs): pass
    async def connect(self): TRACE['local_db_connect_calls'] = TRACE.get('local_db_connect_calls',0)+1
    async def close(self): pass
    async def get_pool(self): return LocalPool()
    async def get_cocos_universe_assets(self):
        eligible=[r for r in MEMBERS if r['_first']<=NOW]
        count=Counter(r['market_date'] for r in eligible)
        valid=[day for day,n in count.items() if n>=50]
        if not valid:
            TRACE['universe_membership']=dict(status='NO_NATIVE_COMPLETE_DAY',count=0)
            return []
        selected_day=max(valid)
        selected={}
        for r in QUOTES:
            if selected_day<=r['_ts'].date().isoformat() and r['_ts']<=NOW:
                t=r['ticker']
                if t not in selected or r['_ts']>selected[t]['_ts']:selected[t]=r
        assets=[dict(ticker=t,asset_type=r['asset_type'],currency=r['currency'],last_price=r['last_price'],change_pct_1d=r.get('change_pct_1d'),ts=r['_ts']) for t,r in sorted(selected.items()) if t in TICKERS]
        TRACE['universe_membership']=dict(status='HISTORICAL_NATIVE_FRESHNESS_RULE',selected_day=selected_day,count=len(assets),tickers=[r['ticker'] for r in assets])
        return assets
    async def get_market_candles(self,ticker,cutoff,limit,asset_type=None,currency=None,**kwargs):
        assert cutoff==NOW
        eligible=[]
        for r in CANDLES.get(ticker,[]):
            if asset_type and r['asset_type']!=asset_type:continue
            if currency and r['currency']!=currency:continue
            if r['interval']!=kwargs.get('interval','1d'):continue
            if r['ts']>NOW or r['scraped_at'] is None or r['scraped_at']>NOW:continue
            # Do not expose a same-session daily OHLC before its close.
            if r['ts'].date()>=NOW.astimezone(ART).date():continue
            eligible.append(r)
        identities={(r['ticker'],r['asset_type'],r['currency'],r['venue'],r['interval']) for r in eligible}
        long_ids={r['long_ticker'] for r in eligible if not r['long_ticker'].startswith(('INTERNAL:','TV:','YAHOO:'))}
        if max(len(identities),len(long_ids))>1:raise ValueError('AMBIGUOUS_SERIES_IDENTITY')
        priority={'COCOS':0,'TRADINGVIEW_BYMA':1,'internal_snapshot':2}
        selected={}
        for r in eligible:
            key=(r['ticker'],r['asset_type'],r['currency'],r['venue'],r['interval'],r['ts'].date())
            rank=(priority.get(r['source'],3),-r['scraped_at'].timestamp(),-r['ts'].timestamp())
            if key not in selected or rank<selected[key][0]:selected[key]=(rank,r)
        rows=sorted([r for rank,r in selected.values()],key=lambda r:r['ts'])[-limit:]
        TRACE.setdefault('candle_reads',[]).append(dict(ticker=ticker,rows=len(rows),latest_ts=rows[-1]['ts'] if rows else None,source_counts=dict(Counter(r['source'] for r in rows)),same_session_excluded=True,mode='NATIVE_SOURCE_RANK_AND_SCRAPED_AT_CUTOFF'))
        return rows

async def load_portfolio(*args,**kwargs):
    positions,nav,cash=BOOK.snapshot(NOW)
    snap=dict(scraped_at=NOW,total_value_ars=nav,cash_ars=cash,positions=positions,source='SIMULATED_USER_CASH_SEQUENTIAL')
    TRACE['input_positions']=clean(positions); TRACE['input_cash_ars']=cash; TRACE['input_nav_ars']=nav
    return positions,nav,cash,[],snap,None

async def empty_events(*args,**kwargs): return []
async def manual_events(*args,**kwargs):
    from src.analysis.manual_market_events import manual_market_event_from_row
    selected=[]
    for row in ROWS['manual_events']:
        if any(dt(row.get(k)) is None or dt(row[k])>NOW for k in ['created_at','updated_at']):continue
        if row['is_active'] and dt(row['active_from'])<=NOW<=dt(row['active_until']):
            r=dict(row,event_date=RealDate.fromisoformat(row['event_date']),active_from=dt(row['active_from']),active_until=dt(row['active_until']))
            selected.append(manual_market_event_from_row(r))
    return selected
async def empty_corporate(*args,**kwargs):
    from src.analysis.corporate_actions import corporate_action_effect_from_row,price_quality_flag_from_mapping
    effects=[]; flags=[]
    for entry in ROWS['corporate_full']:
        e=entry['event']; i=entry['effect']
        if isinstance(e,str):e=json.loads(e)
        if isinstance(i,str):i=json.loads(i)
        if any(dt(x.get(k)) is None or dt(x[k])>NOW for x in [e,i] for k in ['created_at','updated_at']):continue
        if not i['is_active'] or e['lifecycle_status'] not in ['ANNOUNCED','CONFIRMED','EFFECTIVE']:continue
        if not NOW-timedelta(days=730)<=dt(e['effective_at'])<=NOW+timedelta(days=7):continue
        merged={**e,**i,'effect_id':i['id'],'event_id':e['id']}
        effects.append(corporate_action_effect_from_row(merged))
    for r in ROWS['quality_flags']:
        if any(dt(r.get(k)) is None or dt(r[k])>NOW for k in ['created_at','updated_at','observed_at']):continue
        if r['resolution_status']=='OPEN' and (not r.get('expires_at') or dt(r['expires_at'])>=NOW):flags.append(price_quality_flag_from_mapping(r))
    TRACE['corporate_context_counts']=dict(effects=len(effects),flags=len(flags))
    return effects,flags
async def issuer_context(*args,**kwargs):
    grouped={}
    for row in ROWS['issuer_events']:
        if any(dt(row.get(k)) is None or dt(row[k]) > NOW for k in ['created_at','updated_at']): continue
        if row.get('source_published_at') and dt(row['source_published_at']) > NOW: continue
        r=dict(row,event_date=RealDate.fromisoformat(row['event_date']))
        grouped.setdefault(row['ticker'],[]).append(r)
    TRACE['eligible_issuer_event_count']=sum(len(v) for v in grouped.values())
    return grouped
async def empty_ic(*args,**kwargs): return {}

# Native logic is kept; wrappers below observe outputs and calls only.
native_render=engine.render_report
def observe_render(**kwargs):
    for key in ['results','universe_results','execution_plan','rebalance_report','opportunity_report',
                'external_universe_tickers','portfolio_risk','macro_snap','positions','total_ars','cash_ars']:
        TRACE[key]=clean(kwargs.get(key))
    return native_render(**kwargs)
native_guard=engine.guard_history_frames
def observe_guard(*args,**kwargs):
    result=native_guard(*args,**kwargs)
    TRACE.setdefault('guard_calls',[]).append(dict(input_tickers=list(args[0]),
            output_tickers=list(result.frames), flags=clean(result.flags), blocked_by_ticker=clean(result.blocked_by_ticker)))
    return result
native_screen=opportunity.screen_universe
def observe_screen(*args,**kwargs):
    result=native_screen(*args,**kwargs)
    TRACE['screening']=clean(result)
    return result
def observe_call(name,fn):
    def wrapped(*args,**kwargs):
        TRACE['call_counts'][name]+=1
        return fn(*args,**kwargs)
    return wrapped

engine.get_config=lambda: SimpleNamespace(database=SimpleNamespace(url='LOCAL_MEMORY_ONLY'),
    scraper=SimpleNamespace(telegram_bot_token='',telegram_chat_id='',telegram_enabled=False))
engine.PortfolioDatabase=LocalDatabase
engine._load_portfolio=load_portfolio
engine.fetch_macro=recovered_macro
engine._load_active_manual_market_events=manual_events
engine._load_upcoming_earnings_events=empty_events
engine._load_issuer_event_context=issuer_context
engine._load_corporate_action_context=empty_corporate
engine._compute_information_coefficient=empty_ic
engine.render_report=observe_render
engine.guard_history_frames=observe_guard
opportunity.screen_universe=observe_screen
for name in ['run_optimizer','derive_decision_intents','reconcile_funding','run_opportunity_analysis']:
    setattr(engine,name,observe_call(name,getattr(engine,name)))
freeze()


# Memoize deterministic technical outputs for identical frames and provenance.
# Monotone cutoffs with no AFTER_CUTOFF reason preserve eligibility. Timestamp fields
# are updated; all portfolio, sentiment, macro, optimizer and planner calls still run.
import copy
from src.analysis import technical as tech
native_technical=tech.analyze_ticker_from_frame
TECH_CACHE={};CACHE_STATS=dict(hits=0,misses=0,equality_checks=0)
def cached_technical(ticker,frame):
    attrs={k:v for k,v in frame.attrs.items() if k!='cutoff'}
    key=(ticker,hashlib.sha256((frame.to_json(date_format='iso',date_unit='ns',orient='split')+json.dumps(clean(attrs),sort_keys=True)).encode()).hexdigest())
    cutoff=frame.attrs.get('cutoff')
    cutoff=dt(cutoff) if cutoff is not None else None
    entry=TECH_CACHE.get(key)
    if entry and cutoff is not None and cutoff>=entry[0]:
        result=copy.deepcopy(entry[1]);result.generated_at=FrozenDatetime.now(timezone.utc)
        result.data_quality['cutoff']=pd.Timestamp(cutoff).isoformat()
        CACHE_STATS['hits']+=1
        # Independent recomputation on first five hits and first hit of each new date.
        check_key=(NOW.date(),)
        if CACHE_STATS['equality_checks']<5 or check_key not in TECH_CACHE:
            check=native_technical(ticker,frame)
            assert clean(check)==clean(result),('TECHNICAL_CACHE_MISMATCH',ticker)
            CACHE_STATS['equality_checks']+=1;TECH_CACHE[check_key]=True
        return result
    CACHE_STATS['misses']+=1
    result=native_technical(ticker,frame)
    if result is not None and cutoff is not None and cutoff.tzinfo and not any(str(r).startswith(('AFTER_CUTOFF_','UNKNOWN_CUTOFF','NAIVE_CUTOFF')) for r in result.data_quality.get('provenance_reasons',[])):
        TECH_CACHE[key]=(cutoff,copy.deepcopy(result))
    return result
tech.analyze_ticker_from_frame=cached_technical

async def run():
    global NOW,TRACE
    limit=len(SCHEDULE['cutoffs'])
    label='replay'
    output_dir=ROOT/label
    output_dir.mkdir(exist_ok=True)
    summaries=[]; daily=[]
    offset=0
    error=None
    for i,cutoff in enumerate(SCHEDULE['cutoffs'][offset:limit],start=offset):
        if (ROOT/'cancel').exists():raise SystemExit(2)
        NOW=dt(cutoff).astimezone(timezone.utc)
        TRACE=dict(cutoff_art=cutoff,scenario=SCHEDULE['variant'],engine_variant=label,
            call_counts={k:0 for k in ['run_optimizer','derive_decision_intents','reconcile_funding','run_opportunity_analysis']},main_started=True,
            optional_events_status='LIMITED_RECONSTRUCTION_NATIVE_FALLBACKS',local_db_connect_calls=0)
        error=None; stdout=io.StringIO()
        try:
            with contextlib.redirect_stdout(stdout):
                result=await engine.main(tickers_override=[],period='1y',no_telegram=True,no_llm=True,
                    no_sentiment=False,no_optimizer=False,skip_radar=False,no_persist=True,run_intent='exploratory')
            TRACE['main_completed']=True
            TRACE['main_return']=clean(result)
            plan=clean(result['execution_plan'])
            end=min(dt(SCHEDULE['asof']),NOW.astimezone(ART).replace(hour=17,minute=0,second=0,microsecond=0))
            if i+1<len(SCHEDULE['cutoffs']) and dt(SCHEDULE['cutoffs'][i+1]).date()==NOW.astimezone(ART).date():
                end=min(end,dt(SCHEDULE['cutoffs'][i+1]))
            before=len(BOOK.orders)
            TRACE['simulated_fills']=BOOK.execute(plan,NOW,end)
            TRACE['simulation_orders']=BOOK.orders[before:]
            TRACE['valuation_at_cutoff_before_fills']=dict(nav_ars=TRACE['input_nav_ars'],cash_ars=TRACE['input_cash_ars'])
            TRACE['valuation_after_interval']=BOOK.valuation(end-timedelta(microseconds=1))
            TRACE['technical_cache']=dict(CACHE_STATS)
            TRACE['corporate_adjustments']=list(BOOK.adjustments)
            if i==len(SCHEDULE['cutoffs'])-1 or dt(SCHEDULE['cutoffs'][i+1]).date()!=dt(cutoff).date():
                daily.append(BOOK.valuation(end,eod=True))
            if plan is not None:
                assert plan['cash_after']>=-0.01
                assert not ({o['ticker'] for o in plan['buy_orders']} & {o['ticker'] for o in plan['sell_orders']})
        except Exception as exc:
            error=f'{type(exc).__name__}: {exc}'
            TRACE['main_completed']=False; TRACE['error']=error; TRACE['traceback']=traceback.format_exc()
        (output_dir/f'cut_{i+1:03d}.html').write_text(stdout.getvalue(),encoding='utf-8')
        write(output_dir/f'cut_{i+1:03d}.json',clean(TRACE))
        summary=dict(cutoff_art=cutoff,completed=TRACE.get('main_completed'),error=error,held_results=len(TRACE.get('results') or []),
            universe_results=len(TRACE.get('universe_results') or []),plan_present=TRACE.get('execution_plan') is not None,
            fills=len(TRACE.get('simulated_fills') or []),cash_ars=float(BOOK.cash),quantities={t:q for t,q in BOOK.qty.items() if q},call_counts=TRACE['call_counts'])
        summaries.append(summary)
        write(ROOT/'progress.json',dict(completed=i+1,total=limit,cutoff=cutoff,error=error))
        print(json.dumps(summary,ensure_ascii=False),flush=True)
        if error: break
    for name,obj in [('summary',summaries),('orders',BOOK.orders),('fills',BOOK.fills),('daily',daily)]:
        write(output_dir/(name+'.json'),clean(obj))
    if error: raise SystemExit(1)

MEMBERS=[dict(r,_first=dt(r['first_positive_ts'])) for r in ROWS['universe_daily_membership']]
QUOTES=[dict(r,_ts=dt(r['ts'])) for r in ROWS['universe_quote_snapshots']]
CANDLES={}
for source in ROWS['daily_candles']:
    r=dict(source)
    for key in ['ts','scraped_at','bar_start','bar_end','available_at']:r[key]=dt(r.get(key))
    CANDLES.setdefault(r['ticker'],[]).append(r)

# Hard fail on any accidental external I/O, even if the native engine catches it.
import socket
original_socket=socket.socket
NETWORK_ATTEMPTS=[]
class NoNetworkSocket(original_socket):
    def connect_ex(self,address):
        if self.family in (socket.AF_INET,socket.AF_INET6):
            NETWORK_ATTEMPTS.append('network');raise RuntimeError('BACKTEST_NETWORK_FORBIDDEN')
        return super().connect_ex(address)
    def connect(self,address):
        if self.family in (socket.AF_INET,socket.AF_INET6):
            NETWORK_ATTEMPTS.append('network');raise RuntimeError('BACKTEST_NETWORK_FORBIDDEN')
        return super().connect(address)
socket.socket=NoNetworkSocket
import asyncpg
async def forbid_write(*args,**kwargs):
    NETWORK_ATTEMPTS.append('database_or_write');raise RuntimeError('BACKTEST_PERSISTENCE_FORBIDDEN')
asyncpg.connect=forbid_write
engine._save_execution_plan_events=forbid_write
engine._persist_corporate_action_audit=forbid_write
install_cash_bridge(engine)
from src.backtesting.rotation import install_rotation_policy
ROTATION_AUDIT = install_rotation_policy(engine, SCHEDULE, BOOK, lambda: NOW)
try:
    asyncio.run(run())
finally:
    if ROTATION_AUDIT is not None:
        write(ROOT/'rotation_audit.json', ROTATION_AUDIT)
    (ROOT/'isolation.json').write_text(json.dumps(dict(external_io_attempts=NETWORK_ATTEMPTS,no_persist=True,no_telegram=True)),encoding='utf8')
if NETWORK_ATTEMPTS:raise RuntimeError('BACKTEST_EXTERNAL_IO_ATTEMPT')
