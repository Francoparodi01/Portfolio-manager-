"""Pure conservative price-return research; no DB, strategy or order writes."""
from collections import Counter, defaultdict
from datetime import date, timedelta, timezone
import json
import math
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo

ART = ZoneInfo('America/Argentina/Buenos_Aires')
HORIZONS = (5, 10, 20, 40)
SOURCES = ('COCOS', 'TRADINGVIEW_BYMA')
# Historical range verified against BYMA. Never modify the strategy calendar here.
CALENDAR_FROM = date(2026, 1, 1)
CALENDAR_THROUGH = date(2026, 10, 2)
CALENDAR = json.loads((Path(__file__).resolve().parents[2] / 'config/market_holidays_ar.json').read_text())
CLOSURES = {date.fromisoformat(x['date']) for x in CALENDAR['closures']}
CLOSURES |= {date.fromisoformat(x['date']) for x in CALENDAR['special_sessions'] if not x['trading']}


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def trading_day(day):
    return day.weekday() < 5 and day not in CLOSURES


def sessions_after(day, count=40):
    result = []
    while len(result) < count:
        day += timedelta(days=1)
        if trading_day(day):
            result.append(day)
    return result


def eligible(row):
    return (row.get('source') == 'execution_plan' and row.get('feasible') is True
            and row.get('is_executable') is True and row.get('was_blocked') is False
            and str(row.get('side', '')).upper() in ('BUY', 'SELL')
            and bool(str(row.get('ticker') or '').strip()))


def summarize(values, total, reasons):
    return {'n': len(values), 'pending': total-len(values),
            'coverage_pct': 100*len(values)/total if total else None,
            'mean_pct': 100*mean(values) if values else None,
            'median_pct': 100*median(values) if values else None,
            'wins': sum(v > 0 for v in values),
            'win_pct': 100*sum(v > 0 for v in values)/len(values) if values else None,
            'missing_reasons': dict(reasons)}


def compute(rows, candles, *, as_of, cost_bps=75.0, events=(), events_available=True):
    """First eligible intent/ART day/ticker/side, fixed BYMA sessions, no gap skipping.

    Price-only BUY change or SELL avoidance; equal signal weights. Source and
    instrument stay constant through the complete horizon. Known corporate events
    and unexplained >30% daily discontinuities fail closed without rebasing.
    """
    if number(cost_bps) is None or not 0 <= cost_bps <= 400:
        raise ValueError('cost_bps debe estar entre 0 y 400 y ser finito')
    cutoff = as_of.astimezone(ART).date() - timedelta(days=1)
    while not trading_day(cutoff):
        cutoff -= timedelta(days=1)
    quality = Counter(raw_intents=len(rows), unique_signals=0, ineligible_intents=0,
                      same_day_duplicates=0, future_intents=0)
    selected, seen = [], set()
    for row in sorted(rows, key=lambda r: (r['created_at'], r['intent_id'])):
        if row['created_at'] > as_of:
            quality['future_intents'] += 1
            continue
        if not eligible(row):
            quality['ineligible_intents'] += 1
            continue
        key = (row['created_at'].astimezone(ART).date(), str(row['ticker']).strip().upper(), str(row['side']).upper())
        if key in seen:
            quality['same_day_duplicates'] += 1
            continue
        seen.add(key)
        selected.append((key, row))
    quality['unique_signals'] = len(selected)
    series = defaultdict(dict)
    conflicts = set()
    for row in candles:
        source = row.get('source')
        if source not in SOURCES or (row.get('currency'), row.get('venue'), row.get('interval')) != ('ARS', 'BYMA', '1d'):
            quality['excluded_source_or_basis_candles'] += 1
            continue
        day = row['ts'].astimezone(timezone.utc).date()
        scraped = row.get('scraped_at')
        if (day > cutoff or not trading_day(day) or (scraped and
            (scraped > as_of or scraped.astimezone(ART).date() < day or
             (scraped.astimezone(ART).date() == day and scraped.astimezone(ART).hour < 18)))):
            quality['incomplete_or_non_session_candles'] += 1
            continue
        opening, closing = number(row['open_price']), number(row['close_price'])
        value = (opening, closing) if opening and closing and opening > 0 and closing > 0 else None
        if value is None:
            quality['invalid_candles'] += 1
        key = (str(row['ticker']).strip().upper(), source, row['long_ticker'])
        if day in series[key] and series[key][day] != value:
            conflicts.add((key, day))
        series[key][day] = None if (key, day) in conflicts else value
    quality['conflicting_candle_days'] = len(conflicts)
    results = []
    for (day, ticker, side), row in selected:
        dates = sessions_after(day)
        item = {'date': day.isoformat(), 'ticker': ticker, 'side': side,
                'plan_id': str(row['plan_id']), 'intent_id': row['intent_id'],
                'entry_date': dates[0].isoformat(), 'returns': {}, 'details': {}}
        for h in HORIZONS:
            key = str(h)
            path = dates[:h]
            detail = {'status': 'immature', 'exit_date': path[-1].isoformat()}
            value = None
            if path[-1] <= cutoff:
                if day < CALENDAR_FROM or path[-1] > CALENDAR_THROUGH:
                    detail['status'] = 'calendar_unverified'
                elif not events_available:
                    detail['status'] = 'corporate_registry_unavailable'
                elif any(e['ticker'].upper() == ticker and path[0] <= e['effective_at'].astimezone(ART).date() <= path[-1]
                         and e['lifecycle_status'] not in ('CANCELLED', 'DISMISSED', 'SUPERSEDED') for e in events):
                    detail['status'] = 'corporate_event'
                else:
                    options = []
                    for source in SOURCES:
                        candidates = [(k, v) for k, v in series.items() if k[0] == ticker and k[1] == source and all(v.get(d) for d in path)]
                        if candidates:
                            options = candidates
                            break
                    if not options:
                        detail['status'] = 'missing_or_conflicting_prices'
                    elif len({tuple(v[d] for d in path) for _, v in options}) > 1:
                        detail['status'] = 'ambiguous_instrument'
                    else:
                        series_key, prices = sorted(options, key=lambda x: x[0])[0]
                        opening, closing = prices[path[0]][0], prices[path[-1]][1]
                        jump = any(abs(prices[d][1]/prices[d][0]-1) > .30 for d in path)
                        jump |= any(abs(prices[b][0]/prices[a][1]-1) > .30 for a, b in zip(path, path[1:]))
                        if jump:
                            detail['status'] = 'unverified_discontinuity'
                        else:
                            gross = (closing/opening-1) * (1 if side == 'BUY' else -1)
                            value = gross-cost_bps/10000
                            detail.update(status='evaluated', entry_price=opening, exit_price=closing,
                                          source=series_key[1], long_ticker=series_key[2], gross_pct=gross*100)
            item['returns'][key] = value
            item['details'][key] = detail
        results.append(item)
    metrics = {}
    for h in HORIZONS:
        key = str(h)
        values = [s['returns'][key] for s in results if s['returns'][key] is not None]
        reasons = Counter(s['details'][key]['status'] for s in results if s['returns'][key] is None)
        metrics[key] = summarize(values, len(results), reasons)
    weekly = defaultdict(list)
    for s in results:
        if s['returns']['5'] is not None:
            d = date.fromisoformat(s['date'])
            weekly[(d-timedelta(days=d.weekday())).isoformat()].append(s['returns']['5'])
    signal_sides = Counter(s['side'] for s in results)
    evaluated_sources = Counter(
        detail['source']
        for s in results
        for detail in s['details'].values()
        if detail['status'] == 'evaluated'
    )
    return {'metrics': metrics, 'quality': dict(quality), 'signals': list(reversed(results)),
            'weekly_5d': [{'week': w, 'n': len(v), 'mean_pct': 100*mean(v)} for w,v in sorted(weekly.items())],
            'signal_sides': dict(signal_sides), 'evaluated_price_sources': dict(evaluated_sources),
            'price_cutoff': cutoff.isoformat(), 'calendar_verified_through': CALENDAR_THROUGH.isoformat()}
