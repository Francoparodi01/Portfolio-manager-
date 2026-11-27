"""Reference curves for a finished replay: buy-and-hold of one CEDEAR and idle cash.

Same capture, ledger, cutoffs, costs, latency and end-of-day marks as the bot, so the
comparison stays on the same basis (ARS CEDEAR, CCL effect included). Research only.
"""
from datetime import timezone
import json
import math
from zoneinfo import ZoneInfo

from src.backtesting.ledger import Ledger, dt

ART = ZoneInfo('America/Argentina/Buenos_Aires')
REFERENCE = 'SPY'


def _effects(rows, ticker):
    out = []
    for row in rows:
        e = json.loads(row['event']) if isinstance(row['event'], str) else row['event']
        i = json.loads(row['effect']) if isinstance(row['effect'], str) else row['effect']
        if e['lifecycle_status'] in ('ANNOUNCED', 'CONFIRMED', 'EFFECTIVE') and i.get('ticker') == ticker:
            out.append(dict(**i, effective_at=e['effective_at'], event_created_at=e['created_at'],
                            event_updated_at=e['updated_at'], effect_created_at=i['created_at'], effect_updated_at=i['updated_at']))
    return out


def curve_stats(navs, capital):
    navs = [x for x in navs if x is not None]
    if len(navs) < 2:
        return None
    peak, drawdown = navs[0], 0.0
    for x in navs:
        peak = max(peak, x)
        drawdown = min(drawdown, (x/peak-1)*100)
    r = [navs[k]/navs[k-1]-1 for k in range(1, len(navs))]
    mean = sum(r)/len(r)
    sd = math.sqrt(sum((x-mean)**2 for x in r)/(len(r)-1)) if len(r) > 1 else 0.0
    return dict(final_nav_ars=navs[-1], return_pct=(navs[-1]/capital-1)*100,
                max_drawdown_pct=drawdown, vol_annual_pct=sd*math.sqrt(252)*100, days=len(navs))


def reference_curves(spec, observations, bot_daily, ticker=REFERENCE):
    """Buy everything at the first cutoff with an executable quote and hold; cash stays flat."""
    capital = float(spec['initial_cash'])
    raw = {x['name']: x['rows'] for x in observations}
    rows = dict(intraday_points=[r for r in raw.get('intraday_points', []) if r['ticker'] == ticker],
                daily_candles=[r for r in raw.get('daily_candles', []) if r['ticker'] == ticker],
                corporate_effects=_effects(raw.get('corporate_full', []), ticker))
    base = dict(ticker=ticker, assumptions=dict(
        reference=f'{ticker} CEDEAR ARS: compra todo en el primer corte con precio y mantiene, mismos costos y precios que el bot.',
        cash='Efectivo ARS sin interés ni ajuste por inflación.',
        marks='NAV a valor de mercado al cierre, sin costo de salida (igual que el bot).'))
    if not rows['intraday_points']:
        return dict(base, status='NOT_EVALUABLE', reason=f'Sin precios de {ticker} en la captura.', daily=[])
    book = Ledger(rows, [ticker], spec['initial_cash'], spec['fee_rate'], spec['slippage_rate'], spec['latency_seconds'])
    cutoffs, asof = spec['cutoffs'], dt(spec['asof'])
    entry, waited, days = None, None, []
    for i, cut in enumerate(cutoffs):
        now = dt(cut).astimezone(timezone.utc)
        if now > asof:
            break
        end = min(asof, now.astimezone(ART).replace(hour=17, minute=0, second=0, microsecond=0))
        if i+1 < len(cutoffs) and dt(cutoffs[i+1]).date() == now.astimezone(ART).date():
            end = min(end, dt(cutoffs[i+1]))
        if entry is None:  # the ledger caps the quantity by available cash
            order = dict(ticker=ticker, quantity_est=10**9, reason='REFERENCE_BUY_AND_HOLD')
            fills = book.execute(dict(feasible=True, sell_orders=[], buy_orders=[order]), now, end)
            if fills:
                entry, waited = fills[0], i
        if i == len(cutoffs)-1 or dt(cutoffs[i+1]).date() != dt(cut).date():
            try:
                v = book.valuation(end, eod=True)
            except ValueError:
                return dict(base, status='NOT_EVALUABLE', reason=f'Sin precio de {ticker} para valuar {end.date()}.', daily=[])
            mark = v['assets'][0]['mark']['basis'] if v['assets'] else None
            days.append(dict(date=v['asof'][:10], asof=v['asof'], reference_nav_ars=v['nav_ars'], cash_nav_ars=capital, reference_mark=mark))
    if entry is None:
        return dict(base, status='NOT_EVALUABLE', reason=f'{ticker} nunca tuvo precio ejecutable en el período.', daily=days)
    bot = {d['asof'][:10]: d['nav_ars'] for d in bot_daily or []}
    for d in days:
        d['bot_nav_ars'] = bot.get(d['date'])
    qty = book.qty[ticker]
    price = float(book.mark(ticker, dt(days[-1]['asof']), eod=True)[0])
    fee, slip = float(spec['fee_rate']), float(spec['slippage_rate'])
    paired = [d for d in days if d['bot_nav_ars'] is not None]
    reference, bot_stats = curve_stats([d['reference_nav_ars'] for d in days], capital), curve_stats([d['bot_nav_ars'] for d in days], capital)
    return dict(base, status='COMPLETE',
                entry=dict(cutoff=entry['cutoff'], cutoffs_waited=waited, filled_at=entry['filled_at'], quantity=entry['quantity'],
                           fill_price=entry['fill_price'], fee_ars=entry['fee_ars'], slippage_ars=entry['slippage_ars'], cash_left_ars=entry['cash_after']),
                reference=reference, cash=dict(final_nav_ars=capital, return_pct=0.0, max_drawdown_pct=0.0, vol_annual_pct=0.0, days=len(days)),
                bot=bot_stats,
                reference_after_exit_nav_ars=float(book.cash)+qty*price*(1-slip)*(1-fee),
                excess_vs_reference_pp=bot_stats['return_pct']-reference['return_pct'] if bot_stats and reference else None,
                excess_vs_cash_pp=bot_stats['return_pct'] if bot_stats else None,
                bot_days_above_reference_pct=100*sum(d['bot_nav_ars'] > d['reference_nav_ars'] for d in paired)/len(paired) if paired else None,
                bot_days_missing=len(days)-len(paired),
                stale_reference_marks=sum(d['reference_mark'] not in (None, 'RECORDED_INTRADAY_LAST') for d in days),
                daily=days)
