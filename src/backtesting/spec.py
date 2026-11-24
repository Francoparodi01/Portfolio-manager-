from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import re
import shlex
import hashlib
import json
from zoneinfo import ZoneInfo

from src.core.market_calendar import is_trading_day
from src.backtesting.bridge import prepare_spec

ART = ZoneInfo('America/Argentina/Buenos_Aires')
METRICS = ('rentabilidad', 'riesgo', 'operaciones', 'efectividad', 'activos', 'horizontes')
MAX_SESSIONS = 120
ROTATION_DEFAULTS = dict(radar_buy_tiers='ALL', redeploy_cooldown_sessions=0,
                         min_hold_sessions=0, delta_band_mult=1)


def rotation_overrides(spec: dict) -> dict:
    """Research-only options. Absent values never consult runtime configuration."""
    raw = spec.get('rotation_overrides', {})
    if not isinstance(raw, dict) or set(raw) - ROTATION_DEFAULTS.keys():
        raise ValueError('Invalid backtest rotation overrides')
    result = {**ROTATION_DEFAULTS, **raw}
    allowed = dict(radar_buy_tiers=('ALL','SIN_ESPERAR_FUNDING','SOLO_ABRIR_POSICION'),
                   redeploy_cooldown_sessions=(0,3,5), min_hold_sessions=(0,3,5,10),
                   delta_band_mult=(1,2,3))
    for key, value in result.items():
        if value not in allowed[key] or isinstance(value, bool):
            raise ValueError(f'Invalid backtest override: {key}')
    return result


def spec_hash(spec: dict) -> str:
    return hashlib.sha256(json.dumps(spec,sort_keys=True,separators=(',',':'),
                                     ensure_ascii=False).encode('utf-8')).hexdigest()


def parse_date(value: str, today: date) -> date:
    if value.lower() == 'hoy':
        return today
    try:
        if '/' in value:
            parts = value.split('/')
            return date(int(parts[2]) if len(parts) == 3 else today.year, int(parts[1]), int(parts[0]))
        return date.fromisoformat(value)
    except (ValueError, IndexError) as exc:
        raise ValueError('Fecha inválida. Usá 26/10/2026 o 2026-10-26.') from exc


def number(value: str, *, capital=True) -> Decimal:
    text = value.lower().replace('$', '').replace('_', '')
    multiplier = Decimal(1000000) if text.endswith('m') else Decimal(1000) if text.endswith('k') else Decimal(1)
    if multiplier != 1:
        text = text[:-1]
    if capital and re.fullmatch(r'[1-9]\d{0,2}(\.\d{3})+', text):
        text = text.replace('.', '')
    try:
        result = Decimal(text.replace(',', '.')) * multiplier
    except InvalidOperation as exc:
        raise ValueError('Número inválido. Ejemplo: capital=3000000 o capital=3m.') from exc
    if not result.is_finite():
        raise ValueError('Los parámetros numéricos deben ser finitos.')
    return result


def parse_spec(text: str, now: datetime | None = None) -> dict:
    now = now or datetime.now(ART)
    now = now.astimezone(ART)
    tokens = shlex.split(text)
    if tokens and tokens[0].startswith('/backtest'):
        tokens.pop(0)
    options = {}
    names = {'desde', 'hasta', 'ruedas', 'capital', 'tickers', 'horas', 'medir', 'horizontes', 'comision', 'deslizamiento', 'arranque'}
    names.update(ROTATION_DEFAULTS)
    while tokens:
        token = tokens.pop(0)
        if '=' in token:
            key, value = token.split('=', 1)
        elif token.lower() in names and tokens:
            key, value = token, tokens.pop(0)
        else:
            raise ValueError('Parámetro desconocido. Ejemplo: /backtest hasta=26/10/2026 capital=3m')
        key = key.lower()
        if key not in names or key in options or not value:
            raise ValueError(f'Parámetro inválido o repetido: {key}')
        options[key] = value
    last_complete = now.date() if now.time() >= time(17, 0) else now.date() - timedelta(days=1)
    end = parse_date(options['hasta'], now.date()) if 'hasta' in options else last_complete
    if end > now.date():
        raise ValueError('La fecha final no puede estar en el futuro.')
    if end > last_complete:
        end = last_complete
    if 'desde' in options and 'ruedas' in options:
        raise ValueError('Usá desde o ruedas, no ambos.')
    if 'desde' in options:
        start = parse_date(options['desde'], now.date())
        if (end - start).days > 370 or start > end:
            raise ValueError('Revisá las fechas: el período debe ser positivo y de hasta 120 ruedas.')
    else:
        try:
            count = int(options.get('ruedas', '20'))
        except ValueError as exc:
            raise ValueError('Ruedas debe ser un entero entre 1 y 120.') from exc
        if not 1 <= count <= MAX_SESSIONS:
            raise ValueError('Podés elegir entre 1 y 120 ruedas por corrida.')
        start = end
        remaining = count
        while True:
            if is_trading_day(start):
                remaining -= 1
            if remaining == 0:
                break
            start -= timedelta(days=1)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1) if is_trading_day(start + timedelta(days=i))]
    if not days or len(days) > MAX_SESSIONS:
        raise ValueError('El período debe contener entre 1 y 120 ruedas completas.')
    capital = number(options.get('capital', '3000000'))
    if not Decimal(1000) <= capital <= Decimal('1000000000'):
        raise ValueError('Capital permitido: ARS 1.000 a 1.000.000.000.')
    hours = []
    for item in options.get('horas', '11,13,16').split(','):
        try:
            if not re.fullmatch(r'\d{1,2}(:\d{2})?', item):
                raise ValueError('Invalid time')
            hh, mm = (item.split(':') + ['0'])[:2]
            value = time(int(hh), int(mm))
        except ValueError as exc:
            raise ValueError('Horarios inválidos. Ejemplo: horas=11,13,16.') from exc
        if not time(10, 30) <= value < time(17):
            raise ValueError('Los horarios deben estar entre 10:30 y 16:59 ART.')
        hours.append(value)
    hours = sorted(set(hours))
    if not 1 <= len(hours) <= 6:
        raise ValueError('Elegí entre 1 y 6 cortes por rueda.')
    tickers = [] if options.get('tickers', 'auto').lower() in ('auto', 'todos') else sorted(set(options['tickers'].upper().split(',')))
    if len(tickers) > 30 or any(not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,14}', t) for t in tickers):
        raise ValueError('Elegí hasta 30 tickers separados por coma; auto usa el universo histórico.')
    aliases = {'drawdown': 'riesgo', 'pnl': 'rentabilidad', 'por_activo': 'activos'}
    metrics = list(METRICS) if options.get('medir', 'todo').lower() in ('todo', 'todos') else list(dict.fromkeys(aliases.get(x, x) for x in options['medir'].lower().split(',')))
    if any(m not in METRICS for m in metrics):
        raise ValueError('Mediciones: ' + ', '.join(METRICS))
    try:
        horizons = sorted(set(int(v) for v in options.get('horizontes', '5,10,20').split(',')))
    except ValueError as exc:
        raise ValueError('Horizontes inválidos. Ejemplo: horizontes=5,10,20.') from exc
    if len(horizons) > 8 or any(not 1 <= h <= 120 for h in horizons):
        raise ValueError('Elegí hasta 8 horizontes de entre 1 y 120 ruedas.')
    fee, slip = number(options.get('comision', '0.6'), capital=False), number(options.get('deslizamiento', '0.15'), capital=False)
    if not 0 <= fee <= 5 or not 0 <= slip <= 5:
        raise ValueError('Comisión y deslizamiento se expresan en %, de 0 a 5 por lado.')
    mode = options.get('arranque', 'nativo').lower()
    if mode not in ('radar', 'nativo'):
        raise ValueError('Arranque: radar o nativo.')
    result = prepare_spec(dict(schema='backtest-v1', command='/analisis_full', start=start.isoformat(), end=end.isoformat(),
                sessions=[d.isoformat() for d in days], hours=[h.strftime('%H:%M') for h in hours],
                cutoffs=[datetime.combine(d,h,ART).isoformat() for d in days for h in hours],
                asof=datetime.combine(days[-1],time(17),ART).isoformat(), initial_cash=str(capital),
                tickers=tickers, metrics=metrics, horizons=horizons, fee_rate=str(fee / 100),
                slippage_rate=str(slip / 100), latency_seconds=60, startup=mode,
                variant='PAPER_CASH_BRIDGE' if mode == 'radar' else 'NATIVE_FULL',
                methodology='CURRENT_FULL_ENGINE_ON_HISTORICAL_CAPTURE_V1'))
    supplied = {key:(options[key].upper() if key=='radar_buy_tiers' else int(options[key]))
                for key in ROTATION_DEFAULTS if key in options}
    if supplied:
        result['rotation_overrides'] = supplied
        rotation_overrides(result)
    return result


def describe(spec: dict) -> str:
    return (f"{spec['start']} a {spec['end']} · {len(spec['sessions'])} ruedas\n"
            f"Capital ARS {Decimal(spec['initial_cash']):,.0f} · activos: {','.join(spec['tickers']) or 'automático'}\n"
            f"Cortes ART: {', '.join(spec['hours'])}\n"
            f"Medir: {', '.join(spec['metrics'])}\n"
            f"Comisión {Decimal(spec['fee_rate'])*100}% + deslizamiento {Decimal(spec['slippage_rate'])*100}% por lado\n"
            f"Motor: /analisis_full · arranque: {spec['startup']}" +
            ' (compras desde efectivo habilitadas solo en la simulación).' +
            ('\nFiltros del backtest: ' + ', '.join(
                f'{key}={value}' for key,value in spec['rotation_overrides'].items()
            ) if spec.get('rotation_overrides') else ''))
