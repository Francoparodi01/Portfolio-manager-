"""Read-only Phase 0 turnover diagnostic; never invokes the engine or database.

Round trips use FIFO quantities. Buy->sell and sell->buy are separate views:
their costs must not be added together. T+10 uses the stored end-of-day mark
only when it is a same-session captured quote, with no price interpolation.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

D = Decimal
ZERO = D(0)
RUNS = {
    'A': '9356b8b3000853e9abe87143ad056ec5',
    'B': '22eccdb8b14d5c74b0f8f0cab401d415',
    'C': '4621af2c5278527ca02a9b0e2a364b3a',
}
WINDOWS = {
    'W1': ('2026-07-01', '2026-08-26'),
    'W2': ('2026-07-15', '2026-09-09'),
    'W3': ('2026-08-01', '2026-09-26'),
    'W4': ('2026-08-15', '2026-10-10'),
    'W5': ('2026-09-01', '2026-10-26'),
}


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def instant(value):
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def origin(fill, first_day):
    if fill['side'] == 'SELL':
        return 'venta-reduccion'
    if fill['reason'].startswith('Radar elegible:'):
        return 'radar-primer-dia' if fill['day'] == first_day else 'radar-plan'
    if fill['reason'].startswith('Aumentar posici'):
        return 'planner-aumento'
    return 'BUY_ORIGIN_UNKNOWN'


def pair_fifo(fills, opening_side):
    """Allocate each leg once per direction, including partial closes."""
    lots = defaultdict(deque)
    pairs = []
    for fill in fills:
        ticker = fill['ticker']
        if fill['side'] == opening_side:
            lots[ticker].append([fill, fill['qty']])
            continue
        remaining = fill['qty']
        while remaining and lots[ticker]:
            first, available = lots[ticker][0]
            qty = min(available, remaining)
            first_cost = first['cost'] * qty / first['qty']
            last_cost = fill['cost'] * qty / fill['qty']
            pairs.append(dict(ticker=ticker, opening=first, closing=fill, quantity=qty,
                              sessions=fill['session'] - first['session'],
                              cost=first_cost + last_cost,
                              entry_quote_notional=first['quote'] * qty))
            remaining -= qty
            lots[ticker][0][1] -= qty
            if not lots[ticker][0][1]:
                lots[ticker].popleft()
    return pairs


def horizon_10(fill, sessions, daily):
    index = fill['session'] + 10
    if index >= len(sessions):
        return None, 'IMMATURE_AT_WINDOW_END'
    day = sessions[index]
    asset = next((a for a in daily.get(day, {}).get('assets', [])
                  if a['ticker'] == fill['ticker']), None)
    if not asset or not asset.get('price'):
        return None, 'NO_EXPORTED_T10_MARK'
    mark = asset.get('mark') or {}
    if mark.get('basis') != 'RECORDED_INTRADAY_LAST' or str(mark.get('ts', ''))[:10] != day:
        return None, 'STALE_OR_NON_INTRADAY_T10_MARK'
    return (D(str(asset['price'])) / fill['quote'] - 1) * 100, 'OBSERVATIONAL_CAPTURED_MARK'


def summarize_group(fills, capital, total_volume):
    volume = sum((f['gross'] for f in fills), ZERO)
    cost = sum((f['cost'] for f in fills), ZERO)
    return dict(fills=len(fills), gross_ars=volume,
                volume_pct=volume / total_volume * 100 if total_volume else ZERO,
                costs_ars=cost, costs_pct_capital=cost / capital * 100)


def recent_opposite_events(fills, closing_side, horizon):
    """Any trade after a recent opposite trade; no inventory matching assumption."""
    latest = {}
    edges = []
    for fill in fills:
        other = latest.get((fill['ticker'], 'SELL' if fill['side'] == 'BUY' else 'BUY'))
        if (fill['side'] == closing_side and other is not None
                and fill['session'] - other['session'] <= horizon):
            edges.append((other, fill))
        latest[(fill['ticker'], fill['side'])] = fill
    legs = {f['fill_id']: f for edge in edges for f in edge}
    return edges, sum((f['cost'] for f in legs.values()), ZERO)


def analyze_run(root, label, run_id):
    path = root / run_id
    exported = path / 'exports'
    result = json.loads((exported / 'resultado.json').read_text(encoding='utf-8'))
    spec = result['spec']
    capital = D(spec['initial_cash'])
    sessions = spec['sessions']
    session_index = {day: index for index, day in enumerate(sessions)}
    first_cut = instant(spec['cutoffs'][0])
    fills = read_csv(exported / 'operaciones.csv')
    decisions = read_csv(exported / 'decisiones.csv')
    orders = read_csv(exported / 'ordenes.csv')
    cuts = read_csv(exported / 'cortes.csv')
    daily = {day['asof'][:10]: day for day in result['daily']}
    decision_index = defaultdict(list)
    order_index = defaultdict(list)
    for row in decisions:
        decision_index[(instant(row['cutoff']), row['ticker'])].append(row)
    for row in orders:
        order_index[(instant(row['cutoff']), row['ticker'], row['side'])].append(row)
    order_mismatches = []
    for index, fill in enumerate(fills):
        fill.update(fill_id=index, day=fill['filled_at'][:10], qty=D(fill['quantity']),
                    gross=D(fill['gross_ars']), quote=D(fill['quote_price']),
                    cost=D(fill['fee_ars']) + D(fill['slippage_ars']))
        fill['session'] = session_index[fill['day']]
        fill['origin'] = origin(fill, sessions[0])
        fill['is_first_cut'] = instant(fill['cutoff']) == first_cut
        matches = order_index[(instant(fill['cutoff']), fill['ticker'], fill['side'])]
        valid = [o for o in matches if o['filled_at'] and instant(o['filled_at']) == instant(fill['filled_at'])
                 and D(o['filled_qty']) == fill['qty']]
        if len(valid) != 1:
            order_mismatches.append(index)
        candidate_decisions = decision_index[(instant(fill['cutoff']), fill['ticker'])]
        fill['delta'] = (abs(D(candidate_decisions[0]['delta_weight']))
                         if len(candidate_decisions) == 1 and candidate_decisions[0]['delta_weight'] else None)
        fill['t10_return'], fill['t10_status'] = horizon_10(fill, sessions, daily)
    if fills != sorted(fills, key=lambda f: instant(f['filled_at'])):
        raise ValueError(f'{label}: fills are not chronological')
    volume = sum((f['gross'] for f in fills), ZERO)
    cost = sum((f['cost'] for f in fills), ZERO)
    expected_cost = D(str(result['fees_ars'])) + D(str(result['slippage_ars']))
    if abs(cost - expected_cost) > D('0.00001') or order_mismatches:
        raise ValueError(f'{label}: costs/orders fail reconciliation: {order_mismatches}')
    rows = []

    def emit(section, group, **values):
        rows.append(dict(run=label, run_id=run_id, section=section, group=group, **values))

    by_origin = {}
    for name in sorted({f['origin'] for f in fills}):
        stats = summarize_group([f for f in fills if f['origin'] == name], capital, volume)
        by_origin[name] = stats
        emit('origin', name, **stats)
    for reason in sorted({f['reason'] for f in fills}):
        emit('exact_reason', reason, **summarize_group([f for f in fills if f['reason'] == reason], capital, volume))
    first_cut_fills = [f for f in fills if f['is_first_cut']]
    emit('startup_first_cut', spec['cutoffs'][0], **summarize_group(first_cut_fills, capital, volume))
    radar = [f for f in fills if f['origin'].startswith('radar-')]
    radar_stats = summarize_group(radar, capital, volume)

    fifo = pair_fifo(fills, 'BUY')
    reverse = pair_fifo(fills, 'SELL')
    churn = []
    event_churn = []
    for side, name in [('BUY', 'SELL_TO_BUY_RECENT'), ('SELL', 'BUY_TO_SELL_RECENT')]:
        for n in (3, 5, 10):
            edges, leg_costs = recent_opposite_events(fills, side, n)
            record = dict(direction=name, horizon_sessions=n, closing_fills=len(edges),
                          costs_ars=leg_costs,
                          gross_ars=sum((last['gross'] for _, last in edges), ZERO))
            event_churn.append(record)
            emit('churn_recent_event', name, **record,
                 coverage_status='UNIQUE_LEG_COST_WITHIN_VIEW_NOT_AVOIDABLE_SAVING')
    for name, pairs in [('BUY_TO_SELL_FIFO', fifo), ('SELL_TO_BUY_FIFO', reverse)]:
        for n in (3, 5, 10):
            short = [pair for pair in pairs if pair['sessions'] <= n]
            record = dict(direction=name, horizon_sessions=n, pairs=len(short),
                          closing_fills=len({pair['closing']['fill_id'] for pair in short}),
                          costs_ars=sum((pair['cost'] for pair in short), ZERO))
            churn.append(record)
            emit('churn', name, **record)
    for pair in fifo:
        first, last = pair['opening'], pair['closing']
        emit('roundtrip_fifo', pair['ticker'], ticker=pair['ticker'],
             opening_fill_id=first['fill_id'], closing_fill_id=last['fill_id'],
             start=first['day'], end=last['day'], quantity=pair['quantity'],
             holding_sessions=pair['sessions'], costs_ars=pair['cost'],
             roundtrip_cost_pct=pair['cost'] / pair['entry_quote_notional'] * 100,
             gross_return_10d_pct=first['t10_return'], coverage_status=first['t10_status'])
    evaluated = [p for p in fifo if p['opening']['t10_return'] is not None]
    t10_comparison = dict(pairs=len(evaluated), total_pairs=len(fifo),
                         distinct_buys=len({p['opening']['fill_id'] for p in evaluated}),
                         gross_return_below_cost_pairs=sum(
                             p['opening']['t10_return'] <= p['cost']/p['entry_quote_notional']*100
                             for p in evaluated))
    emit('roundtrip_t10_summary', 'FIFO_MATCHED_QUANTITIES', **t10_comparison)
    for fill in fills:
        if fill['side'] == 'BUY':
            emit('buy_t10', fill['ticker'], ticker=fill['ticker'], opening_fill_id=fill['fill_id'],
                 start=fill['day'], gross_return_10d_pct=fill['t10_return'], coverage_status=fill['t10_status'])

    planner = [f for f in fills if not f['origin'].startswith('radar-')]
    matched = [f for f in planner if f['delta'] is not None]
    thresholds = []
    for threshold in (D('.02'), D('.03'), D('.05')):
        removed = [f for f in matched if f['delta'] <= threshold]
        stats = summarize_group(removed, capital, volume)
        record = dict(threshold=threshold, **stats)
        thresholds.append(record)
        emit('delta_threshold_static', str(threshold), **record,
             coverage_status='UPPER_BOUND_NO_RISK_EXCEPTIONS_OR_REPLANNING')
    boundaries = [(ZERO, D('.02')), (D('.02'), D('.03')), (D('.03'), D('.05')), (D('.05'), D('Infinity'))]
    for lower, upper in boundaries:
        group = [f for f in matched if lower < f['delta'] <= upper or lower == 0 and f['delta'] == 0]
        emit('delta_distribution', f'({lower},{upper}]', **summarize_group(group, capital, volume))
    for fill in matched:
        emit('planner_delta_fill', fill['ticker'], opening_fill_id=fill['fill_id'],
             start=fill['day'], direction=fill['side'], abs_delta_weight=fill['delta'],
             gross_ars=fill['gross'], costs_ars=fill['cost'])

    assets = result['assets']
    assets_by_ticker = {a['ticker']: a for a in assets}
    top3 = sorted(assets, key=lambda a: a['total_pnl'], reverse=True)[:3]
    top3_pnl = sum((D(str(a['total_pnl'])) for a in top3), ZERO)
    net = D(str(result['research_net_pnl_ars']))
    for ticker in sorted(assets_by_ticker):
        af = [f for f in fills if f['ticker'] == ticker]
        asset = assets_by_ticker[ticker]
        emit('asset_concentration', ticker, ticker=ticker,
             net_pnl_ars=D(str(asset['total_pnl'])),
             final_nav_weight_pct=D(str(asset['value_ars'])) / D(str(result['nav_ars'])) * 100,
             **summarize_group(af, capital, volume))
    week_fills = defaultdict(list)
    week_days = defaultdict(list)
    for fill in fills:
        year, week, _ = datetime.fromisoformat(fill['day']).isocalendar()
        week_fills[f'{year}-W{week:02}'].append(fill)
    for day in result['daily']:
        year, week, _ = datetime.fromisoformat(day['asof']).isocalendar()
        week_days[f'{year}-W{week:02}'].append(day)
    previous = capital
    for week, days in sorted(week_days.items()):
        nav = D(str(days[-1]['nav_ars']))
        emit('week_concentration', week, net_pnl_ars=nav-previous,
             start=days[0]['asof'][:10], end=days[-1]['asof'][:10],
             **summarize_group(week_fills[week], capital, volume))
        previous = nav
    snapshot = dict(run=label, run_id=run_id, start=spec['start'], end=spec['end'],
                    capital=capital, return_pct=net/capital*100,
                    costs_pct=cost/capital*100, before_costs_pct=(net+cost)/capital*100,
                    turnover=volume/capital, gross_ars=volume, costs_ars=cost,
                    fills=len(fills), buys=sum(f['side']=='BUY' for f in fills),
                    radar_fills=len(radar), radar_first_day=sum(f['origin']=='radar-primer-dia' for f in fills),
                    first_cut_fills=len(first_cut_fills), radar_volume_pct=radar_stats['volume_pct'],
                    by_origin=by_origin, churn=churn, event_churn=event_churn,
                    t10_comparison=t10_comparison, delta_thresholds=thresholds,
                    planner_fills=len(planner), planner_delta_matched=len(matched),
                    roundtrip_pairs=len(fifo), closed_position_cycles=result['completed_cycles'],
                    holding_sessions_mean=sum((D(p['sessions']) for p in fifo), ZERO)/len(fifo) if fifo else None,
                    t10_buy_coverage=dict(Counter(f['t10_status'] for f in fills if f['side']=='BUY')),
                    top3=[a['ticker'] for a in top3], top3_pnl=top3_pnl,
                    top3_pct_net=top3_pnl/net*100 if net else None,
                    return_without_top3_pct=(net-top3_pnl)/capital*100,
                    completed_cuts=result['completed_cuts'], csv_cuts=len(cuts),
                    spec=spec,
                    source_sha256={name:digest(exported/name) for name in
                                   ['operaciones.csv','ordenes.csv','decisiones.csv','cortes.csv','resultado.json','manifest.json']})
    emit('summary', label, **{k:v for k,v in snapshot.items()
                             if k not in ('run', 'run_id') and isinstance(v,(D,int,str))})
    return snapshot, rows


def coverage(root, summaries):
    calendar = sorted({day for summary in summaries for day in summary['spec']['sessions']})
    records = []
    for summary in summaries:
        path = root/summary['run_id']/'input/research_data_capture.json'
        capture = json.loads(path.read_text(encoding='utf-8'))
        quotes = next(o['rows'] for o in capture['observations'] if o['name']=='intraday_points')
        days = Counter(str(row['ts'])[:10] for row in quotes)
        counts = {o['name']:len(o['rows']) for o in capture['observations']}
        capture_hash = digest(path)
        for name,(start,end) in WINDOWS.items():
            sessions = [day for day in calendar if start<=day<=end]
            missing = [day for day in sessions if not days[day]]
            records.append(dict(capture_run=summary['run'],window=name,sessions=len(sessions),
                                missing_sessions=missing,status='TEMPORAL_PRESENCE_ONLY' if not missing else 'MISSING_SESSIONS',
                                capture_sha256=capture_hash,source_rows=counts))
        del quotes,capture
    return records


def gate(summaries):
    findings=[]
    expected={'A':(D('7.98'),D('2.8'),D('10.8'),D('3.7'),30,42),
              'B':(D('3.51'),D('2.8'),D('6.3'),D('3.7'),28,46),
              'C':(D('-7.11'),D('4.7'),D('-2.4'),D('6.3'),None,77)}
    for s in summaries:
        e=expected[s['run']]
        for name,value,tolerance in zip(['return_pct','costs_pct','before_costs_pct','turnover'],e[:4],[D('.01'),D('.1'),D('.1'),D('.1')]):
            if abs(s[name]-value)>tolerance:
                findings.append(f"{s['run']}: {name}={s[name]} contradice {value}")
        if (e[4] is not None and s['radar_fills']!=e[4]) or s['buys']!=e[5]:
            findings.append(f"{s['run']}: conteos BUY/radar no coinciden")
        if s['radar_volume_pct']<25:
            findings.append(f"{s['run']}: radar={s['radar_volume_pct']:.4f}% del volumen (<25%; ejemplo de puerta del usuario)")
        if s['run'] in ('A','B') and s['radar_first_day']!=8:
            findings.append(f"{s['run']}: radar primer dia no coincide con 8")
    return dict(status='STOP_BEFORE_VARIANTS' if findings else 'PASS',findings=findings,
                interpretation='El ejemplo radar<25% se aplica conservadoramente a cada corrida; el usuario debe aclarar si solo se referia a A/B.')


def money(value):
    return f'{value:,.2f}' if value is not None else 'NO EVALUABLE'


def render(summaries, check, cov):
    lines=['# Rotación y costos: diagnóstico Fase 0','',f"Puerta: **{check['status']}**.",'']
    lines += ['- '+item for item in check['findings']]
    lines += ['', '## Resumen', '', '| Corrida | Neto % | Costos % | Antes de costos % | Rotación | Fills | Radar / BUY | Volumen radar % |', '|---|---:|---:|---:|---:|---:|---:|---:|']
    for s in summaries:
        lines.append(f"| {s['run']} | {s['return_pct']:.4f} | {s['costs_pct']:.4f} | {s['before_costs_pct']:.4f} | {s['turnover']:.4f}x | {s['fills']} | {s['radar_fills']}/{s['buys']} | {s['radar_volume_pct']:.4f} |")
    for s in summaries:
        lines += ['',f"## Corrida {s['run']} - {s['run_id']}",'',f"{s['start']} a {s['end']}; capital ARS {money(s['capital'])}.",'', '| Origen | Fills | Volumen ARS | Volumen % | Costos ARS |','|---|---:|---:|---:|---:|']
        for name,stats in s['by_origin'].items():
            lines.append(f"| {name} | {stats['fills']} | {money(stats['gross_ars'])} | {stats['volume_pct']:.2f} | {money(stats['costs_ars'])} |")
        lines += ['',f"Primer corte: {s['first_cut_fills']} fills. Radar del primer día: {s['radar_first_day']}. Día y corte no son equivalentes.",'', '| Churn FIFO | Ruedas máximas | Pares de lotes | Fills de cierre distintos | Costos asignados ARS |','|---|---:|---:|---:|---:|']
        for ch in s['churn']:
            lines.append(f"| {ch['direction']} | {ch['horizon_sessions']} | {ch['pairs']} | {ch['closing_fills']} | {money(ch['costs_ars'])} |")
        lines += ['', '| Cambio tras última operación opuesta | Ruedas máximas | Fills posteriores | Costo de piernas únicas ARS |', '|---|---:|---:|---:|']
        for ch in s['event_churn']:
            lines.append(f"| {ch['direction']} | {ch['horizon_sessions']} | {ch['closing_fills']} | {money(ch['costs_ars'])} |")
        tc = s['t10_comparison']
        lines += ['', f"T+10 versus costo del viaje: {tc['gross_return_below_cost_pairs']}/{tc['pairs']} pares evaluables tienen retorno bruto a 10 ruedas menor o igual al costo porcentual de sus dos piernas. Cobertura: {tc['pairs']}/{tc['total_pairs']} pares, originados en {tc['distinct_buys']} compras distintas. No son observaciones independientes."]
        lines += ['',f"{s['roundtrip_pairs']} pares FIFO con cantidades cerradas; {s['closed_position_cycles']} ciclos de posición totalmente cerrados. Tenencia media de los pares: {money(s['holding_sessions_mean'])} ruedas. Los pares no son trades independientes.", '',f"Cobertura T+10 por compra: `{json.dumps(s['t10_buy_coverage'], ensure_ascii=False)}`.",'','| Umbral absoluto | Fills bajo banda | Volumen total retirado % |','|---|---:|---:|']
        for band in s['delta_thresholds']:
            lines.append(f"| {band['threshold']*100:.0f}% | {band['fills']} | {band['volume_pct']:.4f} |")
        lines += ['',f"Delta asociado de forma unívoca: {s['planner_delta_matched']}/{s['planner_fills']} fills no radar. Remoción estática: no aplica excepciones de riesgo ni recalcula funding; no es una variante ejecutada.", '',f"Top 3 aportes: {', '.join(s['top3'])}; ARS {money(s['top3_pnl'])}, {money(s['top3_pct_net'])}% del PnL neto. Neto sin sus aportes: {s['return_without_top3_pct']:.4f}% del capital. En una corrida perdedora, el cociente firmado no es una cuota de ganancias."]
    lines += ['', '## Cobertura temporal de las ventanas', '', '| Captura | Ventana | Ruedas | Ruedas sin cotizaciones | Estado |','|---|---|---:|---:|---|']
    for row in cov:
        lines.append(f"| {row['capture_run']} | {row['window']} | {row['sessions']} | {len(row['missing_sessions'])} | {row['status']} |")
    available = {r['capture_run'] for r in cov if r['status']=='TEMPORAL_PRESENCE_ONLY'}
    complete = [c for c in available if all(r['status']=='TEMPORAL_PRESENCE_ONLY' for r in cov if r['capture_run']==c)]
    if not cov:
        lines += ['', 'Cobertura no evaluada en esta ejecución.']
    elif not complete:
        lines += ['', 'Ninguna captura existente cubre por sí sola las cinco ventanas. No se consolidaron ni interpolaron datos.']
    else:
        lines += ['', 'Capturas con presencia temporal en las cinco ventanas: '+', '.join(sorted(complete))+'.']
    lines += ['', 'Presencia temporal no acredita completitud por activo, disponibilidad intradiaria ni calidad. Los huecos exactos quedan en rotacion_evidencia.json; las ventanas con MISSING_SESSIONS se omiten de cualquier barrido sobre esa captura.', '', 'W3 termina el 26/9; las corridas A/B terminan el 26/10. El control W3 de 6M sería una nueva comparación, no una reproducción directa de +7,98% vs +3,51%.', '', '## Definiciones y límites', '', '- Rotación = suma exacta de gross_ars / capital inicial; incluye ambos lados. Los costos son fee_ars + slippage_ars, ya incorporados en el PnL.', '- Razones exactas, concentración por activo y semana, deltas por fill y viajes FIFO se conservan en rotacion_diagnostico.csv. No se agrupan motivos parecidos bajo un nombre inventado.', '- Churn por evento: cada fill se compara con la última operación opuesta del mismo activo. Los costos incluyen una sola vez cada pierna participante dentro de cada dirección y horizonte; no son ahorro evitable ni se suman entre vistas.', '- Churn FIFO: emparejamiento FIFO de cantidades, hasta N ruedas inclusive. BUY->SELL y SELL->BUY son vistas distintas que reutilizan piernas: sus costos no se suman. Tampoco se suman los horizontes anidados 3/5/10.', '- T+10: diez sesiones posteriores a la compra, precio de referencia sin slippage contra última marca intradiaria de ese día exportada por la corrida. Sin precio fresco o sin madurez: NO EVALUABLE. No se infieren precios de PnL ni se rellena una serie vendida.', '- El costo del viaje FIFO es observado para las cantidades emparejadas. Su retorno T+10 es contrafactual a plazo fijo; el cierre real puede ocurrir otro día. La comparación no es una prueba causal.', '- Retrospectivo, motor de esas corridas sobre captura histórica; no PIT. Mismos datos para diseño y evaluación, ventanas solapadas y una trayectoria por configuración. El backtest convierte en compras ítems del radar que en producción pueden mostrarse solo como alertas; aquí no se verificaron órdenes reales.', '- La revisión previa detectó discrepancias de precios y fuentes. Este diagnóstico conserva las cifras originales; no certifica rentabilidad económica real.', f"- Fase 1 y barrido no ejecutados; estado de la puerta: {check['status']}. No se modificaron spec, planner, optimizer, configuración productiva ni decision_log. Sin rebuild, deploy, commit ni push."]
    return '\n'.join(lines)+'\n'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path('outputs/backtests'))
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--skip-capture-coverage',action='store_true')
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    summaries=[];rows=[]
    for label,run_id in RUNS.items():
        summary,records=analyze_run(args.root,label,run_id);summaries.append(summary);rows.extend(records)
    cov=[] if args.skip_capture_coverage else coverage(args.root,summaries)
    check=gate(summaries)
    columns=list(dict.fromkeys(k for row in rows for k in row))
    with (args.output/'rotacion_diagnostico.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns);writer.writeheader();writer.writerows(rows)
    (args.output/'rotacion_diagnostico.md').write_text(render(summaries,check,cov),encoding='utf-8')
    evidence=dict(gate=check,runs=summaries,capture_coverage=cov,script_sha256=digest(Path(__file__)))
    (args.output/'rotacion_evidencia.json').write_text(json.dumps(evidence,indent=2,default=str,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'gate':check,'csv_rows':len(rows),'runs':[{'run':s['run'],'fills':s['fills'],'turnover':str(s['turnover']),'radar_pct':str(s['radar_volume_pct']),'t10':s['t10_buy_coverage']} for s in summaries]},ensure_ascii=True))


if __name__=='__main__':
    main()
