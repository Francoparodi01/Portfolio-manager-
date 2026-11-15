"""Offline, preregistered rotation sweep; never queues jobs or contacts production."""
import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal as D, InvalidOperation
from pathlib import Path

MEASUREMENT_LOCK = threading.Lock()


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def write(path, value):
    path.write_text(json.dumps(value,indent=2,ensure_ascii=False,default=str),encoding='utf-8')


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def attribution(result, adjustments):
    """FIFO lot attribution; entry cohort retained through splits and later sales."""
    from collections import defaultdict, deque
    lots=defaultdict(deque); pnl={'startup':D(0),'ongoing':D(0)}; holds=[]
    sessions={d:i for i,d in enumerate(result['spec']['sessions'])}
    first=datetime.fromisoformat(result['spec']['cutoffs'][0])
    events=[(datetime.fromisoformat(f['filled_at']),1,f) for f in result['fills']]
    events += [(datetime.fromisoformat(a['asof']),0,a) for a in adjustments]
    for at,kind,f in sorted(events,key=lambda e:(e[0],e[1])):
        ticker=f['ticker']
        if kind==0:
            factor=D(str(f['quantity_after']))/D(str(f['quantity_before']))
            for lot in lots[ticker]: lot['qty']*=factor
            continue
        qty=D(str(f['quantity']));gross=D(str(f['gross_ars']));fee=D(str(f['fee_ars']))
        if f['side']=='BUY':
            lots[ticker].append(dict(qty=qty,cost=gross+fee,session=sessions[at.date().isoformat()],
                     cohort='startup' if datetime.fromisoformat(f['cutoff'])==first else 'ongoing'))
        else:
            remaining=qty
            while remaining:
                lot=lots[ticker][0]; part=min(remaining,lot['qty'])
                basis=lot['cost']*part/lot['qty']
                pnl[lot['cohort']]+=(gross-fee)*part/qty-basis
                holds.append((sessions[at.date().isoformat()]-lot['session'],basis))
                lot['qty']-=part;lot['cost']-=basis;remaining-=part
                if not lot['qty']:lots[ticker].popleft()
    marks={a['ticker']:a['price'] for a in result['assets']}
    for ticker,queue in lots.items():
        for lot in queue:
            pnl[lot['cohort']]+=lot['qty']*D(str(marks[ticker]))-lot['cost']
    if abs(sum(pnl.values())-D(str(result['research_net_pnl_ars'])))>D('.02'):
        raise AssertionError('STARTUP_ATTRIBUTION_DOES_NOT_RECONCILE')
    denominator=sum((basis for _,basis in holds),D(0))
    hold_mean=sum((D(n)*basis for n,basis in holds),D(0))/denominator if denominator else None
    return pnl,hold_mean,len(holds)


def benchmarks(state, result):
    """First-cut HOLD and fractional equal-weight first-cut universe; entry costs only."""
    from bisect import bisect_left, bisect_right
    from datetime import timedelta
    spec=result['spec'];book=state['BOOK'];cut=datetime.fromisoformat(spec['cutoffs'][0])
    end=datetime.fromisoformat(spec['asof']);next_cut=datetime.fromisoformat(spec['cutoffs'][1])
    first=[f for f in result['fills'] if datetime.fromisoformat(f['cutoff'])==cut]
    capital=D(spec['initial_cash']);passive=capital;stale=set()
    def final_value(ticker,qty,entry_at):
        for effect in sorted(book.rows.get('corporate_effects',[]),key=lambda r:r['effective_at']):
            if (effect['ticker']!=ticker or not effect['is_active']
                    or book.identity[ticker]!=(effect['asset_type'],effect['currency'])):continue
            known=max(datetime.fromisoformat(effect[k]) for k in
                      ('event_created_at','effect_created_at','event_updated_at','effect_updated_at'))
            effective=datetime.fromisoformat(effect['effective_at'])
            if entry_at < max(known,effective) <= end:qty*=D(str(effect['quantity_factor']))
        price,mark=book.mark(ticker,end,eod=True)
        if mark['basis']!='RECORDED_INTRADAY_LAST':stale.add(ticker)
        return qty*price
    for fill in first:
        assert fill['side']=='BUY'
        passive-=D(str(fill['gross_ars']))+D(str(fill['fee_ars']))
        passive+=final_value(fill['ticker'],D(str(fill['quantity'])),datetime.fromisoformat(fill['filled_at']))
    # Membership is known at the first cutoff, not the union of future holdings.
    eligible=[]
    for ticker in sorted(book.points):
        i=bisect_right(book.times[ticker],cut)-1
        if i>=0 and book.points[ticker][i]['_ts'].astimezone(state['ART']).date()==cut.date():
            if ticker not in book.ambiguous:eligible.append(ticker)
    equal=capital;entered=0;missing=[]
    if not eligible:raise ValueError('EMPTY_EX_ANTE_BENCHMARK_UNIVERSE')
    budget=capital/D(len(eligible))
    for ticker in eligible:
        i=bisect_left(book.times[ticker],cut+timedelta(seconds=spec['latency_seconds']))
        if i==len(book.points[ticker]) or book.times[ticker][i]>=next_cut:
            missing.append(ticker);continue
        quote=book.points[ticker][i]
        qty=budget/(D(str(quote['last_price']))*(1+book.slip)*(1+book.fee))
        equal-=budget;equal+=final_value(ticker,qty,quote['_ts']);entered+=1
    return dict(passive_return_pct=float((passive/capital-1)*100),
                equal_weight_return_pct=float((equal/capital-1)*100),
                benchmark_universe_n=len(eligible),benchmark_entered_n=entered,
                benchmark_unfilled_n=len(missing),benchmark_stale_final_n=len(stale),
                benchmark_basis='FIRST_CUTOFF_UNIVERSE_FRACTIONAL_ENTRY_COSTS_NO_EXIT_LIQUIDATION')


def saved_measurement_state(directory):
    """Reload only prices/accounting after replay; keep engine memory separate."""
    sys.path.insert(0,str(Path.cwd()))
    from src.backtesting.ledger import Ledger, ART
    spec=read(directory/'job.json')['spec']
    summary=read(directory/'replay/summary.json')
    if ([r['cutoff_art'] for r in summary]!=spec['cutoffs']
            or not all(r['completed'] and not r.get('error') for r in summary)):
        raise ValueError('INCOMPLETE_SAVED_REPLAY')
    isolation=read(directory/'isolation.json')
    if isolation!=dict(external_io_attempts=[],no_persist=True,no_telegram=True):
        raise ValueError('UNSAFE_SAVED_REPLAY')
    capture=read(directory/'input/research_data_capture.json')
    rows={x['name']:x['rows'] for x in capture['observations']}
    rows['corporate_effects']=[]
    for row in rows['corporate_full']:
        event=json.loads(row['event']) if isinstance(row['event'],str) else row['event']
        effect=json.loads(row['effect']) if isinstance(row['effect'],str) else row['effect']
        if event['lifecycle_status'] not in ('ANNOUNCED','CONFIRMED','EFFECTIVE'):continue
        rows['corporate_effects'].append(dict(**effect,effective_at=event['effective_at'],
            event_created_at=event['created_at'],event_updated_at=event['updated_at'],
            effect_created_at=effect['created_at'],effect_updated_at=effect['updated_at']))
    tickers=spec['tickers'] or sorted({r['ticker'] for r in rows['intraday_points']})
    book=Ledger(rows,tickers,spec['initial_cash'],spec['fee_rate'],spec['slippage_rate'],spec['latency_seconds'])
    final=read(directory/'replay'/f"cut_{len(spec['cutoffs']):03d}.json")
    book.adjustments=final.get('corporate_adjustments',[])
    # Native traces precede the last EOD valuation: reject an unrecorded late event.
    for effect in rows['corporate_effects']:
        at=max(datetime.fromisoformat(effect[k]) for k in ('effective_at','event_created_at',
            'event_updated_at','effect_created_at','effect_updated_at'))
        if datetime.fromisoformat(spec['cutoffs'][-1])<at<=datetime.fromisoformat(spec['asof']):
            raise ValueError('UNRECORDED_CORPORATE_ADJUSTMENT_AT_FINAL_CLOSE')
    return dict(BOOK=book,ART=ART)


def measure_saved(directory):
    state=saved_measurement_state(directory)
    from src.backtesting.report import summarize, export
    from src.backtesting.spec import spec_hash
    result=summarize(directory,0)
    if result['status']!='COMPLETE':raise RuntimeError('RESEARCH_RUN_'+result['status'])
    spec=result['spec'];capital=D(spec['initial_cash'])
    fills=result['fills'];costs=D(str(result['fees_ars']))+D(str(result['slippage_ars']))
    volume=sum((D(str(f['gross_ars'])) for f in fills),D(0))
    top=sorted(result['assets'],key=lambda a:a['total_pnl'],reverse=True)[:3]
    top_pnl=sum(D(str(a['total_pnl'])) for a in top)
    net=D(str(result['research_net_pnl_ars']))
    pnl,hold,n_pairs=attribution(result,state['BOOK'].adjustments)
    row=dict(config=spec['research']['config'],window=spec['research']['window'],
             stage=spec['research']['stage'],status=result['status'],capital_ars=str(capital),
             net_return_pct=float(net/capital*100),before_costs_return_pct=float((net+costs)/capital*100),
             net_pnl_ars=float(net),commissions_ars=result['fees_ars'],slippage_ars=result['slippage_ars'],
             costs_ars=float(costs),costs_pct_capital=float(costs/capital*100),
             gross_ars=float(volume),turnover=float(volume/capital),fills=len(fills),
             completed_position_cycles=result['completed_cycles'],fifo_matched_pairs=n_pairs,
             holding_sessions_mean_cost_weighted=None if hold is None else float(hold),
             daily_max_drawdown_pct=result['daily_max_drawdown_pct'],
             without_top3_return_pct=float((net-top_pnl)/capital*100),
             top3_tickers=','.join(a['ticker'] for a in top),top3_pnl_ars=float(top_pnl),
             top3_pct_of_net_pnl=float(top_pnl/net*100) if net else None,
             startup_return_pp=float(pnl['startup']/capital*100),
             ongoing_return_pp=float(pnl['ongoing']/capital*100),
             first_cut_fills=sum(datetime.fromisoformat(f['cutoff'])==datetime.fromisoformat(spec['cutoffs'][0]) for f in fills),
             first_cut_fills_sha256=hashlib.sha256(json.dumps(
                 [f for f in fills if datetime.fromisoformat(f['cutoff'])==datetime.fromisoformat(spec['cutoffs'][0])],
                 sort_keys=True,separators=(',',':')).encode()).hexdigest(),
             fee_rate=spec['fee_rate'],slippage_rate=spec['slippage_rate'],
             delta_band_absolute=.015*spec.get('rotation_overrides',{}).get('delta_band_mult',1),
             git_commit=spec['research']['git_commit'],spec_sha256=spec_hash(spec),
             capture_version=spec['research']['capture_sha256'],engine_image=spec['research']['image'],
             methodology='RETROSPECTIVE_FROZEN_ENGINE_NOT_PIT',**benchmarks(state,result))
    row['delta_vs_passive_pp']=row['net_return_pct']-row['passive_return_pct']
    export(directory,result)
    write(directory/'metrics.json',row)
    write(directory/'research_complete.json',dict(spec_sha256=spec_hash(spec),status='COMPLETE'))


def prepare_job(base, plan, config, window, stage, slip='0.0015', *, initial_cash='3000000'):
    try:
        capital=D(str(initial_cash))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError('INVALID_INITIAL_CASH') from exc
    if not capital.is_finite() or not D('1000')<=capital<=D('1000000000'):
        raise ValueError('INVALID_INITIAL_CASH')
    cash=format(capital.normalize(),'f')
    source=Path(plan['_repo'])/'outputs/backtests'/plan['capture_run']
    spec=read(source/'job.json')['spec'];start,end=plan['windows'][window]
    sessions=[s for s in spec['sessions'] if start<=s<=end]
    spec.update(start=start,end=end,sessions=sessions,
                cutoffs=[s+'T'+h+':00-03:00' for s in sessions for h in plan['hours_art']],
                asof=sessions[-1]+'T17:00:00-03:00',initial_cash=cash,slippage_rate=slip,
                research=dict(config=config['id'],window=window,stage=stage,
                   git_commit=plan['git_commit'],capture_sha256=plan['capture_sha256'],
                   image=plan['image'],prereg_sha256=plan['_prereg_sha256']))
    if config['overrides']:spec['rotation_overrides']=config['overrides']
    name=f"{config['id']}__{window}__slip_{slip}"
    if cash!='3000000':name+=f'__capital_{cash}'
    directory=base/'runs'/name;directory.mkdir(parents=True,exist_ok=True)
    (directory/'input').mkdir(exist_ok=True)
    job=dict(id=name,spec=spec)
    if (directory/'job.json').exists() and read(directory/'job.json')!=job:
        raise ValueError('IMMUTABLE_JOB_SPEC_CHANGED')
    write(directory/'job.json',job)
    return directory,source/'input'


def run_job(base,plan,config,window,stage,slip='0.0015',*,phase='both',initial_cash='3000000'):
    directory,inputs=prepare_job(base,plan,config,window,stage,slip,initial_cash=initial_cash)
    if (directory/'research_complete.json').exists():return read(directory/'metrics.json')
    command=['docker','run','--rm','--network','none','--read-only','--cpus','2','--memory','4g',
             '--pids-limit','256','--tmpfs','/tmp:rw,size=536870912','--entrypoint','python','--workdir','/app']
    for source,target,readonly in [(base/'implementation/backtesting','/app/src/backtesting',True),
                                   (Path(__file__).resolve(),'/research.py',True),
                                   (directory,'/work',False),(inputs,'/work/input',True)]:
        command+=['--mount',f'type=bind,source={source},target={target}'+(',readonly' if readonly else '')]
    saved=directory/'replay/summary.json'
    complete=(saved.exists() and [r['cutoff_art'] for r in read(saved)]==read(directory/'job.json')['spec']['cutoffs']
              and all(r['completed'] and not r.get('error') for r in read(saved)))
    if not complete:
        if phase=='measure':raise RuntimeError('MEASUREMENT_REQUIRES_COMPLETE_REPLAY')
        with (directory/'replay.log').open('wb') as log:
            code=subprocess.call(command+[plan['image'],'-m','src.backtesting.replay','/work'],stdout=log,stderr=subprocess.STDOUT)
        if code:
            raise RuntimeError(f'{directory.name}: replay failed ({code}); inspect replay.log')
    if phase=='replay':return None
    with MEASUREMENT_LOCK, (directory/'measurement.log').open('wb') as log:
        code=subprocess.call(command+[plan['image'],'/research.py','measure','/work'],stdout=log,stderr=subprocess.STDOUT)
    if code:raise RuntimeError(f'{directory.name}: measurement failed ({code}); inspect measurement.log')
    write(directory/'measurement_provenance.json',dict(launcher_sha256=sha(Path(__file__)),
          saved_replay_reused=complete,engine_implementation='FROZEN_UNCHANGED',mode='SEPARATE_PROCESS'))
    row=read(directory/'metrics.json')
    print(json.dumps({k:row[k] for k in ('config','window','net_return_pct','turnover','fills')}),flush=True)
    return row


def batch(base,plan,jobs):
    # Peak capture/summary allocations must not overlap active native engines.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(run_job,base,plan,*job,phase='replay') for job in jobs]
        try:
            for future in as_completed(futures):future.result()
        except Exception:
            for pending in futures:pending.cancel()
            raise
    return [run_job(base,plan,*job,phase='measure') for job in jobs]


def verdict(rows):
    by_window={r['window']:r for r in rows}
    if len(by_window)!=5 or 'W1' not in by_window or 'W5' not in by_window:
        return 'NOT_EVALUABLE_MISSING_WINDOWS'
    conditions=[sum(r['delta_vs_control_pp']>0 for r in rows)>=4,
                by_window['W1']['delta_vs_control_pp']>0,by_window['W5']['delta_vs_control_pp']>0,
                sum(r['delta_vs_control_pp'] for r in rows)/5>=1,
                all(r['delta_without_top3_pp']>0 for r in rows),
                all(r['drawdown_degradation_pp']<=1.5 for r in rows),
                all(r['turnover_reduction_pct']>=25 for r in rows)]
    return 'PASS_RESEARCH_ONLY' if all(conditions) else 'FAIL'


def known_split_quality(directory):
    """Annotate the audited YPFD capture mismatch; never repair model values."""
    daily_path=directory/'exports/cartera_diaria.csv'
    fills_path=directory/'exports/operaciones.csv'
    if not daily_path.exists() or not fills_path.exists():
        return dict(ypfd_split_mismatch_exposure=None,
                    daily_drawdown_quality='UNKNOWN_MISSING_EXPORTS')
    quantities={day:0 for day in ('2026-08-03','2026-08-04')}
    seen=set()
    with daily_path.open(encoding='utf-8-sig',newline='') as stream:
        for row in csv.DictReader(stream):
            day=row['asof'][:10]
            if day in quantities:
                seen.add(day)
                quantities[day]=sum(float(a['quantity']) for a in json.loads(row['assets'])
                                    if a['ticker']=='YPFD')
    fills=ypfd_fills=0
    with fills_path.open(encoding='utf-8-sig',newline='') as stream:
        for row in csv.DictReader(stream):
            if row['filled_at'][:10]=='2026-08-03':
                fills+=1
                ypfd_fills+=row['ticker']=='YPFD'
    exposed=quantities['2026-08-03']>0 or ypfd_fills>0
    return dict(ypfd_split_mismatch_exposure=exposed,
                ypfd_quantity_aug3=quantities['2026-08-03'] if '2026-08-03' in seen else None,
                ypfd_quantity_aug4=quantities['2026-08-04'] if '2026-08-04' in seen else None,
                aug3_fills=fills,aug3_ypfd_fills=ypfd_fills,
                daily_drawdown_quality=('NOT_EVALUABLE_YPFD_SPLIT_MISMATCH' if exposed
                                        else 'UNVERIFIED_HISTORICAL_PRICES'))


def deliver(base,plan):
    rows=[]
    for path in sorted((base/'runs').glob('*/metrics.json')):
        if not (path.parent/'research_complete.json').exists():continue
        row=read(path)
        row.update(known_split_quality(path.parent))
        rows.append(row)
    controls={(r['window'],r['slippage_rate']):r for r in rows if r['config']=='control'}
    for row in rows:
        control=controls.get((row['window'],row['slippage_rate']))
        if control:
            if row['first_cut_fills_sha256']!=control['first_cut_fills_sha256']:
                raise AssertionError('STARTUP_NOT_IDENTICAL_'+row['config']+'_'+row['window'])
            row['delta_vs_control_pp']=row['net_return_pct']-control['net_return_pct']
            row['delta_without_top3_pp']=row['without_top3_return_pct']-control['without_top3_return_pct']
            row['drawdown_degradation_pp']=control['daily_max_drawdown_pct']-row['daily_max_drawdown_pct']
            row['drawdown_comparison_status']=('NOT_EVALUABLE_YPFD_SPLIT_MISMATCH'
                if row.get('ypfd_split_mismatch_exposure') or control.get('ypfd_split_mismatch_exposure')
                else 'UNVERIFIED_HISTORICAL_PRICES')
            row['turnover_reduction_pct']=(1-row['turnover']/control['turnover'])*100 if control['turnover'] else None
    columns=list(dict.fromkeys(k for row in rows for k in row))
    with (base/'barrido_resultados.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns);writer.writeheader();writer.writerows(rows)
    planned=len(plan['eligible_windows'])*(len(plan['stage1'])+1+4)
    combined=[r for r in rows if r['config']=='combination' and r['slippage_rate']=='0.0015']
    findings=[]
    if len(combined)==len(plan['eligible_windows']):
        n=len(combined)
        avg=lambda key:sum(r[key] for r in combined)/n
        cost_saving=sum(controls[(r['window'],r['slippage_rate'])]['costs_pct_capital']-r['costs_pct_capital'] for r in combined)/n
        before_delta=sum(r['before_costs_return_pct']-controls[(r['window'],r['slippage_rate'])]['before_costs_return_pct'] for r in combined)/n
        findings=[f"**Resultado de la combinación (N={n} ventanas solapadas):** Δ neto medio {avg('delta_vs_control_pp'):+.3f} pp; mejora en {sum(r['delta_vs_control_pp']>0 for r in combined)}/{n}; reducción media de rotación {avg('turnover_reduction_pct'):.2f}%. Ahorra {cost_saving:.3f} pp de capital en costos, mientras el resultado antes de costos cambia {before_delta:+.3f} pp. La ventaja neta media sin los tres mayores aportes de cada corrida es {avg('delta_without_top3_pp'):+.3f} pp. Son comparaciones del modelo, sin evidencia suficiente para aprobar una política.",'']
    lines=['# Barrido de rotación y costos','',
        '**Retrospectivo, no PIT. Ninguna variante puede aprobar el criterio global con W3–W5 sin cobertura.**','',
        f"Estado: {'COMPLETO' if len(rows)==planned else 'PARCIAL / EN EJECUCIÓN'}; {len(rows)}/{planned} corridas previstas con resultado. No se interpretan corridas pendientes como cero.",'',
        f"**Calidad de riesgo:** {sum(r.get('ypfd_split_mismatch_exposure') is True for r in rows)}/{len(rows)} corridas tienen exposición al desajuste de YPFD: precios a escala posterior al split el 3/8 y ajuste de cantidad efectivo en la captura el 4/8. Su caída máxima no es verificable económicamente. Se conservan los valores del modelo; no se corrigieron precios ni se cambiaron los criterios.",'',
        *findings,
        f"Prerregistro: `{plan['_prereg_sha256']}`. Máximo 11 configuraciones de política: 10 OFAT incluido control y una combinación; sensibilidades de costos separadas.",
        'Las ventanas omitidas no se rellenan ni se cuentan como resultado cero. W1 y W2 se solapan; falta W5 para la prueba independiente.', '',
        '| Configuración | Ventana | Slip % | Neto % | Δ control pp | Rotación | Δ pasivo pp | Sin top3 % | DD % | YPFD desajustado |',
        '|---|---|---:|---:|---:|---:|---:|---:|---:|---|']
    for r in rows:
        lines.append(f"| {r['config']} | {r['window']} | {float(r['slippage_rate'])*100:.2f} | {r['net_return_pct']:.3f} | {r.get('delta_vs_control_pp',float('nan')):.3f} | {r['turnover']:.3f} | {r['delta_vs_passive_pp']:.3f} | {r['without_top3_return_pct']:.3f} | {r['daily_max_drawdown_pct']:.3f} | {'Sí' if r.get('ypfd_split_mismatch_exposure') else 'No detectado'} |")
    lines+=['','## Comparación agregada: costos originales','',
            '| Configuración | N ventanas | Δ neto medio pp | Positivas / N | Δ sin top3 medio pp | Menor rotación media % | Peor deterioro DD pp | Veredicto |',
            '|---|---:|---:|---:|---:|---:|---:|---|']
    for config in sorted({r['config'] for r in rows}):
        subset=[r for r in rows if r['config']==config and r['slippage_rate']=='0.0015']
        if config=='control' or not subset:continue
        def mean(key):return sum(r[key] for r in subset)/len(subset)
        dd=f"{max(r['drawdown_degradation_pp'] for r in subset):.3f}"
        if any(r.get('drawdown_comparison_status')=='NOT_EVALUABLE_YPFD_SPLIT_MISMATCH' for r in subset):
            dd='NE económico (modelo: '+dd+')'
        lines.append(f"| {config} | {len(subset)} | {mean('delta_vs_control_pp'):.3f} | {sum(r['delta_vs_control_pp']>0 for r in subset)}/{len(subset)} | {mean('delta_without_top3_pp'):.3f} | {mean('turnover_reduction_pct'):.2f} | {dd} | {verdict(subset)} |")
    sensitivity=[]
    for slip in plan['slippage_rates']:
        subset=[r for r in rows if r['config']=='combination' and r['slippage_rate']==slip]
        if len(subset)!=len(plan['eligible_windows']):continue
        sensitivity.append(f"| {float(slip)*100:.2f} | {len(subset)} | {sum(r['delta_vs_control_pp'] for r in subset)/len(subset):+.3f} | {sum(r['delta_vs_control_pp']>0 for r in subset)}/{len(subset)} |")
    if sensitivity:
        lines+=['','## Sensibilidad: combinación contra su control','',
                'Mismo deslizamiento por lado en ambos; comisión fija 0,60% por lado. Cada sensibilidad vuelve a ejecutar el motor y puede cambiar cantidades o decisiones.',
                '', '| Deslizamiento % | N ventanas | Δ neto medio pp | Positivas / N |',
                '|---:|---:|---:|---:|',*sensitivity]
        lines+=['','La sensibilidad no sustituye el costo de referencia de 0,15% ni permite elegir costos después de ver los resultados. La combinación se mantiene fija en las tres sensibilidades.']
    lines+=['','Criterio sin cambios: Δ neto positivo en al menos 4/5 ventanas y en W1/W5; Δ medio ≥1 pp; ventaja positiva sin top3; deterioro de caída máxima ≤1,5 pp; rotación menor al menos 25%. Faltan tres ventanas y W5: NO EVALUABLE no equivale a demostrar que la rotación no sirve.',
            '', '## Benchmarks del control', '',
            '| Ventana | Slip % | Pasivo % | Equiponderado % | Activos con entrada / universo inicial | Activos con marca final anterior, ambas vistas |',
            '|---|---:|---:|---:|---:|---:|']
    for r in rows:
        if r['config']=='control':
            lines.append(f"| {r['window']} | {float(r['slippage_rate'])*100:.2f} | {r['passive_return_pct']:.3f} | {r['equal_weight_return_pct']:.3f} | {r['benchmark_entered_n']}/{r['benchmark_universe_n']} | {r['benchmark_stale_final_n']} |")
    lines+=['','Las marcas anteriores son valuaciones del modelo; no se interpolaron precios. Los benchmarks tampoco constituyen rentabilidad verificada. El pasivo deja en efectivo la parte del capital no invertida en el primer corte; el equiponderado es fraccional y teórico. La distinta exposición impide interpretar automáticamente su diferencia como capacidad de selección.']
    selection=base/'combined_selection.json'
    if selection.exists():
        lines+=['','## Combinación final','',
                '`'+json.dumps(read(selection)['combination']['overrides'],ensure_ascii=False,sort_keys=True)+'`.',
                'Selección por mayor Δ neto medio disponible de cada palanca, incluido el control; desempate por menor rotación y luego orden preregistrado. Se eligió y evaluó sobre los mismos datos; no es validación externa.']
    lines+=['','## Definiciones y limitaciones','',
        '- Costos = comisión + deslizamiento de fills; antes de costos suma esos costos al PnL de la misma trayectoria, no simula financiación sin costos.',
        '- Viaje completo: posición que vuelve a cero; pares FIFO y tenencia media ponderada por costo asignado se informan por separado.',
        '- Arranque: lotes comprados en el primer corte; sus ventas posteriores conservan esa atribución FIFO, con ajustes corporativos. Sus aportes y los corrientes suman el PnL.',
        '- Pasivo: compras efectivamente llenadas en el primer corte, efectivo residual y tenencia hasta el cierre. Universo equiponderado: universo conocido en ese corte, fracciones teóricas, igual presupuesto por activo, costos de entrada; sin cotización de ejecución conserva su presupuesto en efectivo. No se cobra liquidación final ficticia.',
        '- Sin top3 resta los aportes de cada corrida sin reasignar el capital. En pérdidas, el porcentaje firmado de top3 sobre PnL no es cuota de ganancias.',
        '- Mismos datos para diseñar y evaluar; ventanas solapadas; una trayectoria por configuración; comparaciones múltiples; precios históricos no certificados. El backtest ejecuta como compras ítems que en producción pueden ser alertas.',
        '- YPFD: la inconsistencia interna de fechas de precio y split está documentada en YPFD_capture_audit.json. No se verificó externamente la fecha real del evento. El CSV conserva DD y deterioro brutos, junto a una bandera que invalida su interpretación económica si la variante o su control están afectados. Sin exposición detectada tampoco equivale a precios certificados. No se atribuye ni descarta un efecto sobre el PnL final sin una captura reconciliada y otra evaluación separada.',
        '- Los controles y variantes usan la misma captura C y la misma imagen congelada; el checkout actual tiene diferencias ajenas a este trabajo. Hash de imagen, commit de referencia, spec y captura en CSV; el SHA de commit por sí solo no describe el árbol sucio.',
        '- No se modificaron pesos, thresholds, optimizer, decision_log ni servicios productivos. No se realizó rebuild, deploy, commit o push. El shadow desde 2026-10-27 requiere otra decisión.',
        f"- Corridas medidas en este archivo: {len(rows)}. Etiquetas de configuración medidas: {len({r['config'] for r in rows})}; la combinación puede coincidir con una política OFAT. El control W3 de 6M se omite por cobertura.",
        '- Validación: test dorado y determinismo en golden_validation.json; 39 ruedas del control W1 también coinciden íntegramente con C en control_W1_full_prefix_validation.json. Los 122 archivos nativos cotejados no cambiaron (engine_isolation_validation.json).']
    if (base/'measurement_validation.json').exists():
        lines+=['- Recuperación operativa: la exportación se separó del proceso del motor después de terminaciones con código 137. Se reutilizaron cortes completos guardados; no se recalcularon señales ni fills para recuperar esas corridas. El nuevo proceso reprodujo todas las métricas y operaciones byte por byte del control W1 (measurement_validation.json).']
    (base/'barrido_informe.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return rows


def orchestrate(base,repo):
    plan=read(base/'preregistro.json');plan.update(_repo=str(repo),_prereg_sha256=sha(base/'preregistro.json'))
    validation=read(base/'golden_validation.json')
    if not all(validation[k]['equal'] for k in ('fills','orders','daily','determinism_csv')):
        raise RuntimeError('GOLDEN_GATE_NOT_PASSED')
    implementation=base/'implementation'
    if not implementation.exists():
        implementation.mkdir()
        shutil.copytree(repo/'src/backtesting',implementation/'backtesting',ignore=shutil.ignore_patterns('__pycache__'))
        shutil.copy2(__file__,implementation/'backtest_rotation_sweep.py')
    if sha(repo/'outputs/backtests'/plan['capture_run']/'input/research_data_capture.json')!=plan['capture_sha256']:
        raise RuntimeError('FROZEN_CAPTURE_CHANGED')
    try:
        jobs=[(config,window,'OFAT') for config in plan['stage1'] for window in plan['eligible_windows']]
        stage1=batch(base,plan,jobs)
        controls={r['window']:r for r in stage1 if r['config']=='control'}
        choices={}
        for key in ('radar_buy_tiers','redeploy_cooldown_sessions','min_hold_sessions','delta_band_mult'):
            candidates=[c for c in plan['stage1'] if c['id']=='control' or key in c['overrides']]
            def rank(c):
                subset=[r for r in stage1 if r['config']==c['id']]
                return (sum(r['net_return_pct']-controls[r['window']]['net_return_pct'] for r in subset)/len(subset),
                        -sum(r['turnover'] for r in subset)/len(subset),-plan['stage1'].index(c))
            best=max(candidates,key=rank)
            choices.update(best['overrides'])
        combination=dict(id='combination',overrides=choices)
        write(base/'combined_selection.json',dict(combination=combination,selection='PREREGISTERED_MEAN_DELTA_THEN_TURNOVER_THEN_ORDER'))
        batch(base,plan,[(combination,w,'COMBINED') for w in plan['eligible_windows']])
        control=plan['stage1'][0]
        batch(base,plan,[(c,w,'SLIPPAGE',s) for c in (control,combination)
                        for w in plan['eligible_windows'] for s in ('0.0005','0.003')])
    finally:
        deliver(base,plan)


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='measure':
        measure_saved(Path(sys.argv[2]))
    else:
        parser=argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--output',type=Path,required=True)
        parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[1])
        parser.add_argument('--report-only',action='store_true')
        args=parser.parse_args()
        if args.report_only:
            plan=read(args.output/'preregistro.json');plan['_prereg_sha256']=sha(args.output/'preregistro.json')
            deliver(args.output,plan)
        else:
            orchestrate(args.output.resolve(),args.repo.resolve())
