"""Inspectable research accounting and phone-readable PDF/CSV exports."""
from collections import Counter, defaultdict
from datetime import datetime
import csv
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape
import zipfile

from src.backtesting.benchmarks import reference_curves
from src.backtesting.spec import ART, describe, spec_hash
from src.backtesting.store import write


def read(path, default=None):
    return json.loads(path.read_text(encoding='utf8')) if path.exists() else default


def summarize(path: Path, returncode: int) -> dict:
    spec = read(path/'job.json')['spec']
    directory = path/'replay'
    traces = []
    analyses = []
    cuts = []
    # Do not retain hundreds of multi-megabyte native reports in worker memory.
    for file in sorted(directory.glob('cut_*.json')):
        raw=read(file)
        plan=(raw.get('main_return') or {}).get('execution_plan') or raw.get('execution_plan')
        for cohort,key in [('cartera','results'),('universo','universe_results')]:
            for row in raw.get(key) or []:
                quality=row.get('data_quality') or {}
                analyses.append(dict(cutoff=raw['cutoff_art'],cohort=cohort,ticker=row['ticker'],signal=row.get('decision'),
                    score=row.get('final_score'),confidence=row.get('confidence'),technical_regime=row.get('technical_regime'),
                    technical_signal=row.get('technical_signal'),price_status=quality.get('price_status'),
                    provenance_status=quality.get('provenance_status'),warnings=row.get('warnings'),reasoning=row.get('reasoning')))
        cash_start = (raw.get('main_return') or {}).get('backtest_cash_start') or {}
        cuts.append(dict(cutoff=raw['cutoff_art'],completed=raw.get('main_completed'),plan_present=plan is not None,
            plan_feasible=plan.get('feasible') if plan else None,gate=plan.get('gate') if plan else None,
            universe_count=(raw.get('universe_membership') or {}).get('count',0),analyzed_universe=len(raw.get('universe_results') or []),
            fills=len(raw.get('simulated_fills') or []),status='EXECUTION_PLAN' if plan is not None else 'NO_EXECUTION_PLAN',error=raw.get('error'),
            cash_start_attempted=cash_start.get('attempted'),cash_start_candidates=cash_start.get('candidates'),
            cash_start_buy_orders=cash_start.get('buy_orders'),cash_start_gate=cash_start.get('gate')))
        keys=['main_completed','cutoff_art','universe_membership','candle_reads','simulated_fills','simulation_orders','valuation_after_interval']
        traces.append(dict({k:raw.get(k) for k in keys},execution_plan=plan))
        del raw
    valid = [t for t in traces if t.get('main_completed')]
    summary = read(directory/'summary.json', [])
    fills = read(directory/'fills.json', [])
    orders = read(directory/'orders.json', [])
    daily = read(directory/'daily.json', [])
    # A crashed child may not flush aggregate files; trace records are atomic units.
    if not fills:
        fills = [f for t in valid for f in (t.get('simulated_fills') or [])]
    if not orders:
        orders = [o for t in valid for o in (t.get('simulation_orders') or [])]
    isolation = read(path/'isolation.json', {})
    coverage = len(valid)/len(spec['cutoffs'])
    universe_gaps = sum(not (t.get('universe_membership') or {}).get('count') for t in valid)
    candle_gaps = sum(not any(r['rows'] >= 60 for r in (t.get('candle_reads') or [])) for t in valid)
    unsafe = bool(isolation.get('external_io_attempts')) or not isolation.get('no_persist')
    evaluable = bool(valid) and not unsafe and universe_gaps == 0 and candle_gaps == 0
    status = 'COMPLETE' if evaluable and coverage == 1 and returncode == 0 else 'PARTIAL' if evaluable else 'NOT_EVALUABLE'
    final = valid[-1].get('valuation_after_interval') if evaluable else None
    if daily and final and daily[-1]['asof'] >= final['asof']:
        final = daily[-1]
    capital = float(spec['initial_cash'])
    nav = final['nav_ars'] if final else None
    peak = capital
    drawdown = 0.0
    for row in daily:
        peak = max(peak, row['nav_ars'])
        drawdown = min(drawdown, (row['nav_ars']/peak-1)*100)
    # Round trips are completed only when the simulated position returns to zero.
    pending = defaultdict(float)
    cycles = []
    for f in fills:
        if f['side'] == 'SELL':
            pending[f['ticker']] += f['realized_pnl']
            if f['position_after'] == 0:
                cycles.append(dict(ticker=f['ticker'],closed_at=f['filled_at'],pnl_ars=pending.pop(f['ticker'])))
    wins = sum(c['pnl_ars'] > 0 for c in cycles)
    horizons = []
    for h in spec['horizons']:
        target = spec['sessions'][h-1] if h <= len(spec['sessions']) else None
        match = next((d for d in daily if d['asof'][:10] == target), None)
        horizons.append(dict(ruedas=h,date=target,return_pct=match['return_pct'] if match and evaluable else None))
    decisions = []
    for trace in valid:
        plan = (trace.get('main_return') or {}).get('execution_plan') or trace.get('execution_plan') or {}
        for d in plan.get('decisions', []):
            decisions.append(dict(cutoff=trace['cutoff_art'],**d))
    assets = final['assets'] if final else []
    alerts = [
        'Simulación retrospectiva con el motor actual; no reproduce necesariamente la versión histórica del bot.',
        'Macro reconstruida con cierres diarios anteriores. No conserva la publicación original intradiaria.',
        'Cotización registrada posterior al análisis + costos modelados; sin garantía de liquidez, spread ni ejecución real.',
        'Comisión y deslizamiento elegidos se aplican a los fills simulados. Los criterios y costos internos de planificación conservan la configuración de producción.',
        'Sin dividendos en efectivo, impuestos, inflación, rendimiento del efectivo ni comparación validada con HOLD.',
        'Eventos, sentimiento y ajustes dependen de registros con fechas verificables; las revisiones históricas pueden faltar.',
        'Earnings sin archivo completo e information coefficient sin reconstrucción: se usan los fallbacks del motor. LLM desactivado.',
    ]
    if spec.get('cash_start_policy'):
        alerts.insert(0, 'Arranque desde efectivo habilitado SOLO en el backtest: candidatos elegibles del radar pasan por el planner y sus guardas. No modifica el motor operativo.')
    elif spec['startup'] == 'radar':
        alerts.insert(0, 'VARIANTE EXPERIMENTAL: habilita candidatos del radar al iniciar sólo con efectivo. No es el arranque nativo de producción.')
    if universe_gaps or candle_gaps:
        alerts.insert(0, f'Cobertura insuficiente: {universe_gaps} cortes sin universo y {candle_gaps} sin historial técnico mínimo. Resultado no evaluable.')
    if status == 'PARTIAL':
        alerts.insert(0, 'CORRIDA PARCIAL: las cifras llegan sólo hasta la última valuación disponible, no hasta la fecha solicitada.')
    stale = [a['ticker'] for a in assets if a['quantity'] and a['mark']['basis'] != 'RECORDED_INTRADAY_LAST']
    if stale:
        alerts.insert(0, 'Valuación con precios anteriores: ' + ', '.join(stale))
    if not fills and evaluable:
        alerts.insert(0, 'Sin operaciones simuladas. Revisar candidatos, bloqueos y cotizaciones de ejecución; esto no demuestra efectividad.')
    capture = read(path/'input'/'research_data_capture.json', {})
    touched = {f['ticker'] for f in fills}
    flags = next((x['rows'] for x in capture.get('observations', []) if x['name'] == 'quality_flags'), [])
    flagged = sorted({r['ticker'] for r in flags if r.get('ticker') in touched and r.get('resolution_status') == 'OPEN'})
    if flagged:
        alerts.insert(0, 'Activos operados con alertas abiertas en la captura: ' + ', '.join(flagged) + '. PnL provisional.')
    benchmarks = dict(status='NOT_EVALUABLE', reason='Sin corrida evaluable o sin captura de precios.', daily=[])
    if evaluable and capture.get('observations'):
        try:
            benchmarks = reference_curves(spec, capture['observations'], daily)
        except Exception as exc:  # a reference must never discard a finished replay
            benchmarks = dict(status='NOT_EVALUABLE', reason='Error al calcular referencias: '+type(exc).__name__, daily=[])
    if benchmarks['status'] != 'COMPLETE' and evaluable:
        alerts.append('Referencias SPY / efectivo no evaluables: '+benchmarks['reason'])
    if unsafe:
        alerts.insert(0, 'La validación de aislamiento no finalizó correctamente. No se publica rentabilidad.')
    return dict(status=status, spec=spec, coverage_pct=coverage*100, completed_cuts=len(valid),
                planned_cuts=len(spec['cutoffs']), valued_until=final['asof'] if final else None,
                economic_verification='NOT_VERIFIED', research_net_pnl_ars=nav-capital if nav is not None else None,
                nav_ars=nav, return_pct=(nav/capital-1)*100 if nav is not None else None,
                cash_ars=final['cash_ars'] if final else None, daily_max_drawdown_pct=drawdown if final and daily else None,
                fees_ars=sum(f['fee_ars'] for f in fills), slippage_ars=sum(f['slippage_ars'] for f in fills),
                realized_pnl_ars=sum(f['realized_pnl'] or 0 for f in fills) if final else None,
                unrealized_pnl_ars=sum(a['unrealized_pnl'] for a in assets) if final else None,
                completed_cycles=len(cycles), profitable_cycles=wins, win_rate_pct=wins/len(cycles)*100 if cycles and evaluable else None,
                fill_counts=dict(Counter(f['side'] for f in fills)), order_status_counts=dict(Counter(o['status'] for o in orders)),
                decision_counts=dict(Counter(d['action'] for d in decisions)), horizons=horizons,
                assets=assets, cycles=cycles, fills=fills, orders=orders, daily=daily, decisions=decisions, analyses=analyses, cuts=cuts, alerts=alerts,
                benchmarks=benchmarks, source_rows={x['name']:x['row_count'] for x in capture.get('observations', [])})


def fmt(value, suffix=''):
    return 'No evaluable' if value is None else f'{value:,.2f}'.replace(',', '@').replace('.', ',').replace('@', '.')+suffix


def reference_line(b):
    if not b or b.get('status') != 'COMPLETE':
        return ''
    return (f"{b['ticker']} comprado y mantenido: {fmt(b['reference']['return_pct'],'%')} · efectivo 0,00% · "
            f"bot vs {b['ticker']}: {fmt(b['excess_vs_reference_pp'],' pp')}\n")


def reference_chart(b):
    """Cumulative return of bot, reference and cash on one axis; end labels + legend."""
    from reportlab.lib import colors
    from reportlab.graphics.shapes import Drawing, Line, String
    from reportlab.graphics.charts.lineplots import LinePlot
    days, capital = b['daily'], b['cash']['final_nav_ars']
    ink, muted = colors.HexColor('#253449'), colors.HexColor('#58677A')
    series = [('Bot', 'bot_nav_ars', colors.HexColor('#257de0'), None),
              (b['ticker'], 'reference_nav_ars', colors.HexColor('#f47230'), None),
              ('Efectivo', 'cash_nav_ars', colors.HexColor('#9c9c9c'), (3, 2))]
    data = [[(i, (d[key]/capital-1)*100) for i, d in enumerate(days) if d.get(key) is not None] for _, key, _, _ in series]
    values = [y for line in data for _, y in line] or [0]
    low, high = min(values+[0]), max(values+[0])
    pad = max((high-low)*0.08, 0.5)
    drawing = Drawing(355, 215)
    plot = LinePlot()
    plot.x, plot.y, plot.width, plot.height = 34, 26, 262, 160
    plot.data = data
    for k, (_, _, color, dash) in enumerate(series):
        plot.lines[k].strokeColor, plot.lines[k].strokeWidth = color, 2 if dash is None else 1.5
        if dash: plot.lines[k].strokeDashArray = dash
    plot.xValueAxis.valueMin, plot.xValueAxis.valueMax = 0, max(len(days)-1, 1)
    months = [i for i, d in enumerate(days) if i == 0 or d['date'][5:7] != days[i-1]['date'][5:7]]
    plot.xValueAxis.valueSteps = months
    plot.xValueAxis.labelTextFormat = lambda i: days[int(i)]['date'][8:10]+'/'+days[int(i)]['date'][5:7] if 0 <= int(i) < len(days) else ''
    plot.yValueAxis.valueMin, plot.yValueAxis.valueMax = low-pad, high+pad
    plot.yValueAxis.labelTextFormat = lambda v: f'{v:+.0f}%'
    for axis in (plot.xValueAxis, plot.yValueAxis):
        axis.labels.fontName, axis.labels.fontSize, axis.labels.fillColor = 'Helvetica', 7, muted
        axis.strokeColor = colors.HexColor('#C9D1DB')
    plot.yValueAxis.visibleGrid, plot.yValueAxis.gridStrokeColor = True, colors.HexColor('#E6E9EE')
    plot.xValueAxis.visibleTicks = plot.yValueAxis.visibleTicks = False
    drawing.add(plot)
    scale = lambda y: plot.y+(y-(low-pad))/((high+pad)-(low-pad))*plot.height
    ends = sorted(((scale(line[-1][1]), k) for k, line in enumerate(data) if line), key=lambda e: e[0])
    placed = []
    for y, k in ends:  # keep end labels at least 9 pt apart
        y = max(y, placed[-1]+9) if placed else y
        placed.append(y)
        last = data[k][-1][1]
        drawing.add(String(plot.x+plot.width+5, y-2.5, f"{series[k][0]} {last:+.1f}%".replace('.', ','), fontName='Helvetica', fontSize=7, fillColor=ink))
    x = plot.x
    for name, _, color, dash in series:  # legend row above the plot
        drawing.add(Line(x, 205, x+14, 205, strokeColor=color, strokeWidth=2, strokeDashArray=dash))
        drawing.add(String(x+18, 202.5, name, fontName='Helvetica', fontSize=8, fillColor=ink))
        x += 30+len(name)*5
    return drawing


def brief(result):
    return (f"Backtest {result['status']} · /analisis_full\n"
            f"{result['spec']['start']} a {result['spec']['end']}\n"
            f"Cortes completados: {result['completed_cuts']}/{result['planned_cuts']}\n"
            f"Rendimiento simulado: {fmt(result['return_pct'], '%')}\n"
            f"PnL con costos modelados: ARS {fmt(result['research_net_pnl_ars'])}\n"
            f"Compras: {result['fill_counts'].get('BUY',0)} · ventas: {result['fill_counts'].get('SELL',0)}\n"
            f"Ciclos cerrados: {result['completed_cycles']} · ganadores: {fmt(result['win_rate_pct'],'%')}\n"
            +reference_line(result.get('benchmarks'))+
            'Resultado de investigación; no es rentabilidad real verificada. Ver cobertura y limitaciones en el PDF.')


def export_csv(path, rows, default_fields=()):
    fields = list(dict.fromkeys(k for row in rows for k in row)) or list(default_fields)
    with path.open('w', newline='', encoding='utf-8-sig') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        def cell(value):
            if isinstance(value,(dict,list)): value=json.dumps(value,ensure_ascii=False)
            # Avoid spreadsheet formulas in text fields coming from captured reasons.
            if isinstance(value,str) and value.startswith(('=','+','-','@')): value="'"+value
            return value
        writer.writerows({k:cell(v) for k,v in r.items()} for r in rows)


def pdf(path: Path, result):
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
    styles=getSampleStyleSheet()
    styles['Heading2'].keepWithNext=True
    styles['Heading3'].keepWithNext=True
    styles.add(ParagraphStyle(name='BodyPhone',fontName='Helvetica',fontSize=11,leading=15,spaceAfter=8,textColor=colors.HexColor('#253449')))
    styles.add(ParagraphStyle(name='SmallPhone',parent=styles['BodyPhone'],fontSize=9,leading=12))
    story=[]
    def p(text,style='BodyPhone'):
        return Paragraph(escape(str(text)).replace('\n','<br/>'),styles[style])
    def heading(text): story.append(p(text,'Heading2'))
    def pairs(rows):
        table=Table([[p(a,'SmallPhone'),p(b,'SmallPhone')] for a,b in rows],colWidths=[210,145],hAlign='LEFT')
        table.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(-1,-1),colors.HexColor('#F0F4F9')),('LEFTPADDING',(0,0),(-1,-1),9),('RIGHTPADDING',(0,0),(-1,-1),9),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
        story.append(table);story.append(Spacer(1,10))
    story.extend([p('QUANTIA','Title'),p('Backtest de /analisis_full','Heading1'),p(result['status']+' | Simulación histórica'),p(describe(result['spec']))])
    pairs([('Análisis completados',f"{result['completed_cuts']} / {result['planned_cuts']}"),('Valuado hasta',result['valued_until'] or 'Sin valuación válida'),('Evidencia económica','No verificada')])
    selected=set(result['spec']['metrics'])
    if 'rentabilidad' in selected:
        heading('Rentabilidad simulada')
        pairs([('Capital final ARS',fmt(result['nav_ars'])),('Resultado ARS',fmt(result['research_net_pnl_ars'])),('Rendimiento',fmt(result['return_pct'],'%')),('Realizado ARS',fmt(result['realized_pnl_ars'])),('No realizado ARS',fmt(result['unrealized_pnl_ars'])),('Efectivo final ARS',fmt(result['cash_ars']))])
    b=result.get('benchmarks') or {}
    if 'rentabilidad' in selected:
        heading('Contra referencias')
        if b.get('status')=='COMPLETE':
            rows=[['', 'Rendimiento', 'Caída máx.', 'Capital final ARS']]
            for name,key in [('Bot','bot'),(b['ticker']+' comprado y mantenido','reference'),('Efectivo (no operar)','cash')]:
                s=b.get(key) or {}
                rows.append([name,fmt(s.get('return_pct'),'%'),fmt(s.get('max_drawdown_pct'),'%'),fmt(s.get('final_nav_ars'))])
            table=Table([[p(c,'SmallPhone') for c in r] for r in rows],colWidths=[120,70,70,95],hAlign='LEFT')
            table.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#E3E9F1')),('BACKGROUND',(0,1),(-1,-1),colors.HexColor('#F0F4F9')),('LINEBELOW',(0,0),(-1,-1),0.5,colors.white),('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),2)]))
            story.append(table);story.append(Spacer(1,8))
            story.append(KeepTogether([p('Rendimiento acumulado','Heading3'),reference_chart(b)]))
            e=b['entry']
            pairs([('Bot vs '+b['ticker'],fmt(b['excess_vs_reference_pp'],' pp')),('Bot vs efectivo',fmt(b['excess_vs_cash_pp'],' pp')),
                   ('Días con el bot arriba de '+b['ticker'],fmt(b['bot_days_above_reference_pct'],'%')),
                   (b['ticker']+' si se liquida al final, ARS',fmt(b['reference_after_exit_nav_ars'])),
                   ('Entrada '+b['ticker'],f"{e['quantity']} u. a {fmt(e['fill_price'])} ({datetime.fromisoformat(e['filled_at']).astimezone(ART).strftime('%d/%m %H:%M')} ART)"),
                   ('Cierres de '+b['ticker']+' con precio anterior',str(b['stale_reference_marks']))])
            story.append(p(' '.join(b['assumptions'].values())+' Un solo camino histórico: sin rango de confianza.','SmallPhone'))
        else:
            story.append(p('No evaluable: '+b.get('reason','sin datos')))
    if 'riesgo' in selected:
        heading('Riesgo')
        pairs([('Caída máxima entre cierres diarios',fmt(result['daily_max_drawdown_pct'],'%'))])
        story.append(p('Incluye el capital inicial. No mide el peor movimiento intradiario ni liquida posiciones abiertas.'))
    if 'operaciones' in selected:
        heading('Operaciones y costos')
        pairs([('Compras / ventas',f"{result['fill_counts'].get('BUY',0)} / {result['fill_counts'].get('SELL',0)}"),('Comisiones modeladas ARS',fmt(result['fees_ars'])),('Deslizamiento modelado ARS',fmt(result['slippage_ars']))])
        story.append(p('Órdenes: '+json.dumps(result['order_status_counts'],ensure_ascii=False)))
        story.append(p('Decisiones: '+json.dumps(result['decision_counts'],ensure_ascii=False)))
    if 'efectividad' in selected:
        heading('Efectividad de ciclos cerrados')
        pairs([('Ciclos cerrados',str(result['completed_cycles'])),('Ciclos ganadores',str(result['profitable_cycles'])),('Proporción ganadora',fmt(result['win_rate_pct'],'%'))])
        story.append(p('Un ciclo termina cuando la posición vuelve a cero. Acumula ventas parciales y costos. No es precisión predictiva ni una muestra de operaciones independientes. Sin ciclos, no hay porcentaje.'))
    if 'horizontes' in selected:
        heading('Horizontes acumulados desde T0')
        pairs([(str(h['ruedas'])+' ruedas',fmt(h['return_pct'],'%')) for h in result['horizons']])
    if 'activos' in selected:
        heading('Resultado por activo')
        if not result['assets']:story.append(p('Sin posiciones con resultado evaluable.'))
        for a in sorted(result['assets'],key=lambda a:a['total_pnl'],reverse=True):
            story.append(KeepTogether([p(a['ticker'],'Heading3'),p(f"PnL ARS {fmt(a['total_pnl'])} | Cantidad abierta {a['quantity']}\nRealizado {fmt(a['realized_pnl'])} | No realizado {fmt(a['unrealized_pnl'])}")]))
    heading('Qué decisiones se tomaron')
    story.append(p('El ZIP contiene análisis y señales por activo, decisiones operativas por corte, órdenes, compras y ventas simuladas, motivos, precios, costos y evolución diaria. La señal del activo se distingue de la decisión del plan. Si el motor no genera un plan, se registra NO_EXECUTION_PLAN.'))
    heading('Alcance y calidad de los datos')
    for alert in result['alerts']:story.append(p('- '+alert,'SmallPhone'))
    story.append(p('Fuentes: captura de market_candles, market_prices, sentiment_aggregated y eventos de Quantia; series públicas Yahoo Finance y ArgentinaDatos. El archivo manifest.json conserva huellas de fuentes y código; cada corte conserva la procedencia en el servidor.','SmallPhone'))
    def footer(canvas,doc):
        canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#58677A'));canvas.drawString(32,20,'QUANTIA | Investigación | '+str(doc.page))
    SimpleDocTemplate(str(path),pagesize=(419.5,650),rightMargin=32,leftMargin=32,topMargin=28,bottomMargin=35,title='Quantia - Backtest /analisis_full',author='Quantia').build(story,onFirstPage=footer,onLaterPages=footer)


def export(path: Path, result):
    out=path/'exports';out.mkdir(exist_ok=True)
    write(out/'resultado.json',result)
    write(out/'parametros.json',result['spec'])
    for name,field,columns in [('operaciones','fills',['cutoff','filled_at','ticker','side','quantity','fill_price','fee_ars','realized_pnl']),
        ('ordenes','orders',['cutoff','ticker','side','planned_qty','status','filled_qty']),('cartera_diaria','daily',['asof','cash_ars','nav_ars','return_pct']),
        ('activos','assets',['ticker','quantity','realized_pnl','unrealized_pnl','total_pnl']),('decisiones','decisions',['cutoff','ticker','action','reason_primary','block_code']),
        ('horizontes','horizons',['ruedas','date','return_pct']),('analisis_por_activo','analyses',['cutoff','cohort','ticker','signal','score']),
        ('cortes','cuts',['cutoff','completed','plan_present','status'])]:
        export_csv(out/(name+'.csv'),result[field],columns)
    export_csv(out/'referencias_diarias.csv',(result.get('benchmarks') or {}).get('daily',[]),
               ['date','asof','bot_nav_ars','reference_nav_ars','cash_nav_ars','reference_mark'])
    app=Path(__file__).resolve().parents[2]
    inputs=[*sorted((path/'input').glob('*.json')),*sorted((app/'src'/'backtesting').glob('*.py')),app/'scripts'/'run_analysis.py']
    inputs += sorted((app/'src'/'analysis').rglob('*.py'))
    manifest={}
    for p in inputs:
        if p.is_file():
            with p.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
            manifest[str(p.relative_to(path) if p.is_relative_to(path) else p.relative_to(app))]=digest
    write(out/'manifest.json',dict(sha256=manifest,source_rows=result['source_rows'],
          methodology=result['spec']['methodology'],spec_sha256=spec_hash(result['spec']),
          rotation_overrides=result['spec'].get('rotation_overrides',{})))
    pdf(out/'informe.pdf',result)
    with zipfile.ZipFile(out/'datos.zip','w',zipfile.ZIP_DEFLATED) as archive:
        for p in sorted(out.iterdir()):
            if p.suffix in ('.json','.csv'):archive.write(p,p.name)
    return out
