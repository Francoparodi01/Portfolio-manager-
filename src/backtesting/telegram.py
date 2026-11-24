"""Self-service Telegram interface. Caller must apply the production chat guard."""
from datetime import datetime, timezone
import json

from telegram import InlineKeyboardButton as Button, InlineKeyboardMarkup as Keyboard

from src.backtesting import store
from src.backtesting.spec import describe, parse_spec

HELP = '''BACKTEST · /analisis_full
Simulá decisiones con efectivo ficticio y recibí PDF + ZIP con CSV.

Ejemplo simple:
/backtest hasta=26/10/2026

Personalizado:
/backtest desde=01/08/2026 hasta=26/10/2026 capital=3m tickers=auto medir=todo

Podés elegir:
• desde + hasta, o ruedas=50 + hasta
• capital=3000000 (ARS)
• tickers=auto o AMD,NVDA,PYPL,AXP,MU
• horas=11,13,16 (Argentina)
• medir=rentabilidad,riesgo,operaciones,efectividad,activos,horizontes o todo
• horizontes=5,10,20 (ruedas acumuladas)
• comision=0.6 deslizamiento=0.15 (% por lado en fills simulados)

Filtros opcionales, solo para el backtest:
• delta_band_mult=1|2|3 (multiplicador de la banda de rebalanceo)
• radar_buy_tiers=ALL|SIN_ESPERAR_FUNDING|SOLO_ABRIR_POSICION
• redeploy_cooldown_sessions=0|3|5 (ruedas antes de recomprar)
• min_hold_sessions=0|3|5|10 (tenencia mínima para ventas discrecionales)
Elegí un solo valor por parámetro. Sin estos filtros se conserva el control.
El primer corte desde efectivo y las excepciones de riesgo se conservan.

Por defecto: últimas 20 ruedas completas, $3 millones, universo automático, 11/13/16 y todas las métricas. Hasta 120 ruedas por corrida.

/backtest estado
/backtest cancelar
/backtest resultado
/backtest historial

Se usa el motor actual con datos históricos disponibles. Puede mantener el efectivo; los faltantes se informan. No se envían órdenes al broker.
El backtest permite compras desde efectivo cuando hay candidatos elegibles, conservando las guardas. No modifica /analisis_full operativo. arranque=nativo y arranque=radar se conservan por compatibilidad.'''


def keyboard(job=None):
    if job:
        jid=job['id']
        buttons=[[Button('Actualizar estado',callback_data='bt:estado:'+jid)]]
        if job['status'] in store.ACTIVE:buttons.append([Button('Cancelar corrida',callback_data='bt:cancelar:'+jid)])
        else:buttons.append([Button('Descargar resultados',callback_data='bt:resultado:'+jid)])
        return Keyboard(buttons)
    return Keyboard([[Button('Últimas 20 ruedas',callback_data='bt:preset:20'),Button('Últimas 50 ruedas',callback_data='bt:preset:50')],
                     [Button('Estado',callback_data='bt:estado'),Button('Último informe',callback_data='bt:resultado')]])


def latest(owner, job_id=None):
    if job_id:
        path=store.directory(owner,job_id)
        return path,json.loads((path/'job.json').read_text())
    jobs=store.jobs(owner)
    if not jobs:raise ValueError('Todavía no tenés corridas. Usá /backtest para ver los parámetros.')
    return store.directory(owner,jobs[0]['id']),jobs[0]


def status(path,job):
    progress={}
    if (path/'progress.json').exists():progress=json.loads((path/'progress.json').read_text())
    total=len(job['spec']['cutoffs'])
    done=progress.get('completed',0)
    return (f"Backtest {job['id'][:8]} · {job['status']}\n"+describe(job['spec'])+
            f"\nProgreso: {done}/{total} cortes procesados"+
            ('\nÚltimo corte: '+progress['cutoff'] if progress.get('cutoff') else '')+
            ('\n'+job['error'] if job.get('error') else '')+
            ('\nCancelación solicitada.' if (path/'cancel').exists() and job['status'] in store.ACTIVE else ''))


async def handle(update, context, args=None):
    owner=int(update.effective_chat.id)
    args=list(context.args or []) if args is None else args
    async def reply(text,markup=None):
        await context.bot.send_message(chat_id=owner,text=text,reply_markup=markup)
    try:
        if not args or args[0].lower() in ('ayuda','help'):
            return await reply(HELP,keyboard())
        action=args[0].lower()
        if action=='historial':
            jobs=store.jobs(owner)[:8]
            text='Tus últimas corridas:\n'+'\n'.join(f"{j['spec']['start']} a {j['spec']['end']} · {j['status']}\n/backtest resultado {j['id']}" for j in jobs)
            return await reply(text if jobs else 'Todavía no tenés corridas.')
        if action in ('estado','cancelar','resultado'):
            if len(args)>2:raise ValueError('Usá /backtest '+action+' y opcionalmente el ID de la corrida.')
            path,job=latest(owner,args[1] if len(args)>1 else None)
            if action=='cancelar':job=store.cancel(owner,job['id'])
            if action=='resultado' and job['status'] in store.TERMINAL and (path/'exports'/'informe.pdf').exists():
                await reply(job.get('result_summary',status(path,job)))
                for name in ('informe.pdf','datos.zip'):
                    with (path/'exports'/name).open('rb') as stream:
                        await context.bot.send_document(chat_id=owner,document=stream,filename='Quantia_'+job['id'][:8]+'_'+name,read_timeout=90,write_timeout=90)
                return
            return await reply(status(path,job),keyboard(job))
        spec=parse_spec(' '.join(args))
        health=store.root()/'worker.json'
        if not health.exists() or (datetime.now(timezone.utc)-datetime.fromisoformat(json.loads(health.read_text())['updated_at'])).total_seconds()>120:
            raise ValueError('El worker de backtest no está disponible ahora. No se inició la corrida; reintentá más tarde.')
        job=store.enqueue(owner,str(update.update_id),spec)
        await reply('Corrida en cola: '+job['id'][:8]+'\n'+describe(spec)+'\nTe enviaré el PDF y los CSV cuando termine. Podés seguir usando el bot.',keyboard(job))
    except ValueError as exc:
        await reply(str(exc))


async def callback(update,context):
    await update.callback_query.answer()
    fields=update.callback_query.data.split(':')
    if len(fields)==3 and fields[1]=='preset':
        if fields[2] not in ('20','50'):return
        return await handle(update,context,['ruedas='+fields[2]])
    if len(fields) in (2,3) and fields[1] in ('estado','resultado','cancelar'):
        return await handle(update,context,fields[1:])
