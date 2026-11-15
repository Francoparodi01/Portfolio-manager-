"""Dedicated, bounded worker. No Telegram polling and no broker access."""
import asyncio
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from src.backtesting import store
from src.backtesting.bridge import prepare_spec
from src.backtesting.report import brief, export, read, summarize


def heartbeat():
    store.write(store.root()/'worker.json',dict(updated_at=store.stamp(),pid=os.getpid()))


def child(path: Path, module: str, *, capture=False):
    env={k:v for k,v in os.environ.items() if k in ('PATH','HOME','LANG','TZ','PYTHONPATH') or capture and k=='DATABASE_URL'}
    env.update(OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',PYTHONDONTWRITEBYTECODE='1')
    with (path/('capture.log' if capture else 'replay.log')).open('wb') as log:
        proc=subprocess.Popen([sys.executable,'-m',module,str(path)],env=env,stdout=log,stderr=log,start_new_session=True)
        start=time.monotonic()
        limit=900 if capture else 6*3600
        try:
            while proc.poll() is None:
                heartbeat()
                if (path/'cancel').exists() or time.monotonic()-start > limit:
                    os.killpg(proc.pid,signal.SIGTERM)
                    try:proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid,signal.SIGKILL);proc.wait()
                    return 2 if (path/'cancel').exists() else 124
                time.sleep(2)
            return proc.returncode
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid,signal.SIGKILL);proc.wait()


def run(path: Path):
    try:
        job = read(path/'job.json', {})
        store.update(path, spec=prepare_spec(job['spec']))
        code=child(path,'src.backtesting.capture',capture=True)
        if code == 2 or (path/'cancel').exists():
            return store.update(path,status='CANCELLED')
        if code:
            details=read(path/'capture_error.json',{})
            return store.update(path,status='NOT_EVALUABLE',error=details.get('message','No se pudo capturar el histórico. Reintentá o acortá el período.'),error_code=details.get('code','CAPTURE_FAILED'))
        store.update(path,status='RUNNING')
        code=child(path,'src.backtesting.replay')
        if code == 2 or (path/'cancel').exists():
            return store.update(path,status='CANCELLED')
        store.update(path,status='EXPORTING')
        result=summarize(path,code)
        export(path,result)
        return store.update(path,status=result['status'],result_summary=brief(result),replay_returncode=code,
                            completed_cuts=result['completed_cuts'],planned_cuts=result['planned_cuts'])
    except Exception as exc:
        # Never put connection URLs, credentials, tracebacks or provider replies in Telegram.
        print(json.dumps(dict(event='backtest_failed',id=path.name,code=type(exc).__name__)),flush=True)
        return store.update(path,status='FAILED',error='La corrida falló. Los registros se conservaron para diagnóstico.',error_code=type(exc).__name__)


async def notify(path: Path, job):
    if not job.get('notify') or job.get('delivered') or job.get('notify_attempts',0)>=3:return
    from telegram import Bot
    token=os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('SCRAPER_TELEGRAM_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN')
    if not token:return
    store.update(path,notify_attempts=job.get('notify_attempts',0)+1)
    try:
        async with Bot(token) as bot:
            sent=set(job.get('delivered_parts',[]))
            if 'summary' not in sent:
                text=job.get('result_summary') or f"Backtest {job['status']}. "+job.get('error','')
                await bot.send_message(chat_id=job['owner'],text=text+'\n/backtest resultado')
                sent.add('summary');store.update(path,delivered_parts=sorted(sent))
            for name in ('informe.pdf','datos.zip'):
                file=path/'exports'/name
                if file.exists() and name not in sent:
                    with file.open('rb') as stream:
                        await bot.send_document(chat_id=job['owner'],document=stream,filename='Quantia_'+path.name[:8]+'_'+name,read_timeout=90,write_timeout=90)
                    sent.add(name);store.update(path,delivered_parts=sorted(sent))
        store.update(path,delivered=True,delivery_error=None)
    except Exception as exc:
        store.update(path,delivery_error=type(exc).__name__)


def main():
    with (store.root()/'.worker.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        # Never silently resume a portfolio with ambiguous last fills after a crash.
        for job in store.jobs():
            if job['status'] in store.ACTIVE-{'QUEUED'}:
                store.update(store.root()/job['id'],status='INTERRUPTED',error='El worker se reinició durante la corrida. Iniciá una nueva prueba.')
        while True:
            heartbeat()
            chosen=None
            with store.queue_lock():
                queued=[j for j in reversed(store.jobs()) if j['status']=='QUEUED']
                if queued:
                    chosen=store.root()/queued[0]['id']
                    store.update(chosen,status='CAPTURING')
            if chosen:run(chosen)
            for job in store.jobs():
                if job['status'] in store.TERMINAL:
                    asyncio.run(notify(store.root()/job['id'],job))
            if '--once' in sys.argv:return
            time.sleep(5)


if __name__=='__main__':main()
