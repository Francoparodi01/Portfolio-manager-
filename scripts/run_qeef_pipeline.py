"""QEEF operator: read-only status or explicitly enabled research consumer. No migration."""
import argparse
import asyncio
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.analysis.qeef.delivery import Outbox
from src.analysis.qeef.hooks import enabled,settings
from src.analysis.qeef.monitor import report,markdown
from src.analysis.qeef.worker import cycle


async def run(args):
    import asyncpg
    if args.command=='work' and not enabled(): raise ValueError('QEEF_CAPTURE_ENABLED must be explicitly true')
    owner=int(os.environ['TELEGRAM_CHAT_ID'])
    start=datetime.fromisoformat(os.environ['QEEF_CAPTURE_START_AT']) if os.getenv('QEEF_CAPTURE_START_AT') else None
    outbox=Outbox(os.environ['QEEF_SPOOL_PATH']) if os.getenv('QEEF_SPOOL_PATH') else None
    conn=await asyncpg.connect(os.environ['DATABASE_URL'],timeout=10)
    try:
        if args.command=='work':
            owner,start,path=settings()
            code=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
            result=await cycle(conn,Outbox(path),owner=owner,start=start,as_of=datetime.now(timezone.utc),code_version=code)
        else:
            result=await report(conn,owner=owner,start=start,outbox=outbox)
        if args.output:
            path=Path(args.output);path.mkdir(parents=True,exist_ok=True)
            (path/'status.json').write_text(json.dumps(result,indent=2,default=str),encoding='utf-8')
            if args.command=='status': (path/'status.md').write_text(markdown(result),encoding='utf-8')
        print(json.dumps(result,default=str))
    finally:
        await conn.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['status','work'])
    parser.add_argument('--output')
    asyncio.run(run(parser.parse_args()))
