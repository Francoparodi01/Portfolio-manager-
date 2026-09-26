import asyncio, contextlib, json
from pathlib import Path
from types import SimpleNamespace
import asyncpg
from src.core.config import get_config
from src.agentic.tools import read_only_dsn
from scripts.run_agent import async_main
async def check():
    cfg=get_config()
    conn=await asyncpg.connect(read_only_dsn(cfg.database.url))
    try:
        assert await conn.fetchval('SHOW default_transaction_read_only') == 'on'
        try:
            await conn.execute('UPDATE decision_log SET id=id WHERE FALSE')
        except asyncpg.ReadOnlySQLTransactionError:
            print('DATABASE_WRITE_GUARD_VERIFIED')
        else:
            raise AssertionError('read-only protection failed')
    finally:
        await conn.close()
    target=Path('/verification/smoke-agent.json')
    args=SimpleNamespace(force=True,owner_chat_id=None,model='qwen2.5:3b',max_steps=4,json=True,
        goal='Consultá mi cartera con get_portfolio_snapshot y los outcomes registrados con get_performance. Resumí exposición y límites de evidencia. No uses otras herramientas ni propongas operaciones.')
    with target.open('w',encoding='utf-8') as stream, contextlib.redirect_stdout(stream):
        code=await async_main(args)
    report=json.loads(target.read_text(encoding='utf-8'))
    print(json.dumps({'status':report['status'],'stop':report['stop_reason'],'audit':report['audit_persisted'],'run_id':report['run_id'],
        'steps':[{'kind':s['decision']['kind'],'tool':s['decision']['tool_name'],'ok':s['observation']['ok'] if s['observation'] else None,'error':s['observation']['error'] if s['observation'] else None} for s in report['steps']]}))
    assert code==0 and report['audit_persisted'], 'live loop smoke failed'
asyncio.run(check())
