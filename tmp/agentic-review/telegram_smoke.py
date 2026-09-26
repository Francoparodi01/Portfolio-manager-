import asyncio, hashlib, json, logging, tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from src.core.config import get_config
from src.agentic.read_only import connect_read_only
from scripts import telegram_bot as bot
logging.disable(logging.CRITICAL)

class Sink:
    def __init__(self): self.messages=[]; self.result=None
    async def send_message(self, **kwargs): self.messages.append(kwargs['text'])
    async def send_document(self, **kwargs):
        self.result=json.load(kwargs['document'])
        Path('/verification/telegram-agent-smoke.json').write_text(json.dumps(self.result,ensure_ascii=False,indent=2),encoding='utf-8')

async def main():
    cfg=get_config(); owner=int(cfg.scraper.telegram_chat_id)
    sink=Sink()
    bot.ensure_allowed_chat=AsyncMock(return_value=True)
    goal='Prueba técnica: consultá únicamente get_portfolio_snapshot una vez y resumí brevemente la fecha y cobertura del snapshot. No evalúes compras ni ventas.'
    before=set(Path(tempfile.gettempdir()).glob('quantia_agent_*'))
    await bot.agent_handler(SimpleNamespace(message=True,effective_chat=SimpleNamespace(id=owner)),SimpleNamespace(args=[goal],bot=sink))
    assert sink.result is not None, 'handler did not deliver trace'
    result=sink.result
    assert result['status'] in {'COMPLETE','LIMIT_REACHED'} and result['audit_persisted'], result['status']
    assert set(Path(tempfile.gettempdir()).glob('quantia_agent_*'))==before
    conn=await connect_read_only(cfg.database.url)
    try:
        run=await conn.fetchrow('SELECT status, owner_chat_id FROM agent_runs WHERE id=$1::uuid',result['run_id'])
        steps=await conn.fetch('SELECT step_no, observation, observation_sha256 FROM agent_steps WHERE run_id=$1::uuid ORDER BY step_no',result['run_id'])
        assert run['status']==result['status'] and run['owner_chat_id']==owner
        assert len(steps)==len(result['steps'])
        for persisted,step in zip(steps,result['steps']):
            obs=step['observation']
            if obs:
                expected=hashlib.sha256(obs['content'].encode()).hexdigest()
                assert obs['content_sha256']==expected==persisted['observation_sha256']
                assert persisted['observation']==obs['content']
    finally: await conn.close()
    print(json.dumps({'status':result['status'],'run_id':result['run_id'],'audit':result['audit_persisted'],'steps':len(steps),'tools':[s['decision']['tool_name'] for s in result['steps'] if s['observation']],'delivered_texts':len(sink.messages),'json_delivered':True,'cleanup':True,'persisted_hashes_match':True}))
asyncio.run(main())
