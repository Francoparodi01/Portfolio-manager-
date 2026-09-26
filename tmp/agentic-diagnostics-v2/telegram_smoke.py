import asyncio, hashlib, json, logging, os, tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
from src.core.config import get_config
from src.agentic.read_only import connect_read_only
from src.agentic.persistence import AgentRunStore
from scripts import telegram_bot as bot

logging.disable(logging.CRITICAL)
os.environ['QUANTIA_AGENT_CONTEXT_NAMESPACE']='smoke-'+str(uuid4())

class Sink:
    def __init__(self): self.messages=[]; self.result=None
    async def send_message(self, **kwargs): self.messages.append(kwargs['text'])
    async def send_document(self, **kwargs): self.result=json.load(kwargs['document'])

async def main():
    cfg=get_config(); owner=int(cfg.scraper.telegram_chat_id)
    bot.ensure_allowed_chat=AsyncMock(return_value=True)
    cases=[
        ('nuevo revisa mi cartera y explica que evidencia falta para decidir', 'portfolio_review','PARTIAL'),
        ('el riesgo país no tiene tan en cuenta los cedears por ser posiciones en wall street de empresas estadounidense', 'cedear_risk',None),
        ('con el boom de la ia la recesión de los últimos 2 meses y ahora una nueva tendencia alcista. No es favorable la compra/hold?', 'market_thesis','PARTIAL'),
        ('porque no es recomendable comprar?', 'explain_plan','PARTIAL'),
        ('nuevo el riesgo país afecta mis cedears?', 'cedear_risk',None),
    ]
    summaries=[]; prior=[]; first_conversation=None
    before=set(Path(tempfile.gettempdir()).glob('quantia_agent_*'))
    for i,(goal,intent,objective) in enumerate(cases):
        sink=Sink()
        await bot.agent_handler(SimpleNamespace(message=True,effective_chat=SimpleNamespace(id=owner)),SimpleNamespace(args=[goal],bot=sink))
        result=sink.result
        assert result, sink.messages
        Path(f'/verification/case-{i+1}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        assert result['status']=='COMPLETE', result
        assert result['audit_persisted'] and result['answer_origin']=='diagnostics_v2'
        assert result['metadata']['question_intent']==intent
        if objective: assert result['objective_status']==objective, result['objective_status']
        if i==0: first_conversation=result['metadata']['conversation_id']
        if i==4:
            assert result['metadata']['conversation_id']!=first_conversation
            assert result['metadata']['context_run_ids']==[]
        else:
            assert result['metadata']['conversation_id']==first_conversation
            assert result['metadata']['context_run_ids']==prior[-3:]
        prior.append(result['run_id'])
        conn=await connect_read_only(cfg.database.url)
        try:
            row=await conn.fetchrow('SELECT owner_chat_id,metadata FROM agent_runs WHERE id=$1::uuid',result['run_id'])
            meta=json.loads(row['metadata'])
            assert row['owner_chat_id']==owner and meta['objective_status']==result['objective_status']
            steps=await conn.fetch('SELECT observation,observation_sha256 FROM agent_steps WHERE run_id=$1::uuid ORDER BY step_no',result['run_id'])
            assert len(steps)==len(result['steps'])
            for saved,step in zip(steps,result['steps']):
                if step['observation']:
                    obs=step['observation']; digest=hashlib.sha256(obs['content'].encode()).hexdigest()
                    assert digest==obs['content_sha256']==saved['observation_sha256']
            for file,digest in result['metadata']['source_hashes'].items():
                assert hashlib.sha256(Path(file).read_bytes()).hexdigest()==digest
        finally: await conn.close()
        isolated=await AgentRunStore(cfg.database.url).recent_context(owner+1000000,namespace=os.environ['QUANTIA_AGENT_CONTEXT_NAMESPACE'])
        assert isolated==[]
        assert set(Path(tempfile.gettempdir()).glob('quantia_agent_*'))==before
        assert 'Resultado:' in '\n'.join(sink.messages)
        summary={'case':i+1,'run':result['run_id'],'status':result['status'],'objective':result['objective_status'],
                 'tools':[s['observation']['tool_name'] for s in result['steps'] if s['observation']],
                 'context_turns':len(result['metadata']['context_run_ids']), 'audit_hashes_verified':True,'message_parts':len(sink.messages)}
        summaries.append(summary); print(json.dumps(summary),flush=True)
    Path('/verification/smoke-summary.json').write_text(json.dumps(summaries,indent=2),encoding='utf-8')

asyncio.run(main())
