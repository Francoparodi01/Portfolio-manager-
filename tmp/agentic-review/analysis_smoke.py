import asyncio, json
from pathlib import Path
from src.agentic import AgentOrchestrator, AgentRunStore, ToolContext, build_default_registry
from src.agentic.contracts import AgentDecision
from src.agentic.tools import verify_single_owner
from src.core.config import get_config
class SmokeModel:
 name='deterministic-registry-smoke'
 def __init__(self): self.n=0
 async def decide(self,**kwargs):
  self.n+=1
  return AgentDecision(kind='tool',tool_name='analyze_ticker',arguments={'ticker':'AAPL'}) if self.n==1 else AgentDecision(kind='final',answer='Prueba técnica de la herramienta finalizada; no es una decisión financiera.')
async def main():
 cfg=get_config(); owner=int(cfg.scraper.telegram_chat_id)
 context=ToolContext(cfg.database.url,owner_chat_id=owner,repo_root='/app',tool_timeout_seconds=120,
                     legacy_single_owner=not cfg.multiuser_enabled and await verify_single_owner(cfg.database.url,owner))
 result=await AgentOrchestrator(model=SmokeModel(),registry=build_default_registry(context),store=AgentRunStore(cfg.database.url),max_steps=2).run(goal='Smoke técnico del wrapper de análisis AAPL sin persistencia ni Telegram.',owner_chat_id=owner,metadata={'trigger':'integration_smoke','read_only':True})
 Path('/verification/analysis-tool-smoke.json').write_text(json.dumps(result.to_dict(),ensure_ascii=False),encoding='utf-8')
 print(json.dumps({'status':result.status,'run_id':result.run_id,'audit':result.audit_persisted,
  'observations':[{'ok':s.observation.ok,'error':s.observation.error,'chars':len(s.observation.content)} for s in result.steps if s.observation]}))
asyncio.run(main())
