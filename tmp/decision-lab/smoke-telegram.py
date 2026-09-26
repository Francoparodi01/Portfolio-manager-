import asyncio,json,os,time
from pathlib import Path
from types import SimpleNamespace
from src.core.config import get_config
from src.agentic.telegram import run_report
from src.decision_lab.queries import query_evidence
os.environ['QUANTIA_AGENT_CONTEXT_NAMESPACE']='decision-lab-deployment-validation'
messages=[]; traces=[]
async def send_text(context,chat_id,text):messages.append(text)
async def send_document(**kwargs):traces.append(json.load(kwargs['document']))
async def command(argv,timeout):
 start=time.monotonic()
 p=await asyncio.create_subprocess_exec(*argv,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
 out,err=await asyncio.wait_for(p.communicate(),timeout)
 assert p.returncode==0,err.decode()[-1000:]
 return p.returncode,out.decode(),err.decode(),time.monotonic()-start
async def main():
 cfg=get_config();owner=int(cfg.scraper.telegram_chat_id)
 context=SimpleNamespace(bot=SimpleNamespace(send_document=send_document))
 goals=[('PLAN vs HOLD 20D','compare_plan_vs_hold'),('Decision Lab contrafactuales CASH 5D','get_decision_counterfactuals'),('Compara versiones en Decision Lab 20D','compare_strategy_versions'),('Calidad del replay 20D','get_replay_evidence_quality')]
 for goal,expected in goals:
  await run_report(context,owner,'nuevo '+goal,run_command=command,send_text=send_text)
  trace=traces[-1]
  assert trace['audit_persisted'] and trace['status']=='COMPLETE'
  assert any(s['decision'].get('tool_name')==expected for s in trace['steps'])
  assert trace['steps'][-1]['decision']['answer_origin']=='diagnostics_v2'
  if expected=='compare_plan_vs_hold':assert '-0.653' in trace['answer'] and 'LOW' in trace['answer'] and 'no demuestra superioridad' in trace['answer']
  if expected=='get_decision_counterfactuals':assert 'PARTIAL_50' in trace['answer'] and 'CASH' in trace['answer']
  print(json.dumps({'goal':goal,'status':trace['status'],'tool':expected,'chars':len(trace['answer'])}))
 foreign=await query_evidence(cfg.database.url,-999999999,{})
 assert foreign['status']=='INSUFFICIENT' and not foreign['metrics']
 receipt={'telegram_messages':messages,'traces':traces,'foreign_owner':'INSUFFICIENT','real_telegram_sends':0}
 Path('/receipts/telegram-smoke-before-activation.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
 print(json.dumps({'fake_text_messages':len(messages),'fake_documents':len(traces),'foreign_owner':'PASS','real_telegram_sends':0}))
asyncio.run(main())
