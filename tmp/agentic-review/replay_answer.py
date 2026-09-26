import asyncio, json, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from src.agentic.model import OllamaAgentModel
from src.agentic.tools import ToolContext, build_default_registry
trace = json.loads(Path(sys.argv[2]).read_text(encoding='utf-8'))
history = []
for step in trace['steps']:
    if step['observation'] is None:
        continue
    decision = dict(step['decision']); decision['tool'] = decision.pop('tool_name', None)
    observation = dict(step['observation']); observation['tool'] = observation.pop('tool_name', None)
    history.append({'decision': decision, 'observation': observation})
async def main():
    started=time.monotonic()
    model=OllamaAgentModel(base_url='http://localhost:11434')
    original=model._call
    async def observed_call(payload):
        content=await original(payload)
        with Path(sys.argv[3] + '.raw.jsonl').open('a',encoding='utf-8') as stream:
            stream.write(json.dumps({'content':content,'prompt_chars':sum(len(m['content']) for m in payload['messages'])},ensure_ascii=False) + '\n')
        return content
    model._call=observed_call
    registry=build_default_registry(ToolContext('postgresql://unused',owner_chat_id=1))
    decision=await model.decide(goal=trace['goal'], tools=registry.specs(),history=history,step_no=5,max_steps=4,force_final=True)
    payload={'original_run_id':trace['run_id'],'model':model.name,'context_tokens':model.context_tokens,'elapsed_seconds':round(time.monotonic()-started,2),'answer':decision.answer,'rationale':decision.rationale,'tools_executed':0}
    Path(sys.argv[3]).write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(payload,ensure_ascii=True))
asyncio.run(main())
