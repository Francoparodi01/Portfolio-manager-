import asyncio
import json

import httpx
import pytest

from src.agentic.contracts import AgentModelError
from src.agentic.model import OllamaAgentModel
from src.agentic.model_transport import (
    NEMOTRON_MODEL, OPENROUTER_URL, agent_base_url, agent_provider, chat_content,
)
from src.agentic.harness.models import ModelRoles
from src.agentic.harness.schemas import ConversationState, TaskSpec
from src.agentic.harness.semantic_router import SemanticTaskRouter
from src.agentic.harness.synthesis import GroundedSynthesizer


@pytest.fixture(autouse=True)
def provider_env(monkeypatch):
    monkeypatch.setenv('QUANTIA_AGENT_PROVIDER', 'openrouter')
    for key in ('OPENROUTER_API_KEY', 'openrouter', 'QUANTIA_AGENT_MODEL',
                'QUANTIA_LLM_ROUTER', 'QUANTIA_LLM_REASONING', 'QUANTIA_LLM_SYNTHESIS',
                'QUANTIA_LLM_VERIFIER', 'QUANTIA_OPENROUTER_MAX_TOKENS',
                'QUANTIA_OPENROUTER_REASONING_EFFORT'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('openrouter', 'test-credential-only')


def payload():
    return dict(model=NEMOTRON_MODEL, messages=[dict(role='user', content='Return JSON')],
                stream=False, format='json', keep_alive='30m',
                options=dict(temperature=0.15, num_ctx=4096, num_predict=220))


def completion(content='{"answer":"ok"}', finish='stop'):
    return {'choices': [{'finish_reason': finish, 'message': {'content': content}}]}


def call(handler, provider='openrouter'):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await chat_content(client, provider=provider, base_url=OPENROUTER_URL,
                                      payload=payload())
    return asyncio.run(run())


def test_openrouter_translates_contract_without_ollama_fields():
    def handle(request):
        assert str(request.url)==OPENROUTER_URL+'/chat/completions'
        assert request.headers['authorization']=='Bearer test-credential-only'
        body=json.loads(request.content)
        assert body['model']==NEMOTRON_MODEL
        assert body['response_format']=={'type':'json_object'}
        assert body['provider']['require_parameters'] is True
        assert body['reasoning']=={'effort':'low','exclude':True}
        assert body['max_tokens']==4096
        assert not {'options','format','keep_alive'} & body.keys()
        assert body['temperature']==0.15
        return httpx.Response(200,json=completion())
    assert json.loads(call(handle))=={'answer':'ok'}


def test_missing_key_fails_before_network(monkeypatch):
    monkeypatch.delenv('openrouter')
    def handle(_):
        pytest.fail('network must not be called without credentials')
    with pytest.raises(AgentModelError,match='credential missing'):
        call(handle)


def test_standard_key_has_priority(monkeypatch):
    monkeypatch.setenv('OPENROUTER_API_KEY','standard-test-key')
    def handle(request):
        assert request.headers['authorization']=='Bearer standard-test-key'
        return httpx.Response(200,json=completion())
    call(handle)


@pytest.mark.parametrize('status',[401,402,429,500])
def test_provider_errors_are_sanitized(status):
    def handle(_):
        return httpx.Response(status,json={'error':{'message':'secret provider body'}})
    with pytest.raises(AgentModelError) as exc:
        call(handle)
    assert str(exc.value)==f'OpenRouter HTTP {status}'


@pytest.mark.parametrize('body',[
    {'error':{'message':'private'}}, {'choices':[]}, completion('', 'stop'),
    completion('{"answer":"truncated"}', 'length'), completion(None, 'stop'),
    completion('refusal','content_filter'),
])
def test_invalid_or_truncated_completions_are_rejected(body):
    with pytest.raises(AgentModelError):
        call(lambda _:httpx.Response(200,json=body))


def test_no_key_sent_to_custom_endpoint():
    with pytest.raises(ValueError,match='official HTTPS'):
        agent_base_url('openrouter','http://localhost:11434')


def test_ollama_request_is_unchanged_and_has_no_auth():
    def handle(request):
        assert request.url.path.endswith('/api/chat')
        assert 'authorization' not in request.headers
        assert json.loads(request.content)==payload()
        return httpx.Response(200,json={'message':{'content':'local'}})
    assert call(handle,provider='ollama')=='local'


def test_all_roles_resolve_to_nemotron():
    roles=ModelRoles.from_env()
    assert set(vars(roles).values())=={NEMOTRON_MODEL}
    assert OllamaAgentModel().name==NEMOTRON_MODEL
    assert SemanticTaskRouter().base_url==OPENROUTER_URL
    assert GroundedSynthesizer(model=roles.synthesis).base_url==OPENROUTER_URL


def test_unknown_provider_rejected(monkeypatch):
    monkeypatch.setenv('QUANTIA_AGENT_PROVIDER','typo')
    with pytest.raises(ValueError):agent_provider()


def test_router_and_synthesis_use_openrouter(monkeypatch):
    monkeypatch.setenv('QUANTIA_SEMANTIC_ROUTER_ENABLED','true')
    monkeypatch.setenv('QUANTIA_HARNESS_SYNTHESIS_ENABLED','true')
    calls=[]
    async def post(_self,url,**kwargs):
        calls.append((url,kwargs))
        content=json.dumps({'intent':'system_status','entities':[], 'confidence':0.95}) if len(calls)==1 else json.dumps({'answer':'No hay evidencia suficiente.'})
        return httpx.Response(200,json=completion(content))
    monkeypatch.setattr(httpx.AsyncClient,'post',post)
    task=asyncio.run(SemanticTaskRouter().route('Estado del sistema',ConversationState(owner_chat_id=1)))
    assert task.routing_source=='semantic'
    result=asyncio.run(GroundedSynthesizer(model=NEMOTRON_MODEL).synthesize(
        task=TaskSpec(raw_message='Estado del sistema'),evidence=[],fallback='fallback'))
    assert result=='No hay evidencia suficiente.'
    assert len(calls)==2 and all(url==OPENROUTER_URL+'/chat/completions' for url,_ in calls)
