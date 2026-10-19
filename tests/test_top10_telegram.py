"""Shared-source parity, account isolation and Telegram rendering; no messages sent."""
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import pytest

from src.monitor.opportunities import present_top_opportunities, fetch_top_opportunities
from src.core.top10_telegram import render_top10
from src.core.telegram_format import validate_telegram_html

NOW=datetime(2026,10,16,20,tzinfo=timezone.utc)


def records():
    run=dict(run_id='fixture-run',captured_at=NOW-timedelta(minutes=10),evaluated_at=NOW,
             captured_session=NOW.date(),scoring_version='synthetic-fixture',universe_count=40,eligible_count=12)
    rows=[]
    for n in [2,4,7]:
        rows.append(dict(ticker=f'TEST{n}',rank_position=n,reference_ts=NOW-timedelta(minutes=15),
                         reference_price=123.45,ranking_score=60-n,technical_regime='BULL_TREND',
                         radar_status='COMPRABLE_AHORA',price_quality_flag='CANONICAL_COCOS',
                         data_source_mode='cocos',data_source_counts={'COCOS':220},
                         acceptance_reason='existing-rule',metadata={'top10_evidence':{
                             'currency':'ARS','technical_score':4.,'volume_ratio':1.2,
                             'volume_definition':'current_volume_over_inclusive_sma20',
                             'principal_risk':'Gap de apertura','selection_reason':'Regla guardada',
                             'data_quality':dict(price_status='VALID',volume_status='VALID',provenance_status='VALID')}},
                         setup_features={}))
    return run, rows


@asynccontextmanager
async def context(value=None):
    yield value


def connection(run,rows):
    return SimpleNamespace(fetchval=AsyncMock(return_value=True),fetchrow=AsyncMock(return_value=run),
                           fetch=AsyncMock(return_value=rows),close=AsyncMock(),
                           transaction=Mock(side_effect=lambda **kwargs:context()))


def test_render_preserves_saved_rank_not_ticker_or_score_order():
    run, rows=records();rows[0]['ranking_score']=-100
    result=present_top_opportunities(run,list(reversed(rows)),as_of=NOW)
    assert [r['rank_position'] for r in result['items']]==[2,4,7]
    assert result['items'][0]['ranking_score']==-100
    cards=render_top10(result)
    assert [f'#{r["rank_position"]} · {r["ticker"]}' in card for r,card in zip(result['items'],cards[1:])]==[True]*3
    assert all('COMPRAR' in card for card in cards[1:])
    assert all('123.45 ARS' in card and 'Score técnico: 4.00' in card and 'COCOS' in card for card in cards[1:])


@pytest.mark.parametrize('fault',['legacy','stale','future_price','missing_price','missing_volume',
                                   'missing_currency','missing_score','provisional','invalid_quality'])
def test_incomplete_evidence_never_creates_buy_recommendation(fault):
    run,rows=records();r=rows[0];e=r['metadata']['top10_evidence']
    if fault=='legacy':r['metadata']={}
    if fault=='stale':run['captured_at']=NOW-timedelta(days=8);r['reference_ts']=NOW-timedelta(days=9)
    if fault=='future_price':r['reference_ts']=NOW+timedelta(days=1)
    if fault=='missing_price':r['reference_price']=float('nan')
    if fault=='missing_volume':e['volume_ratio']=None
    if fault=='missing_currency':e['currency']=None
    if fault=='missing_score':e['technical_score']=None
    if fault=='provisional':r['price_quality_flag']='INTRADAY_PROVISIONAL'
    if fault=='invalid_quality':e['data_quality']['price_status']='INVALID'
    result=present_top_opportunities(run,rows[:1],as_of=NOW)
    item=result['items'][0]
    assert item['display_state']=='ESPERAR' and item['quality']=='PARTIAL' and item['missing_data']
    assert item['radar_status']=='COMPRABLE_AHORA' and item['rank_position']==2
    assert '— ESPERAR' in render_top10(result)[1]
    json.dumps(result,allow_nan=False)


@pytest.mark.parametrize('raw,state',[('COMPRABLE_AHORA','COMPRAR'),('COMPRA_HABILITADA','VIGILAR'),
    ('SWAP_CANDIDATO','VIGILAR'),('VIGILANCIA_A','VIGILAR'),('VIGILANCIA_B','VIGILAR'),
    ('VIGILANCIA_C','VIGILAR'),('NO_OPERABLE','ESPERAR'),('DESCARTAR','ESPERAR'),('UNKNOWN','ESPERAR')])
def test_display_mapping_preserves_recorded_rules(raw,state):
    run,rows=records();rows[0]['radar_status']=raw
    assert present_top_opportunities(run,rows[:1],as_of=NOW)['items'][0]['display_state']==state


def test_html_is_escaped_bounded_and_preserves_all_ten():
    run,rows=records();template=rows[0];rows=[]
    for i in range(10):
        row=deepcopy(template);row.update(ticker=f'<T&{i}>',rank_position=i+1)
        row['metadata']['manual_event_risk']='<script>&'*3000
        row['metadata']['top10_evidence']['selection_reason']='<bad>'*3000
        rows.append(row)
    cards=render_top10(present_top_opportunities(run,rows,as_of=NOW))
    assert len(cards)==11
    for card in cards:
        assert len(card.encode('utf-16-le'))//2<3900
        assert validate_telegram_html(card)[0]
        assert '<script>' not in card and '<bad>' not in card


@pytest.mark.parametrize('status',['NO_CAPTURE','SCHEMA_MISSING','INVALID_CAPTURE','OWNER_NOT_CONFIGURED'])
def test_empty_states_do_not_invent_candidates(status):
    result=render_top10({'status':status})
    assert len(result)==1 and 'No se generaron recomendaciones' in result[0]


def test_duplicate_rank_is_invalid_instead_of_reranked():
    run,rows=records();rows[1]['rank_position']=2
    assert present_top_opportunities(run,rows,as_of=NOW)['status']=='INVALID_CAPTURE'


def test_zero_owner_never_queries():
    conn=connection(*records())
    assert asyncio.run(fetch_top_opportunities(conn,owner_chat_id=0))['status']=='OWNER_NOT_CONFIGURED'
    conn.fetchval.assert_not_awaited()


def test_bot_and_api_use_identical_owner_scoped_read_model(monkeypatch):
    import asyncpg
    from scripts import telegram_bot as bot
    from src.monitor import api
    run,rows=records();conn=connection(run,rows)
    connect=AsyncMock(return_value=conn)
    monkeypatch.setattr(asyncpg,'connect',connect)
    config=SimpleNamespace(scraper=SimpleNamespace(telegram_chat_id='123'),database=SimpleNamespace(url='postgresql+asyncpg://fixture/db'))
    monkeypatch.setattr(bot,'get_config',lambda:config)
    monkeypatch.setattr(api,'get_config',lambda:config)
    monkeypatch.setattr(bot,'ensure_allowed_chat',AsyncMock(return_value=True))
    send=AsyncMock();monkeypatch.setattr(bot,'send_text',send)
    update=SimpleNamespace(effective_chat=SimpleNamespace(id=123))
    asyncio.run(bot.top10_handler(update,SimpleNamespace()))
    pool=SimpleNamespace(acquire=lambda:context(conn))
    request=SimpleNamespace(query={},app={'pool':pool})
    response=asyncio.run(api.opportunities_top10(request))
    payload=json.loads(response.body)
    assert [call.args[2] for call in send.await_args_list]==render_top10(payload)
    assert connect.await_args.kwargs['server_settings']=={'default_transaction_read_only':'on'}
    assert connect.await_args.args[0]=='postgresql://fixture/db'
    assert conn.transaction.call_args.kwargs==dict(isolation='repeatable_read',readonly=True)
    assert all(call.args[1]==123 for call in conn.fetchrow.await_args_list)
    conn.close.assert_awaited_once()


@pytest.mark.parametrize('allowed,requesting_owner',[(False,123),(True,999)])
def test_bot_rejects_unauthorized_or_other_owner(monkeypatch,allowed,requesting_owner):
    import asyncpg
    from scripts import telegram_bot as bot
    connect=AsyncMock();monkeypatch.setattr(asyncpg,'connect',connect)
    monkeypatch.setattr(bot,'ensure_allowed_chat',AsyncMock(return_value=allowed))
    monkeypatch.setattr(bot,'get_config',lambda:SimpleNamespace(scraper=SimpleNamespace(telegram_chat_id='123')))
    monkeypatch.setattr(bot,'send_text',AsyncMock())
    asyncio.run(bot.top10_handler(SimpleNamespace(effective_chat=SimpleNamespace(id=requesting_owner)),SimpleNamespace()))
    connect.assert_not_awaited()


def test_bot_connection_failure_is_explicit_without_secret_error(monkeypatch):
    import asyncpg
    from scripts import telegram_bot as bot
    monkeypatch.setattr(bot,'ensure_allowed_chat',AsyncMock(return_value=True))
    monkeypatch.setattr(bot,'get_config',lambda:SimpleNamespace(scraper=SimpleNamespace(telegram_chat_id='123'),database=SimpleNamespace(url='postgresql://fixture/db')))
    monkeypatch.setattr(asyncpg,'connect',AsyncMock(side_effect=RuntimeError('secret fixture')))
    send=AsyncMock();monkeypatch.setattr(bot,'send_text',send)
    asyncio.run(bot.top10_handler(SimpleNamespace(effective_chat=SimpleNamespace(id=123)),SimpleNamespace()))
    assert 'No se generaron recomendaciones' in send.await_args.args[2]
    assert 'secret' not in send.await_args.args[2]


def test_bot_read_failure_closes_connection(monkeypatch):
    import asyncpg
    from scripts import telegram_bot as bot
    conn=connection(*records());conn.fetchrow.side_effect=RuntimeError('private error')
    monkeypatch.setattr(asyncpg,'connect',AsyncMock(return_value=conn))
    monkeypatch.setattr(bot,'ensure_allowed_chat',AsyncMock(return_value=True))
    monkeypatch.setattr(bot,'get_config',lambda:SimpleNamespace(scraper=SimpleNamespace(telegram_chat_id='123'),database=SimpleNamespace(url='postgresql://fixture/db')))
    send=AsyncMock();monkeypatch.setattr(bot,'send_text',send)
    asyncio.run(bot.top10_handler(SimpleNamespace(effective_chat=SimpleNamespace(id=123)),SimpleNamespace()))
    conn.close.assert_awaited_once()
    assert 'No se generaron recomendaciones' in send.await_args.args[2]


def test_top10_is_registered_in_application_and_menu(monkeypatch):
    from scripts import telegram_bot as bot
    app=Mock();builder=Mock()
    builder.token.return_value=builder
    builder.post_init.return_value=builder
    builder.post_shutdown.return_value=builder
    builder.build.return_value=app
    monkeypatch.setattr(bot,'Application',SimpleNamespace(builder=lambda:builder))
    # Other suites replace telegram.ext globally; isolate the handler factory.
    monkeypatch.setattr(bot,'CommandHandler',lambda command,callback:SimpleNamespace(commands={command},callback=callback))
    monkeypatch.setattr(bot,'_get_token',lambda:'fixture-token')
    bot.build_app()
    handlers=[call.args[0] for call in app.add_handler.call_args_list]
    assert any(getattr(h,'commands',set())=={'top10'} and h.callback==bot.top10_handler for h in handlers)
    assert 'top10' in dict(bot.BOT_COMMAND_SPECS)
