"""Independent boundary checks for current-market capture, not strategy changes."""
from dataclasses import fields
from datetime import datetime, timezone
import json
from pathlib import Path
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
import pytest

from scripts.build_radar_market_snapshot import market_evidence, valid_ccl
from src.analysis.radar_discovery import RadarDiscoveryStore, RadarDiscoveryObservation
from src.monitor.opportunities import present_top_opportunities, fetch_top_opportunities
from src.core.top10_telegram import render_top10
from tests.test_top10_telegram import records, connection
from src.core.telegram_format import validate_telegram_html

NOW=datetime(2026,10,19,21,30,tzinfo=timezone.utc)


def raw(received='2026-10-19T21:25:00+00:00'):
    return dict(evidence=dict(received_at=received,symbol_metadata=dict(exchange='BYMA',currency_code='ARS',
                name='TEST',isin='ARTEST',delay=1200,full_name='BCBA_DLY:TEST')),
                candles=[dict(ticker='TEST',asset_type='CEDEAR',ts='2026-10-16T13:30:00+00:00',close_price=99,volume=100),
                         dict(ticker='TEST',asset_type='CEDEAR',ts='2026-10-19T13:30:00+00:00',close_price=100,volume=200)])


def test_after_close_requires_independent_matching_quote():
    data=raw(); ratio=dict(isin='ARTEST',ratio='10:1')
    result=market_evidence(data,ratio,NOW)
    assert result['confirmed_close'] is None
    assert result['price_basis']=='POST_SESSION_REPORTED_CLOSE_UNCONFIRMED'
    quote=dict(currency='ARS',ts='2026-10-19T21:10:00+00:00',last_price=100)
    result=market_evidence(data,ratio,NOW,quote)
    assert result['confirmed_close']['price']==100
    assert result['price_updated_at'] is None  # bar start is NOT trade update time
    assert result['source_delay_seconds']==1200
    assert result['ratio']==ratio
    quote['last_price']=101
    assert market_evidence(data,ratio,NOW,quote)['confirmed_close'] is None


@pytest.mark.parametrize('received',['2026-10-19T16:00:00+00:00','2026-10-19T20:20:00+00:00'])
def test_open_session_or_feed_delay_is_provisional(received):
    result=market_evidence(raw(received),None,NOW)
    assert result['price_basis']=='INTRADAY_PROVISIONAL'
    assert result['confirmed_close'] is None


def test_ratio_requires_isin_and_does_not_infer_adr_share_identity():
    result=market_evidence(raw(),dict(isin='WRONG',ratio='10:1'),NOW)
    assert result['ratio'] is None and 'ratio' in result['missing_data'][0]


@pytest.mark.parametrize('field,value',[('currency_code','USD'),('exchange','NASDAQ'),('name','WRONG')])
def test_incompatible_instrument_rejected(field,value):
    data=raw();data['evidence']['symbol_metadata'][field]=value
    with pytest.raises(ValueError):market_evidence(data,None,NOW)


def test_duplicate_and_future_bars_rejected_without_dedup():
    for stamp in ('2026-10-16T13:30:00+00:00','2026-10-20T13:30:00+00:00'):
        data=raw();data['candles'][-1]['ts']=stamp
        with pytest.raises(ValueError):market_evidence(data,None,NOW)


@pytest.mark.parametrize('stamp,value',[('2026-10-19T23:00:00Z',None),('2026-10-16T21:00:00Z',None),('2026-10-19T21:00:00Z',1625.6)])
def test_ccl_rejects_future_and_stale_source_dates(stamp,value):
    calls=[dict(url='https://dolarapi.com/v1/dolares/contadoconliqui',status=200,
                received_at=NOW.isoformat(),body=json.dumps(dict(fechaActualizacion=stamp,venta=1625.6,compra=1623.6)))]
    assert valid_ccl(calls,NOW)['value']==value


def test_new_provenance_preserves_ranking_and_forces_wait_with_missing_data():
    run,rows=records()
    run['parameters']=dict(top10_publication='v1',input_sha256='a'*64)
    for row in rows:
        row['metadata']['top10_evidence'].update(market_evidence(raw(),None,NOW),ccl={'value':None})
    payload=present_top_opportunities(run,rows,as_of=NOW)
    assert [r['rank_position'] for r in payload['items']]==[r['rank_position'] for r in rows]
    assert payload['input_sha256']=='a'*64
    assert all(r['display_state']=='ESPERAR' for r in payload['items'])
    for card in render_top10(payload):
        assert validate_telegram_html(card)[0]
        assert len(card.encode('utf-16-le'))//2<3900
    assert 'No es CCL implícito' in render_top10(payload)[1]


def test_shared_reader_prefers_explicit_publication_and_remains_owner_scoped():
    conn=connection(*records())
    asyncio.run(fetch_top_opportunities(conn,owner_chat_id=123))
    sql,owner=conn.fetchrow.await_args.args
    assert "parameters->>'top10_publication'" in sql and 'owner_chat_id = $1' in sql
    assert owner==123


def test_publisher_can_skip_schema_initialization():
    store=RadarDiscoveryStore(None)
    store.ensure_schema=AsyncMock(side_effect=AssertionError('DDL forbidden'))
    conn=SimpleNamespace(fetchval=AsyncMock(return_value=uuid4()),execute=AsyncMock())
    @asynccontextmanager
    async def context():yield conn
    conn.transaction=context
    store.pool=SimpleNamespace(acquire=context)
    values={f.name:None for f in fields(RadarDiscoveryObservation)}
    values.update(ticker='TEST',asset_type='CEDEAR',radar_eligible=False,selected_top_n=False,
                  data_source_counts={},setup_features={},setup_warnings=(),metadata={})
    observation=RadarDiscoveryObservation(**values)
    asyncio.run(store.save_snapshot(run_id=uuid4(),owner_chat_id=123,captured_at=NOW,
             scoring_version='fixture',observations=[observation],parameters={},initialize_schema=False))
    store.ensure_schema.assert_not_awaited()
    assert all(call.args[0].strip().startswith('INSERT INTO') for call in conn.execute.await_args_list)
