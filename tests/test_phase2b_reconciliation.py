"""Synthetic statement and broker fixtures. No network or operational data."""
from copy import deepcopy
from decimal import Decimal as D
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from capture_phase2b import request_plan
from reconcile_phase2b import parse_statement, parse_csv, daily_reconciliation, cash_legs, cash_report_currency, legacy_crosswalk, unique_history_balances


def pages():
    header='Nro de comitente: 9\nDesde 01-10-2026- Hasta 01-10-2026\n'
    return [header+'''POSICION AL CIERRE DEL 30-09-2026
Company (ABC)  2,00  ARS  10,000  20,00  20,00
Saldo ARS  100,00  ARS  100,00  100,00
Total Posición al 30-09-2026  120,00
''', header+'''POSICION AL CIERRE DEL 01-10-2026
Company (ABC)  3,00  ARS  11,000  33,00  33,00
Saldo ARS  89,00  ARS  89,00  89,00
Total Posición al 01-10-2026  122,00
''']


def activity():
    return {'identifier':'SETTLEMENT','identifier_id':'77','ticket_id':77,'ticker':'ABC',
        'movement_type':'BUY','settlement_date':'2026-10-01','execution_date':'2026-09-30',
        'detail_quantity':'1','economic_event_eligibility':'REQUIRES_ACCOUNTING_RECONCILIATION',
        'original_detail_record':{'amount':'-11','currency':'ARS','charges':{'commissions':{'ARS':'-1'}}}}


def test_statement_tables_and_settlement_basis_close_without_pnl():
    parsed=parse_statement(pages(),'9','20261001')
    out=daily_reconciliation(parsed,[activity()],'2026-10-01')
    assert all(x['table_sum_status']=='VERIFIED' for x in parsed['tables'].values())
    assert out['quantity_rows'][0]['listed_vector_residual']==0
    assert out['cash_rows'][0]['residual']==0
    assert out['economic_pnl_net'] is None and out['status']=='PARTIAL'


@pytest.mark.parametrize('owner,date',[('8','20261001'),('9','20261002')])
def test_foreign_owner_or_report_date_rejected(owner,date):
    with pytest.raises(ValueError,match='mismatch'):parse_statement(pages(),owner,date)


def test_each_pdf_page_requires_account_identity():
    data=pages();data[1]=data[1].replace('comitente: 9','comitente: 8')
    with pytest.raises(ValueError,match='account'):parse_statement(data,'9','20261001')


def test_opening_date_cannot_silently_expand_daily_window():
    data=pages();data[0]=data[0].replace('30-09-2026','29-09-2026')
    with pytest.raises(ValueError,match='previous-day'):parse_statement(data,'9','20261001')


def test_unknown_numeric_row_cannot_hide_behind_matching_nav():
    data=pages();data[0]+='Unrecognized adjustment  0,00\n'
    parsed=parse_statement(data,'9','20261001')
    assert parsed['tables']['2026-09-30']['table_sum_status']=='PARTIAL'


def test_pending_settlement_and_broker_result_are_separate_from_table_nav_and_pnl():
    data=pages();data[1]+='Posición pendiente de liquidación al 01-10-2026  ARS  -11,00\nTOTAL DE LA INVERSION  ARS  120,00\nRESULTADO  ARS  -9,00\n'
    parsed=parse_statement(data,'9','20261001');table=parsed['tables']['2026-10-01']
    assert table['reported_nav']==122 and table['table_sum_residual']==0
    assert table['table_sum_status']=='VERIFIED' and len(table['reported_summary_fields'])==3
    assert table['reported_summary_fields'][0]['value']==-11
    assert parsed['economic_pnl_net'] is None


def test_pending_settlement_wrong_date_rejected():
    data=pages();data[1]+='Posición pendiente de liquidación al 02-10-2026  ARS  -11,00\n'
    with pytest.raises(ValueError,match='summary date'):parse_statement(data,'9','20261001')


def test_nav_residual_kept_instead_of_rewriting_reported_total():
    data=pages();data[1]=data[1].replace('122,00','123,00')
    out=parse_statement(data,'9','20261001')['tables']['2026-10-01']
    assert out['reported_nav']==123 and out['table_sum_residual']==-1
    assert out['table_sum_status']=='PARTIAL'


def test_duplicate_holding_fails_even_if_amounts_net_to_zero():
    data=pages();data[0]+='Company (ABC)  0,00  ARS  10,000  0,00  0,00\n'
    with pytest.raises(ValueError,match='Duplicate'):parse_statement(data,'9','20261001')


def test_missing_cash_boundary_stays_unknown():
    parsed=parse_statement(pages(),'9','20261001')
    parsed['tables']['2026-09-30']['rows'].append({'name':'Saldo USD MEP','currency':'USD MEP','quantity':D('1.53')})
    row=activity();row['original_detail_record']={'amount':'-1.53','currency':'USD'};row['movement_type']='WITHDRAWAL'
    cash=daily_reconciliation(parsed,[row],'2026-10-01')['cash_rows']
    dollar=next(x for x in cash if x['currency']=='USD MEP')
    assert dollar['closing'] is None and dollar['residual'] is None
    assert dollar['status']=='MISSING_EXPLICIT_BOUNDARY'


def test_net_already_contains_fees_no_double_charge():
    legs=cash_legs(activity())
    assert len(legs)==1 and legs[0]['amount']==D('-11')


def test_ext_is_not_cash_principal_and_fee_keeps_its_own_currency():
    row=activity();row['original_detail_record']={'received':{'currency':'EXT','amount':'2'},'charges':{'fees':{'ARS':'-0.12'}}}
    legs=cash_legs(row)
    assert legs==[{'currency':'ARS','amount':D('-.12'),'basis':'EXPLICIT_FEES'}]
    assert cash_report_currency('EXT')=='EXT'


def test_failed_movement_never_enters_cash_legs():
    row=activity();row['economic_event_eligibility']='EXCLUDED_FAILED_OR_CANCELED'
    assert cash_legs(row)==[]


def test_duplicate_identity_never_added_twice():
    with pytest.raises(ValueError,match='Repeated activity'):
        daily_reconciliation(parse_statement(pages(),'9','20261001'),[activity(),activity()],'2026-10-01')


HEADER='instrumento;cantidad;precio;precioAsOf;moneda;total;usedFallbackPrice;missingPrice\n'


def test_ragged_csv_cash_schema_not_shifted_under_holdings_header():
    rows=parse_csv(HEADER+'Company (ABC);3;11;2026-10-01;ARS;33;0;0\nUSD;0,53;1;USD;0,53\n')
    assert rows[1]['kind']=='CASH_RAGGED_FIVE_COLUMNS'
    assert rows[1]['price_as_of'] is None and rows[1]['currency']=='USD'
    assert rows[1]['native_total']==D('.53')


@pytest.mark.parametrize('line',['USD;0,53;1;USD\n','USD;0,53;1;ARS;0,53\n','ABC;;1;2026-10-01;ARS;3;0;0\n'])
def test_unknown_csv_shape_and_missing_number_rejected(line):
    with pytest.raises(ValueError):parse_csv(HEADER+line)


def test_csv_price_quality_flags_preserved():
    row=parse_csv(HEADER+'Company (ABC);3;11;2026-09-30;ARS;33;1;0\n')[0]
    assert row['fallback_price']=='1' and row['price_as_of']=='2026-09-30'


def test_duplicate_history_date_is_not_silently_overwritten_or_used_for_comparison():
    points=[{'time':'2026-10-01','balance':'10'},{'time':'2026-10-01','balance':'12'},
            {'time':'2026-10-02','balance':'13'}]
    assert unique_history_balances(points)=={'2026-10-02':D('13')}


def test_capture_plan_only_retries_observed_failures():
    source={'schema_version':'cocos-activity-capture-v1','account':'9','owner_context_sha256':'ctx',
            'tickets':[{'ticket_id':77,'status':500},{'ticket_id':88,'status':200}]}
    plan=request_plan(['20261001'],source,'9','ctx')
    assert len(plan)==9
    assert any('/tickets/77/download' in r['url'] for r in plan)
    assert not any('/tickets/88/download' in r['url'] for r in plan)
    assert all(r['url'].startswith('https://api.cocos.capital/api/') for r in plan)


def test_retry_source_cannot_mix_owners_or_dates():
    with pytest.raises(ValueError):request_plan(['20261001'],{'account':'8'},'9','ctx')
    with pytest.raises(ValueError):request_plan(['20261001','20261001'],None,'9','ctx')
    with pytest.raises(ValueError):request_plan(['20260230'],None,'9','ctx')


def test_legacy_crosswalk_retains_channels_conflicts_and_unscoped_rows():
    import json
    raw={'id_account':9,'id_ticket':77,'id_cash_movement':1,'id_currency':'ARS','quantity':'-10'}
    source={'tables':{'broker_movements':[{'id':'a','raw_payload':json.dumps(raw)},
        {'id':'b','raw_payload':json.dumps({**raw,'id_account':None})}]}}
    rows,counts=legacy_crosswalk(source,[activity()],'9')
    assert counts['cash_value_conflicts']==1 and counts['without_explicit_account_match']==1
    assert rows[0]['cash_residual']==1
    assert rows[0]['included_in_economic_ledger'] is False
