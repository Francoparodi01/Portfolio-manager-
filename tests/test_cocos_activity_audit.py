"""Isolated fixtures for broker detail provenance; never contact Cocos."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from cocos_activity_audit import consolidate


def fixture():
    row = {'identifier': 'SETTLEMENT', 'identifierId': 77, 'idTicket': 77, 'idOrder': None,
           'executionDate': '2026-10-09', 'settlementDate': '2026-10-13', 'movementType': 'BUY',
           'ticker': 'ABC', 'currency': 'ARS', 'amount': -101.25, 'quantity': {'executed': 2, 'total': 2}}
    return {'schema_version': 'cocos-activity-capture-v1', 'status': 'CAPTURED', 'account': '9',
            'account_verification': 'ACTIVE_UI_ACCOUNT_MATCH', 'started_at': '2026-10-16T12:00:00Z',
            'finished_at': '2026-10-16T12:01:00Z', 'list_complete': True, 'details_complete': True,
            'tickets_complete': False, 'errors': [], 'tickets': [],
            'pages': [{'status': 200, 'observed_at': '2026-10-16T12:00:01Z', 'body': json.dumps({
                'pagination': {'offset': 0, 'limit': 100, 'lastPage': True},
                'data': [{'date': '2026-10-09', 'movements': [row]}]})}],
            'details': [{'identifier': 'SETTLEMENT', 'identifier_id': '77', 'status': 200,
                'body': '{"data":{"idAccount":9,"idTicket":77,"grossAmount":100.00,"amount":101.25,"charges":{"commissions":{"ARS":1.25},"fees":{"ARS":0}}}}'}]}


def test_explicit_costs_preserve_currency_zero_and_decimal():
    rows, summary = consolidate(fixture(), '9', [])
    assert rows[0]['commissions_by_currency'] == {'ARS': Decimal('1.25')}
    assert rows[0]['fees_by_currency'] == {'ARS': 0}
    assert rows[0]['list_amount'] == Decimal('-101.25')
    assert summary['economic_pnl_net'] is None
    assert summary['cost_total'] is None


def test_missing_charge_map_is_unknown_not_zero():
    data = fixture()
    data['details'][0]['body'] = '{"data":{"idAccount":9,"idTicket":77,"amount":101.25}}'
    rows, _ = consolidate(data, '9', [])
    assert rows[0]['commissions_by_currency'] is None
    assert rows[0]['fees_by_currency'] is None


def test_other_account_detail_stops_consolidation():
    data = fixture()
    data['details'][0]['body'] = '{"data":{"idAccount":10}}'
    with pytest.raises(ValueError, match='another account'):
        consolidate(data, '9', [])


def test_active_account_mismatch_stops_consolidation():
    with pytest.raises(ValueError, match='Capture account mismatch'):
        consolidate(fixture(), '8', [])


def test_blocked_access_does_not_report_zero_movements():
    data = fixture()
    data['account_verification'] = 'NOT_VERIFIED'
    rows, summary = consolidate(data, '9', [])
    assert rows == []
    assert summary['movements_observed'] is None
    assert summary['status'] == 'SOURCE_BLOCKED'


def test_http403_does_not_report_empty_history():
    data = fixture()
    data['pages'] = [{'status': 403, 'body': 'Access denied'}]
    data['status'] = 'SOURCE_BLOCKED'
    _, summary = consolidate(data, '9', [])
    assert summary['movements_observed'] is None
    assert summary['status'] == 'SOURCE_BLOCKED'


def test_failed_detail_is_retained_with_unknown_costs():
    data = fixture()
    data['details'][0]['status'] = 404
    rows, _ = consolidate(data, '9', [])
    assert rows[0]['owner_evidence'] == 'DETAIL_MISSING'
    assert rows[0]['fees_by_currency'] is None


def test_two_versions_are_not_silently_selected():
    data = fixture()
    data['details'].append(deepcopy(data['details'][0]))
    rows, _ = consolidate(data, '9', [])
    assert rows[0]['owner_evidence'] == 'AMBIGUOUS_DETAIL_VERSION'
    assert rows[0]['detail_amount'] is None


def test_repeated_identity_retained_as_rows_not_added_as_trades():
    data = fixture()
    data['pages'].append(deepcopy(data['pages'][0]))
    rows, summary = consolidate(data, '9', [])
    assert len(rows) == 2
    assert summary['repeated_identity_rows'] == 1
    assert summary['economic_pnl_net'] is None


def test_exact_id_reference_does_not_merge_events():
    fills = [{'source': 'cocos_movements', 'external_fill_id': '77', 'id': 'existing',
              'raw_payload': '{"idTicket":77}'},
             {'source': 'different', 'external_fill_id': '77', 'id': 'exclude',
              'raw_payload': '{"idTicket":77}'},
             {'source': 'cocos_movements', 'external_fill_id': '77', 'id': 'order-collision',
              'raw_payload': '{"idOrder":77}'}]
    rows, _ = consolidate(fixture(), '9', fills)
    assert rows[0]['historical_fill_id_references'] == ['existing']
    assert rows[0]['identity_link_status'] == 'EXACT_ID_REFERENCE_ONLY'


def test_scalar_fee_has_no_assumed_unit():
    data = fixture()
    data['details'][0]['body'] = '{"data":{"orderId":"order-a","fee":0.5,"currency":"ARS"}}'
    rows, _ = consolidate(data, '9', [])
    assert rows[0]['scalar_fee_unit_unverified'] == Decimal('.5')
    assert rows[0]['commissions_by_currency'] is None

def test_capture_resumes_saved_batches_after_interruption(tmp_path, monkeypatch):
    import argparse
    import gzip
    import subprocess
    from types import SimpleNamespace
    import cocos_activity_audit as audit
    context_file = tmp_path/'context.gz'
    context_file.write_bytes(b'isolated-owner-context')
    monkeypatch.setattr(audit, 'context', lambda _: ('9', []))
    monkeypatch.setattr(audit.shutil, 'which', lambda _: 'npx.cmd')
    args = argparse.Namespace(context_capture=context_file, output=tmp_path/'capture.gz',
        parts=tmp_path/'parts', session='fixture', max_pages=3, max_details=100, no_tickets=True)
    calls = []
    interrupted = [False]

    def fake_run(command, **kwargs):
        code = Path(command[command.index('--filename')+1]).read_text()
        cfg = json.loads(code.split('const cfg = ', 1)[1].split(';', 1)[0])
        calls.append((cfg['stage'], len(cfg.get('identities', []))))
        if cfg['stage'] == 'details' and len(cfg['identities']) == 1 and not interrupted[0]:
            interrupted[0] = True
            raise subprocess.TimeoutExpired('fixture', 120)
        part = fixture()
        part.update(status='BATCH_CAPTURED', pages=[], details=[], tickets=[])
        body = json.dumps({'pagination': {'offset': 0, 'limit': 100, 'lastPage': True},
                          'data': [{'date': '2026-10-09', 'movements': [
                              {'identifier': 'SETTLEMENT', 'identifierId': i, 'idTicket': None}
                              for i in range(1, 22)]}]})
        if cfg['stage'] == 'list':
            part['pages'] = [{'status': 200, 'body': body}]
        if cfg['stage'] == 'details':
            part['details'] = [{'identifier': kind, 'identifier_id': value,
                               'status': 200, 'body': '{"data":{"idAccount":9}}'}
                              for kind, value in cfg['identities']]
        if cfg['stage'] == 'check':
            part['first_page_recheck'] = {'status': 200, 'body': body}
        return SimpleNamespace(returncode=0, stdout=json.dumps({'result': json.dumps(part)}).encode())

    monkeypatch.setattr(audit.subprocess, 'run', fake_run)
    with pytest.raises(subprocess.TimeoutExpired):
        audit.capture(args)
    assert not args.output.exists()
    assert len(list(args.parts.glob('*.json.gz'))) == 2
    assert not list(args.parts.glob('*.runner-private.js'))
    audit.capture(args)
    saved = json.load(gzip.open(args.output, 'rt'))
    assert len(saved['details']) == 21
    assert saved['details_complete'] is True
    assert saved['tickets_complete'] is False
    assert saved['status'] == 'PARTIAL'
    assert calls == [('list', 0), ('details', 20), ('details', 1), ('details', 1), ('check', 0)]
    with pytest.raises(FileExistsError):
        audit.capture(args)

def test_financial_quality_requires_known_cost_maps_and_retains_payload_hash():
    from cocos_activity_audit import financial_quality
    data=fixture()
    data['details'][0]['body']='{"data":{"idAccount":9,"idTicket":77,"quantity":2,"grossAmount":-100.00,"amount":-101.25,"charges":{"commissions":{"ARS":-1.25},"fees":{"ARS":0}}}}'
    fills=[{'source':'cocos_movements','id':'fill','external_fill_id':'77','ticker':'ABC','side':'BUY',
            'quantity':'2','gross_amount_ars':'101.25','raw_payload':'{"idTicket":77,"amount":101.25}'}]
    rows,_=consolidate(data,'9',fills)
    counts,comparisons=financial_quality(rows,fills)
    assert counts['signed_cost_equation_within_001']==1
    assert counts['historical_financial_identity_matches']==1
    assert comparisons[0]['checks']['stored_gross_matches_broker_net'] is True
    assert comparisons[0]['checks']['stored_gross_matches_broker_gross'] is False
    assert len(comparisons[0]['historical_payload_sha256'])==64
    rows[0]['commissions_by_currency']=None
    counts,_=financial_quality(rows,fills)
    assert counts.get('complete_currency_cost_map_rows',0)==0
    assert rows[0]['signed_gross_costs_net_residual'] is None

def test_failed_withdrawal_is_preserved_but_excluded_from_economic_events():
    data=fixture()
    data['details'][0]['body']='{"data":{"idAccount":9,"status":"FAILED"}}'
    rows,_=consolidate(data,'9',[])
    assert len(rows)==1
    assert rows[0]['broker_status']=='FAILED'
    assert rows[0]['economic_event_eligibility']=='EXCLUDED_FAILED_OR_CANCELED'

def test_unavailable_ticket_does_not_discard_later_batches(tmp_path, monkeypatch):
    import argparse
    import gzip
    from types import SimpleNamespace
    import cocos_activity_audit as audit
    context_file=tmp_path/'context.gz';context_file.write_bytes(b'owner-context')
    monkeypatch.setattr(audit,'context',lambda _:('9',[]))
    monkeypatch.setattr(audit.shutil,'which',lambda _:'npx.cmd')
    args=argparse.Namespace(context_capture=context_file,output=tmp_path/'first.gz',parts=tmp_path/'parts',
                           session='fixture',max_pages=3,max_details=100,no_tickets=False)
    calls=[]
    body=json.dumps({'pagination':{'offset':0,'limit':100,'lastPage':True},'data':[{'date':'2026-10-09',
        'movements':[{'identifier':'SETTLEMENT','identifierId':i,'idTicket':i} for i in range(1,22)]}]})
    def fake_run(command,**kwargs):
        code=Path(command[command.index('--filename')+1]).read_text()
        cfg=json.loads(code.split('const cfg = ',1)[1].split(';',1)[0]);calls.append(cfg['stage'])
        part=fixture();part.update(status='BATCH_CAPTURED',pages=[],details=[],tickets=[])
        if cfg['stage']=='list':part['pages']=[{'status':200,'body':body}]
        if cfg['stage']=='details':
            part['details']=[{'identifier':kind,'identifier_id':value,'status':200,
                'body':json.dumps({'data':{'idAccount':9,'idTicket':int(value)}})} for kind,value in cfg['identities']]
        if cfg['stage']=='tickets':
            part['tickets']=[{'ticket_id':value,'status':500 if value=='1' else 200} for value in cfg['ticket_ids']]
            if '1' in cfg['ticket_ids']:part.update(status='PARTIAL',errors=['TICKET_UNAVAILABLE'])
        if cfg['stage']=='check':part['first_page_recheck']={'status':200,'body':body}
        return SimpleNamespace(returncode=0,stdout=json.dumps({'result':json.dumps(part)}).encode())
    monkeypatch.setattr(audit.subprocess,'run',fake_run)
    audit.capture(args)
    result=json.load(gzip.open(args.output,'rt'))
    assert len(result['tickets'])==21
    assert result['tickets_complete'] is False
    assert result['details_complete'] is True
    assert result['status']=='PARTIAL'
    assert calls==['list','details','details','tickets','tickets','tickets','check']
    calls.clear();args.output=tmp_path/'resumed.gz';audit.capture(args)
    assert calls==['check']
    assert json.load(gzip.open(args.output,'rt'))['errors']==['TICKET_UNAVAILABLE']

def test_public_receipt_allowlist_drops_raw_and_account_fields(tmp_path):
    import argparse
    import gzip
    import cocos_activity_audit as audit
    capture_path=tmp_path/'capture.gz'
    data=fixture();data['account']='PRIVATE_ACCOUNT_SENTINEL';data['owner_context_sha256']='context-hash'
    data['collector_code_sha256_lf']='collector-hash';data['eligible_ticket_ids_observed']=1
    capture_path.write_bytes(gzip.compress(json.dumps(data).encode(),mtime=0))
    rows,summary=consolidate(fixture(),'9',[])
    summary.update(input_sha256=audit.file_hash(capture_path),coverage={},financial_quality={},ticket_files={},
                   private_output_sha256={'PRIVATE_FILENAME':'hash'},raw_payload='PRIVATE_PAYLOAD_SENTINEL')
    consolidated=tmp_path/'consolidated';consolidated.mkdir();(consolidated/'summary.json').write_text(json.dumps(summary))
    pdf=tmp_path/'pdf';pdf.mkdir()
    pdf_summary={k:None for k in ['pdfs_processed','status_counts','owner_evidence_counts','boleto_count','execution_legs',
        'with_explicit_cost_lines','pypdf_version','parsed_json_sha256','csv_sha256','economic_pnl_net','account_cost_total',
        'limitations','pdf_vs_api_net_residual','pdf_vs_api_gross_residual','pdf_api_quantity_residual']}
    pdf_summary.update(input_sha256=audit.file_hash(capture_path),raw_text='PRIVATE_TEXT_SENTINEL')
    (pdf/'summary.json').write_text(json.dumps(pdf_summary))
    args=argparse.Namespace(capture=capture_path,consolidated=consolidated,pdf_output=pdf,orders_capture=None,output=tmp_path/'public.json')
    audit.receipt_command(args)
    public=args.output.read_text()
    assert 'PRIVATE_' not in public
    assert json.loads(public)['movements_observed']==1
