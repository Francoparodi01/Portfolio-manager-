"""Capture original Cocos movement details and consolidate private evidence offline.

Uses a user-authenticated Playwright CLI session. Does not log in, solve access
challenges, send email reports, trade, bootstrap the app or connect to a database.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
import csv
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from measurement_baseline import digest, file_hash, owner_scope, write_new


def json_body(value):
    return json.loads(value, parse_float=Decimal)


def encode(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError('Unsupported output type')


def identity_tokens(row):
    """Explicit identifiers only; order and ticket namespaces never collide."""
    tokens = set()
    for kind, keys in [('ORDER', ('idOrder', 'orderId', 'id_order')),
                       ('TICKET', ('idTicket', 'id_ticket'))]:
        for key in keys:
            value = row.get(key)
            if value not in (None, '', 0, '0'):
                tokens.add((kind, str(value)))
    if row.get('identifier') and row.get('identifierId') not in (None, '', 0, '0'):
        tokens.add(('MOVEMENT:'+row['identifier'], str(row['identifierId'])))
    return tokens


def profile(rows, historical_fills):
    """Coverage and explicit cross-references, never an additive trade ledger."""
    linked = {fid for row in rows for fid in row['historical_fill_id_references']}
    groups = defaultdict(list)
    for fill in historical_fills:
        groups[(fill['source'], fill.get('executed_at'), fill.get('ticker'),
                fill.get('side'), Decimal(fill['quantity']) if fill.get('quantity') is not None else None)].append(fill)
    pairs = []
    for group in groups.values():
        if len(group) != 2:
            continue
        identifiers = [json_body(fill['raw_payload']).get('identifier') for fill in group]
        if sorted(str(x) for x in identifiers) != ['ORDER', 'SETTLEMENT']:
            continue
        ids = {fill['id'] for fill in group}
        support = [row for row in rows if ids <= set(row['historical_fill_id_references'])]
        pairs.append({'historical_fill_ids': sorted(ids),
                      'status': 'EXPLICIT_ID_BRIDGE_OBSERVED' if support else 'UNRESOLVED',
                      'supporting_activity_identities': [[r['identifier'], r['identifier_id']] for r in support],
                      'deduplicated': False})
    dates = [str(r['execution_date']) for r in rows if r['execution_date']]
    return {'rows_by_identifier': dict(Counter(r['identifier'] for r in rows)),
            'rows_by_movement_type': dict(Counter(str(r['movement_type']) for r in rows)),
            'rows_by_currency': dict(Counter(str(r['currency']) for r in rows)),
            'execution_date_strings_observed': {'min': min(dates) if dates else None, 'max': max(dates) if dates else None},
            'historical_owner_fill_rows': len(historical_fills),
            'historical_owner_fills_referenced': len(linked),
            'historical_owner_fills_without_reference': sum(f['id'] not in linked for f in historical_fills),
            'prior_order_settlement_candidate_pairs': len(pairs),
            'candidate_pairs_with_explicit_bridge': sum(p['status'] == 'EXPLICIT_ID_BRIDGE_OBSERVED' for p in pairs),
            'missing_execution_date_rows': sum(not r['execution_date'] for r in rows)}, pairs


def financial_quality(rows, historical_fills):
    counts, comparisons = Counter(), []
    by_fill = defaultdict(list)
    for row in rows:
        detail = row['original_detail_record']
        currency = row['currency']
        commissions, fees = row['commissions_by_currency'], row['fees_by_currency']
        gross, net = row['detail_gross_amount'], row['detail_amount']
        if (gross is not None and net is not None and isinstance(commissions, dict) and
                isinstance(fees, dict) and currency in commissions and currency in fees):
            residual = Decimal(gross)+Decimal(commissions[currency])+Decimal(fees[currency])-Decimal(net)
            row['signed_gross_costs_net_residual'] = residual
            counts['complete_currency_cost_map_rows'] += 1
            counts['signed_cost_equation_within_001'] += int(abs(residual) <= Decimal('.01'))
        else:
            row['signed_gross_costs_net_residual'] = None
        if row['list_amount'] is not None and net is not None:
            counts['list_detail_amount_comparable'] += 1
            counts['list_detail_amount_equal'] += int(Decimal(row['list_amount']) == Decimal(net))
            counts['list_detail_amount_within_001'] += int(abs(Decimal(row['list_amount'])-Decimal(net)) <= Decimal('.01'))
        counts['list_detail_execution_date_text_differences'] += int(bool(detail.get('executionDate')) and
            row['execution_date'] != detail['executionDate'])
        counts['list_detail_execution_calendar_label_differences'] += int(bool(detail.get('executionDate')) and
            str(row['execution_date'])[:10] != str(detail['executionDate'])[:10])
        counts['failed_or_canceled_records_excluded'] += int(row['economic_event_eligibility'] == 'EXCLUDED_FAILED_OR_CANCELED')
        for fid in row['historical_fill_id_references']:
            by_fill[fid].append(row)
    for fill in historical_fills:
        linked = by_fill.get(fill['id'], [])
        comparison = {'historical_fill_id': fill['id'], 'source_matches': len(linked),
                      'historical_payload_sha256': hashlib.sha256(fill['raw_payload'].encode('utf-8')).hexdigest(),
                      'status': 'NO_UNIQUE_BROKER_REFERENCE', 'economic_pnl_net': None}
        if len(linked) == 1:
            row = linked[0]
            detail = row['original_detail_record']
            old_amount = Decimal(fill['gross_amount_ars']) if fill.get('gross_amount_ars') is not None else None
            qty = detail.get('quantity')
            checks = {'ticker_equal': fill['ticker'] == row['ticker'], 'side_equal': fill['side'] == row['movement_type'],
                      'currency_ars': row['currency'] == 'ARS',
                      'quantity_magnitude_equal': abs(Decimal(fill['quantity'])) == abs(Decimal(qty)) if qty is not None and fill.get('quantity') is not None else None,
                      'stored_gross_matches_broker_net': abs(old_amount-abs(Decimal(row['detail_amount']))) <= Decimal('.01') if old_amount is not None and row['detail_amount'] is not None else None,
                      'stored_gross_matches_broker_gross': abs(old_amount-abs(Decimal(row['detail_gross_amount']))) <= Decimal('.01') if old_amount is not None and row['detail_gross_amount'] is not None else None}
            comparison.update(status='EXACT_ID_REFERENCE_ONLY', activity_identity=[row['identifier'], row['identifier_id']],
                              checks=checks, broker_execution_date=row['execution_date'],
                              historical_executed_at=fill.get('executed_at'),
                              historical_clock_precision=fill.get('executed_at_precision'),
                              broker_commissions_by_currency=row['commissions_by_currency'],
                              broker_fees_by_currency=row['fees_by_currency'])
            for key, value in checks.items():
                counts['historical_'+key+'_true'] += int(value is True)
            identity_fields = ['ticker_equal', 'side_equal', 'currency_ars', 'quantity_magnitude_equal']
            if all(checks[key] is True for key in identity_fields):
                comparison['status'] = 'EXACT_REFERENCE_FINANCIAL_IDENTITY_MATCH'
                counts['historical_financial_identity_matches'] += 1
                counts['historical_matches_with_both_cost_maps'] += int(row['commissions_by_currency'] is not None and row['fees_by_currency'] is not None)
        comparisons.append(comparison)
    return dict(counts), comparisons


def context(path):
    source = json.load(gzip.open(path, 'rt', encoding='utf-8'))
    if source.get('schema_version') != 'phase2a-capture-v1' or source.get('transaction_read_only') != 'on':
        raise ValueError('Expected the frozen owner-scoped Phase 2A capture')
    fills = [row for row in source['tables']['broker_fills'] if owner_scope(row, source['owner_digest']) == 'EXACT_OWNER']
    accounts = {str(json_body(row['raw_payload'])['id_account']) for row in fills
                if json_body(row['raw_payload']).get('id_account') is not None}
    if len(accounts) != 1:
        raise ValueError('A unique explicit broker account anchor is required')
    return next(iter(accounts)), fills


def save_capture(path, result):
    payload = json.dumps(result, sort_keys=True, ensure_ascii=True).encode()
    with path.open('xb') as f:
        f.write(gzip.compress(payload, mtime=0))


def capture(args):
    """Checkpoint each bounded browser batch; resume only the same source plan."""
    from uuid import uuid4
    account, historical_fills = context(args.context_capture)
    if args.output.exists():
        raise FileExistsError('Immutable output already exists')
    npx = shutil.which('npx.cmd') or shutil.which('npx')
    if not npx:
        raise RuntimeError('Playwright CLI requires npx')
    template = Path(__file__).with_name('capture_cocos_activity.js')
    source_code = template.read_text(encoding='utf-8')
    code_hash = hashlib.sha256(source_code.encode()).hexdigest()
    context_hash = file_hash(args.context_capture)
    parts = args.parts or args.output.with_name(args.output.name+'.parts')
    parts.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fragments = []

    def nonblocking(part):
        return (part['status'] == 'BATCH_CAPTURED' or
                (part['status'] == 'PARTIAL' and bool(part['errors']) and
                 set(part['errors']) <= {'DETAIL_UNAVAILABLE', 'TICKET_UNAVAILABLE'}))

    def batch(stage, **items):
        config = {'expected_account': account, 'max_pages': args.max_pages,
                  'max_details': args.max_details, 'download_tickets': not args.no_tickets,
                  'stage': stage, **items}
        key = digest([config, code_hash, context_hash])
        # Final recheck is always fresh. Every failed attempt stays on disk.
        for path in sorted(parts.glob(key+'-*.json.gz')) if stage != 'check' else []:
            saved = json.load(gzip.open(path, 'rt', encoding='utf-8'))
            if saved.get('batch_key') == key and nonblocking(saved):
                fragments.append((path, saved))
                print(json.dumps({'batch': stage, 'resumed': True, 'parts_saved': len(fragments)}), flush=True)
                return saved
        runner = parts/(str(uuid4())+'.runner-private.js')
        with runner.open('x', encoding='utf-8', newline='\n') as f:
            f.write(source_code.replace('/* AUDIT_CONFIG */ {}', json.dumps(config)))
        try:
            proc = subprocess.run([npx, '--yes', '--package', '@playwright/cli', 'playwright-cli',
                                   '-s='+args.session, 'run-code', '--filename', str(runner.resolve()), '--json'],
                                  capture_output=True, timeout=120)
        finally:
            runner.unlink(missing_ok=True)
        if proc.returncode:
            raise RuntimeError('Browser batch failed; earlier immutable parts retained')
        envelope = json.loads(proc.stdout)
        if not envelope.get('result'):
            raise RuntimeError('Browser returned no batch; earlier immutable parts retained')
        result = json.loads(envelope['result'])
        if result.get('schema_version') != 'cocos-activity-capture-v1':
            raise ValueError('Unknown browser capture format')
        result.update(owner_context_sha256=context_hash, collector_code_sha256_lf=code_hash, batch_key=key)
        path = parts/(key+'-'+str(uuid4())+'.json.gz')
        save_capture(path, result)
        fragments.append((path, result))
        print(json.dumps({'batch': stage, 'status': result['status'], 'errors': result['errors'],
                          'pages': len(result['pages']), 'details': len(result['details']),
                          'tickets': len(result['tickets']), 'parts_saved': len(fragments)}), flush=True)
        return result

    if getattr(args, 'historical_orders_only', False):
        identities = sorted({('ORDER', str(json_body(f['raw_payload'])['idOrder']))
                             for f in historical_fills
                             if json_body(f['raw_payload']).get('identifier') == 'ORDER'
                             and json_body(f['raw_payload']).get('idOrder')})
        if len(identities) > 100:
            raise ValueError('Historical order probe exceeds bounded scope')
        result = batch('details', identities=identities)
        result['source_origin'] = 'HISTORICAL_EXACT_OWNER_ORDER_IDS'
        result['source_fragment_sha256'] = [file_hash(path) for path, _ in fragments]
        save_capture(args.output, result)
        print(json.dumps({'status': result['status'], 'details_observed': len(result['details']),
                          'sha256': file_hash(args.output)}), flush=True)
        return

    result = dict(batch('list'))
    result['details'], result['tickets'] = [], []
    identities, tickets = set(), set()
    for page in result['pages']:
        if page['status'] == 200:
            for group in json.loads(page['body'])['data']:
                for row in group['movements']:
                    if row.get('identifierId') not in (None, ''):
                        identities.add((row['identifier'], str(row['identifierId'])))
                    if isinstance(row.get('idTicket'), int) and row['idTicket'] > 0:
                        tickets.add(str(row['idTicket']))
    planned = sorted(identities)
    ok = result['status'] == 'BATCH_CAPTURED'
    for start in range(0, min(len(planned), args.max_details), 20):
        if not ok:
            break
        part = batch('details', identities=planned[start:min(start+20, args.max_details)])
        result['details'].extend(part['details'])
        result['errors'].extend(part['errors'])
        ok = nonblocking(part)
    for item in result['details']:
        if item['status'] == 200:
            value = json.loads(item['body']).get('data', {})
            if isinstance(value.get('idTicket'), int) and value['idTicket'] > 0:
                tickets.add(str(value['idTicket']))
    result['details_complete'] = ok and len(result['details']) == len(identities) and all(r['status'] == 200 for r in result['details'])
    if not result['details_complete']:
        result['errors'].append('DETAIL_COVERAGE_INCOMPLETE')
    planned_tickets = sorted(tickets)
    for start in range(0, len(planned_tickets), 10) if not args.no_tickets else []:
        if not ok:
            break
        part = batch('tickets', ticket_ids=planned_tickets[start:start+10])
        result['tickets'].extend(part['tickets'])
        result['errors'].extend(part['errors'])
        ok = nonblocking(part)
    result['tickets_complete'] = not args.no_tickets and ok and len(result['tickets']) == len(tickets) and all(r['status'] == 200 for r in result['tickets'])
    result['eligible_ticket_ids_observed'] = len(tickets)
    if ok:
        check = batch('check')
        result['first_page_recheck'] = check.get('first_page_recheck')
        result['first_page_stable'] = bool(result['pages'] and result['first_page_recheck'] and
            result['first_page_recheck']['status'] == 200 and
            result['first_page_recheck']['body'] == result['pages'][0]['body'])
        result['errors'].extend(check['errors'])
        if not result['first_page_stable']:
            result['errors'].append('FIRST_PAGE_CHANGED_DURING_CAPTURE')
    result['started_at'] = min(p['started_at'] for _, p in fragments)
    result['finished_at'] = max(p['finished_at'] for _, p in fragments)
    result['source_fragment_sha256'] = [file_hash(path) for path, _ in fragments]
    result['status'] = 'CAPTURED' if all(result[k] for k in ['list_complete', 'details_complete', 'tickets_complete']) and not result['errors'] else 'PARTIAL'
    save_capture(args.output, result)
    print(json.dumps({'status': result['status'], 'errors': result['errors'],
                      'pages_observed': len(result['pages']), 'details_observed': len(result['details']),
                      'tickets_observed': len(result['tickets']), 'sha256': file_hash(args.output)}), flush=True)


def consolidate(data, expected_account, historical_fills):
    if data.get('schema_version') != 'cocos-activity-capture-v1':
        raise ValueError('Unsupported capture')
    if data.get('account_verification') != 'ACTIVE_UI_ACCOUNT_MATCH':
        return [], {'status': 'SOURCE_BLOCKED', 'reason': 'ACCOUNT_NOT_VERIFIED',
                    'movements_observed': None, 'economic_pnl_net': None}
    if str(data.get('account')) != expected_account:
        raise ValueError('Capture account mismatch')
    details = defaultdict(list)
    for item in data['details']:
        details[(item['identifier'], str(item['identifier_id']))].append(item)
    fill_ids = defaultdict(list)
    for fill in historical_fills:
        if fill['source'] == 'cocos_movements':
            for token in identity_tokens(json_body(fill['raw_payload'])):
                fill_ids[token].append(fill['id'])
    rows, counts, seen = [], Counter(), Counter()
    for page_number, page in enumerate(data['pages']):
        if page['status'] != 200:
            continue
        document = json_body(page['body'])
        if not isinstance(document.get('data'), list) or not isinstance(document.get('pagination'), dict):
            raise ValueError('Unknown movement page schema')
        for group in document['data']:
            for row in group['movements']:
                identity = (row['identifier'], str(row['identifierId']))
                seen[identity] += 1
                values = details.get(identity, [])
                detail = None
                state = 'DETAIL_MISSING'
                if len(values) == 1 and values[0]['status'] == 200:
                    detail = json_body(values[0]['body']).get('data')
                    if not isinstance(detail, dict):
                        raise ValueError('Unknown movement detail schema')
                    if detail.get('idAccount') is not None and str(detail['idAccount']) != expected_account:
                        raise ValueError('Movement detail belongs to another account')
                    state = 'EXPLICIT_ACCOUNT_MATCH' if detail.get('idAccount') is not None else 'SESSION_ACCOUNT_BOUND'
                elif len(values) > 1:
                    state = 'AMBIGUOUS_DETAIL_VERSION'
                detail = detail or {}
                broker_status = detail.get('status') or row.get('statusCashOut') or row.get('statusOrder')
                eligibility = ('EXCLUDED_FAILED_OR_CANCELED' if broker_status in
                               ('FAILED', 'CANCELED', 'CANCELLED', 'REJECTED') else 'REQUIRES_ACCOUNTING_RECONCILIATION')
                charges = detail.get('charges')
                if charges is not None and not isinstance(charges, dict):
                    raise ValueError('Unknown charges schema')
                # Preserve absence, explicit zero and currency-specific maps separately.
                commissions = charges.get('commissions') if charges is not None else None
                fees = charges.get('fees') if charges is not None else None
                counts['commissions_map_present'] += int(commissions is not None)
                counts['fees_map_present'] += int(fees is not None)
                counts['scalar_fee_present_unit_unverified'] += int(detail.get('fee') is not None)
                counts[state] += 1
                possible_ids = identity_tokens(row) | identity_tokens(detail)
                matched = sorted({fid for key in possible_ids for fid in fill_ids.get(key, [])})
                counts['with_exact_historical_id_reference'] += int(bool(matched))
                rows.append({'page_number': page_number, 'page_observed_at': page['observed_at'],
                    'detail_observed_at': values[0].get('observed_at') if len(values) == 1 else None,
                    'identifier': identity[0], 'identifier_id': identity[1], 'owner_evidence': state,
                    'date_group': group.get('date'), 'execution_date': row.get('executionDate'),
                    'settlement_date': row.get('settlementDate'), 'movement_type': row.get('movementType'),
                    'ticker': row.get('ticker'), 'currency': row.get('currency'),
                    'broker_status': broker_status, 'economic_event_eligibility': eligibility,
                    'list_amount': row.get('amount'), 'list_quantity': row.get('quantity'),
                    'detail_amount': detail.get('amount'), 'detail_gross_amount': detail.get('grossAmount'),
                    'detail_price': detail.get('price'), 'detail_quantity': detail.get('quantity'),
                    'commissions_by_currency': commissions, 'fees_by_currency': fees,
                    'scalar_fee_unit_unverified': detail.get('fee'), 'ticket_id': detail.get('idTicket', row.get('idTicket')),
                    'historical_fill_id_references': matched, 'identity_link_status': 'EXACT_ID_REFERENCE_ONLY',
                    'economic_pnl_net': None, 'original_list_record': row, 'original_detail_record': detail})
    summary = {'status': 'PARTIAL', 'source_capture_status': data['status'], 'started_at': data['started_at'],
               'finished_at': data['finished_at'], 'list_complete': data['list_complete'],
               'details_complete': data['details_complete'], 'tickets_complete': data['tickets_complete'],
               'movements_observed': len(rows), 'distinct_detail_identities': len(seen),
               'repeated_identity_rows': sum(n-1 for n in seen.values()), 'checks': dict(counts),
               'economic_pnl_net': None, 'cost_total': None,
               'limitations': ['HTTP_CAPTURE_NOT_AN_ATOMIC_ACCOUNT_SNAPSHOT', 'SCALAR_FEE_UNIT_NOT_ASSUMED',
                               'ABSENT_COST_MAP_NOT_ZERO', 'ID_LINKS_DO_NOT_MERGE_ECONOMIC_EVENTS',
                               'BROKER_HISTORY_COMPLETENESS_NOT_ATTESTED'], 'capture_errors': data['errors']}
    if not any(p['status'] == 200 for p in data['pages']):
        summary.update(status='SOURCE_BLOCKED', movements_observed=None, distinct_detail_identities=None,
                       repeated_identity_rows=None)
    return rows, summary


def consolidate_command(args):
    account, fills = context(args.context_capture)
    data = json.load(gzip.open(args.capture, 'rt', encoding='utf-8'))
    if data.get('owner_context_sha256') != file_hash(args.context_capture):
        raise ValueError('Capture owner context has changed')
    args.output.mkdir(parents=True, exist_ok=False)
    rows, summary = consolidate(data, account, fills)
    summary['coverage'], pair_review = profile(rows, fills)
    summary['financial_quality'], financial_comparisons = financial_quality(rows, fills)
    write_new(args.output/'identity-review-private.json', pair_review)
    with (args.output/'historical-comparison-private.json').open('x', encoding='utf-8') as f:
        json.dump(financial_comparisons, f, default=encode, ensure_ascii=True, indent=2)
        f.write('\n')
    with (args.output/'movements-private.json').open('x', encoding='utf-8') as f:
        json.dump(rows, f, default=encode, ensure_ascii=True, indent=2)
        f.write('\n')
    if rows:
        fields = [k for k in rows[0] if not k.startswith('original_')]
        with (args.output/'movements-private.csv').open('x', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: json.dumps(row[k], default=encode, ensure_ascii=True) if isinstance(row[k], (list, dict))
                                 else row[k] for k in fields})
    ticket_counts = Counter()
    for ticket in data['tickets']:
        if ticket['status'] != 200 or 'base64' not in ticket:
            ticket_counts['unavailable'] += 1
            continue
        payload = base64.b64decode(ticket['base64'], validate=True)
        if not payload.startswith(b'%PDF-'):
            ticket_counts['not_pdf'] += 1
            continue
        name = 'ticket-'+digest(str(ticket['ticket_id']))+'.pdf'
        with (args.output/name).open('xb') as f:
            f.write(payload)
        ticket_counts['pdf_saved'] += 1
    summary['ticket_files'] = dict(ticket_counts)
    summary['input_sha256'] = file_hash(args.capture)
    summary['private_output_sha256'] = {p.name: file_hash(p) for p in args.output.iterdir() if p.is_file()}
    write_new(args.output/'summary.json', summary)
    print(json.dumps({key: summary[key] for key in ['status', 'movements_observed', 'coverage',
                                                  'financial_quality', 'ticket_files', 'input_sha256']}))


def receipt_command(args):
    """Publish only an explicit aggregate allowlist, never raw broker records."""
    capture_data = json.load(gzip.open(args.capture, 'rt', encoding='utf-8'))
    summary = json.loads((args.consolidated/'summary.json').read_text(encoding='utf-8'))
    pdf_summary = json.loads((args.pdf_output/'summary.json').read_text(encoding='utf-8'))
    source_hash = file_hash(args.capture)
    if summary['input_sha256'] != source_hash or pdf_summary['input_sha256'] != source_hash:
        raise ValueError('Receipt source mismatch')
    public_keys = ['status', 'source_capture_status', 'started_at', 'finished_at', 'list_complete',
                   'details_complete', 'tickets_complete', 'movements_observed', 'distinct_detail_identities',
                   'repeated_identity_rows', 'checks', 'economic_pnl_net', 'cost_total', 'limitations',
                   'coverage', 'financial_quality', 'ticket_files', 'input_sha256']
    pdf_keys = ['pdfs_processed', 'status_counts', 'owner_evidence_counts', 'boleto_count', 'execution_legs',
                'with_explicit_cost_lines', 'input_sha256', 'pypdf_version', 'parsed_json_sha256', 'csv_sha256',
                'economic_pnl_net', 'account_cost_total', 'limitations', 'pdf_vs_api_net_residual',
                'pdf_vs_api_gross_residual', 'pdf_api_quantity_residual']
    receipt = {key: summary[key] for key in public_keys}
    receipt.update(schema_version='cocos-activity-receipt-v1',
        owner_context_sha256=capture_data['owner_context_sha256'],
        capture_error_counts=dict(Counter(capture_data['errors'])),
        first_page_stable=capture_data.get('first_page_stable'),
        ticket_http_status_counts=dict(Counter(str(t['status']) for t in capture_data['tickets'])),
        eligible_ticket_ids_observed=capture_data.get('eligible_ticket_ids_observed'),
        collector_code_sha256_lf=capture_data['collector_code_sha256_lf'],
        source_fragment_count=len(capture_data.get('source_fragment_sha256', [])),
        private_manifest_sha256=digest(summary['private_output_sha256']),
        pdf_analysis={key: pdf_summary[key] for key in pdf_keys})
    by_ticket = {str(r['idTicket']): r for p in capture_data['pages'] if p['status'] == 200
                 for g in json_body(p['body'])['data'] for r in g['movements'] if r.get('idTicket')}
    unavailable = [by_ticket.get(str(t['ticket_id']), {}) for t in capture_data['tickets'] if t['status'] != 200]
    receipt['unavailable_tickets_by_year'] = dict(Counter(str(r.get('executionDate', 'UNKNOWN'))[:4] for r in unavailable))
    receipt['unavailable_tickets_by_movement_type'] = dict(Counter(str(r.get('movementType', 'UNKNOWN')) for r in unavailable))
    if args.orders_capture:
        orders = json.load(gzip.open(args.orders_capture, 'rt', encoding='utf-8'))
        if (orders.get('owner_context_sha256') != capture_data['owner_context_sha256'] or
                str(orders.get('account')) != str(capture_data['account'])):
            raise ValueError('Historical order probe context mismatch')
        receipt['historical_order_probe'] = {'input_sha256': file_hash(args.orders_capture),
            'source_origin': orders.get('source_origin'), 'responses': len(orders['details']),
            'status_counts': dict(Counter(str(r['status']) for r in orders['details'])),
            'started_at': orders['started_at'], 'finished_at': orders['finished_at']}
    paths = ['scripts/capture_cocos_activity.js', 'scripts/cocos_activity_audit.py',
             'scripts/extract_cocos_tickets.py', 'tests/test_capture_cocos_activity.cjs',
             'tests/test_cocos_activity_audit.py', 'tests/test_cocos_tickets.py']
    root = Path(__file__).resolve().parents[1]
    receipt['code_sha256_utf8_lf'] = {name: hashlib.sha256((root/name).read_text(encoding='utf-8').encode()).hexdigest() for name in paths}
    write_new(args.output, receipt)
    print(json.dumps({'receipt_sha256': file_hash(args.output), 'movements_observed': summary['movements_observed']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    cap = sub.add_parser('capture')
    cap.add_argument('--session', default='quantia-cocos-access')
    cap.add_argument('--context-capture', type=Path, required=True)
    cap.add_argument('--output', type=Path, required=True)
    cap.add_argument('--max-pages', type=int, default=100)
    cap.add_argument('--max-details', type=int, default=2000)
    cap.add_argument('--no-tickets', action='store_true')
    cap.add_argument('--parts', type=Path, help='Private immutable checkpoint directory; reuse to resume')
    cap.add_argument('--historical-orders-only', action='store_true', help='Probe explicit order IDs from owner-scoped history')
    off = sub.add_parser('consolidate')
    off.add_argument('--context-capture', type=Path, required=True)
    off.add_argument('--capture', type=Path, required=True)
    off.add_argument('--output', type=Path, required=True)
    receipt = sub.add_parser('receipt')
    receipt.add_argument('--capture', type=Path, required=True)
    receipt.add_argument('--consolidated', type=Path, required=True)
    receipt.add_argument('--pdf-output', type=Path, required=True)
    receipt.add_argument('--orders-capture', type=Path)
    receipt.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'capture':
        if not 1 <= args.max_pages <= 100 or not 1 <= args.max_details <= 2000:
            parser.error('Capture bounds exceeded')
        capture(args)
    elif args.command == 'consolidate':
        consolidate_command(args)
    else:
        receipt_command(args)


if __name__ == '__main__':
    main()
