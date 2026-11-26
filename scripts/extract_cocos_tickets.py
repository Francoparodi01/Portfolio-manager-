"""Offline extraction of original Cocos PDFs. No database or broker calls.

Only the observed BOLETO layout is eligible for arithmetic checks. Other
documents retain their text/labels and remain PARTIAL. pypdf is a CLI dependency.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
from decimal import Decimal
import csv
import gzip
import json
from pathlib import Path
import re
import unicodedata

from cocos_activity_audit import context, encode, json_body
from measurement_baseline import digest, file_hash, write_new


NUMBER = r'-?(?:\d{1,3}(?:\.\d{3})+|\d+),\d+'
DATE = r'(?:\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})'
HEADER = re.compile(rf'^(\d+) ({DATE}) ({DATE}) (\d+|---)$')
LEG = re.compile(rf'^({NUMBER}) ({NUMBER}) ({NUMBER}) (\d+) (\d+|-) (\d+|-) ({DATE}) (\d{{2}}:\d{{2}}:\d{{2}})$')
TOTAL = re.compile(rf'^(Importe Bruto(?: en [A-Z]{{3}})?|Importe Neto en [A-Z]{{3}}) ({NUMBER})(?: ([A-Z]{{3}}))?$')
CHARGE = re.compile(rf'^(Comisiones(?: [A-Z]{{3}})?|Debito .+?|Credito .+?|IVA [\d.,]+%) ({NUMBER})(?: ([A-Z]{{3}}))?$')


def local_decimal(value):
    if not re.fullmatch(NUMBER, value):
        raise ValueError('Unsupported localized number')
    return Decimal(value.replace('.', '').replace(',', '.'))


def plain(value):
    return ''.join(c for c in unicodedata.normalize('NFD', value) if not unicodedata.combining(c))


def parse_ticket(text, expected_account):
    lines = [' '.join(line.split()) for line in text.splitlines() if line.strip()]
    headers = [HEADER.fullmatch(line) for line in lines if HEADER.fullmatch(line)]
    if any(match[1] != expected_account for match in headers):
        raise ValueError('PDF account mismatch')
    result = {'status': 'PARTIAL', 'owner_evidence': 'ACCOUNT_NOT_PARSED', 'document_type': 'OTHER',
              'side': None, 'execution_date': None, 'settlement_date': None, 'document_number': None,
              'gross': None, 'gross_currency': None, 'net': None, 'currency': None, 'charges': [], 'execution_legs': [],
              'clock_timezone': None, 'checks': {}, 'economic_pnl_net': None}
    normalized_lines = [plain(line) for line in lines]
    fci_types = {'LIQUIDACION DE RESCATE DE FCI': 'FCI_REDEMPTION',
                 'LIQUIDACION DE SUSCRIPCION DE FCI': 'FCI_SUBSCRIPTION'}
    fci_titles = {fci_types[line] for line in normalized_lines if line in fci_types}
    if fci_titles:
        accounts = [match[1] for line in normalized_lines
                    if (match := re.search(r'\bNumero de cuenta: (\d+)\b', line))]
        if any(account != expected_account for account in accounts):
            raise ValueError('PDF account mismatch')
        if len(fci_titles) == 1 and accounts:
            result.update(owner_evidence='EXPLICIT_PDF_ACCOUNT_MATCH',
                          document_type=next(iter(fci_titles)))
        # The FCI layout identifies the account, but its cash/unit arithmetic
        # and document dates are not the BOLETO execution/settlement fields.
        return result
    if len(headers) != 1:
        return result
    account, execution, settlement, number = headers[0].groups()
    if account != expected_account:
        raise ValueError('PDF account mismatch')
    result.update(owner_evidence='EXPLICIT_PDF_ACCOUNT_MATCH', execution_date=execution,
                  settlement_date=settlement, document_number=None if number == '---' else number,
                  document_number_text=number)
    result.update(execution_date_text=execution, settlement_date_text=settlement)
    for key in ['execution_date', 'settlement_date']:
        value = result[key]
        try:
            result[key] = datetime.strptime(value, '%d/%m/%Y' if '/' in value else '%Y-%m-%d').date().isoformat()
        except ValueError:
            result['checks']['invalid_document_date'] = True
    result['document_type'] = 'BOLETO' if 'BOLETO' in lines else 'OTHER'
    for line in lines:
        normalized = plain(line)
        if re.fullmatch(r'Operacion: Compra(?: .+)?', normalized):
            result['side'] = 'BUY'
        elif re.fullmatch(r'Operacion: Venta(?: .+)?', normalized):
            result['side'] = 'SELL'
        match = TOTAL.fullmatch(normalized)
        if match:
            label, value, explicit_currency = match.groups()
            key = 'gross' if label.startswith('Importe Bruto') else 'net'
            # Repeated total labels are ambiguous, never choose the last silently.
            if result[key] is not None:
                result['checks']['ambiguous_totals'] = True
            result[key] = local_decimal(value)
            if key == 'gross':
                result['gross_currency'] = label[-3:] if ' en ' in label else explicit_currency
                if explicit_currency and result['gross_currency'] != explicit_currency:
                    result['checks']['conflicting_currency'] = True
            if key == 'net':
                result['currency'] = label[-3:]
                if explicit_currency and explicit_currency != result['currency']:
                    result['checks']['conflicting_currency'] = True
        match = CHARGE.fullmatch(normalized)
        if match:
            label, value, explicit_currency = match.groups()
            currency_in_label = re.search(r'\b([A-Z]{3})$', label)
            if currency_in_label and explicit_currency and currency_in_label[1] != explicit_currency:
                result['checks']['conflicting_currency'] = True
            currency = explicit_currency or (currency_in_label[1] if currency_in_label else None)
            result['charges'].append({'label': label, 'amount': local_decimal(value),
                                      'currency': currency, 'currency_basis': 'EXPLICIT_LINE' if currency else None})
        match = LEG.fullmatch(normalized)
        if match:
            q, price, amount, register, counterparty, order, date, clock = match.groups()
            result['execution_legs'].append({'quantity': local_decimal(q), 'price': local_decimal(price),
                'amount': local_decimal(amount), 'register': register,
                'counterparty': None if counterparty == '-' else counterparty, 'counterparty_text': counterparty,
                'document_order_number': None if order == '-' else order, 'document_order_number_text': order,
                'date': date, 'local_clock_text': clock,
                'clock_timezone': None})
    if result['document_type'] == 'BOLETO' and result['currency']:
        if result['gross_currency'] is not None and result['gross_currency'] != result['currency']:
            result['checks']['conflicting_currency'] = True
        for charge in result['charges']:
            if charge['currency'] is None:
                charge.update(currency=result['currency'], currency_basis='DOCUMENT_NET_DENOMINATION')
    if result['document_type'] != 'BOLETO' or result['side'] is None:
        return result
    legs = result['execution_legs']
    if legs and result['gross'] is not None:
        result['checks']['legs_amount_vs_gross_residual'] = sum(leg['amount'] for leg in legs)-result['gross']
        result['checks']['leg_price_residuals'] = [abs(leg['quantity'])*leg['price']-leg['amount'] for leg in legs]
        result['quantity_abs'] = sum(abs(leg['quantity']) for leg in legs)
    known_labels = all(c['label'].startswith(('Comisiones', 'Debito Derechos Mercado Byma', 'Credito Derechos Mercado Byma', 'IVA '))
                       and (not c['label'].startswith('Credito') or c['amount'] >= 0)
                       and c['currency'] == result['currency'] for c in result['charges'])
    if result['gross'] is not None and result['net'] is not None and result['charges'] and known_labels:
        costs = sum(-c['amount'] if c['label'].startswith('Credito') else c['amount'] for c in result['charges'])
        sign = 1 if result['side'] == 'BUY' else -1
        result['checks']['gross_charges_net_residual'] = result['gross'] + sign*costs - result['net']
        result['document_charges_sum'] = costs
    checks = result['checks']
    if (not checks.get('ambiguous_totals') and not checks.get('conflicting_currency') and not checks.get('invalid_document_date')
            and legs and abs(checks.get('legs_amount_vs_gross_residual', Decimal('Infinity'))) <= Decimal('.01')
            and all(abs(v) <= Decimal('.01') for v in checks.get('leg_price_residuals', []))
            and abs(checks.get('gross_charges_net_residual', Decimal('Infinity'))) <= Decimal('.01')):
        result['status'] = 'VERIFIED_DOCUMENT_ARITHMETIC'
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context-capture', required=True, type=Path)
    parser.add_argument('--capture', required=True, type=Path)
    parser.add_argument('--consolidated', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    from pypdf import PdfReader, __version__ as pypdf_version
    account, _ = context(args.context_capture)
    capture = json.load(gzip.open(args.capture, 'rt', encoding='utf-8'))
    if capture.get('owner_context_sha256') != file_hash(args.context_capture) or str(capture.get('account')) != account:
        raise ValueError('Capture context mismatch')
    summary = json.loads((args.consolidated/'summary.json').read_text())
    if summary['input_sha256'] != file_hash(args.capture):
        raise ValueError('Consolidation source mismatch')
    args.output.mkdir(parents=True, exist_ok=False)
    details_by_ticket = {}
    for item in capture['details']:
        if item['status'] == 200:
            detail = json_body(item['body']).get('data', {})
            if detail.get('idTicket'):
                details_by_ticket.setdefault(str(detail['idTicket']), []).append(detail)
    rows = []
    for ticket in capture['tickets']:
        name = 'ticket-'+digest(str(ticket['ticket_id']))+'.pdf'
        path = args.consolidated/name
        if not path.exists():
            continue
        if file_hash(path) != summary['private_output_sha256'][name]:
            raise ValueError('PDF bytes changed after consolidation')
        reader = PdfReader(path)
        text = '\n'.join(page.extract_text() or '' for page in reader.pages)
        parsed = parse_ticket(text, account)
        parsed.update(ticket_id=ticket['ticket_id'], pdf_file=name, pdf_sha256=file_hash(path),
                      text_sha256=digest(text), page_count=len(reader.pages))
        ds = details_by_ticket.get(str(ticket['ticket_id']), [])
        parsed['detail_matches'] = len(ds)
        parsed['api_ticker'] = ds[0].get('ticker') if len(ds) == 1 else None
        if len(ds) == 1 and parsed['document_type'] == 'BOLETO':
            detail = ds[0]
            checks = parsed['checks']
            for field, key in [('amount', 'net'), ('grossAmount', 'gross')]:
                if detail.get(field) is not None and parsed[key] is not None:
                    checks['pdf_vs_api_'+key+'_residual'] = parsed[key]-abs(Decimal(detail[field]))
            checks['pdf_api_execution_date_equal'] = parsed['execution_date'] == detail.get('executionDate')
            checks['pdf_api_settlement_date_equal'] = parsed['settlement_date'] == detail.get('settlementDate')
            if detail.get('quantity') is not None and parsed.get('quantity_abs') is not None:
                checks['pdf_api_quantity_residual'] = parsed['quantity_abs']-abs(Decimal(detail['quantity']))
        rows.append(parsed)
        (args.output/(name+'.txt')).write_text(text, encoding='utf-8')
    with (args.output/'tickets-private.json').open('x', encoding='utf-8') as f:
        json.dump(rows, f, default=encode, indent=2, ensure_ascii=True)
        f.write('\n')
    for field, name in [('execution_legs', 'ticket-executions-private.csv'), ('charges', 'ticket-charges-private.csv')]:
        flat = []
        for row in rows:
            for index, value in enumerate(row[field]):
                flat.append({'ticket_id': row['ticket_id'], 'document_number': row['document_number'],
                             'api_ticker': row['api_ticker'], 'document_status': row['status'],
                             'source_pdf': row['pdf_file'], 'source_pdf_sha256': row['pdf_sha256'],
                             'line_index': index, **value})
        if flat:
            with (args.output/name).open('x', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=list(flat[0]))
                writer.writeheader()
                writer.writerows(flat)
    counts = {'pdfs_processed': len(rows), 'status_counts': dict(Counter(r['status'] for r in rows)),
              'owner_evidence_counts': dict(Counter(r['owner_evidence'] for r in rows)),
              'boleto_count': sum(r['document_type'] == 'BOLETO' for r in rows),
              'execution_legs': sum(len(r['execution_legs']) for r in rows),
              'with_explicit_cost_lines': sum(bool(r['charges']) for r in rows),
              'input_sha256': file_hash(args.capture), 'pypdf_version': pypdf_version,
              'parsed_json_sha256': file_hash(args.output/'tickets-private.json'),
              'csv_sha256': {p.name: file_hash(p) for p in args.output.glob('*.csv')},
              'economic_pnl_net': None, 'account_cost_total': None,
              'limitations': ['DOCUMENT_ARITHMETIC_IS_NOT_ACCOUNT_PNL', 'CLOCK_TIMEZONE_NOT_ATTESTED',
                              'OTHER_PDF_LAYOUTS_REMAIN_PARTIAL', 'ABSENT_CHARGE_LINES_NOT_ZERO']}
    for key in ['pdf_vs_api_net_residual', 'pdf_vs_api_gross_residual', 'pdf_api_quantity_residual']:
        values = [r['checks'][key] for r in rows if key in r['checks']]
        counts[key] = {'available': len(values), 'within_001': sum(abs(v) <= Decimal('.01') for v in values)}
    write_new(args.output/'summary.json', counts)
    print(json.dumps(counts))


if __name__ == '__main__':
    main()
