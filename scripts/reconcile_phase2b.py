"""Offline, owner-scoped reconciliation of original Cocos historical statements.

Requires pypdf only for CLI PDF extraction. No operational database or app writes.
Statement arithmetic, settled quantities and cash are distinct from realized PnL.
"""
from __future__ import annotations
import argparse
import base64
from collections import Counter, defaultdict
import csv
from datetime import datetime, timedelta
from decimal import Decimal as D
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import unicodedata

from cocos_activity_audit import context, encode, json_body
from measurement_baseline import file_hash, write_new, digest

TOL = D('.01')
NUM = r'-?(?:\d{1,3}(?:\.\d{3})+|\d+),\d+'
ROW = re.compile(rf'^(.*?)\s{{2,}}({NUM})\s+(ARS|USD MEP|USD CABLE)\s+(?:({NUM})\s+)?({NUM})\s+({NUM}\*?)$')


def number(value):
    if not re.fullmatch(NUM, value.rstrip('*')): raise ValueError('Unknown document number format')
    return D(value.rstrip('*').replace('.', '').replace(',', '.'))


def plain(value):
    return ''.join(c for c in unicodedata.normalize('NFD', value) if not unicodedata.combining(c))


def parse_statement(pages, account, date):
    """Recognize position tables only; retain all pages and do not infer blanks."""
    requested = datetime.strptime(date, '%Y%m%d').strftime('%d-%m-%Y')
    positions, current, issues = {}, None, []
    for page_index, text in enumerate(pages):
        accounts = re.findall(r'Nro de comitente:\s*(\d+)', text)
        if not accounts or any(x != account for x in accounts): raise ValueError('Statement account mismatch')
        ranges = re.findall(r'Desde\s+(\d{2}-\d{2}-\d{4})-\s*Hasta\s+(\d{2}-\d{2}-\d{4})', text)
        if not ranges or any(a != requested or b != requested for a,b in ranges): raise ValueError('Statement date mismatch')
        for line_index, line in enumerate(text.splitlines()):
            line = line.strip(); normalized = plain(line)
            heading = re.fullmatch(r'POSICION AL CIERRE DEL (\d{2}-\d{2}-\d{4})', normalized)
            if heading:
                current = datetime.strptime(heading[1], '%d-%m-%Y').date().isoformat()
                if current in positions: raise ValueError('Repeated statement position section')
                positions[current] = {'rows': [], 'reported_nav': None, 'fx_reported': [],
                                      'reported_summary_fields': [], 'unparsed_numeric_lines': []}
                continue
            if normalized in ['INCREMENTOS/DECREMENTOS DE LA INVERSION', 'MOVIMIENTOS', 'MOVIMIENTOS POR ESPECIE']:
                current = None
            if current is None: continue
            section = positions[current]
            match = ROW.fullmatch(line)
            if match:
                name, quantity, currency, price, total, ars = match.groups()
                if not name: raise ValueError('Unnamed statement holding')
                section['rows'].append({'name': name, 'quantity': number(quantity), 'currency': currency,
                    'price': number(price) if price else None, 'native_total': number(total), 'ars_total': number(ars),
                    'ars_converted': ars.endswith('*'), 'page': page_index+1, 'line': line_index+1})
            elif re.fullmatch(r'\([A-Z0-9.]+\)', line) and section['rows']:
                section['rows'][-1]['name'] += ' '+line
            elif normalized.startswith('Total Posicion al '):
                values = re.findall(NUM, line)
                if len(values) != 1 or section['reported_nav'] is not None: raise ValueError('Ambiguous NAV total')
                section['reported_nav'] = number(values[0])
            elif normalized.startswith(('* Valores calculados', '** Valores calculados')):
                section['fx_reported'].append(line)
            else:
                # These separately printed summary lines are not position rows
                # or a canonical reconciled PnL. Preserve their exact labels.
                summary = re.fullmatch(rf'(Posicion pendiente de liquidacion al (\d{{2}}-\d{{2}}-\d{{4}})|TOTAL DE LA INVERSION|RESULTADO)\s+(ARS|USD MEP|USD CABLE)\s+({NUM})', normalized)
                if summary:
                    label, summary_day, currency, value = summary.groups()
                    if summary_day and datetime.strptime(summary_day, '%d-%m-%Y').date().isoformat() != current:
                        raise ValueError('Pending settlement summary date mismatch')
                    if any(r['label'] == label for r in section['reported_summary_fields']):
                        raise ValueError('Duplicate statement summary field')
                    section['reported_summary_fields'].append({'label':label,'currency':currency,'value':number(value),
                        'page':page_index+1,'line':line_index+1,'original_text':line})
                elif re.search(NUM,line):
                    section['unparsed_numeric_lines'].append({'page':page_index+1,'line':line_index+1,'text':line})
    if len(positions) != 2: raise ValueError('Expected opening and closing statement tables')
    end=datetime.strptime(date,'%Y%m%d').date()
    if set(positions)!={end.isoformat(),(end-timedelta(days=1)).isoformat()}:
        raise ValueError('Expected previous-day opening and report-day closing')
    for day, section in positions.items():
        keys = [(r['name'],r['currency']) for r in section['rows']]
        if len(keys) != len(set(keys)): raise ValueError('Duplicate statement holding')
        if not section['rows'] or section['reported_nav'] is None: raise ValueError('Incomplete statement table')
        section['table_sum_residual'] = sum((r['ars_total'] for r in section['rows']),D(0))-section['reported_nav']
        section['table_sum_status'] = 'VERIFIED' if abs(section['table_sum_residual']) <= TOL and not section['unparsed_numeric_lines'] else 'PARTIAL'
        if abs(section['table_sum_residual']) > TOL: issues.append('STATEMENT_TABLE_NAV_RESIDUAL')
        if section['unparsed_numeric_lines']: issues.append('UNRECOGNIZED_STATEMENT_NUMERIC_LINE')
    return {'tables': positions, 'issues': issues, 'economic_pnl_net': None}


def parse_csv(text):
    """The observed export has eight-column holdings and FIVE-column cash rows."""
    reader = csv.reader(io.StringIO(text.lstrip('\ufeff')), delimiter=';')
    header = next(reader)
    if header != ['instrumento','cantidad','precio','precioAsOf','moneda','total','usedFallbackPrice','missingPrice']:
        raise ValueError('CSV schema changed')
    rows = []
    for index, row in enumerate(reader, 2):
        if not row: continue
        if len(row) == 8:
            name, quantity, price, at, currency, total, fallback, missing = row
            if fallback not in ['0','1'] or missing not in ['0','1']: raise ValueError('Unknown price flag')
            kind = 'HOLDING'
        elif len(row) == 5 and row[0] in ['ARS','USD','USD CABLE','CABLE'] and row[0] == row[3]:
            name, quantity, price, currency, total = row
            at = fallback = missing = None
            kind = 'CASH_RAGGED_FIVE_COLUMNS'
        else: raise ValueError('Unknown CSV row shape')
        def decimal(value):
            if not re.fullmatch(r'-?\d+(?:,\d+)?',value): raise ValueError('Unknown CSV number')
            return D(value.replace(',','.'))
        rows.append({'name':name,'quantity':decimal(quantity),'price':decimal(price),'price_as_of':at,
                     'currency':currency,'native_total':decimal(total),'fallback_price':fallback,
                     'missing_price':missing,'kind':kind,'line':index,'original_cells':row})
    keys = [(r['name'],r['currency']) for r in rows]
    if len(keys) != len(set(keys)): raise ValueError('Duplicate CSV instrument')
    return rows


def cash_legs(row):
    if row['economic_event_eligibility'] == 'EXCLUDED_FAILED_OR_CANCELED': return []
    detail = row['original_detail_record']; currency = detail.get('currency'); amount = detail.get('amount')
    if currency is not None and amount is not None:
        # Broker net already incorporates costs: never subtract maps again.
        return [{'currency':currency,'amount':D(str(amount)),'basis':'EXPLICIT_BROKER_NET'}]
    # No cash principal is invented for EXT. Preserve only explicitly named costs.
    legs = []
    for kind, values in (detail.get('charges') or {}).items():
        if isinstance(values,dict):
            legs.extend({'currency':c,'amount':D(str(v)),'basis':'EXPLICIT_'+kind.upper()} for c,v in values.items())
    return legs


def ticker(row):
    match = re.search(r'\(([A-Z0-9.]+)\)$',row['name'])
    return match[1] if match else None


def cash_report_currency(currency):
    # Public Cocos UI module 4448 portfolioCashTitles.web: USD = Dolar MEP.
    # Only cash denomination. EXT activity is NOT relabeled USD or USD Cable.
    return 'USD MEP' if currency == 'USD' else currency


def unique_history_balances(points):
    """Do not silently choose a version when history repeats a calendar label."""
    versions = defaultdict(list)
    for point in points:
        versions[point['time']].append(D(str(point['balance'])))
    return {day: values[0] for day, values in versions.items() if len(values) == 1}


def daily_reconciliation(statement, activity, report_day):
    tables = statement['tables']; days = sorted(tables); opening, closing = (tables[x] for x in days)
    if days[-1] != report_day: raise ValueError('Closing table date mismatch')
    selected = [r for r in activity if r.get('settlement_date') == report_day and
                r['economic_event_eligibility'] != 'EXCLUDED_FAILED_OR_CANCELED']
    for side in [opening,closing]:
        keys = [ticker(r) for r in side['rows'] if ticker(r)]
        if len(keys) != len(set(keys)): raise ValueError('Ambiguous ticker key')
    q0 = {ticker(r):r['quantity'] for r in opening['rows'] if ticker(r)}
    q1 = {ticker(r):r['quantity'] for r in closing['rows'] if ticker(r)}
    traded = defaultdict(lambda:D(0)); identities = set(); unknown = []
    for row in selected:
        identity = (row['identifier'],row['identifier_id'])
        if identity in identities: raise ValueError('Repeated activity identity')
        identities.add(identity)
        if row['movement_type'] in ['BUY','SELL']:
            qty = row.get('detail_quantity')
            if qty is None or not row.get('ticker'): unknown.append(identity); continue
            traded[row['ticker']] += abs(D(str(qty)))*(1 if row['movement_type']=='BUY' else -1)
    # These are listed-vector differences in full statement tables, not an
    # inferred opening lot basis. Untickered holdings remain explicitly excluded.
    quantity_rows = [{'ticker':key,'opening_listed':q0.get(key),'closing_listed':q1.get(key),
        'settled_trade_delta':traded.get(key),'listed_vector_residual':q1.get(key,D(0))-q0.get(key,D(0))-traded.get(key,D(0))}
        for key in sorted(set(q0)|set(q1)|set(traded))]
    c0 = {r['currency']:r['quantity'] for r in opening['rows'] if r['name'].startswith('Saldo ')}
    c1 = {r['currency']:r['quantity'] for r in closing['rows'] if r['name'].startswith('Saldo ')}
    legs = [dict(leg,identity=[r['identifier'],r['identifier_id']]) for r in selected for leg in cash_legs(r)]
    cash = []
    for currency in sorted(set(c0)|set(c1)|{cash_report_currency(x['currency']) for x in legs}):
        relevant = [x['amount'] for x in legs if cash_report_currency(x['currency'])==currency]
        delta = sum(relevant,D(0))
        residual = c1[currency]-c0[currency]-delta if currency in c0 and currency in c1 else None
        cash.append({'currency':currency,'opening':c0.get(currency),'closing':c1.get(currency),
            'observed_net_delta':delta,'observed_cash_legs':len(relevant),'residual':residual,
            'status':'MISSING_EXPLICIT_BOUNDARY' if residual is None else 'VERIFIED_ARITHMETIC' if abs(residual)<=TOL else 'PARTIAL'})
    return {'start_day':days[0],'end_day':days[1],'activity_rows_by_settlement':len(selected),
            'quantity_rows':quantity_rows,'unknown_trade_quantities':unknown,'cash_rows':cash,'cash_legs':legs,
            'untickered_non_cash_rows':sum(not ticker(r) and not r['name'].startswith('Saldo ') for s in [opening,closing] for r in s['rows']),
            'status':'PARTIAL','economic_pnl_net':None}


def legacy_crosswalk(source, activity, account):
    by_ticket = defaultdict(list)
    for row in activity:
        if row.get('ticket_id'): by_ticket[str(row['ticket_id'])].append(row)
    rows = []; counts = Counter()
    for old in source['tables']['broker_movements']:
        raw = json_body(old['raw_payload'])
        if str(raw.get('id_account')) != account:
            counts['without_explicit_account_match'] += 1; continue
        linked = by_ticket.get(str(raw.get('id_ticket')),[])
        counts['explicit_account_rows'] += 1
        result = {'local_row_id':old['id'],'raw_ticket_id':raw.get('id_ticket'),
            'channel':'CASH' if raw.get('id_cash_movement') else 'SECURITY_OR_OTHER',
            'exact_ticket_matches':len(linked),'status':'RECOVERABLE','included_in_economic_ledger':False}
        if len(linked)==1:
            new=linked[0];detail=new['original_detail_record'];counts['unique_ticket_reference']+=1
            result['current_identity']=[new['identifier'],new['identifier_id']]
            if result['channel']=='CASH':
                counts['cash_ticket_references']+=1
                candidates=[leg for leg in cash_legs(new) if leg['currency']==raw.get('id_currency')]
                if len(candidates)==1 and raw.get('quantity') is not None:
                    residual=D(str(raw['quantity']))-candidates[0]['amount']
                    result.update(cash_comparison_basis=candidates[0]['basis'],cash_residual=residual,
                                  historical_value=raw['quantity'],current_value=candidates[0]['amount'])
                    counts['cash_values_compared']+=1
                    counts['cash_comparison_'+candidates[0]['basis']]+=1
                    counts['cash_values_within_001']+=abs(residual)<=TOL
                    counts['cash_value_conflicts']+=abs(residual)>TOL
        rows.append(result)
    return rows,dict(counts)


def public_receipt(summary):
    keys=['schema_version','status','economic_pnl_net','capture_sha256','activity_sha256',
          'owner_context_sha256','started_at','finished_at','reports','report_dates','counts','histories',
          'legacy_crosswalk_counts','statement_history_comparison_counts','cash_currency_mapping','daily_counts',
          'pypdf_version','qeef_preflight','retry_status_counts','current_orders_rows']
    public={key:summary[key] for key in keys}
    public['private_manifest_sha256']=digest(summary['private_output_sha256'])
    root=Path(__file__).resolve().parents[1]
    paths=['scripts/capture_phase2b.py','scripts/capture_phase2b.js','scripts/reconcile_phase2b.py',
           'tests/test_phase2b_reconciliation.py','tests/test_capture_phase2b.cjs']
    public['code_sha256_utf8_lf']={name:hashlib.sha256((root/name).read_text(encoding='utf8').encode()).hexdigest() for name in paths}
    return public


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context-capture',type=Path,required=True)
    parser.add_argument('--capture',type=Path,required=True)
    parser.add_argument('--activity',type=Path,required=True,help='Frozen original Cocos activity gzip')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--public-receipt',type=Path)
    args=parser.parse_args()
    from pypdf import PdfReader, __version__ as pdf_version
    from cocos_activity_audit import consolidate
    account,fills=context(args.context_capture)
    source=json.load(gzip.open(args.capture,'rt',encoding='utf8'))
    old=json.load(gzip.open(args.activity,'rt',encoding='utf8'))
    for item in [source,old]:
        if str(item.get('account'))!=account or item.get('owner_context_sha256')!=file_hash(args.context_capture):
            raise ValueError('Source context mismatch')
    if source.get('schema_version')!='phase2b-capture-v1' or any(b['account_verification']!='ACTIVE_UI_ACCOUNT_MATCH' or b['status']=='SOURCE_BLOCKED' for b in source['batches']):
        raise ValueError('Unverified statement source')
    activity,_=consolidate(old,account,fills)
    args.output.mkdir(parents=True,exist_ok=False)
    records={}
    for row in source['records']:
        key=row['key']
        if key in records or not re.fullmatch(r'[A-Za-z0-9.-]+',key):raise ValueError('Unsafe/repeated output key')
        records[key]=row
        if row['status']==200:
            payload=base64.b64decode(row['base64'],validate=True) if 'base64' in row else row['body'].encode()
            if key.endswith('.pdf') and not payload.startswith(b'%PDF-'):raise ValueError('Invalid PDF payload')
            with (args.output/key).open('xb') as f:f.write(payload)
    reports=[]
    for key,row in sorted(records.items()):
        if not key.startswith('holdings-') or not key.endswith('.pdf') or row['status']!=200:continue
        date=key[len('holdings-'):-4];pdf=PdfReader(args.output/key)
        pages=[page.extract_text(extraction_mode='layout') or '' for page in pdf.pages]
        with (args.output/(key+'.txt')).open('x',encoding='utf8') as f:f.write('\n\f\n'.join(pages))
        statement=parse_statement(pages,account,date)
        csv_path=args.output/key.replace('.pdf','.csv')
        csv_rows=parse_csv(csv_path.read_text(encoding='utf8')) if csv_path.exists() else []
        day=datetime.strptime(date,'%Y%m%d').date().isoformat();closing=statement['tables'][day]
        comparisons=[]
        for c in csv_rows:
            candidates=[r for r in closing['rows'] if r['name']==c['name'] and r['currency']==c['currency']]
            if c['kind']=='CASH_RAGGED_FIVE_COLUMNS':
                currency=cash_report_currency(c['currency'])
                candidates=[r for r in closing['rows'] if r['name']=='Saldo '+currency and r['currency']==currency]
            match=candidates[0] if len(candidates)==1 else None
            comparisons.append({'name':c['name'],'currency':c['currency'],'kind':c['kind'],
                'status':'EXACT_NAME_CURRENCY_MATCH' if match else 'NO_UNIQUE_EXACT_NAME_CURRENCY_MATCH',
                'quantity_residual':c['quantity']-match['quantity'] if match else None,
                'native_total_residual':c['native_total']-match['native_total'] if match else None})
        reports.append({'date':day,'pdf_sha256':file_hash(args.output/key),'statement':statement,
                        'csv_rows':csv_rows,'csv_pdf_comparisons':comparisons,
                        'reconciliation':daily_reconciliation(statement,activity,day)})
    histories={}
    for currency in ['ARS','USD']:
        r=records.get('history-'+currency)
        if not r or r['status']!=200:continue
        data=json_body(r['body']);points=data['data'];times=[x['time'] for x in points]
        histories[currency]={'count':len(points),'min':min(times) if times else None,'max':max(times) if times else None,
                             'duplicate_dates':len(times)-len(set(times)),'display_currency':data['displayCurrency']}
    counts=Counter()
    for report in reports:
        for table in report['statement']['tables'].values():
            counts['statement_tables']+=1
            counts['statement_nav_sums_within_001']+=abs(table['table_sum_residual'])<=TOL
            counts['statement_tables_fully_parsed']+=not table['unparsed_numeric_lines']
            counts['reported_pending_settlement_fields']+=sum(r['label'].startswith('Posicion pendiente') for r in table['reported_summary_fields'])
        rec=report['reconciliation'];counts['quantity_comparisons']+=len(rec['quantity_rows'])
        counts['untickered_non_cash_table_rows']+=rec['untickered_non_cash_rows']
        counts['unknown_trade_quantities']+=len(rec['unknown_trade_quantities'])
        counts['activity_rows_by_settlement']+=rec['activity_rows_by_settlement']
        counts['quantity_residuals_zero']+=sum(r['listed_vector_residual']==0 for r in rec['quantity_rows'])
        for row in rec['cash_rows']:
            counts['cash_'+row['currency']+'_'+row['status']]+=1
        for c in report['csv_pdf_comparisons']:
            counts['csv_rows']+=1;counts['csv_pdf_exact_name_currency']+=c['status']=='EXACT_NAME_CURRENCY_MATCH'
            counts['csv_pdf_quantity_equal']+=c['quantity_residual']==0
            counts['csv_pdf_native_total_within_001']+=c['native_total_residual'] is not None and abs(c['native_total_residual'])<=TOL
        for c in report['csv_rows']:
            counts['csv_ragged_cash_rows']+=c['kind']=='CASH_RAGGED_FIVE_COLUMNS'
            if c['kind']=='HOLDING':
                counts['csv_missing_price_flags']+=c['missing_price']=='1'
                counts['csv_fallback_price_flags']+=c['fallback_price']=='1'
                counts['csv_missing_price_date']+=not bool(c['price_as_of'])
                counts['csv_price_date_different_from_report']+=bool(c['price_as_of']) and c['price_as_of']!=report['date']
    local=json.load(gzip.open(args.context_capture,'rt',encoding='utf8'))
    legacy,legacy_counts=legacy_crosswalk(local,activity,account)
    # Compare reported values by date label only; this does not align intraday
    # clocks, settlement valuation policies or retrospective price revisions.
    history_comparisons=[]
    if records.get('history-ARS',{}).get('status')==200:
        points=unique_history_balances(json_body(records['history-ARS']['body'])['data'])
        for report in reports:
            for day,table in report['statement']['tables'].items():
                history_comparisons.append({'day':day,'report_day':report['date'],'history_balance':points.get(day),
                    'statement_nav':table['reported_nav'],'residual':points[day]-table['reported_nav'] if day in points else None,
                    'alignment':'DATE_LABEL_ONLY_NOT_SAME_OBSERVATION'})
    date_values=defaultdict(list)
    for row in history_comparisons:date_values[row['day']].append(row['statement_nav'])
    history_counts={'available':sum(r['residual'] is not None for r in history_comparisons),
        'within_001':sum(r['residual'] is not None and abs(r['residual'])<=TOL for r in history_comparisons),
        'unique_dates':len(date_values),'repeated_statement_date_sections':sum(len(v)-1 for v in date_values.values()),
        'conflicting_same_date_statement_nav':sum(len(set(v))>1 for v in date_values.values())}
    result={'reports':reports,'histories':histories,'legacy_crosswalk':legacy,
        'statement_history_comparisons':history_comparisons,'economic_pnl_net':None,
        'qeef_preflight':{'status':'NOT_IDENTIFIABLE','reasons':['OPENING_COST_BASIS_NOT_PROVIDED',
            'DATE_ONLY_EVENTS_NO_ATTESTED_CLOCK_TIMEZONE','FINANCING_COVERAGE_UNKNOWN',
            'FX_AND_UNTICKERED_SECURITY_ATTRIBUTION_REQUIRED','LEGACY_ORDER_IDENTITIES_UNRESOLVED'],
            'contract':'src/analysis/qeef/accounting.py','canonical_reconcile_called':False}}
    with (args.output/'reconciliation-private.json').open('x',encoding='utf8') as f:json.dump(result,f,default=encode,ensure_ascii=True,indent=2)
    summary={'schema_version':'phase2b-reconciliation-v1','status':'PARTIAL','economic_pnl_net':None,
        'capture_sha256':file_hash(args.capture),'activity_sha256':file_hash(args.activity),'owner_context_sha256':file_hash(args.context_capture),
        'started_at':source['started_at'],'finished_at':source['finished_at'],'reports':len(reports),'report_dates':[r['date'] for r in reports],
        'counts':dict(counts),'histories':histories,'legacy_crosswalk_counts':legacy_counts,
        'daily_counts':[{'start_day':r['reconciliation']['start_day'],'end_day':r['date'],
            'quantity_comparisons':len(r['reconciliation']['quantity_rows']),
            'quantity_residuals_zero':sum(q['listed_vector_residual']==0 for q in r['reconciliation']['quantity_rows']),
            'activity_rows_by_settlement':r['reconciliation']['activity_rows_by_settlement'],
            'cash_status':{c['currency']:c['status'] for c in r['reconciliation']['cash_rows']}} for r in reports],
        'statement_history_comparison_counts':history_counts,
        'cash_currency_mapping':{'USD':'USD MEP','basis':'Cocos public web portfolioCashTitles module 4448; cash only'},
        'pypdf_version':pdf_version,'qeef_preflight':result['qeef_preflight'],
        'retry_status_counts':dict(Counter(str(r['status']) for k,r in records.items() if k.startswith('retry-ticket-'))),
        'current_orders_rows':len(json_body(records['orders-current']['body'])['data']) if records.get('orders-current',{}).get('status')==200 else None,
        'private_output_sha256':{p.name:file_hash(p) for p in args.output.iterdir() if p.is_file()}}
    write_new(args.output/'summary.json',summary)
    if args.public_receipt:write_new(args.public_receipt,public_receipt(summary))
    print(json.dumps({k:v for k,v in summary.items() if k!='private_output_sha256'}))


if __name__=='__main__':main()
