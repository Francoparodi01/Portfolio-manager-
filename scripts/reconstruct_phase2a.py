"""Offline, source-versioned Phase 2A reconstruction; never connects to a DB.

Creates NEW private artifacts only. Amount differences are not fees or profit.
No owner inference, mutable source selection, FX estimates, or event deduplication.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
import csv
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3

from measurement_baseline import copy_blocks, digest, file_hash, normalize, owner_scope, write_new

D = Decimal
# PostgreSQL numerics can retain the full binary-float expansion (>28 digits).
getcontext().prec = 100
TOL = D('.01')
ART = timezone(timedelta(hours=-3))
SELECTED = ('portfolio_snapshots', 'positions', 'raw_snapshots', 'broker_fills',
            'broker_movements', 'decision_log', 'execution_plans', 'position_hold_observations',
            'plan_execution_attributions', 'plan_execution_attribution_plans',
            'plan_execution_attribution_movements', 'corporate_events', 'corporate_event_applications')


def num(value):
    if value is None or value == '':
        return None
    result = D(str(value))
    if not result.is_finite():
        raise ValueError('Non-finite financial value')
    return result


def stamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('Timestamp without timezone')
    return result.astimezone(timezone.utc)


def obj(value):
    return json.loads(value, parse_float=D) if isinstance(value, str) else value or {}


def amount_text(value):
    return str(value) if value is not None else None


def ref(table, row):
    key = row.get('id', row.get('snapshot_id'))
    return table + ':' + digest(str(key))[:24]


def instrument(row):
    # A ticker/currency observation key is NOT a verified instrument master ID.
    return (row['ticker'].strip().upper(), row['currency'].strip().upper())


def unique(rows, key):
    result = {}
    for row in rows:
        k = key(row)
        if k in result:
            raise ValueError('Ambiguous duplicate key; cannot silently choose a version')
        result[k] = row
    return result


def broker_ticket(raw):
    value = raw.get('id_ticket', raw.get('idTicket'))
    return str(value).strip() if value is not None and str(value).strip() not in {'', '0'} else None


def forensic_checks(tables, owner):
    """Investigate repeated tickets, numeric serialization and account-cash candidates."""
    tickets = defaultdict(list)
    cash_rows = []
    raw_qdiff = []
    positions = {(r['snapshot_id'], *instrument(r)): r for r in tables['positions']}
    for row in tables['raw_snapshots']:
        raw = obj(row['payload'])
        for p in raw.get('positions', []):
            stored = positions.get((row['snapshot_id'], *instrument(p)))
            if stored and num(p['quantity']) is not None and num(stored['quantity']) is not None:
                diff = num(p['quantity']) - num(stored['quantity'])
                if diff:
                    raw_qdiff.append(abs(diff))
    for row in tables['broker_movements']:
        raw = obj(row['raw_payload'])
        ticket = broker_ticket(raw)
        if ticket:
            tickets[(row['source'], ticket)].append(row)
        if raw.get('id_cash_movement') is not None:
            cash_rows.append(row)
    repeated = [rows for rows in tickets.values() if len(rows) > 1]
    channel_pairs = sum(len(rows) == 2 and sum(obj(r['raw_payload']).get('id_cash_movement') is not None for r in rows) == 1 for rows in repeated)
    # Cash legs and security legs may share a ticket. They are not two trades.
    cash_types = Counter((r['movement_type'], r['currency']) for r in cash_rows)
    cash_ids = [(r['source'], str(obj(r['raw_payload'])['id_cash_movement'])) for r in cash_rows]
    costs = Counter()
    for table in ('broker_fills', 'broker_movements'):
        for row in tables[table]:
            if table == 'broker_fills' and owner_scope(row, owner) != 'EXACT_OWNER':
                continue
            raw = obj(row['raw_payload'])
            # A key census, not proof that no charge was embedded in another field.
            for key, value in raw.items():
                if any(word in key.lower() for word in ('commission', 'fee', 'tax', 'comision', 'arancel')) and value is not None:
                    costs[table + '.' + key] += 1
    return {'valid_ticket_repeated_groups': len(repeated), 'rows_in_repeated_ticket_groups': sum(map(len, repeated)),
            'cash_trade_ticket_pairs': channel_pairs,
            'repeated_ticket_currency_conflicts': sum(len({r['currency'] for r in rows}) > 1 for rows in repeated),
            'cash_channel_rows': len(cash_rows), 'cash_channel_duplicate_ids': len(cash_ids)-len(set(cash_ids)),
            'cash_channel_types': [{'type': t, 'currency': c, 'n': n} for (t, c), n in sorted(cash_types.items())],
            'raw_quantity_precision_differences': len(raw_qdiff),
            'raw_quantity_max_abs_difference': amount_text(max(raw_qdiff)) if raw_qdiff else '0',
            'explicit_cost_payload_keys_found': dict(costs),
            'ticket_link_status': 'RECOVERABLE_CHANNEL_IDENTITY_ONLY',
            'cash_reconciliation_status': 'NOT_IDENTIFIABLE'}


def reconcile_observed_quantities(tables, owner, intervals, states):
    """Cross-check observational clocks without promoting them to execution time.

    Grouping is only a collision diagnostic. Similar rows are never deduplicated.
    """
    by_end = {stamp(r['end_at']): r for r in intervals}
    groups = defaultdict(list)
    counts = Counter()
    for fill in tables['broker_fills']:
        if owner_scope(fill, owner) != 'EXACT_OWNER':
            continue
        at = stamp(fill['executed_at'])
        if states:
            counts['before_first_snapshot'] += int(at < stamp(states[0]['at']))
            counts['within_snapshot_window'] += int(stamp(states[0]['at']) <= at <= stamp(states[-1]['at']))
        groups[(fill['source'], at, fill['ticker'], fill['side'], num(fill['quantity']))].append(fill)
        if fill['executed_at_precision'] == 'observed_after' and at in by_end:
            counts['observed_clock_matches_snapshot'] += 1
            deltas = {(r['ticker'], r['currency']): num(r['delta']) for r in by_end[at]['quantity_deltas']}
            delta = deltas.get((fill['ticker'], 'ARS'), D(0))
            quantity = num(fill['quantity'])
            if quantity is not None:
                signed = quantity * (1 if fill['side'] == 'BUY' else -1)
                counts['individual_quantity_delta_matches'] += int(delta == signed)
    overlaps = []
    for rows in groups.values():
        if len(rows) <= 1:
            continue
        counts['overlap_candidate_groups'] += 1
        counts['rows_in_overlap_candidates'] += len(rows)
        same_gross = len({num(r['gross_amount_ars']) for r in rows}) == 1
        same_price = len({num(r['avg_fill_price']) for r in rows}) == 1
        one_each = len(rows) == 2 and sum('idOrder' in obj(r['raw_payload']) for r in rows) == 1
        counts['overlap_groups_equal_amount'] += int(same_gross)
        counts['overlap_groups_equal_price'] += int(same_price)
        counts['overlap_order_movement_pairs'] += int(one_each)
        overlaps.append({'references': [ref('broker_fills', r) for r in rows], 'status': 'PARTIAL',
                         'reason': 'SAME_OBSERVED_CLOCK_TICKER_SIDE_QUANTITY', 'same_amount': same_gross,
                         'same_price': same_price, 'order_and_movement_channels': one_each,
                         'deduplicated': False, 'canonical_execution_id': None})
    return dict(counts), overlaps


def reconstruct(tables, owner):
    snapshots = [r for r in tables['portfolio_snapshots'] if owner_scope(r, owner) == 'EXACT_OWNER']
    snapshots.sort(key=lambda r: stamp(r['scraped_at']))
    unique(snapshots, lambda r: r['snapshot_id'])
    unique(snapshots, lambda r: stamp(r['scraped_at']))
    children = defaultdict(list)
    for row in tables['positions']:
        children[row['snapshot_id']].append(row)
    raws = unique(tables['raw_snapshots'], lambda r: r['snapshot_id'])
    states, changes = [], []
    counts, raw_counts = Counter(), Counter()
    previous = None
    for snap in snapshots:
        sid = snap['snapshot_id']
        positions = unique(children[sid], instrument)
        missing = any(any(num(r.get(c)) is None for c in ('quantity', 'current_price', 'market_value'))
                      for r in positions.values())
        mixed_currency = any(k[1] != 'ARS' for k in positions)
        # Empty positions cannot prove an empty portfolio without the raw envelope.
        raw = obj(raws.get(sid, {}).get('payload'))
        empty_unconfirmed = not positions and raw.get('positions') != []
        complete = not (missing or mixed_currency or empty_unconfirmed)
        calculated = sum((num(r['quantity']) * num(r['current_price']) for r in positions.values()), D(0)) if complete else None
        total, cash = num(snap['total_value_ars']), num(snap['cash_ars'])
        residual = calculated - total if calculated is not None and total is not None else None
        counts['valuation_match' if residual is not None and abs(residual) <= TOL else 'valuation_gap_or_unknown'] += 1
        counts['missing_valuation_components'] += int(not complete or total is None or cash is None)
        for field in ('total_value_ars', 'cash_ars'):
            a, b = num(raw.get(field)), num(snap[field])
            raw_counts[field + '_within_cent'] += int(a is not None and b is not None and abs(a - b) <= TOL)
        raw_counts['owner_exact'] += int(owner_scope({'owner_chat_id': str(raw['owner_chat_id']) if raw.get('owner_chat_id') is not None else None}, owner) == 'EXACT_OWNER')
        raw_counts['id_equal'] += int(raw.get('snapshot_id') == sid)
        raw_counts['timestamp_equal'] += int(bool(raw.get('scraped_at')) and stamp(raw['scraped_at']) == stamp(snap['scraped_at']))
        raw_positions = unique(raw.get('positions', []), instrument)
        raw_counts['position_keys_equal'] += int(set(positions) == set(raw_positions))
        raw_counts['position_quantities_equal'] += int(set(positions) == set(raw_positions) and all(num(v['quantity']) == num(raw_positions[k]['quantity']) for k, v in positions.items()))
        raw_counts['position_prices_within_cent'] += int(set(positions) == set(raw_positions) and all(num(v['current_price']) is not None and num(raw_positions[k].get('current_price')) is not None and abs(num(v['current_price']) - num(raw_positions[k]['current_price'])) <= TOL for k, v in positions.items()))
        quantities = {k: num(v['quantity']) for k, v in positions.items()}
        state = {'reference': ref('portfolio_snapshots', snap), 'at': stamp(snap['scraped_at']).isoformat(),
                 'available_at': stamp(snap['created_at']).isoformat(), 'status': 'PARTIAL',
                 'reported_invested_ars': amount_text(total), 'reported_cash_ars': amount_text(cash),
                 'reported_nav_ars': amount_text(total + cash) if total is not None and cash is not None else None,
                 'calculated_invested_ars': amount_text(calculated), 'valuation_residual_ars': amount_text(residual),
                 'positions': [{'ticker': k[0], 'currency': k[1], 'quantity': amount_text(quantities[k]),
                                'observed_price': v['current_price'], 'observed_avg_cost': v['avg_cost']}
                               for k, v in sorted(positions.items())],
                 'economic_pnl_net': None}
        states.append(state)
        if previous:
            prev_state, prev_quantities = previous
            diffs = []
            for k in sorted(set(quantities) | set(prev_quantities)):
                # Absent holding = zero only for the observed snapshot vector, not for event coverage.
                a, b = prev_quantities.get(k, D(0)), quantities.get(k, D(0))
                if a is None or b is None or b != a:
                    diffs.append({'ticker': k[0], 'currency': k[1], 'delta': amount_text(b-a) if a is not None and b is not None else None})
            counts['intervals_quantity_changed' if diffs else 'intervals_quantity_unchanged'] += 1
            changes.append({'start': prev_state['reference'], 'end': state['reference'],
                            'start_at': prev_state['at'], 'end_at': state['at'], 'quantity_deltas': diffs,
                            'attribution': 'NOT_IDENTIFIABLE', 'cash_reconciled': False,
                            'economic_pnl_net': None})
        previous = state, quantities
    daily = {}
    for state in states:
        day = stamp(state['at']).astimezone(ART).date().isoformat()
        daily[day] = state
    fills_all = tables['broker_fills']
    fills = [r for r in fills_all if owner_scope(r, owner) == 'EXACT_OWNER']
    movements = tables['broker_movements']
    movement_index = defaultdict(list)
    for row in movements:
        movement_index[(row['source'], row['external_movement_id'])].append(row)
    decisions = unique(tables['decision_log'], lambda r: r['id'])
    fill_counts, ledger = Counter(), []
    account_owners = defaultdict(set)
    for fill in fills_all:
        account = obj(fill.get('raw_payload')).get('id_account')
        scope = owner_scope(fill, owner)
        if account is not None and scope not in {'OWNER_NULL', 'OWNER_ZERO'}:
            account_owners[str(account)].add(scope)
    for fill in fills:
        payload = obj(fill['raw_payload'])
        matches = movement_index[(fill['source'], fill['external_fill_id'])]
        matches = [r for r in matches if r['ticker'] == fill['ticker'] and r['currency'] == 'ARS' and r['movement_type'] == fill['side']]
        q, price, gross = (num(fill[k]) for k in ('quantity', 'avg_fill_price', 'gross_amount_ars'))
        residual = gross - q * price if all(v is not None for v in (q, price, gross)) else None
        fill_counts['side_' + fill['side']] += 1
        fill_counts['precision_' + str(fill['executed_at_precision'])] += 1
        fill_counts['fees_missing'] += int(fill['fees_ars'] is None)
        fill_counts['amount_product_gap'] += int(residual is not None and abs(residual) > TOL)
        fill_counts['derived_price'] += int(payload.get('_price_derived_from_amount') is True)
        fill_counts['amount_matches_raw'] += int(gross is not None and num(payload.get('amount')) is not None and abs(gross - abs(num(payload['amount']))) <= TOL)
        fill_counts['raw_explicit_price_matches'] += int(price is not None and num(payload.get('price')) is not None and abs(price - num(payload['price'])) <= TOL)
        fill_counts['movement_identity_unique'] += int(len(matches) == 1)
        if len(matches) == 1:
            mq = num(matches[0]['quantity'])
            fill_counts['movement_abs_quantity_match'] += int(mq is not None and q is not None and abs(mq) == abs(q))
            fill_counts['movement_signed_quantity_diff'] += int(mq is not None and q is not None and mq != q)
        target = decisions.get(fill['decision_log_id'])
        fill_counts['decision_' + (owner_scope(target, owner) if target else 'MISSING')] += 1
        if payload.get('id_account') is not None:
            fill_counts['explicit_account_anchor'] += 1
        ledger.append({'reference': ref('broker_fills', fill), 'side': fill['side'], 'ticker': fill['ticker'],
                       'currency': 'ARS', 'quantity': fill['quantity'], 'observed_price': fill['avg_fill_price'],
                       'recorded_amount_ars': fill['gross_amount_ars'], 'fees_ars': fill['fees_ars'],
                       'amount_minus_qty_price': amount_text(residual), 'at': fill['executed_at'],
                       'time_precision': fill['executed_at_precision'], 'raw_execution_day': payload.get('executionDate', payload.get('execution_date')),
                       'movement_reference': ref('broker_movements', matches[0]) if len(matches) == 1 else None,
                       'status': 'PARTIAL', 'economic_pnl_net': None})
    movement_counts, movement_ledger = Counter(), []
    for row in movements:
        raw = obj(row['raw_payload'])
        account = raw.get('id_account')
        owners = account_owners.get(str(account), set()) if account is not None else set()
        status = 'ACCOUNT_ANCHORED_CANDIDATE' if owners == {'EXACT_OWNER'} else 'ACCOUNT_CONFLICT' if len(owners) > 1 else 'UNSCOPED'
        movement_counts[status] += 1
        movement_counts['type_' + row['movement_type'] + '_' + row['currency']] += 1
        movement_counts['amount_missing'] += int(row['amount'] is None)
        movement_ledger.append({'reference': ref('broker_movements', row), 'ownership': status,
            'account_token': digest(str(account)) if account is not None else None,
            'type': row['movement_type'], 'currency': row['currency'], 'amount': row['amount'],
            'recorded_balance': row['balance'], 'at': row['executed_at'], 'time_precision': row['executed_at_precision'],
            'ticket_token': digest(broker_ticket(raw)) if broker_ticket(raw) else None,
            'evidence_channel': 'CASH' if raw.get('id_cash_movement') is not None else 'SECURITY_OR_OTHER',
            'included_in_owner_cash_ledger': False})
    # Legacy timestamp identities are candidates, supported by historical producer code.
    # Never repair operational references or assert an immutable decision thesis.
    by_time = defaultdict(list)
    by_id = {r['snapshot_id']: r for r in snapshots}
    for r in snapshots:
        by_time[stamp(r['scraped_at'])].append(r)
    holds, hold_counts = [], Counter()
    for hold in tables['position_hold_observations']:
        if owner_scope(hold, owner) != 'EXACT_OWNER':
            continue
        key = hold['portfolio_snapshot_id']
        match = by_id.get(key)
        kind = 'EXPLICIT_ID' if match else 'UNRESOLVED'
        if match is None and key:
            try:
                candidates = by_time[stamp(key)]
            except ValueError:
                candidates = []
            if len(candidates) == 1:
                match, kind = candidates[0], 'LEGACY_TIMESTAMP_CANDIDATE'
        hold_counts[kind] += 1
        holds.append({'hold': ref('position_hold_observations', hold), 'link_kind': kind,
                      'snapshot': ref('portfolio_snapshots', match) if match else None,
                      'status': 'RECOVERABLE' if kind == 'LEGACY_TIMESTAMP_CANDIDATE' else 'VERIFIED' if kind == 'EXPLICIT_ID' else 'NOT_IDENTIFIABLE'})
    first, last = (states[0], states[-1]) if states else ({}, {})
    quantity_checks, overlaps = reconcile_observed_quantities(tables, owner, changes, states)
    summary = {'snapshots': len(states), 'positions': sum(len(children[r['snapshot_id']]) for r in snapshots),
               'start': first.get('at'), 'end': last.get('at'), 'days_observed': len(daily),
               'snapshot_checks': dict(counts), 'raw_envelope_checks': dict(raw_counts),
               'fills': len(fills), 'fill_checks': dict(fill_counts), 'movements': len(movements),
               'movement_checks': dict(movement_counts), 'hold_links': dict(hold_counts),
               'quantity_fill_checks': quantity_checks,
               'economic_pnl_net': None, 'realized_pnl_gross': None, 'fees_total': None,
               'status': 'PARTIAL', 'net_pnl_status': 'NOT_IDENTIFIABLE',
               'blockers': ['OPENING_COST_BASIS_UNATTESTED', 'OWNER_SCOPED_EXTERNAL_FLOW_COVERAGE_UNKNOWN',
                            'OBSERVED_FEES_TAXES_FINANCING_UNKNOWN', 'EVENT_TIME_NOT_EXACT',
                            'INSTRUMENT_AND_CORPORATE_ACTION_COVERAGE_UNKNOWN', 'EXECUTION_IDENTITY_OVERLAP_UNRESOLVED',
                            'RAW_ENVELOPES_ARE_NORMALIZED_NOT_BROKER_STATEMENTS']}
    return {'summary': summary, 'states': states, 'daily_last_observation': daily,
            'quantity_intervals': changes, 'fills': ledger, 'fill_overlap_candidates': overlaps,
            'movement_quarantine': movement_ledger, 'hold_links': holds}


def archive_sources(db_path, sources, manifest, owner):
    """Persist immutable versions in a NEW local SQLite, with no canonical merge."""
    db_path = Path(db_path)
    if db_path.exists():
        raise FileExistsError('Archive already exists')
    db = sqlite3.connect(db_path)
    db.execute('CREATE TABLE evidence(source TEXT,tbl TEXT,key_hash TEXT,content_hash TEXT,scope TEXT,row_json TEXT)')
    summaries = {}
    for source, tables in sources.items():
        parents = {r['snapshot_id']: owner_scope(r, owner) for r in tables.get('portfolio_snapshots', [])}
        counts = Counter()
        for table, rows in tables.items():
            spec = manifest['tables'][table] if 'tables' in manifest else manifest['specs'][table]
            for row in rows:
                keys = spec['keys']
                if not keys or any(row.get(k) is None for k in keys):
                    raise ValueError('Evidence without declared identity')
                scope = owner_scope(row, owner)
                if table in {'positions', 'raw_snapshots'}:
                    scope = parents.get(row['snapshot_id'], 'PARENT_NOT_CAPTURED')
                # Comparison explicitly projects the backup column version.
                common = {k: normalize(row[k], spec['types'][k]) for k in spec['columns']}
                key = digest([common[k] for k in keys])
                counts[(table, scope)] += 1
                db.execute('INSERT INTO evidence VALUES(?,?,?,?,?,?)',
                           (source, table, key, digest(common), scope, json.dumps(row, sort_keys=True)))
        summaries[source] = [{'table': k[0], 'scope': k[1], 'n': v} for k, v in sorted(counts.items())]
    db.execute('CREATE INDEX evidence_identity ON evidence(source,tbl,key_hash)')
    duplicates = db.execute('SELECT source,tbl,count(*) FROM (SELECT source,tbl,key_hash FROM evidence GROUP BY 1,2,3 HAVING count(*)>1) GROUP BY 1,2').fetchall()
    if duplicates:
        raise ValueError('Duplicate declared source keys; archive rejected')
    comparison = []
    for table in SELECTED:
        # Holdings capture is owner bounded; other relations are complete diagnostics.
        scope_clause = "AND a.scope='EXACT_OWNER'" if table in {'portfolio_snapshots', 'positions', 'raw_snapshots'} else ''
        counts = db.execute(f'''SELECT count(*),sum(b.key_hash IS NULL),sum(a.content_hash<>b.content_hash)
            FROM evidence a LEFT JOIN evidence b ON b.source='local' AND b.tbl=a.tbl AND b.key_hash=a.key_hash
            WHERE a.source='supabase_backup' AND a.tbl=? {scope_clause}''', (table,)).fetchone()
        comparison.append({'table': table, 'backup_comparable_rows': counts[0], 'missing_local': counts[1] or 0, 'changed': counts[2] or 0})
    missing = []
    for table in ('plan_execution_attributions', 'plan_execution_attribution_plans', 'plan_execution_attribution_movements'):
        for key, scope, row in db.execute('''SELECT a.key_hash,a.scope,a.row_json FROM evidence a
            LEFT JOIN evidence b ON b.source='local' AND b.tbl=a.tbl AND b.key_hash=a.key_hash
            WHERE a.source='supabase_backup' AND a.tbl=? AND b.key_hash IS NULL''', (table,)):
            missing.append({'table': table, 'key_hash': key, 'scope': scope, 'backup_row': json.loads(row)})
    db.commit()
    db.close()
    return {'sources': summaries, 'comparison': comparison, 'duplicate_declared_keys': 0}, missing


def compare_capture(path, tables, owner, expected_token):
    old = json.loads(Path(path).read_text(encoding='utf-8'))
    if old['owner_token'] != expected_token:
        raise ValueError('Historical capture owner mismatch')
    result = {'sha256': file_hash(path), 'captured_at': old['captured_at'], 'tables': {}}
    for table, keys in [('portfolio_snapshots', ['snapshot_id']), ('positions', ['id']), ('broker_fills', ['id'])]:
        rows = tables[table]
        if table == 'broker_fills':
            rows = [r for r in rows if owner_scope(r, owner) == 'EXACT_OWNER']
        current = unique(rows, lambda r: tuple(str(r[k]) for k in keys))
        missing, changed = 0, 0
        for r in old['tables'][table]:
            found = current.get(tuple(str(r[k]) for k in keys))
            if found is None:
                missing += 1
                continue
            differs = False
            for col in ('quantity', 'avg_fill_price', 'gross_amount_ars', 'fees_ars', 'current_price', 'total_value_ars', 'cash_ars'):
                if col in r and num(r[col]) != num(found[col]):
                    differs = True
            changed += differs
        result['tables'][table] = {'historical_n': len(old['tables'][table]), 'current_n': len(rows),
                                  'missing_keys': missing, 'financial_fields_changed': changed,
                                  'comparison': 'KEYS_AND_SELECTED_FINANCIAL_FIELDS_ONLY'}
    return result


def compare_csv(directory, tables):
    results = {}
    for path in sorted(Path(directory).glob('*.csv')):
        table = path.stem
        if table not in tables:
            continue
        keys = ['snapshot_id'] if table in {'raw_snapshots', 'portfolio_snapshots'} else ['id']
        current = {tuple(r[k] for k in keys) for r in tables[table]}
        with path.open(encoding='utf-8-sig', newline='') as f:
            rows = list(csv.DictReader(f))
        csvkeys = [tuple(r[k] for k in keys) for r in rows]
        results[table] = {'sha256': file_hash(path), 'n': len(rows), 'duplicate_keys': len(csvkeys)-len(set(csvkeys)),
                          'keys_in_capture': sum(k in current for k in csvkeys),
                          'scope': 'LOCAL_HOLDINGS_ARE_OWNER_BOUNDED; CSV_IS_NOT',
                          'content_equality': 'NOT_ASSERTED_EMPTY_NULL_AMBIGUITY'}
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('capture', 'backup', 'manifest', 'old-capture', 'baseline-capture', 'csv-dir', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    live = json.load(gzip.open(args.capture, 'rt', encoding='utf-8'))
    if live['schema_version'] != 'phase2a-capture-v1' or live['transaction_read_only'] != 'on':
        raise ValueError('Unsupported or non-read-only capture')
    manifest = json.loads(args.manifest.read_text())
    if file_hash(args.backup) != manifest['source_sha256']:
        raise ValueError('Backup differs from frozen manifest')
    if manifest['owner_digest'] != live['owner_digest']:
        raise ValueError('Source owner mismatch')
    backup = {t: [] for t in SELECTED}
    seen = set()
    for _, table, _, row in copy_blocks(args.backup):
        if table in backup and row is None:
            seen.add(table)
        if table in backup and row is not None:
            backup[table].append(row)
    # Fail closed if any selected source table is missing, rather than assuming empty.
    if seen != set(SELECTED):
        raise ValueError('Selected table absent from backup; not an empty relation')
    local = live['tables']
    source_summary, missing = archive_sources(args.output/'evidence.sqlite',
        {'supabase_backup': backup, 'local': local}, manifest, live['owner_digest'])
    result = reconstruct(local, live['owner_digest'])
    exact_owners = {r['owner_chat_id'] for r in local['broker_fills'] if owner_scope(r, live['owner_digest']) == 'EXACT_OWNER'}
    if len(exact_owners) != 1:
        raise ValueError('Exact capture owner cannot be checked')
    owner_token = hashlib.sha256(next(iter(exact_owners)).encode()).hexdigest()[:16]
    historical = {k: compare_capture(v, local, live['owner_digest'], owner_token) for k, v in
                  [('qeef_original', args.old_capture), ('baseline', args.baseline_capture)]}
    public = {'schema_version': 'phase2a-reconstruction-v1', 'base_main_sha': '1687cdcce50895a9dfacb6dae04188fd8d8d5626',
              'source_as_of': live['captured_at'], 'transaction_read_only': live['transaction_read_only'],
              'input_sha256': {k: file_hash(v) for k, v in [('capture', args.capture), ('supabase_backup', args.backup), ('manifest', args.manifest)]},
              'reconstruction': result['summary'], 'forensic_checks': forensic_checks(local, live['owner_digest']),
              'backup_quantity_fill_checks': reconcile_observed_quantities(backup, live['owner_digest'], [], [])[0],
              'source_versions': source_summary,
              'historical_captures': historical, 'csv_key_checks': compare_csv(args.csv_dir, local),
              'missing_attribution_rows_recovered': len(missing),
              'published_scope': 'AGGREGATES_ONLY; RAW_RECORDS_AND_ACCOUNT_BALANCES_PRIVATE'}
    write_new(args.output/'reconstruction-private.json', result)
    write_new(args.output/'missing-attributions-private.json', missing)
    with (args.output/'daily-observed-nav-private.csv').open('x', encoding='utf-8', newline='') as f:
        fields = ['local_day', 'reference', 'at', 'reported_invested_ars', 'reported_cash_ars', 'reported_nav_ars',
                  'valuation_residual_ars', 'status', 'economic_pnl_net']
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for day, row in result['daily_last_observation'].items():
            writer.writerow({'local_day': day, **row})
    public['private_output_sha256'] = {p.name: file_hash(p) for p in args.output.iterdir() if p.is_file()}
    root = Path(__file__).resolve().parents[1]
    public['code_sha256_lf_utf8'] = {name: hashlib.sha256((root/name).read_text(encoding='utf-8').replace('\r\n', '\n').encode()).hexdigest()
        for name in ('scripts/capture_phase2a.py', 'scripts/reconstruct_phase2a.py', 'tests/test_phase2a_reconstruction.py')}
    public['code_hash_contract'] = 'UTF-8 decoded, CRLF normalized to LF; separate from immutable raw artifact hashes'
    write_new(args.output/'summary.json', public)
    print(json.dumps(public['reconstruction'], sort_keys=True))


if __name__ == '__main__':
    main()
