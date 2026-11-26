"""Synthetic, isolated accounting evidence; no DB, broker, or app bootstrap."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from reconstruct_phase2a import (D, archive_sources, broker_ticket, compare_capture, digest,
                                 forensic_checks, num, reconstruct, stamp, unique)

OWNER = digest('123')


def dataset():
    position = dict(id='1', snapshot_id='s1', ticker='AAA', currency='ARS', quantity='2',
                    current_price='10', market_value='20', avg_cost='8')
    snap = dict(snapshot_id='s1', owner_chat_id='123', scraped_at='2026-10-01T15:00:00+00:00',
                created_at='2026-10-01T15:00:01+00:00', total_value_ars='20', cash_ars='5')
    raw = dict(snap, positions=[position])
    fill = dict(id='f1', owner_chat_id='123', source='broker', external_fill_id='ticket1',
                ticker='AAA', side='BUY', quantity='2', avg_fill_price='10', gross_amount_ars='21',
                fees_ars=None, raw_payload=json.dumps({'amount': -21, 'price': 10, 'id_account': 9}),
                executed_at='2026-10-01T14:00:00+00:00', executed_at_precision='observed_after', decision_log_id='d1')
    movement = dict(id='m1', source='broker', external_movement_id='ticket1', ticker='AAA',
                    currency='ARS', movement_type='BUY', quantity='-2', amount='21', balance=None,
                    executed_at=fill['executed_at'], executed_at_precision='date_only',
                    raw_payload=json.dumps({'id_account': 9, 'id_ticket': 'ticket1'}))
    return {'portfolio_snapshots': [snap], 'positions': [position],
            'raw_snapshots': [dict(snapshot_id='s1', payload=json.dumps(raw))],
            'broker_fills': [fill], 'broker_movements': [movement],
            'decision_log': [dict(id='d1', owner_chat_id=None)], 'position_hold_observations': []}


def test_amount_gap_never_becomes_fee_or_profit():
    result = reconstruct(dataset(), OWNER)
    assert result['fills'][0]['amount_minus_qty_price'] == '1'
    assert result['fills'][0]['fees_ars'] is None
    assert result['summary']['economic_pnl_net'] is None
    assert result['summary']['realized_pnl_gross'] is None
    assert result['summary']['fees_total'] is None


def test_owner_null_decision_not_reassigned_by_link():
    result = reconstruct(dataset(), OWNER)
    assert result['summary']['fill_checks']['decision_OWNER_NULL'] == 1


def test_other_owner_fill_excluded():
    data = dataset()
    other = dict(data['broker_fills'][0], id='f2', owner_chat_id='999')
    data['broker_fills'].append(other)
    result = reconstruct(data, OWNER)
    assert len(result['fills']) == 1
    assert result['movement_quarantine'][0]['ownership'] == 'ACCOUNT_CONFLICT'


def test_account_anchor_is_candidate_not_owner_cash():
    result = reconstruct(dataset(), OWNER)
    row = result['movement_quarantine'][0]
    assert row['ownership'] == 'ACCOUNT_ANCHORED_CANDIDATE'
    assert row['included_in_owner_cash_ledger'] is False


def test_sign_convention_compared_by_magnitude_separately():
    counts = reconstruct(dataset(), OWNER)['summary']['fill_checks']
    assert counts['movement_abs_quantity_match'] == counts['movement_signed_quantity_diff'] == 1


def test_missing_cash_not_zero():
    data = dataset()
    data['portfolio_snapshots'][0]['cash_ars'] = None
    result = reconstruct(data, OWNER)
    assert result['states'][0]['reported_nav_ars'] is None
    assert result['summary']['snapshot_checks']['missing_valuation_components'] == 1


def test_multi_currency_not_silently_added_to_ars():
    data = dataset()
    data['positions'][0]['currency'] = 'USD'
    assert reconstruct(data, OWNER)['states'][0]['calculated_invested_ars'] is None


def test_observed_nav_is_not_return():
    result = reconstruct(dataset(), OWNER)
    assert result['states'][0]['reported_nav_ars'] == '25'
    assert result['states'][0]['economic_pnl_net'] is None
    assert result['states'][0]['status'] == 'PARTIAL'


def test_snapshot_duplicate_fails_closed():
    data = dataset()
    data['portfolio_snapshots'].append(deepcopy(data['portfolio_snapshots'][0]))
    with pytest.raises(ValueError, match='Ambiguous duplicate'):
        reconstruct(data, OWNER)


def test_position_duplicate_fails_closed():
    data = dataset()
    data['positions'].append(dict(data['positions'][0], id='different'))
    with pytest.raises(ValueError, match='Ambiguous duplicate'):
        reconstruct(data, OWNER)


def test_ambiguous_movement_is_not_matched():
    data = dataset()
    data['broker_movements'].append(dict(data['broker_movements'][0], id='m2'))
    assert reconstruct(data, OWNER)['fills'][0]['movement_reference'] is None


def test_timestamp_hold_is_candidate_not_repaired():
    data = dataset()
    original = data['portfolio_snapshots'][0]['scraped_at']
    data['position_hold_observations'] = [dict(id='h1', owner_chat_id='123', portfolio_snapshot_id=original)]
    link = reconstruct(data, OWNER)['hold_links'][0]
    assert link['status'] == 'RECOVERABLE'
    assert data['position_hold_observations'][0]['portfolio_snapshot_id'] == original


def test_legacy_owner_hold_not_included():
    data = dataset()
    data['position_hold_observations'] = [dict(id='h1', owner_chat_id=None, portfolio_snapshot_id='s1')]
    assert reconstruct(data, OWNER)['hold_links'] == []


@pytest.mark.parametrize('value', [None, '', 0, '0'])
def test_empty_ticket_is_not_shared_identity(value):
    assert broker_ticket({'id_ticket': value}) is None


def test_ticket_cash_security_legs_not_two_trades():
    data = dataset()
    data['broker_movements'].append(dict(data['broker_movements'][0], id='cash', external_movement_id='cash1',
        raw_payload=json.dumps({'id_ticket': 'ticket1', 'id_cash_movement': 'cash1'})))
    checks = forensic_checks(data, OWNER)
    assert checks['cash_trade_ticket_pairs'] == 1
    assert checks['cash_channel_rows'] == 1
    assert checks['cash_reconciliation_status'] == 'NOT_IDENTIFIABLE'


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity'])
def test_nonfinite_rejected(value):
    with pytest.raises(ValueError):
        num(value)


def test_naive_timestamp_rejected():
    with pytest.raises(ValueError, match='timezone'):
        stamp('2026-10-01T12:00:00')


def test_historical_owner_mismatch_rejected(tmp_path):
    path = tmp_path/'capture.json'
    path.write_text(json.dumps({'owner_token': 'different'}))
    with pytest.raises(ValueError, match='owner mismatch'):
        compare_capture(path, dataset(), OWNER, 'expected')


def test_versions_retained_and_existing_archive_refused(tmp_path):
    path = tmp_path/'evidence.sqlite'
    spec = {'keys': ['id'], 'columns': ['id', 'amount', 'owner_chat_id'],
            'types': {'id': 'bigint', 'amount': 'numeric', 'owner_chat_id': 'bigint'}}
    a = dict(id='1', amount='10', owner_chat_id=None)
    b = dict(a, amount='11')
    summary, _ = archive_sources(path, {'supabase_backup': {'broker_fills': [a]},
                                       'local': {'broker_fills': [b]}}, {'tables': {'broker_fills': spec}}, OWNER)
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(DISTINCT content_hash) FROM evidence').fetchone()[0] == 2
        assert db.execute('SELECT DISTINCT scope FROM evidence').fetchall() == [('OWNER_NULL',)]
    assert next(r for r in summary['comparison'] if r['table'] == 'broker_fills')['changed'] == 1
    with pytest.raises(FileExistsError):
        archive_sources(path, {}, {}, OWNER)


def test_decimal_precision_not_float_rounded():
    value = '0.5300000000000000266453525910037569701671600341796875'
    assert num(value) - D('.53') == D('0.0000000000000000266453525910037569701671600341796875')


def test_order_movement_collision_is_retained_not_merged():
    data = dataset()
    data['broker_fills'].append(dict(data['broker_fills'][0], id='f2', external_fill_id='order2',
        raw_payload=json.dumps({'idOrder': 'order2', 'amount': -21}), avg_fill_price='10.5'))
    result = reconstruct(data, OWNER)
    assert len(result['fills']) == 2
    assert result['summary']['quantity_fill_checks']['overlap_order_movement_pairs'] == 1
    assert result['fill_overlap_candidates'][0]['deduplicated'] is False
    assert result['fill_overlap_candidates'][0]['canonical_execution_id'] is None
