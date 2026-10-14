"""Reproducible aggregate probes of captured real evidence; no DB writes."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.analysis.qeef.historical import adapt_history, obj
from src.analysis.viability_audit import _max_drawdown
from src.analysis.dcl.statistical_auditor import _bootstrap_mean_ci
from src.decision_lab.models import canonical


def probes(capture):
    cases = adapt_history(capture)
    groups = defaultdict(list)
    for row in cases:
        key = (row['source'], row['metric_scope'], row['source_status'], row['action'], row['evidence_quality'], row['regime'])
        if row['outcomes']['5']['legacy_recorded_directional_return'] is not None:
            groups[key].append(row)
    distributions = []
    for key, group in sorted(groups.items()):
        # Reproduce Viability's sequence calculation on the same real returns,
        # with its default 75 bps research deduction. Never call it account risk.
        values = [r['outcomes']['5']['legacy_recorded_directional_return']-.0075 for r in group]
        original = _max_drawdown(values)
        rng = np.random.default_rng(42)
        permutations = [_max_drawdown(rng.permutation(values).tolist()) for _ in range(100)]
        overlapped = 0
        end = None
        for row in sorted(group, key=lambda r: r['decision_at']):
            start = datetime.fromisoformat(row['decision_at'])
            if end and start <= end:
                overlapped += 1
            end = max(end or start, start+timedelta(days=5))
        distributions.append({'cohort': list(key), 'n': len(values), 'recorded_mean_less_75bps': float(np.mean(values)),
            'legacy_sequence_drawdown_original': original, 'permuted_min': min(permutations), 'permuted_max': max(permutations),
            'nominal_5_calendar_day_overlaps': overlapped,
            'account_drawdown': None, 'interpretation': 'SEQUENCE_DIAGNOSTIC_NOT_A_CAPITAL_CURVE'})
    candles = capture['tables'].get('market_candle_observations', [])
    missingness = Counter(v for r in candles for v in obj(r['missingness']))
    # Probe a deterministic real record (first chronological case), not a chosen winner.
    first = next((c for c in cases if c['outcomes']['5']['legacy_recorded_directional_return'] is not None), None)
    v = first['outcomes']['5']['legacy_recorded_directional_return'] if first else None
    return {'schema_version': 'qeef-empirical-probes-v1', 'source_as_of': capture['captured_at'],
        'same_rows_sequence_risk_probe': distributions,
        'singleton_ci_probe': {'source_reference': first['source_reference'] if first else None,
            'legacy_interval': _bootstrap_mean_ci([v]) if v is not None else None,
            'qeef_interval': None, 'qeef_reason': 'N1_NO_INFERENCE'},
        'new_candle_missingness': dict(sorted(missingness.items())),
        'legacy_unscoped_counts': {k: v['legacy_unscoped_n'] for k,v in capture['profiles'].items() if 'legacy_unscoped_n' in v},
        'raw_snapshot_scope': 'OWNER_LINKED_METADATA_ONLY_NO_RAW_HTML_OR_ACCOUNT_PAYLOAD',
        'old_historical_numbers_revalidated': False,
        'reason': 'ORIGINAL_HISTORICAL_COHORT_MANIFESTS_NOT_IDENTIFIED_AS_COMPARABLE'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raw = Path(args.capture).read_bytes()
    result = probes(json.loads(raw))
    result['capture_sha256'] = hashlib.sha256(raw).hexdigest()
    path = Path(args.output)
    text = canonical(result)+'\n'
    if path.exists() and path.read_text(encoding='utf-8') != text:
        raise FileExistsError('immutable probe conflict')
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open('x', encoding='utf-8', newline='\n') as f:
            f.write(text)
    print(json.dumps({'output': str(path), 'n_cohorts': len(result['same_rows_sequence_risk_probe']),
        'n_order_sensitive': sum(r['permuted_min'] != r['permuted_max'] for r in result['same_rows_sequence_risk_probe']),
        'missingness': result['new_candle_missingness']}))


if __name__ == '__main__':
    main()
