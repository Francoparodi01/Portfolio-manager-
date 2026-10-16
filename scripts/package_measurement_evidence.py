"""Package reviewed aggregate receipts; never copies private rows or credentials."""
import argparse
import json
from pathlib import Path

from measurement_baseline import file_hash,write_new


def package(root,output):
    root=Path(root)
    def read(name):return json.loads((root/name).read_text())
    catalog=read('catalog-verified.json');catalog.pop('owner_digest')
    access=read('source-access.json')
    for source in access['sources']:
        if 'catalog' in source:source['catalog'].pop('owner_digest',None)
    comparison=read('comparison/reconciliation.json')
    offline=read('offline-audit-final.json')
    files=['catalog-verified.json','source-access.json','comparison/reconciliation.json',
           'backup/manifest.json','backup/fingerprints.jsonl.gz','live-fingerprints.jsonl.gz',
           'live-owner-20261016.json','quality-checks-v2.json','changed-fields-summary.json',
           'changed-field-digests.json','offline-audit-final.json',
           'qeef-original-replay.json','qeef-current-replay.json']
    source_root=Path(__file__).resolve().parents[1]
    scripts=['scripts/measurement_baseline.py','scripts/measurement_checks.sql',
             'scripts/probe_measurement_sources.py','scripts/audit_measurement_offline.py',
             'scripts/package_measurement_evidence.py','scripts/capture_qeef.py',
             'scripts/audit_qeef_phase2.py','tests/test_measurement_baseline.py']
    result={'schema_version':'measurement-baseline-v1-evidence',
            'base_main_sha':'071ac22b52b8e9c7955f76cdf549421c50a6addd',
            'privacy':'Aggregates, schema, dates and digests only; no account IDs, balances, raw rows or credentials.',
            'source_limits':['Backup origin project and extraction clock are not authenticated.',
                             'Supabase live failed before reading tables.',
                             'Broker direct statements were not obtained; persisted movement rows are secondary evidence.',
                             'Multiple immutable source cuts; see their individual timestamps.',
                             'Cross-table totals measure record coverage, never independent trades.'],
            'private_artifact_sha256':{name:file_hash(root/name) for name in files},
            'code_sha256':{name:file_hash(source_root/name) for name in scripts},
            'catalog':catalog,'source_access':access,'backup_vs_local':comparison,
            'quality_checks':read('quality-checks-v2.json'),'changed_fields':read('changed-fields-summary.json'),
            'offline':offline,'qeef_original':read('qeef-original-replay.json'),'qeef_current':read('qeef-current-replay.json')}
    published=json.loads((source_root/'docs/qeef/evidence/phase2-readiness.json').read_text())
    result['original_qeef_exact_replay_match']=result['qeef_original']==published
    assert result['original_qeef_exact_replay_match']
    assert not offline['schema_check']['type_differences']
    assert len(comparison['tables'])==59
    assert sum(v['backup']['n'] for v in comparison['tables'].values())==2232721
    assert sum(v['backup_missing_local'] for v in comparison['tables'].values())==5
    assert all(not v['changed_scope'] for v in comparison['tables'].values())
    write_new(output,result)
    print(json.dumps({'path':str(output),'sha256':file_hash(output)}))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--private-dir',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();package(a.private_dir,a.output)
