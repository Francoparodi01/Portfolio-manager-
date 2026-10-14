"""Offline QEEF research evaluation. Never connects to a broker or database."""
import argparse
from datetime import datetime
import hashlib
import json
from importlib.metadata import version
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.analysis.qeef.service import audit_capture, evaluate, write_report
from src.analysis.qeef.contracts import Benchmark
from src.decision_lab.models import Dataset, Episode, digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    audit = sub.add_parser('audit')
    audit.add_argument('--capture', required=True)
    run = sub.add_parser('evaluate')
    run.add_argument('--episode', required=True)
    run.add_argument('--dataset', required=True)
    run.add_argument('--benchmarks')
    run.add_argument('--as-of', required=True)
    run.add_argument('--horizons', nargs='+', type=int, default=[5, 10, 20])
    for p in (audit, run):
        p.add_argument('--output', required=True)
        p.add_argument('--code-version')
    args = parser.parse_args()
    revision = args.code_version or subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    # Record the implementation as well as HEAD, including pre-commit edits.
    files = sorted((ROOT/'src/analysis/qeef').glob('*.py')) + sorted((ROOT/'src/decision_lab').glob('*.py'))
    files += list((ROOT/'src/analysis/analytics_v2').glob('*.py')) + [ROOT/'src/analysis/date_block_statistics.py']
    files += [ROOT/'scripts/run_qeef.py', ROOT/'scripts/capture_qeef.py', ROOT/'scripts/validate_qeef_empirical.py']
    code_hash = digest({str(p.relative_to(ROOT)).replace('\\', '/'): hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
    code_version = revision+':source:'+code_hash
    if args.command == 'audit':
        raw = Path(args.capture).read_bytes()
        report = audit_capture(json.loads(raw), code_version=code_version, capture_digest=hashlib.sha256(raw).hexdigest())
    else:
        episode = Episode.model_validate_json(Path(args.episode).read_text(encoding='utf-8'))
        dataset = Dataset.model_validate_json(Path(args.dataset).read_text(encoding='utf-8'))
        benchmarks = tuple(Benchmark.model_validate(b) for b in json.loads(Path(args.benchmarks).read_text(encoding='utf-8'))) if args.benchmarks else ()
        report = evaluate(episode=episode, dataset=dataset, benchmarks=benchmarks, code_version=code_version,
                          evaluated_as_of=datetime.fromisoformat(args.as_of), horizons=tuple(args.horizons))
    report['runtime'] = {'python': sys.version.split()[0], 'dependencies': {p: version(p) for p in ('numpy', 'pandas', 'pydantic', 'asyncpg')}}
    report['evaluation_id'] = digest({k: v for k, v in report.items() if k != 'evaluation_id'})
    path = write_report(report, args.output)
    print(json.dumps({'output': str(path), 'evaluation_id': report['evaluation_id'], 'summary': report.get('summary')}))


if __name__ == '__main__':
    main()
