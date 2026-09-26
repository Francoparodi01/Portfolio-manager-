"""python -m src.analysis.analytics_v2.cli build|report|validate (offline)."""
import argparse
import json
import subprocess
from pathlib import Path

from .models import AnalyticsPolicy, Dataset, digest
from .reporting import build, persist, report, validate


def code_identity():
    root = Path(__file__).resolve().parents[3]
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True).stdout.strip()
    files = {p.name: p.read_text(encoding="utf-8") for p in sorted(Path(__file__).parent.glob("*.py"))}
    return commit, digest(files)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Quantia Analytics v2: offline audit only")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("build")
    create.add_argument("--input", type=Path, required=True)
    create.add_argument("--policy", type=Path, required=True)
    create.add_argument("--output", type=Path, default=Path("outputs/analytics"))
    for command in ("report", "validate"):
        commands.add_parser(command).add_argument("run", type=Path)
    args = parser.parse_args(argv)
    if args.command == "build":
        dataset = Dataset.model_validate_json(args.input.read_text(encoding="utf-8"))
        policy = AnalyticsPolicy.model_validate_json(args.policy.read_text(encoding="utf-8"))
        commit, code_hash = code_identity()
        print(persist(build(dataset, policy, code_commit=commit, code_hash=code_hash), args.output))
    elif args.command == "report":
        print(report(args.run))
    else:
        bundle = validate(args.run)
        _, current_code_hash = code_identity()
        if current_code_hash != bundle["identity"]["code_hash"]:
            raise ValueError("code hash differs from frozen run")
        print(json.dumps({"status": "VALID", "analysis_run_id": bundle["analysis_run_id"]}))


if __name__ == "__main__":
    main()
