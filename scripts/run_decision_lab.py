#!/usr/bin/env python
"""Decision Lab CLI. Compute is offline; export is guarded read-only."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.decision_lab.models import (
    Dataset,
    Experiment,
    StrategySpec,
    CostModel,
    canonical,
    digest,
)
from src.decision_lab.strategies import current_spec
from src.decision_lab.runner import replay, rolling_windows
from src.decision_lab.persistence import (
    write_artifacts,
    validate_artifacts,
    SCHEMA,
    persist_run,
)


def stamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            "timestamp must include timezone, e.g. 2026-03-03T17:00:00-03:00"
        )
    return parsed


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "command",
        choices=[
            "export",
            "run",
            "validate",
            "report",
            "migrate",
            "windows",
            "register",
        ],
    )
    p.add_argument("--dataset")
    p.add_argument("--export-manifest")
    p.add_argument("--folder")
    p.add_argument("--output", default="outputs/decision-lab")
    p.add_argument("--owner", type=int)
    p.add_argument("--from", dest="start", type=stamp)
    p.add_argument("--to", dest="end", type=stamp)
    p.add_argument("--as-of", type=stamp)
    p.add_argument("--evaluated-as-of", type=stamp)
    p.add_argument("--plan-id")
    p.add_argument("--portfolio-id")
    p.add_argument("--experiment")
    p.add_argument("--strategies", nargs="+")
    p.add_argument(
        "--adapter",
        choices=["recorded_plan_v1", "hold_v1", "quantia_core_v1"],
        default="recorded_plan_v1",
    )
    p.add_argument("--strategy-version")
    p.add_argument("--config-hash", default="recorded-source-plan")
    p.add_argument(
        "--mode",
        choices=[
            "RECORDED_PLAN_EVALUATION",
            "CURRENT_POLICY_ON_HISTORICAL_DATA",
            "HISTORICAL_POLICY_REPLAY",
        ],
        default="RECORDED_PLAN_EVALUATION",
    )
    p.add_argument(
        "--operation",
        choices=[
            "single_episode",
            "date_range",
            "walk_forward",
            "strategy_compare",
            "backfill",
        ],
        default="walk_forward",
    )
    p.add_argument("--cost-model")
    p.add_argument("--legacy-single-owner", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-persist", action="store_true")
    p.add_argument("--persist-db", action="store_true")
    p.add_argument("--recompute", action="store_true")
    p.add_argument("--train-sessions", type=int, default=60)
    p.add_argument("--validation-sessions", type=int, default=20)
    p.add_argument("--test-sessions", type=int, default=20)
    p.add_argument("--step-sessions", type=int, default=20)
    p.add_argument("--expanding", action="store_true")
    a = p.parse_args(argv)
    if a.no_persist and a.persist_db:
        p.error("--no-persist conflicts with --persist-db")
    if a.command == "validate":
        if not a.folder:
            p.error("--folder required")
        print(canonical(validate_artifacts(a.folder, recompute=a.recompute)))
        return
    if a.command == "report":
        validate_artifacts(a.folder)
        print((Path(a.folder) / "report.md").read_text(encoding="utf-8"))
        return
    if a.command == "migrate":
        if a.dry_run or a.no_persist:
            print(SCHEMA)
            return

        async def migrate():
            import asyncpg

            conn = await asyncpg.connect(os.environ["DATABASE_URL"])
            try:
                async with conn.transaction():
                    await conn.execute(SCHEMA)
            finally:
                await conn.close()

        asyncio.run(migrate())
        print("decision-lab additive schema installed")
        return
    if a.command == "register":
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        spec = current_spec(
            a.adapter,
            registered_at=datetime.now(timezone.utc),
            code_version=commit,
            config_hash=a.config_hash,
            strategy_version=a.strategy_version,
        )
        text = canonical(spec)
        if a.dry_run or a.no_persist:
            print(text)
        else:
            path = Path(a.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as f:
                f.write(text)
            print(str(path))
        return
    if a.command == "export":
        if not all([a.owner, a.start, a.end, a.evaluated_as_of]):
            p.error("export requires --owner --from --to --evaluated-as-of")
        if a.dry_run:
            print(
                canonical(
                    {
                        "operation": "read-only bulk DB export",
                        "owner": a.owner,
                        "from": a.start,
                        "to": a.end,
                        "cutoff": a.evaluated_as_of,
                        "legacy_owner_inference": a.legacy_single_owner,
                    }
                )
            )
            return
        from src.decision_lab.exporter import export_legacy

        dataset, manifest = asyncio.run(
            export_legacy(
                os.environ["DATABASE_URL"],
                owner=a.owner,
                start=a.start,
                end=a.end,
                evaluated_as_of=a.evaluated_as_of,
                legacy_single_owner=a.legacy_single_owner,
            )
        )
        if not a.no_persist:
            folder = Path(a.output) / manifest["dataset_hash"]
            folder.mkdir(parents=True, exist_ok=True)
            for name, value in [("dataset.json", dataset), ("export.json", manifest)]:
                path = folder / name
                text = canonical(value)
                if path.exists() and path.read_text(encoding="utf-8") != text:
                    raise ValueError("immutable export conflict")
                if not path.exists():
                    path.write_text(text, encoding="utf-8")
            print(str(folder))
        print(canonical({k: v for k, v in manifest.items() if k != "requests"}))
        return
    if not a.dataset:
        p.error("--dataset required")
    data = Dataset.model_validate_json(Path(a.dataset).read_text(encoding="utf-8"))
    if a.command == "windows":
        print(
            canonical(
                rolling_windows(
                    sorted(data.sessions, key=lambda s: s.open_at),
                    train_sessions=a.train_sessions,
                    validation_sessions=a.validation_sessions,
                    test_sessions=a.test_sessions,
                    step_sessions=a.step_sessions,
                    expanding=a.expanding,
                )
            )
        )
        return
    if not all([a.owner, a.evaluated_as_of, a.experiment, a.strategies]):
        p.error("run requires --owner --evaluated-as-of --experiment --strategies")
    experiment = Experiment.model_validate_json(
        Path(a.experiment).read_text(encoding="utf-8")
    )
    strategies = [
        StrategySpec.model_validate_json(Path(path).read_text(encoding="utf-8"))
        for path in a.strategies
    ]
    costs = (
        CostModel.model_validate_json(Path(a.cost_model).read_text(encoding="utf-8"))
        if a.cost_model
        else CostModel()
    )
    requests = (
        json.loads(Path(a.export_manifest).read_text(encoding="utf-8"))["requests"]
        if a.export_manifest
        else []
    )
    if a.as_of:
        requests = [
            {
                "as_of": a.as_of.isoformat(),
                "plan_id": a.plan_id,
                "portfolio_id": a.portfolio_id,
            }
        ]
    requests = [
        r
        for r in requests
        if (not a.start or stamp(r["as_of"]) >= a.start)
        and (not a.end or stamp(r["as_of"]) <= a.end)
    ]
    if not requests:
        p.error("no explicit opportunities; provide --export-manifest or --as-of")
    if a.dry_run:
        print(
            canonical(
                {
                    "operation": a.operation,
                    "opportunities": len(requests),
                    "strategies": [s.strategy_version for s in strategies],
                    "cost_model": costs.model_dump(mode="json"),
                    "persist_db": a.persist_db,
                }
            )
        )
        return
    run, frozen = replay(
        data,
        requests=requests,
        owner=a.owner,
        strategies=strategies,
        mode=a.mode,
        evaluated_as_of=a.evaluated_as_of,
        experiment=experiment,
        costs=costs,
        operation=a.operation,
    )
    if not a.no_persist:
        print(str(write_artifacts(a.output, run, frozen)))
    if a.persist_db:

        async def persist():
            import asyncpg

            conn = await asyncpg.connect(os.environ["DATABASE_URL"])
            try:
                await persist_run(conn, run)
            finally:
                await conn.close()

        asyncio.run(persist())
    print(
        canonical(
            {
                "run_id": run["replay_run_id"],
                "episodes": len(run["episodes"]),
                "failures": run["failures"],
                "metrics": [r for r in run["metrics"] if r["segment"] == "ALL"],
            }
        )
    )


if __name__ == "__main__":
    main()
