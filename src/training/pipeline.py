from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .contracts import SCHEMA_VERSION, TrainingManifest
from .dataset import build_training_dataset, temporal_split
from .gates import PromotionGateConfig, evaluate_promotion_gate
from .preflight import run_preflight


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            text = raw.strip()
            if not text:
                continue
            value = json.loads(text)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_no}: expected JSON object")
            rows.append(value)
    return rows


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def build_artifacts(
    *,
    agent_runs: Sequence[Mapping[str, Any]],
    agent_steps: Sequence[Mapping[str, Any]],
    jev_assessments: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    output_dir: str | Path,
    validation_fraction: float = 0.20,
) -> TrainingManifest:
    examples, exclusions = build_training_dataset(
        agent_runs=agent_runs,
        agent_steps=agent_steps,
        jev_assessments=jev_assessments,
        outcomes=outcomes,
        require_outcome=True,
    )
    train, validation = temporal_split(
        examples, validation_fraction=validation_fraction
    )

    out = Path(output_dir)
    write_jsonl(out / "dataset.jsonl", [row.to_dict() for row in examples])
    write_jsonl(out / "train.jsonl", [row.to_dict() for row in train])
    write_jsonl(out / "validation.jsonl", [row.to_dict() for row in validation])

    versions = Counter(row.jev_version for row in examples)
    manifest = TrainingManifest(
        created_at=datetime.now(timezone.utc),
        schema_version=SCHEMA_VERSION,
        total_runs=len(agent_runs),
        eligible_examples=len(examples),
        excluded_examples=sum(exclusions.values()),
        exclusion_reasons=exclusions,
        train_examples=len(train),
        validation_examples=len(validation),
        earliest_decision_at=(examples[0].decided_at.isoformat() if examples else None),
        latest_decision_at=(examples[-1].decided_at.isoformat() if examples else None),
        jev_version_counts=dict(sorted(versions.items())),
        notes=(
            "Outcomes are labels only and never copied into pre-decision features.",
            "Raw tool observations and raw user goals are not persisted in training artifacts.",
            "No model is promoted automatically by this harness.",
        ),
    )
    write_json(out / "manifest.json", manifest.to_dict())
    return manifest


def run_pipeline_from_files(
    *,
    agent_runs_path: str | Path,
    agent_steps_path: str | Path,
    jev_assessments_path: str | Path,
    outcomes_path: str | Path,
    output_dir: str | Path,
    jev_module: str | None = None,
    validation_fraction: float = 0.20,
    champion_metrics_path: str | Path | None = None,
    challenger_metrics_path: str | Path | None = None,
) -> dict[str, Any]:
    preflight = run_preflight(jev_module=jev_module)
    if not preflight.ready:
        return {
            "status": "BLOCKED",
            "stage": "preflight",
            "preflight": preflight.to_dict(),
        }

    manifest = build_artifacts(
        agent_runs=load_jsonl(agent_runs_path),
        agent_steps=load_jsonl(agent_steps_path),
        jev_assessments=load_jsonl(jev_assessments_path),
        outcomes=load_jsonl(outcomes_path),
        output_dir=output_dir,
        validation_fraction=validation_fraction,
    )

    receipt: dict[str, Any] = {
        "status": "DATASET_READY",
        "preflight": preflight.to_dict(),
        "manifest": manifest.to_dict(),
        "promotion": {
            "evaluated": False,
            "eligible_for_review": False,
            "auto_promoted": False,
        },
    }

    if champion_metrics_path and challenger_metrics_path:
        champion = json.loads(Path(champion_metrics_path).read_text(encoding="utf-8"))
        challenger = json.loads(Path(challenger_metrics_path).read_text(encoding="utf-8"))
        gate = evaluate_promotion_gate(
            champion=champion,
            challenger=challenger,
            total_samples=manifest.eligible_examples,
            holdout_samples=manifest.validation_examples,
            temporal_holdout=manifest.validation_examples > 0,
            config=PromotionGateConfig(),
        )
        receipt["promotion"] = {
            "evaluated": True,
            **gate.to_dict(),
        }
        receipt["status"] = (
            "CHALLENGER_ELIGIBLE_FOR_REVIEW"
            if gate.eligible_for_review
            else "CHALLENGER_BLOCKED"
        )

    write_json(Path(output_dir) / "receipt.json", receipt)
    return receipt
