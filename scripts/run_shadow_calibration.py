"""Fit and persist the audit-only v3 calibration over shadow v2 evidence."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import replace
from uuid import UUID, uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.shadow_calibration import (
    MODEL_VERSION,
    SOURCE_MODEL_VERSION,
    SUPPORTED_HORIZONS,
    apply_calibration,
    calibration_gate_status,
    examples_from_rows,
    fit_calibration_model,
    walk_forward_metrics,
)
from src.analysis.shadow_calibration_store import ShadowCalibrationStore
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


async def run(args: argparse.Namespace) -> dict:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        pool = await db.get_pool()
        if pool is None:
            raise RuntimeError("database pool unavailable")
        store = ShadowCalibrationStore(pool)
        await store.ensure_schema()

        if args.latest_report:
            return {
                "ok": True,
                "model_version": MODEL_VERSION,
                "models": await store.latest_models(owner_chat_id=args.owner_chat_id),
                "prospective_metrics": await store.prospective_metrics(
                    owner_chat_id=args.owner_chat_id
                ),
                "boundary": _boundary(),
            }

        source_run = await store.latest_source_run(
            owner_chat_id=args.owner_chat_id,
            source_model_version=args.source_model_version,
        )
        if source_run is None:
            return {
                "ok": False,
                "error": "No source shadow run available",
                "source_model_version": args.source_model_version,
                "boundary": _boundary(),
            }

        train_cutoff = source_run["as_of_ts"]
        rows = await store.training_examples(
            owner_chat_id=args.owner_chat_id,
            source_model_version=args.source_model_version,
            train_cutoff=train_cutoff,
        )
        examples = examples_from_rows(rows)
        models = {}
        walk_forward = {}
        skipped = {}
        for horizon in SUPPORTED_HORIZONS:
            try:
                model = fit_calibration_model(
                    examples,
                    horizon_sessions=horizon,
                )
                walk_forward[horizon] = walk_forward_metrics(
                    examples,
                    horizon_sessions=horizon,
                )
                models[horizon] = replace(
                    model,
                    diagnostics={
                        **model.diagnostics,
                        "promotion_gate": calibration_gate_status(walk_forward[horizon]),
                    },
                )
            except ValueError as exc:
                skipped[horizon] = str(exc)

        if not models:
            return {
                "ok": False,
                "error": "Insufficient evidence for every supported horizon",
                "skipped": skipped,
                "boundary": _boundary(),
            }

        source_forecasts = await store.source_forecasts(
            source_run_id=UUID(str(source_run["run_id"]))
        )
        projections = []
        for source in source_forecasts:
            horizon = int(source["horizon_sessions"])
            model = models.get(horizon)
            if model is None:
                continue
            projection = apply_calibration(
                source_forecast_id=int(source["source_forecast_id"]),
                raw_expected_return=float(source["raw_expected_return"]),
                raw_probability_up=float(source["raw_probability_up"]),
                model=model,
            )
            projections.append((source, projection))

        stored_run_id, persisted, gate_changes = await store.save_calibration(
            calibration_run_id=uuid4(),
            owner_chat_id=args.owner_chat_id,
            source_run_id=UUID(str(source_run["run_id"])),
            source_model_version=args.source_model_version,
            train_cutoff=train_cutoff,
            models=models,
            walk_forward=walk_forward,
            projections=projections,
        )
        prospective = await store.prospective_metrics(owner_chat_id=args.owner_chat_id)
        return {
            "ok": True,
            "calibration_run_id": str(stored_run_id),
            "source_run_id": str(source_run["run_id"]),
            "source_as_of": source_run["as_of_ts"],
            "source_model_version": args.source_model_version,
            "model_version": MODEL_VERSION,
            "forecasts_persisted": persisted,
            "gate_changes": gate_changes,
            "models": [_model_payload(model, walk_forward[horizon]) for horizon, model in sorted(models.items())],
            "skipped": skipped,
            "prospective_metrics": prospective,
            "boundary": _boundary(),
        }
    finally:
        await db.close()


def _model_payload(model, walk_forward: dict) -> dict:
    return {
        "horizon_sessions": model.horizon_sessions,
        "sample_count": model.sample_count,
        "cohort_count": model.cohort_count,
        "train_start_ts": model.train_start_ts,
        "train_end_ts": model.train_end_ts,
        "parameters": model.to_parameters(),
        "fit_metrics": model.fit_metrics,
        "walk_forward_metrics": walk_forward,
        "diagnostics": model.diagnostics,
    }


def _boundary() -> dict:
    return {
        "reads": ["shadow_thesis_runs", "shadow_thesis_forecasts", "shadow_thesis_outcomes"],
        "writes": [
            "shadow_calibration_runs",
            "shadow_calibration_models",
            "shadow_calibrated_forecasts",
            "shadow_calibration_gate_state",
            "shadow_calibration_gate_events",
        ],
        "affects_analysis": False,
        "affects_execution": False,
        "visible_in_telegram": False,
    }


def render_report(payload: dict) -> str:
    if not payload.get("ok"):
        return f"Shadow calibration v3: {payload.get('error', 'unavailable')}"
    lines = [
        "Shadow calibration v3 (audit-only)",
        f"Model: {payload.get('model_version')}",
    ]
    for model in payload.get("models", []):
        horizon = model.get("horizon_sessions")
        params = _json_object(model.get("parameters"))
        fit = _json_object(model.get("fit_metrics"))
        walk = _json_object(model.get("walk_forward_metrics"))
        diagnostics = _json_object(model.get("diagnostics"))
        lines.append(
            f"{horizon}r: n={model.get('sample_count')} cohorts={model.get('cohort_count')} "
            f"prob_slope={params.get('probability_slope', 0):+.3f} "
            f"return_slope={params.get('return_slope', 0):+.3f} "
            f"inverted={bool(diagnostics.get('direction_inverted'))} "
            f"gate={diagnostics.get('promotion_gate', 'PENDING')}"
        )
        if fit:
            lines.append(
                f"  fit only: Brier {fit.get('raw_brier', 0):.4f} -> "
                f"{fit.get('calibrated_brier', 0):.4f}; MAE "
                f"{fit.get('raw_mae', 0):.2%} -> {fit.get('calibrated_mae', 0):.2%}"
            )
        lines.append(
            f"  walk-forward: {walk.get('status', 'PENDING')} "
            f"n={walk.get('samples', 0)} cohorts={walk.get('cohorts', 0)}"
        )
    if "forecasts_persisted" in payload:
        lines.append(f"Persisted forecasts: {payload['forecasts_persisted']}")
    lines.append("Telegram unchanged; no analysis or execution effect.")
    return "\n".join(lines)


def _json_object(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-chat-id", type=int, default=0)
    parser.add_argument("--source-model-version", default=SOURCE_MODEL_VERSION)
    parser.add_argument("--latest-report", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    parsed = _parse_args()
    result = asyncio.run(run(parsed))
    if parsed.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(render_report(result))
