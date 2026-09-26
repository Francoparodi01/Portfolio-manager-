"""Deterministic orchestration and immutable, inspectable artifact bundles."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path

from .episodes import build_episodes, unique_rows
from .gates import build_gates
from .matching import match_actions, paired_inference
from .metrics import cohort_metrics
from .models import AnalyticsPolicy, Dataset, canonical, digest
from .outcomes import build_outcomes
from .pnl import economic_pnl, execution_facts


def json_ready(value):
    # Models and intermediate datetime objects are normalized before hashing.
    from datetime import datetime
    import numpy as np
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    return value


def canonical_snapshot(dataset, cutoff):
    visible = dataset.as_of(cutoff).model_dump(mode="json")
    for key, rows in visible.items():
        if isinstance(rows, list):
            visible[key] = sorted({canonical(row): row for row in rows}.values(), key=canonical)
    return visible


def build(dataset, policy, *, code_commit, code_hash):
    snapshot = canonical_snapshot(dataset, policy.evaluated_as_of)
    data = Dataset.model_validate(snapshot)
    if policy.primary_hypotheses and any(r.decision_as_of <= policy.preregistered_at for r in data.recommendations):
        raise ValueError("preregistration must precede every evaluated decision")
    manifest_identity = {"input_hash": digest(snapshot), "policy_hash": digest(policy), "code_commit": code_commit, "code_hash": code_hash}
    run_id = digest(manifest_identity)[:24]
    episodes, links = build_episodes(data.recommendations, policy.dedup_policy_version)
    outcomes = build_outcomes(data, episodes, policy)
    # ALL is a union of Radar cohorts; never mix it with CORE or count it twice in a primary family.
    radar_rows = [r for r in outcomes if r["source_module"] == "RADAR" and r["cohort"] != "ALL"]
    if radar_rows and not any(r["source_module"] == "RADAR" and r["cohort"] == "ALL" for r in outcomes):
        outcomes += [{**row, "cohort": "ALL"} for row in radar_rows]
    metrics, inference, outliers, calibration = cohort_metrics(outcomes, policy)
    matches = match_actions(data, outcomes, policy)
    economics, equity = economic_pnl(data)
    for row in metrics:
        # A provided shadow curve must identify this exact cohort and horizon.
        curve = [p for p in equity if p["kind"] == "BOT_SHADOW" and p["account_id"] == row["account_id"]
                 and p["cohort"] == row["cohort"] and p["horizon_days"] == row["horizon_days"]]
        conventions = {(p["sizing_policy"], p["currency"]) for p in curve}
        if curve and len(conventions) == 1 and all("max_drawdown" in p for p in curve):
            row.update({"shadow_max_drawdown": curve[0]["max_drawdown"], "shadow_sharpe_hac": curve[0]["sharpe_hac"],
                        "shadow_sortino": curve[0]["sortino"], "shadow_volatility": curve[0]["volatility"],
                        "risk_reason": None, "sizing_policy": curve[0]["sizing_policy"]})
    gates, hypotheses = build_gates(metrics, policy)
    base = [r for r in metrics if r["cost_scenario"] == "RESEARCH_BASE"]
    tables = {
        "recommendations": [r.model_dump(mode="json") | {"source_row_hash": digest(r)} for r in unique_rows(data.recommendations, lambda r: r.recommendation_id)],
        "episodes": episodes, "episode_links": links, "outcomes": outcomes,
        "execution_facts": execution_facts(data, links), "economic_pnl": economics, "equity": equity,
        "cohort_metrics": base, "bot_vs_human": matches, "paired_inference": paired_inference(matches, policy),
        "radar": [r for r in base if r["source_module"] == "RADAR"],
        "swaps": [r for r in base if r["source_module"] == "SWAP"],
        "cost_sensitivity": metrics, "outlier_analysis": outliers,
        "maturity": [{k: v for k, v in r.items() if k.startswith("n_") or k in {"account_id", "source_module", "cohort", "horizon_days", "maturity_coverage"}} for r in base],
        "gates": gates, "inference": inference, "hypotheses": hypotheses, "score_calibration": calibration,
    }
    # Cohort/horizon positive fractions are descriptive, not independent tests.
    positive_fraction = []
    for scenario in policy.costs.scenarios:
        valid = [r for r in metrics if r["cost_scenario"] == scenario.name and r["ev_net"] is not None]
        positive_fraction.append({"cost_scenario": scenario.name, "cost_bps": scenario.bps,
                                  "n_cohort_horizons": len(valid), "positive_fraction": sum(r["ev_net"] > 0 for r in valid) / len(valid) if valid else None})
    tables["cost_positive_fraction"] = positive_fraction
    tables["signal_decay"] = [{k: r[k] for k in ("account_id", "source_module", "cohort", "horizon_days", "ev_net", "median_net", "win_rate", "mean_alpha", "median_alpha", "positive_alpha_fraction", "alpha_n", "ic", "n_mature")} for r in base]
    tables["return_ecdf"] = []
    from collections import Counter, defaultdict
    match_groups = defaultdict(list)
    for row in matches:
        if row["cost_scenario"] == "RESEARCH_BASE":
            match_groups[(row["account_id"], row["cohort"], row["horizon_days"])].append(row)
    tables["matched_metrics"] = []
    for (account, cohort, horizon), rows in sorted(match_groups.items()):
        valid = [r for r in rows if r["comparable"]]
        tables["matched_metrics"].append({"account_id": account, "cohort": cohort, "horizon_days": horizon,
                                         "n_opportunities": len(rows), "n_pairs": len(valid),
                                         "n_ambiguous": sum(r["ambiguous"] for r in rows),
                                         "n_unmatched": len(rows)-len(valid),
                                         **{key: sum(r[key] for r in valid)/len(valid) if valid else None for key in ("bot_cf_return", "human_cf_return", "paired_delta_bot_minus_human")}})
    swap_groups = defaultdict(list)
    for row in outliers:
        if row["source_module"] == "SWAP" and row["cost_scenario"] == "RESEARCH_BASE":
            swap_groups[(row["account_id"], row["original_instrument_id"], row["ticker"], row["horizon_days"])].append(row)
    tables["swap_pairs"] = [{"account_id": key[0], "original_instrument_id": key[1], "replacement_ticker": key[2],
                             "horizon_days": key[3], "n_episodes": len(rows),
                             "mean_net_alpha": sum(r["contribution_unit_notional"] for r in rows)/len(rows)} for key, rows in sorted(swap_groups.items())]
    ecdf_groups = defaultdict(Counter)
    for row in outliers:
        if row["cost_scenario"] == "RESEARCH_BASE":
            key = tuple(row[k] for k in ("account_id", "source_module", "cohort", "horizon_days"))
            ecdf_groups[key][row["contribution_unit_notional"]] += 1
    for key, counts in sorted(ecdf_groups.items()):
        n, cumulative = sum(counts.values()), 0
        for value, count in sorted(counts.items()):
            cumulative += count
            tables["return_ecdf"].append(dict(zip(("account_id", "source_module", "cohort", "horizon_days"), key)) | {"net_return": value, "ecdf": cumulative / n, "n": n})
    persistence = {}
    for row in outcomes:
        if row["cost_scenario"] == "RESEARCH_BASE":
            persistence.setdefault((row["episode_id"], row["cohort"]), []).append(row)
    tables["signal_persistence"] = []
    for (episode_id, cohort), rows in sorted(persistence.items()):
        rows.sort(key=lambda r: r["horizon_days"])
        valid = [r for r in rows if r["outcome_status"] == "MATURE" and r["gross_alpha"] is not None]
        initial = next((r for r in valid if r["gross_alpha"] != 0), None)
        flip = next((r["horizon_days"] for r in valid if initial and r["gross_alpha"] * initial["gross_alpha"] < 0), None)
        tables["signal_persistence"].append({"episode_id": episode_id, "cohort": cohort,
                                             "first_sign_flip_horizon": flip,
                                             "interpretation": "first_observed_horizon_not_continuous_event_time",
                                             "pending_horizons": [r["horizon_days"] for r in rows if r["outcome_status"] == "PENDING"],
                                             "reason_code": None if flip else "NO_OBSERVED_FLIP_OR_INSUFFICIENT_ALPHA"})
    common = {"analysis_run_id": run_id, "evaluated_as_of": policy.evaluated_as_of.isoformat(),
              "metric_policy_version": policy.metric_policy_version, "dedup_policy_version": policy.dedup_policy_version,
              "cost_policy_version": policy.costs.version, "gate_policy_version": policy.gates.version,
              "code_commit": code_commit, "code_hash": code_hash, "analysis_input_hash": manifest_identity["input_hash"]}
    tables = json_ready({name: [{**row, **common} for row in rows] for name, rows in tables.items()})
    summary = {**common, "dataset_label": data.label, "economic_status": [r["status"] for r in economics],
               "gate_counts": {s: sum(r["status"] == s for r in gates) for s in sorted({r["status"] for r in gates})},
               "recommendations": len(tables["recommendations"]), "episodes": len(episodes),
               "operational_authority": "AUDIT_ONLY", "legacy_reports_changed": False,
               "limitations": ["Economic inputs require attested fills, valuation and flow coverage.",
                               "n_effective is a conservative proxy, not proof of independence.",
                               "No runtime capital authority or retrospective M12A reinterpretation.",
                               "Timing, sizing and realized round-trip attribution require identifiable evidence.",
                               "No automatic import converts legacy rows into PIT evidence."]}
    return {"identity": manifest_identity, "analysis_run_id": run_id, "input": snapshot,
            "policy": policy.model_dump(mode="json"), "summary": summary, "tables": tables}


def bytes_hash(value):
    return hashlib.sha256(value).hexdigest()


def write_exact(path, content):
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(f"immutable output conflict: {path.name}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as file:
        file.write(content)


def csv_bytes(rows):
    stream = io.StringIO(newline="")
    fields = sorted({key for row in rows for key in row}) or ["status", "reason_code"]
    writer = csv.DictWriter(stream, fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: canonical(v) if isinstance(v, (list, dict)) else v for k, v in row.items()})
    return stream.getvalue().encode("utf-8")


def persist(bundle, output_root):
    folder = Path(output_root) / bundle["analysis_run_id"]
    if (folder / "manifest.json").exists():
        validate(folder, replay=False)
    artifacts = {"derived.json": canonical(bundle).encode(), "summary.json": canonical(bundle["summary"]).encode()}
    artifacts.update({f"tables/{name}.csv": csv_bytes(rows) for name, rows in bundle["tables"].items()})
    for name, content in artifacts.items():
        write_exact(folder / name, content)
    update_manifest(folder, bundle)
    return folder


def update_manifest(folder, bundle):
    files = {p.relative_to(folder).as_posix(): bytes_hash(p.read_bytes()) for p in sorted(folder.rglob("*")) if p.is_file() and p.name != "manifest.json"}
    manifest = {**bundle["identity"], **bundle["summary"], "policy": bundle["policy"],
                "row_counts": {name: len(rows) for name, rows in bundle["tables"].items()}, "output_hashes": files}
    (folder / "manifest.json").write_text(canonical(manifest), encoding="utf-8")


def validate(folder, *, replay=True):
    folder = Path(folder).resolve()
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    actual = {p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file() and p.name != "manifest.json"}
    if actual != set(manifest["output_hashes"]):
        raise ValueError("manifest file inventory mismatch")
    for name, expected in manifest["output_hashes"].items():
        path = (folder / name).resolve()
        if not path.is_relative_to(folder) or bytes_hash(path.read_bytes()) != expected:
            raise ValueError(f"output hash mismatch: {name}")
    bundle = json.loads((folder / "derived.json").read_text(encoding="utf-8"))
    expected_fields = {**bundle["identity"], **bundle["summary"], "policy": bundle["policy"],
                       "row_counts": {name: len(rows) for name, rows in bundle["tables"].items()}}
    if any(manifest.get(k) != v for k, v in expected_fields.items()):
        raise ValueError("manifest metadata mismatch")
    if replay:
        regenerated = build(Dataset.model_validate(bundle["input"]), AnalyticsPolicy.model_validate(bundle["policy"]),
                            code_commit=bundle["identity"]["code_commit"], code_hash=bundle["identity"]["code_hash"])
        if canonical(regenerated) != canonical(bundle):
            raise ValueError("deterministic replay mismatch")
    return bundle


def render_markdown(bundle):
    summary, tables = bundle["summary"], bundle["tables"]
    lines = ["# Quantia Analytics v2", "", f"Dataset: **{summary['dataset_label']}**. Corte: {summary['evaluated_as_of']}.",
             f"Run: `{bundle['analysis_run_id']}`. Auditoría offline; sin autoridad operativa.", "", "## Resultado económico", ""]
    for row in tables["economic_pnl"]:
        value = row.get("economic_pnl_net")
        lines.append(f"- {row.get('account_id', 'Cuenta')}: {row['status']}; Economic PnL Net = {value if value is not None else 'N/D'}. {row.get('reason_code') or ''}")
    lines += ["", "## Evidencia estadística", "", "EV por episodio, BASE 150 bps. Retornos fraccionales; no equivalen al PnL real.", "",
              "| Módulo/cohorte | H | raw | episodios | n_eff proxy | EV net | IC | Madurez |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    fmt = lambda x: "N/D" if x is None else f"{x:.4f}"
    for row in tables["cohort_metrics"]:
        lines.append(f"| {row['source_module']}/{row['cohort']} | {row['horizon_days']} | {row['n_raw']} | {row['n_episodes']} | {row['n_effective']} | {fmt(row['ev_net'])} | {fmt(row['ic'])} | {row['maturity_coverage']:.1%} |")
    lines += ["", "## Bot y humano sobre la misma oportunidad", "", "Delta = bot menos humano, BASE 150 bps. No es una comparación causal.", "",
              "| Cohorte | H | Pares | Bot EV | Humano EV | Delta | Sin match |", "|---|---:|---:|---:|---:|---:|---:|"]
    for row in tables["matched_metrics"]:
        lines.append(f"| {row['cohort']} | {row['horizon_days']} | {row['n_pairs']} | {fmt(row['bot_cf_return'])} | {fmt(row['human_cf_return'])} | {fmt(row['paired_delta_bot_minus_human'])} | {row['n_unmatched']} |")
    lines += ["", "## Gate de auditoría", "", "| Cohorte | H | Estado | Motivos |", "|---|---:|---|---|"]
    for row in tables["gates"]:
        lines.append(f"| {row['source_module']}/{row['cohort']} | {row['horizon_days']} | {row['status']} | {row['reasons'].replace('|', ', ')} |")
    lines += ["", "## Calidad, madurez y límites", "", "PENDING/UNAVAILABLE/AMBIGUOUS se conservan en maturity.csv; no son retornos cero.",
              "Los pares humano/bot usan la misma ventana; IGNORE representa cash local y no otras inversiones.",
              "Los gráficos proceden de las tablas. CI, costos, contribuciones, matching y hashes están en archivos separados.", ""]
    lines.extend(f"- {item}" for item in summary["limitations"])
    return "\n".join(lines) + "\n"


def report(folder):
    from .charts import render_charts
    folder = Path(folder)
    bundle = validate(folder, replay=False)
    write_exact(folder / "report.md", render_markdown(bundle).encode("utf-8"))
    render_charts(bundle["tables"], folder / "charts", dataset_label=bundle["summary"]["dataset_label"])
    update_manifest(folder, bundle)
    return folder / "report.md"
