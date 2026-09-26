"""Owner-scoped read-only Telegram bridge to Analytics v2 primitives.

Legacy outcomes are observations captured now, NOT canonical PIT outcomes.
They can describe EV/concentration but can never confirm a v2 statistical gate.
"""
from __future__ import annotations

import hashlib
import math
import os
import platform
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from html import escape
from pathlib import Path
from importlib.metadata import version

from src.analysis.analytics_v2.bootstrap import interval
from src.analysis.analytics_v2.episodes import build_episodes
from src.analysis.analytics_v2.metrics import distribution
from src.analysis.analytics_v2.models import AnalyticsPolicy, RecommendationFact, canonical, digest
from src.analysis.analytics_v2.reporting import csv_bytes

VERSION = "telegram-observational-v1"
DEDUP_VERSION = "legacy-conservative-episode-v1"
HORIZONS = (5, 10, 20, 40)
MAX_ROWS = 20000
DECISION_COLUMNS = (
    "id", "owner_chat_id", "decided_at", "ticker", "decision", "final_score", "status",
    "metric_scope", "is_primary_metric", "decision_type", "outcome_basis", "outcome_filled_at",
    "closed_at", "next_executable_at", "next_executable_price",
    *(f"outcome_{h}d" for h in HORIZONS), *(f"executable_outcome_{h}d" for h in HORIZONS),
)
ATTRIBUTION_COLUMNS = (
    "id", "owner_chat_id", "ticker", "side", "plan_decided_at", "executed_at", "follow_status",
    "temporal_quality", "eligible_for_viability", "outcome_basis", "outcome_filled_at", "updated_at",
    *(f"outcome_{h}d" for h in HORIZONS),
)


def normalize(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return normalize(float(value))
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def scope_sql(alias):
    return f"({alias}.owner_chat_id=$1 OR ($3::boolean AND {alias}.owner_chat_id IS NULL))"


async def capture(conn, *, owner_chat_id: int, days: int = 180, allow_legacy_null: bool = False):
    if not owner_chat_id or not 1 <= days <= 730:
        raise ValueError("owner and bounded window required")
    async with conn.transaction(readonly=True, isolation="repeatable_read"):
        captured_at = await conn.fetchval("SELECT CURRENT_TIMESTAMP")
        columns = await conn.fetch("""SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name=ANY($1::text[])""",
            ["decision_log", "broker_fills", "plan_execution_attributions", "plan_execution_attribution_movements", "broker_movements"])
        schema = defaultdict(set)
        for row in columns:
            schema[row["table_name"]].add(row["column_name"])
        if not {"id", "owner_chat_id", "decided_at", "ticker", "decision", "source", "status"} <= schema["decision_log"]:
            raise ValueError("decision_log missing required owner/source columns")
        if not {"id", "owner_chat_id", "executed_at", "fees_ars", "raw_payload", "decision_log_id"} <= schema["broker_fills"]:
            raise ValueError("broker_fills missing required ownership columns")
        # Null-owner legacy evidence is readable only in a verified single-owner database.
        attribution_owners = ("UNION ALL SELECT 1 FROM plan_execution_attributions WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1"
                              if "owner_chat_id" in schema["plan_execution_attributions"] else "")
        other_owners = await conn.fetchval(f"""SELECT EXISTS (
            SELECT 1 FROM decision_log WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
            UNION ALL SELECT 1 FROM broker_fills WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
            {attribution_owners}
        )""", owner_chat_id)
        legacy_null = bool(allow_legacy_null and not other_owners)
        cutoff = captured_at - timedelta(days=days)
        fields = [f"dl.{name}" for name in DECISION_COLUMNS if name in schema["decision_log"]]
        fields.append("COALESCE(dl.source, dl.layers->>'source') AS source" if "layers" in schema["decision_log"] else "dl.source")
        ready = {"id", "owner_chat_id", "ticker", "side", "executed_at", "eligible_for_viability"} <= schema["plan_execution_attributions"]
        linking = ready and "external_fill_id" in schema["broker_fills"] and {"attribution_id", "broker_movement_id"} <= schema["plan_execution_attribution_movements"] and {"id", "external_movement_id"} <= schema["broker_movements"]
        if linking:
            movement_scope = f"AND {scope_sql('bm')}" if "owner_chat_id" in schema["broker_movements"] else ""
            # Movement metadata has no owner in legacy schemas. The decision,
            # fill and attribution endpoints still require the caller's owner.
            via_layers = (f"""OR EXISTS (SELECT 1 FROM broker_movements bm
                JOIN plan_execution_attribution_movements link ON link.broker_movement_id=bm.id
                JOIN plan_execution_attributions a ON a.id=link.attribution_id
                WHERE COALESCE(dl.layers->'broker_movement'->'external_fill_ids','[]'::jsonb)
                      @> jsonb_build_array(bm.external_movement_id)
                  AND {scope_sql('a')} {movement_scope})""" if "layers" in schema["decision_log"] else "")
            fields.append(f"""(EXISTS (SELECT 1 FROM broker_fills bf
                JOIN broker_movements bm ON bm.external_movement_id=bf.external_fill_id
                JOIN plan_execution_attribution_movements link ON link.broker_movement_id=bm.id
                JOIN plan_execution_attributions a ON a.id=link.attribution_id
                WHERE bf.decision_log_id=dl.id AND {scope_sql('bf')} AND {scope_sql('a')} {movement_scope})
                {via_layers}) AS attributed_followed""")
        else:
            fields.append("NULL::boolean AS attributed_followed")
        superseded = "AND dl.superseded_by_id IS NULL" if "superseded_by_id" in schema["decision_log"] else ""
        decisions = await conn.fetch(f"""SELECT {', '.join(fields)} FROM decision_log dl
            WHERE {scope_sql('dl')} AND dl.decided_at >= $2 AND dl.decided_at <= $4 {superseded}
              AND NOT EXISTS (SELECT 1 FROM broker_fills old WHERE old.decision_log_id=dl.id
                  AND COALESCE(old.raw_payload,'{{}}'::jsonb) ? 'superseded_by_real'
                  AND NOT EXISTS (SELECT 1 FROM broker_fills live WHERE live.decision_log_id=dl.id
                    AND NOT (COALESCE(live.raw_payload,'{{}}'::jsonb) ? 'superseded_by_real')))
            ORDER BY dl.decided_at, dl.id LIMIT {MAX_ROWS+1}""", owner_chat_id, cutoff, legacy_null, captured_at)
        attrs = []
        if ready:
            fields = [f"a.{name}" for name in ATTRIBUTION_COLUMNS if name in schema["plan_execution_attributions"]]
            attrs = await conn.fetch(f"""SELECT {', '.join(fields)} FROM plan_execution_attributions a
                WHERE {scope_sql('a')} AND a.executed_at >= $2 AND a.executed_at <= $4
                ORDER BY a.executed_at, a.id LIMIT {MAX_ROWS+1}""", owner_chat_id, cutoff, legacy_null, captured_at)
        if len(decisions) > MAX_ROWS or len(attrs) > MAX_ROWS:
            raise ValueError("capture row limit exceeded; narrow window")
        fills = await conn.fetchrow(f"""SELECT COUNT(*) AS n_fills, COUNT(bf.fees_ars) AS n_fees_observed,
                SUM(bf.fees_ars) AS observed_fees_sum
            FROM broker_fills bf WHERE {scope_sql('bf')} AND bf.executed_at >= $2 AND bf.executed_at <= $4
              AND NOT (COALESCE(bf.raw_payload,'{{}}'::jsonb) ? 'superseded_by_real')""", owner_chat_id, cutoff, legacy_null, captured_at)
    return normalize({"version": VERSION, "captured_at": captured_at, "owner_chat_id": owner_chat_id,
                      "days": days, "legacy_null_included": legacy_null, "schema": {k: sorted(v) for k, v in schema.items()},
                      "decisions": [dict(r) for r in decisions], "attributions": [dict(r) for r in attrs],
                      "fill_coverage": dict(fills), "source": "LIVE_DB_READ_ONLY"})


def finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def classify(row):
    source, status, scope = row.get("source"), row.get("status"), row.get("metric_scope")
    if source == "execution_plan" and status == "EXECUTED" and scope in {"primary", "planner_audit"}:
        return "CORE", "BOT_ONLY"
    if source in {"broker_fill", "broker_movement"} and status in {"EXECUTED", "EXECUTED_MANUAL"} and row.get("is_primary_metric"):
        if row.get("attributed_followed") is False:
            return "MANUAL", "MANUAL_ONLY"
        return None
    if source == "radar" and scope == "radar_audit":
        return "RADAR", "RADAR_ALL"
    return None


def summarize(snapshot):
    if snapshot.get("source") != "LIVE_DB_READ_ONLY":
        raise ValueError("Telegram accepts only an owner-scoped live capture")
    owner = snapshot["owner_chat_id"]
    at = datetime.fromisoformat(snapshot["captured_at"])
    policy = AnalyticsPolicy(experiment_id=VERSION, evaluated_as_of=at, horizons=HORIZONS,
                             dedup_policy_version=DEDUP_VERSION)
    recommendations, source_rows, excluded = [], {}, Counter()
    rows = list(snapshot["decisions"])
    for row in snapshot["attributions"]:
        rows.append({**row, "id": f"attribution:{row['id']}", "source": "plan_execution_attribution",
                     "decided_at": row["executed_at"], "decision": row["side"], "status": "FOLLOWED"})
    for row in rows:
        if row.get("owner_chat_id") not in {None, owner} or (row.get("owner_chat_id") is None and not snapshot["legacy_null_included"]):
            raise ValueError("capture owner mismatch")
        if row["source"] == "plan_execution_attribution":
            identity = ("MANUAL", "FOLLOWED")
        else:
            identity = classify(row)
        if identity is None or row.get("decision") not in {"BUY", "SELL", "SELL_PARTIAL", "SELL_FULL"}:
            excluded["OUTSIDE_COHORT_OR_ATTRIBUTED"] += 1
            continue
        side = "SELL" if str(row["decision"]).startswith("SELL") else "BUY"
        rec_id = str(row["id"])
        # Separate followed/manual estimands without suggesting independent opportunities.
        instrument = row["ticker"] if identity[1] != "FOLLOWED" else f"FOLLOWED:{row['ticker']}"
        ambiguous = row["source"] == "plan_execution_attribution" and not row.get("eligible_for_viability", False)
        recommendations.append(RecommendationFact(
            recommendation_id=rec_id, account_id=str(owner), source_module=identity[0], cohort=identity[1],
            instrument_id=instrument, ticker=row["ticker"], direction=side, decision_as_of=row["decided_at"],
            score=finite(row.get("final_score")), ambiguous=ambiguous,
            ambiguity_reason="ATTRIBUTION_NOT_CONFIRMED" if ambiguous else None,
            available_at=at, ingested_at=at, sealed_at=at, source_id=f"live-capture:{row['source']}:{rec_id}"))
        source_rows[rec_id] = row
    episodes, links = build_episodes(recommendations, DEDUP_VERSION)
    by_cohort = defaultdict(list)
    for episode in episodes:
        by_cohort[episode["cohort"]].append(episode)
    metrics, outcomes = [], []
    for cohort, eps in sorted(by_cohort.items()):
        for h in HORIZONS:
            gross, dates = [], []
            counts = Counter()
            observed_recs = 0
            for episode in eps:
                raw = source_rows[episode["anchor_id"]]
                value = finite(raw.get(f"executable_outcome_{h}d"))
                basis = str(raw.get("outcome_basis") or "")
                reason = None
                if value is None:
                    value = finite(raw.get(f"outcome_{h}d"))
                if raw.get("source") == "plan_execution_attribution" and not raw.get("eligible_for_viability"):
                    reason = "AMBIGUOUS_ATTRIBUTION"
                elif not basis.startswith("canonical_cocos"):
                    reason = "UNVERIFIED_PRICE_BASIS"
                elif raw.get("outcome_filled_at") and datetime.fromisoformat(raw["outcome_filled_at"]) > at:
                    reason = "OUTCOME_AVAILABLE_AFTER_CAPTURE"
                elif value is None:
                    reason = "OUTCOME_NOT_RECORDED_MATURITY_UNKNOWN"
                status = "RECORDED_OBSERVATION" if reason is None else "AMBIGUOUS" if reason == "AMBIGUOUS_ATTRIBUTION" else "UNAVAILABLE"
                counts[status] += 1
                if reason is None:
                    gross.append(value)
                    dates.append(episode["episode_start"].date().isoformat())
                    observed_recs += episode["recommendation_count"]
                outcomes.append({"episode_id": episode["decision_episode_id"], "anchor_id": episode["anchor_id"],
                                 "ticker": episode["ticker"], "cohort": cohort, "horizon_days": h,
                                 "legacy_outcome_status": status, "gross_return": value if reason is None else None,
                                 "canonical_outcome_status": "UNAVAILABLE", "pit_eligible": False,
                                 "reason_code": reason or "LEGACY_WINDOW_AND_REVISION_LINEAGE_UNVERIFIED",
                                 "source_row_hash": digest(raw)})
            for cost in policy.costs.scenarios:
                values = [v-cost.bps/10000 for v in gross]
                result = {"cohort": cohort, "horizon_days": h, "cost_scenario": cost.name, "cost_bps": cost.bps,
                          "n_raw": observed_recs, "n_recommendations": sum(e["recommendation_count"] for e in eps),
                          "n_episodes": len(eps), "n_observed": len(gross), "n_dates": len(set(dates)),
                          "n_effective": None, "n_unavailable": counts["UNAVAILABLE"], "n_ambiguous": counts["AMBIGUOUS"],
                          "gate": "OBSERVE", "pit_eligible": False, "inference_scope": "OBSERVATIONAL_DATE_BLOCK",
                          "reason_code": "LEGACY_WINDOW_AND_REVISION_LINEAGE_UNVERIFIED", **distribution(values)}
                if cost.name == "RESEARCH_BASE":
                    ci = interval(dates, values, policy, policy.bootstrap_block_length)
                    result.update({f"ev_{key}": ci[key] for key in ("lower", "upper", "resamples", "seed", "block_length", "reason_code", "valid_resamples")})
                metrics.append(result)
    package = {"snapshot": snapshot, "policy": policy.model_dump(mode="json"), "episodes": normalize(episodes),
               "episode_links": links, "outcomes": outcomes, "metrics": metrics, "excluded_rows": dict(excluded),
               "economic_pnl": {"status": "INCOMPLETE", "economic_pnl_net": None,
                                "reason_code": "NAV_EXTERNAL_FLOWS_AND_OBSERVED_COSTS_NOT_RECONCILED"},
               "matching": {"status": "UNAVAILABLE", "reason_code": "EXACT_CONTEMPORANEOUS_BOT_HUMAN_WINDOW_REQUIRED"},
               "swaps": {"status": "DISABLED_SHADOW", "reason_code": "EXACT_PAIR_REVALIDATION_REQUIRED"}}
    root = Path(__file__).resolve().parents[2]
    code_files = [Path(__file__), root / "scripts/run_analytics_v2.py", *sorted((Path(__file__).parent / "analytics_v2").glob("*.py"))]
    code_hash = digest({p.relative_to(root).as_posix(): p.read_text(encoding="utf-8") for p in code_files})
    runtime = {"python": platform.python_version(), "numpy": version("numpy"), "pandas": version("pandas"),
               "pydantic": version("pydantic"), "platform": platform.system(), "machine": platform.machine()}
    package["manifest"] = {"analysis_run_id": digest([snapshot, package["policy"], VERSION, DEDUP_VERSION, code_hash, runtime]), "input_hash": digest(snapshot),
                           "capture_version": VERSION, "metric_policy_version": policy.metric_policy_version,
                           "cost_policy_version": policy.costs.version, "dedup_policy_version": DEDUP_VERSION,
                           "gate_policy_version": "legacy-no-promotion-v1", "evaluated_as_of": at.isoformat(),
                           "code_commit": os.environ.get("QUANTIA_ANALYTICS_CODE_COMMIT", "UNRECORDED"),
                           "code_hash": code_hash,
                           "numeric_runtime": runtime,
                           "bootstrap_resamples": policy.bootstrap_resamples, "bootstrap_seed": policy.bootstrap_seed,
                           "source": "LIVE_DB_READ_ONLY", "data_status": "OBSERVATIONAL_NOT_PIT_VALIDATED"}
    return normalize(package)


def render_telegram(package):
    snapshot = package["snapshot"]
    fmt = lambda value: "N/D" if value is None else f"{value:+.1%}"
    lines = ["<b>📐 Quantia Analytics v2</b>", f"{snapshot['days']} días · corte {escape(snapshot['captured_at'][:16].replace('T', ' '))} UTC",
             "<b>Lectura observacional · gate OBSERVE</b>", "", "<b>Resultado económico</b>",
             "PnL neto real: <b>N/D</b>. Falta reconciliar NAV, flujos externos y costos.", "",
             "<b>EV por episodio · costo BASE 1,50%</b>", "<pre>Cohorte     H   n/out   EV neto"]
    labels = {"BOT_ONLY": "Bot", "MANUAL_ONLY": "Manual", "FOLLOWED": "Seguido", "RADAR_ALL": "Radar"}
    for row in package["metrics"]:
        if row["cost_scenario"] == "RESEARCH_BASE":
            lines.append(f"{labels[row['cohort']]:<10} {row['horizon_days']:>2}D {row['n_observed']:>3}/{row['n_episodes']:<3} {fmt(row['ev_net']):>7}")
    lines.extend(["</pre>", "n/out = episodios con outcome registrado / episodios totales.", ""])
    bot5 = next((r for r in package["metrics"] if r["cohort"] == "BOT_ONLY" and r["horizon_days"] == 5 and r["cost_scenario"] == "RESEARCH_BASE"), None)
    if bot5:
        lines.extend([f"Bot: {bot5['n_recommendations']} recomendaciones → {bot5['n_episodes']} episodios; n efectivo: N/D.",
                      f"Intervalo 95% EV 5D: {fmt(bot5.get('ev_lower'))} a {fmt(bot5.get('ev_upper'))} · bloques 20 fechas.",
                      f"Top1/Top3 positivos: {fmt(bot5['top1_positive'])} / {fmt(bot5['top3_positive'])}."])
        costs = [r for r in package["metrics"] if r["cohort"] == "BOT_ONLY" and r["horizon_days"] == 5 and r["cost_bps"] in {75, 250, 400}]
        lines.append("Bot 5D según costo: " + " · ".join(f"{r['cost_bps']:g} bps {fmt(r['ev_net'])}" for r in costs))
    lines.extend(["", "<b>Límites de la evidencia</b>",
                  "Outcomes históricos tal como están guardados; no acreditan ventanas ni revisiones point-in-time. Los costos son escenarios, no gastos reales.",
                  "Un outcome ausente no se cuenta como cero ni se declara inmaduro sin calendario verificable.",
                  "Bot vs humano pareado: pendiente. Radar: discovery/shadow. Swaps: <b>DISABLED_SHADOW</b> en esta auditoría.",
                  "La deduplicación agrupa repeticiones hasta un cambio de dirección; puede unir reaperturas sin cierre documentado.",
                  "<i>Adjunto: episodios, costos 0/75/150/250/400 bps, faltantes y manifest. No habilita capital.</i>"])
    return "\n".join(lines)


def write_archive(package, path):
    files = {"capture.json": canonical(package["snapshot"]).encode(),
             "report.json": canonical(package).encode(), "report.html": render_telegram(package).encode(),
             "metrics.csv": csv_bytes(package["metrics"]), "episodes.csv": csv_bytes(package["episodes"]),
             "episode_links.csv": csv_bytes(package["episode_links"]), "outcomes.csv": csv_bytes(package["outcomes"])}
    manifest = {**package["manifest"], "output_hashes": {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}}
    files["manifest.json"] = canonical(manifest).encode()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(files.items()):
            archive.writestr(name, data)
    return path
