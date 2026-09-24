"""Agent-facing reads of already-computed evidence. No returns calculated here."""

import json
from src.agentic.read_only import connect_read_only

TOOLS = (
    "get_decision_counterfactuals",
    "get_decision_value_added",
    "get_similar_historical_episodes",
    "compare_plan_vs_hold",
    "compare_strategy_versions",
    "get_replay_evidence_quality",
)


def select_evidence(summary, config, arguments, view="get_decision_value_added"):
    horizon = arguments.get("horizon", 20)
    ticker = arguments.get("ticker")
    segment = "contains_ticker=" + ticker if ticker else "ALL"
    metrics = [
        r
        for r in summary["metrics"]
        if r["horizon"] == horizon and r["segment"] == segment
    ]
    comparisons = [
        r
        for r in summary["comparisons"]
        if r["horizon"] == horizon and (not ticker or ticker in r.get("tickers", []))
    ]
    ids = {r["episode_id"] for r in comparisons}
    quality = [r for r in summary["quality"] if r["episode_id"] in ids]
    return {
        "schema_version": "decision-lab-agent-evidence-v1",
        "view": view,
        "status": "AVAILABLE" if metrics else "INSUFFICIENT",
        "replay_run_id": summary["replay_run_id"],
        "evaluated_as_of": config["evaluated_as_of"],
        "mode": config["mode"],
        "horizon": horizon,
        "ticker": ticker,
        "scope": "ACCOUNT_EPISODES_CONTAINING_TICKER" if ticker else "ACCOUNT_EPISODES",
        "cost_model": config["costs"],
        "metrics": metrics,
        "alternatives": (
            [
                r
                for r in summary.get("alternatives", [])
                if r["horizon"] == horizon
                and r["episode_id"] in {c["episode_id"] for c in comparisons[-2:]}
            ]
            if view == "get_decision_counterfactuals"
            else []
        ),
        "episodes": comparisons[-5:],
        "quality": quality[-5:],
        "strategy_comparisons": [
            {
                "version_a": r["version_a"],
                "version_b": r["version_b"],
                "n_pairs": r["n_pairs"],
                "n_unmatched_a": r["n_unmatched_a"],
                "n_unmatched_b": r["n_unmatched_b"],
                "metrics": [m for m in r["metrics"] if m["horizon"] == horizon],
            }
            for r in summary["strategy_comparisons"]
        ],
        "limitations": [
            "Counterfactual, not executed economic PnL or causal evidence",
            "No comparable historical cohort proves the current plan optimal",
            "Ticker filtering selects whole-account episodes, not ticker-attributed returns",
        ],
    }


async def query_evidence(dsn, owner, arguments, view="get_decision_value_added"):
    if not owner:
        raise ValueError("explicit owner required")
    conn = await connect_read_only(dsn, command_timeout=30)
    try:
        if not await conn.fetchval("SELECT to_regclass('public.decision_lab_runs')"):
            return {
                "schema_version": "decision-lab-agent-evidence-v1",
                "status": "INSUFFICIENT",
                "reason": "DECISION_LAB_NOT_INITIALIZED",
                "metrics": [],
            }
        rows = await conn.fetch(
            """SELECT replay_run_id,summary,config FROM decision_lab_runs
            WHERE owner_chat_id=$1 AND ($2::text IS NULL OR replay_run_id=$2)
            AND evaluated_as_of<=now() ORDER BY evaluated_as_of DESC,created_at DESC,replay_run_id LIMIT 20""",
            owner,
            arguments.get("run_id"),
        )
        for row in rows:
            summary = (
                json.loads(row["summary"])
                if isinstance(row["summary"], str)
                else row["summary"]
            )
            config = (
                json.loads(row["config"])
                if isinstance(row["config"], str)
                else row["config"]
            )
            result = select_evidence(summary, config, arguments, view)
            if result["metrics"] or arguments.get("run_id"):
                return result
        return {
            "schema_version": "decision-lab-agent-evidence-v1",
            "status": "INSUFFICIENT",
            "reason": "NO_STORED_COMPARABLE_EPISODES",
            "metrics": [],
        }
    finally:
        await conn.close()


def explain_evidence(payload):
    if not payload or payload.get("schema_version") != "decision-lab-agent-evidence-v1":
        return (
            "Evidencia económica: no obtuve un resultado verificable del Decision Lab. No puedo afirmar un DVA.",
            "INSUFFICIENT",
        )
    if not payload.get("metrics"):
        return (
            "Evidencia económica: INSUFFICIENT. No hay episodios comparables calculados para esta consulta. El motivo del planner no demuestra que PLAN supere HOLD.",
            "INSUFFICIENT",
        )
    lines = [
        f"Evidencia económica — Decision Lab · {payload['horizon']} sesiones · corte {payload['evaluated_as_of']}.",
        f"Modo: {payload['mode']}. Run: {payload['replay_run_id']}.",
    ]
    if payload.get("ticker"):
        lines.append(
            f"Cohorte: episodios de la cuenta que incluyen {payload['ticker']}; los retornos son de la cartera completa, no atribuibles exclusivamente a ese ticker."
        )

    def pp(value):
        return "N/D" if value is None else f"{100*value:+.3f}"

    for r in payload["metrics"][:8]:
        lines.append(
            f"{r['strategy_version']} · {r['population']}: n={r['n']}, fechas={r['n_dates']}, n efectivo conservador={r['n_effective']}."
        )
        if r["mean"] is None:
            lines.append("Sin muestra elegible para estimar DVA en esta población.")
            continue
        lines.append(
            f"PLAN {pp(r['plan_mean'])}% · HOLD {pp(r['hold_mean'])}% · DVA {pp(r['mean'])} pp."
        )
        ci = r["ci"]
        if ci.get("lower") is None:
            lines.append(
                "Sin intervalo inferencial válido: "
                + str(ci.get("reason_code"))
                + ". Resultado descriptivo; no demuestra superioridad."
            )
        else:
            lines.append(
                f"IC bootstrap {100*ci.get('confidence_level',.95):g}%: [{pp(ci['lower'])}, {pp(ci['upper'])}] pp."
            )
            if ci["lower"] <= 0 <= ci["upper"]:
                lines.append("El intervalo incluye cero: evidencia no concluyente.")
            else:
                lines.append(
                    "El intervalo describe esta cohorte; no establece causalidad ni autoriza cambios operativos."
                )
    costs = payload.get("cost_model", {})
    lines.append(
        f"Base: {costs.get('return_basis')}; cash {costs.get('cash_convention')}; comisión simulada {costs.get('fee_bps')} bps por lado, más los restantes costos del modelo. No son costos reales observados."
    )
    quality_seen = set()
    for q in payload.get("quality", []):
        quality = q["quality"]
        key = json.dumps(quality, sort_keys=True)
        if key in quality_seen:
            continue
        quality_seen.add(key)
        lines.append(
            f"Calidad {quality['level']}. Faltantes: {', '.join(quality['missing_fields']) or 'ninguno'}. Supuestos: {', '.join(quality['reconstruction_assumptions']) or 'ninguno'}."
        )
        if len(quality_seen) == 2:
            break
    if payload.get("view") == "get_similar_historical_episodes":
        lines.append(
            "Ejemplos de la cohorte filtrada; no son vecinos causales ni equivalentes en todas sus variables:"
        )
        for row in payload.get("episodes", []):
            lines.append(
                f"{row['as_of']} · {row['status']} · PLAN {pp(row['plan_return'])}% · HOLD {pp(row['hold_return'])}% · DVA {pp(row['dva'])} pp."
            )
    for row in payload.get("alternatives", []):
        lines.append(
            f"Episodio {row['episode_id'][:12]} · {row['alternative']} · {row['status']} · neto {pp(row['net_return'])}% · {row.get('reason') or 'capital y horizonte comunes'}."
        )
    if not payload.get("strategy_comparisons"):
        lines.append(
            "Comparación entre versiones: no hay pares de versiones evaluados en este run."
        )
    for comparison in payload.get("strategy_comparisons", []):
        lines.append(
            f"Versiones {comparison['version_b']} − {comparison['version_a']}: {comparison['n_pairs']} pares exactos en todos los horizontes del run."
        )
        for metric in comparison["metrics"]:
            lines.append(
                f"{metric['horizon']}D · {metric['population']} · n={metric['n']} · diferencia {pp(metric['mean'])} pp; {metric['interpretation']}."
            )
            ci = metric.get("ci", {})
            lines.append(
                f"IC95 diferencia: [{pp(ci.get('lower'))}, {pp(ci.get('upper'))}] pp; {ci.get('reason_code') or 'bootstrap por fechas'}."
            )
    return "\n".join(lines), "PARTIAL"
