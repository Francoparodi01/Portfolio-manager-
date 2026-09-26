"""Point-in-time Decision Lab context for Economic Meta Policy.

This adapter is deliberately read-only. Decision Lab's current ticker filter
selects whole-account episodes containing a ticker; it does not provide
per-ticker attributed DVA. Therefore the evidence may be displayed as context
but must not be promoted into META-C edge inputs for an individual ticker.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.agentic.read_only import connect_read_only
from src.analysis.economic_meta_store import DEFAULT_SHADOW_PATH, EconomicMetaShadowStore
from src.decision_lab.queries import select_evidence


def _as_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_meta_cutoff(path: str | Path = DEFAULT_SHADOW_PATH) -> datetime | None:
    try:
        rows = EconomicMetaShadowStore(path).read_all()
    except Exception:
        return None
    automatic = [
        row for row in rows
        if str(row.get("opportunity_id") or "").startswith("analysis:")
    ]
    source = automatic or rows
    values = [_as_dt(row.get("as_of")) for row in source]
    values = [value for value in values if value is not None]
    return max(values) if values else None


async def load_decision_lab_context(
    dsn: str,
    owner_chat_id: int,
    *,
    ticker: str | None = None,
    horizon: int = 20,
    cutoff: datetime | None = None,
    path: str | Path = DEFAULT_SHADOW_PATH,
) -> dict[str, Any]:
    """Load latest Decision Lab evidence that already existed at the META cutoff."""
    if not owner_chat_id:
        return {"status": "INSUFFICIENT", "reason": "OWNER_REQUIRED"}
    cutoff = cutoff or _latest_meta_cutoff(path)
    if cutoff is None:
        return {"status": "INSUFFICIENT", "reason": "META_CUTOFF_UNAVAILABLE"}

    conn = await connect_read_only(dsn, command_timeout=30)
    try:
        if not await conn.fetchval("SELECT to_regclass('public.decision_lab_runs')"):
            return {"status": "INSUFFICIENT", "reason": "DECISION_LAB_NOT_INITIALIZED"}
        rows = await conn.fetch(
            """SELECT replay_run_id,summary,config,evaluated_as_of
               FROM decision_lab_runs
               WHERE owner_chat_id=$1 AND evaluated_as_of<=$2
               ORDER BY evaluated_as_of DESC,created_at DESC,replay_run_id
               LIMIT 20""",
            int(owner_chat_id),
            cutoff,
        )
        arguments = {"horizon": int(horizon), "ticker": str(ticker).upper() if ticker else None}
        for row in rows:
            summary = json.loads(row["summary"]) if isinstance(row["summary"], str) else row["summary"]
            config = json.loads(row["config"]) if isinstance(row["config"], str) else row["config"]
            payload = select_evidence(summary, config, arguments, "compare_plan_vs_hold")
            if payload.get("metrics"):
                payload["meta_cutoff"] = cutoff.isoformat()
                payload["ticker_attributed"] = False
                payload["meta_c_eligible"] = False
                payload["meta_c_ineligible_reason"] = "ACCOUNT_LEVEL_EPISODES_NOT_TICKER_ATTRIBUTED"
                return payload
        return {
            "status": "INSUFFICIENT",
            "reason": "NO_STORED_COMPARABLE_EPISODES_AT_META_CUTOFF",
            "meta_cutoff": cutoff.isoformat(),
            "ticker_attributed": False,
            "meta_c_eligible": False,
        }
    finally:
        await conn.close()


def _pct(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{100.0 * float(value):+.2f}%"
    except Exception:
        return "N/D"


def render_decision_lab_context(payload: dict[str, Any]) -> str:
    """Compact Telegram-safe explanation of the evidence behind META-C."""
    if not payload or not payload.get("metrics"):
        reason = str((payload or {}).get("reason") or "SIN_EVIDENCIA_COMPARABLE")
        return (
            "\n\n📐 Decision Lab · META-C\n"
            f"Evidencia PIT: INSUFFICIENT ({reason})\n"
            "META-C permanece fail-closed."
        )

    metric = payload["metrics"][0]
    ci = metric.get("ci") or {}
    lower = ci.get("lower")
    upper = ci.get("upper")
    ci_text = "N/D" if lower is None or upper is None else f"[{_pct(lower)}, {_pct(upper)}]"
    quality_levels = []
    for row in payload.get("quality") or []:
        level = str((row.get("quality") or {}).get("level") or "UNKNOWN")
        if level not in quality_levels:
            quality_levels.append(level)
    quality_text = "/".join(quality_levels) if quality_levels else "N/D"
    scope = "cuenta completa"
    if payload.get("ticker"):
        scope = f"episodios de cuenta que incluyen {payload['ticker']}"

    return (
        "\n\n📐 Decision Lab · META-C\n"
        f"20D · {scope}\n"
        f"PLAN {_pct(metric.get('plan_mean'))} · HOLD {_pct(metric.get('hold_mean'))} · DVA {_pct(metric.get('mean'))}\n"
        f"n={metric.get('n', 0)} · n efectivo={metric.get('n_effective', 0)} · IC95 {ci_text} · calidad {quality_text}\n"
        f"Corte evidencia: {payload.get('evaluated_as_of', 'N/D')}\n"
        "Uso: contexto solamente. El DVA actual es de cartera, no atribuible al ticker; "
        "por eso no habilita META-C individual y se mantiene fail-closed."
    )


__all__ = ["load_decision_lab_context", "render_decision_lab_context"]
