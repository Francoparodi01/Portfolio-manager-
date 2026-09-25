"""Read-only Telegram presentation for Economic Meta Policy shadow records."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.analysis.economic_meta_policy import ECONOMIC_META_POLICY_VERSION
from src.analysis.economic_meta_store import DEFAULT_SHADOW_PATH, EconomicMetaShadowStore


def _fmt_dt(value: Any) -> str:
    if not value:
        return "—"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _decision_icon(value: str) -> str:
    return "✅" if value == "ALLOW_SHADOW" else "⏸"


def _reason(value: Any) -> str:
    labels = {
        "SOURCE_ALREADY_HOLD": "ya era HOLD",
        "SCORE_BELOW_PREREGISTERED_GATE": "score bajo",
        "EDGE_EVIDENCE_REQUIRED": "falta edge vs HOLD",
        "EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY_BUFFER": "edge insuficiente",
        "EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY": "edge insuficiente",
        "COST_ABOVE_SHADOW_GATE": "costo alto",
        "TURNOVER_ABOVE_SHADOW_GATE": "turnover alto",
        "REGIME_BLOCKED": "régimen bloqueado",
        "INVALID_SCORE": "score inválido",
        "INVALID_COST": "costo inválido",
        "INVALID_TURNOVER": "turnover inválido",
        "UNSUPPORTED_ACTION": "acción no soportada",
        "MISSING_TICKER": "ticker faltante",
        "FAIL_CLOSED": "fail-closed",
    }
    if not value:
        return ""
    parts = [part for part in str(value).split("|") if part]
    return ", ".join(labels.get(part, part.lower()) for part in parts)


def _latest_run(rows: list[dict]) -> tuple[str | None, list[dict]]:
    if not rows:
        return None, []
    newest = max(rows, key=lambda row: str(row.get("as_of") or ""))
    run_id = str(newest.get("run_id") or "") or None
    if run_id:
        selected = [row for row in rows if str(row.get("run_id") or "") == run_id]
    else:
        as_of = str(newest.get("as_of") or "")
        selected = [row for row in rows if str(row.get("as_of") or "") == as_of]
    return run_id, selected


def render_latest_meta(
    *,
    ticker: str | None = None,
    path: str | Path = DEFAULT_SHADOW_PATH,
) -> str:
    store = EconomicMetaShadowStore(path)
    try:
        rows = store.read_all()
    except Exception as exc:
        return f"🧪 Economic Meta Policy · error\nNo pude leer el store: {type(exc).__name__}: {exc}"

    ticker_filter = str(ticker or "").upper().strip() or None
    if ticker_filter:
        rows = [row for row in rows if str(row.get("ticker") or "").upper() == ticker_filter]

    run_id, selected = _latest_run(rows)
    if not selected:
        scope = f" para {ticker_filter}" if ticker_filter else ""
        return (
            "🧪 Economic Meta Policy v1 · SHADOW_ONLY\n\n"
            f"No hay registros shadow persistidos{scope}.\n"
            "Capital effect: NO"
        )

    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in selected:
        by_ticker[str(row.get("ticker") or "?").upper()].append(row)

    lines = [
        "🧪 Economic Meta Policy v1 · SHADOW_ONLY",
        f"Run: {run_id or '—'}",
        "Capital effect: NO",
        "",
    ]
    for symbol in sorted(by_ticker):
        records = sorted(by_ticker[symbol], key=lambda row: str(row.get("policy_name") or ""))
        first = records[0]
        score = first.get("candidate_score")
        try:
            score_text = f"{float(score):+.3f}"
        except Exception:
            score_text = "N/D"
        lines.append(f"{symbol} · {first.get('candidate_action') or '?'} · score {score_text}")
        parts = []
        for row in records:
            name = str(row.get("policy_name") or "META-?").replace("META-", "")
            decision = str(row.get("decision") or "")
            reason = _reason(row.get("rejection_reason"))
            label = f"{name}{_decision_icon(decision)}"
            if reason and decision != "ALLOW_SHADOW":
                label += f"({reason})"
            parts.append(label)
        lines.append(" · ".join(parts))

    lines.extend(
        [
            "",
            f"Corte: {_fmt_dt(selected[0].get('as_of'))}",
            "Primario: 20D · research: 5/10/20/40D",
            "Vista read-only: /meta no crea ni modifica decisiones.",
        ]
    )
    return "\n".join(lines)


def render_meta_status(path: str | Path = DEFAULT_SHADOW_PATH) -> str:
    store = EconomicMetaShadowStore(path)
    try:
        rows = store.read_all()
    except Exception as exc:
        return f"🧪 Economic Meta Policy · status\nNo pude leer el store: {type(exc).__name__}: {exc}"

    lines = [
        "🧪 Economic Meta Policy · status",
        f"Version: {ECONOMIC_META_POLICY_VERSION}",
        "Mode: SHADOW_ONLY · Capital effect: NO",
        f"Store: {Path(path)}",
    ]
    if not rows:
        lines.append("Records: 0")
        return "\n".join(lines)

    runs = {str(row.get("run_id")) for row in rows if row.get("run_id")}
    tickers = {str(row.get("ticker")) for row in rows if row.get("ticker")}
    by_policy: dict[str, Counter[str]] = defaultdict(Counter)
    latest = max((str(row.get("as_of") or "") for row in rows), default="")
    for row in rows:
        by_policy[str(row.get("policy_name") or "UNKNOWN")][str(row.get("decision") or "UNKNOWN")] += 1

    lines.extend(
        [
            f"Records: {len(rows)} · runs: {len(runs)} · tickers: {len(tickers)}",
            f"Último corte: {_fmt_dt(latest)}",
            "",
        ]
    )
    for policy in sorted(by_policy):
        counts = by_policy[policy]
        lines.append(f"{policy}: allow={counts.get('ALLOW_SHADOW', 0)} · hold={counts.get('REJECT_TO_HOLD', 0)}")
    return "\n".join(lines)


__all__ = ["render_latest_meta", "render_meta_status"]
