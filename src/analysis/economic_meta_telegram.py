"""Read-only Telegram presentation for Economic Meta Policy shadow records."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.analysis.economic_meta_policy import ECONOMIC_META_POLICY_VERSION
from src.analysis.economic_meta_store import DEFAULT_SHADOW_PATH, EconomicMetaShadowStore


def _as_of_dt(value: Any) -> datetime:
    try:
        dt = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_dt(value: Any) -> str:
    if not value:
        return "—"
    dt = _as_of_dt(value)
    if dt == datetime.min.replace(tzinfo=timezone.utc):
        return str(value)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def _decision_icon(value: str) -> str:
    return "✅" if value == "ALLOW_SHADOW" else "⏸"


def _reason(value: Any) -> str:
    labels = {
        "SOURCE_ALREADY_HOLD": "ya era HOLD",
        "SCORE_BELOW_PREREGISTERED_GATE": "score bajo",
        "EDGE_EVIDENCE_REQUIRED": "falta edge vs HOLD",
        "EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY_BUFFER": "edge insuficiente",
        "EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY": "edge insuficiente",
        "HISTORICAL_EDGE_REQUIRED": "sin histórico PIT",
        "HIST_SAMPLE_LT_20_EPISODES": "histórico n<20",
        "HIST_SAMPLE_LT_8_DATES": "histórico fechas<8",
        "HIST_WIN_RATE_LT_55PCT": "win histórico <55%",
        "HIST_EV_NET_LT_25BPS": "EV histórico <+0,25%",
        "HIST_MEDIAN_NET_NOT_POSITIVE": "mediana histórica <=0",
        "HIST_PROFIT_FACTOR_LT_1_10": "PF histórico <1,10",
        "HIST_TOP1_CONCENTRATION_GT_40PCT": "histórico concentrado top1",
        "HIST_TOP3_CONCENTRATION_GT_75PCT": "histórico concentrado top3",
        "HIST_DVA_SEMANTICS_UNEXPECTED": "semántica histórica inválida",
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


def _is_auto_analysis_row(row: dict) -> bool:
    return str(row.get("opportunity_id") or "").startswith("analysis:")


def _is_db_analysis_row(row: dict) -> bool:
    opportunity = str(row.get("opportunity_id") or "")
    return opportunity.startswith("analysis:") and not opportunity.startswith("analysis:report:")


def _preferred_rows(rows: list[dict]) -> tuple[list[dict], str]:
    """Use the freshest automatic source; prefer DB only when equally fresh.

    The DB watcher can attach point-in-time history, but it must never hide a newer
    rendered analysis report. This keeps /meta aligned with the latest analysis the
    user actually saw while still preferring richer DB evidence on timestamp ties.
    """
    automatic = [row for row in rows if _is_auto_analysis_row(row)]
    if automatic:
        newest = max(
            automatic,
            key=lambda row: (
                _as_of_dt(row.get("as_of")),
                1 if _is_db_analysis_row(row) else 0,
            ),
        )
        if _is_db_analysis_row(newest):
            return [row for row in automatic if _is_db_analysis_row(row)], "AUTO_ANALYSIS_DB"
        return [row for row in automatic if not _is_db_analysis_row(row)], "AUTO_ANALYSIS"
    return rows, "SHADOW_MANUAL"


def _latest_run(rows: list[dict]) -> tuple[str | None, list[dict]]:
    if not rows:
        return None, []
    newest = max(rows, key=lambda row: _as_of_dt(row.get("as_of")))
    run_id = str(newest.get("run_id") or "") or None
    if run_id:
        selected = [row for row in rows if str(row.get("run_id") or "") == run_id]
    else:
        as_of = str(newest.get("as_of") or "")
        selected = [row for row in rows if str(row.get("as_of") or "") == as_of]
    return run_id, selected


def _pct(value: Any, *, signed: bool = False) -> str:
    try:
        number = float(value) * 100.0
    except (TypeError, ValueError):
        return "N/D"
    return f"{number:+.1f}%" if signed else f"{number:.1f}%"


def _ratio(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if number == float("inf"):
        return "∞"
    return f"{number:.2f}"


def _historical_line(records: list[dict]) -> str | None:
    meta_d = next((row for row in records if str(row.get("policy_name") or "") == "META-D"), None)
    if not meta_d:
        return None
    metadata = meta_d.get("metadata") or {}
    historical = metadata.get("historical_edge") if isinstance(metadata, dict) else None
    if not isinstance(historical, dict) or not historical:
        return "Hist20D · evidencia PIT no disponible"
    specificity = str(historical.get("specificity") or "N/D").replace("_", "/")
    quality = str(historical.get("quality") or "N/D")
    return (
        "Hist20D · "
        f"{specificity} · n {int(historical.get('n_episodes') or 0)} / "
        f"{int(historical.get('n_dates') or 0)} fechas · "
        f"win {_pct(historical.get('win_rate_net'))} · "
        f"EV net {_pct(historical.get('mean_net_return'), signed=True)} · "
        f"PF {_ratio(historical.get('profit_factor_net'))} · {quality}"
    )


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

    rows, source_mode = _preferred_rows(rows)
    run_id, selected = _latest_run(rows)
    if not selected:
        scope = f" para {ticker_filter}" if ticker_filter else ""
        return (
            "🧪 Economic Meta Policy v1 · SHADOW_ONLY\n\n"
            f"No hay registros shadow persistidos{scope}.\n"
            "Capital effect: NO"
        )

    # JSONL is append-only. If the same run is recalculated with corrected input
    # semantics, keep the last record for each ticker/policy instead of rendering
    # duplicate A/B/C/D entries.
    by_ticker_policy: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in selected:
        symbol = str(row.get("ticker") or "?").upper()
        policy = str(row.get("policy_name") or "META-?")
        by_ticker_policy[symbol][policy] = row

    source_label = (
        "análisis automático DB"
        if source_mode == "AUTO_ANALYSIS_DB"
        else "análisis automático"
        if source_mode == "AUTO_ANALYSIS"
        else "registro manual"
    )
    lines = [
        "🧪 Economic Meta Policy v1 · SHADOW_ONLY",
        f"Run: {run_id or '—'}",
        f"Fuente: {source_label}",
        "Capital effect: NO",
        "",
    ]
    for symbol in sorted(by_ticker_policy):
        records = sorted(
            by_ticker_policy[symbol].values(),
            key=lambda row: str(row.get("policy_name") or ""),
        )
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
        historical_line = _historical_line(records)
        if historical_line:
            lines.append(historical_line)

    latest_cut = max((str(row.get("as_of") or "") for row in selected), default="")
    lines.extend(
        [
            "",
            f"Corte: {_fmt_dt(latest_cut)}",
            "Primario: 20D · research: 5/10/20/40D",
            "Hist20D = retorno direccional neto de costo research; no es DVA vs HOLD.",
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

    auto_rows = [row for row in rows if _is_auto_analysis_row(row)]
    manual_rows = [row for row in rows if not _is_auto_analysis_row(row)]
    preferred, _ = _preferred_rows(rows)

    runs = {str(row.get("run_id")) for row in rows if row.get("run_id")}
    tickers = {str(row.get("ticker")) for row in rows if row.get("ticker")}
    auto_runs = {str(row.get("run_id")) for row in auto_rows if row.get("run_id")}
    auto_tickers = {str(row.get("ticker")) for row in auto_rows if row.get("ticker")}
    by_policy: dict[str, Counter[str]] = defaultdict(Counter)
    latest = max((str(row.get("as_of") or "") for row in preferred), default="")
    for row in preferred:
        by_policy[str(row.get("policy_name") or "UNKNOWN")][str(row.get("decision") or "UNKNOWN")] += 1

    lines.extend(
        [
            f"Records: {len(rows)} · runs: {len(runs)} · tickers: {len(tickers)}",
            f"Auto análisis: {len(auto_rows)} records · {len(auto_runs)} runs · {len(auto_tickers)} tickers",
            f"Manual/pruebas: {len(manual_rows)} records",
            f"Último corte análisis: {_fmt_dt(latest)}",
            "",
        ]
    )
    for policy in sorted(by_policy):
        counts = by_policy[policy]
        lines.append(f"{policy}: allow={counts.get('ALLOW_SHADOW', 0)} · hold={counts.get('REJECT_TO_HOLD', 0)}")
    return "\n".join(lines)


__all__ = ["render_latest_meta", "render_meta_status"]
