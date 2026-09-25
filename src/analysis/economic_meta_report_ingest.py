"""Ingest the latest rendered Quantia analysis into Economic Meta Policy shadow.

This presentation-side adapter accepts both the detailed /analisis_full report
and the compact /analisis report. It evaluates the rendered portfolio snapshot
with the same preregistered META-A/B/C challengers without writing decision_log,
orders, portfolio weights or broker state.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.analysis.economic_meta_policy import evaluate_all_preregistered
from src.analysis.economic_meta_store import DEFAULT_SHADOW_PATH, EconomicMetaShadowStore

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

# Detailed /analisis_full portfolio line:
# MU → SELL_PARTIAL → -$348.400 ARS | score -0.110 | ... | peso 12.0% → 0.0%
_POSITION_RE = re.compile(
    r"^[^A-Z0-9]*(?P<ticker>[A-Z0-9.\-]+)\s*→\s*(?P<action>[A-Z_]+)"
    r"(?:\s*→[^|]*)?\s*\|\s*score\s*(?P<score>[+-]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Compact /analisis portfolio line after markdown removal:
# MU -0.110 T+0.060 ... R=RANGE ... 12.0%→0.0% SELL_PARTIAL
_COMPACT_POSITION_RE = re.compile(
    r"^[^A-Z0-9]*(?P<ticker>[A-Z0-9.\-]+)\s+"
    r"(?P<score>[+-]?\d+(?:\.\d+)?)\b.*?"
    r"(?P<current>\d+(?:\.\d+)?)%\s*→\s*(?P<target>\d+(?:\.\d+)?)%\s+"
    r"(?P<action>SELL_PARTIAL|SELL_FULL|SELL|REDUCE|BUY_PARTIAL|BUY_FULL|BUY|WATCH|HOLD)\b",
    re.IGNORECASE,
)

_WEIGHT_RE = re.compile(
    r"peso\s*(?P<current>\d+(?:\.\d+)?)%\s*→\s*(?P<target>\d+(?:\.\d+)?)%",
    re.IGNORECASE,
)
_REGIME_RE = re.compile(r"Régimen técnico:\s*(?P<regime>[A-Z_]+)", re.IGNORECASE)
_INLINE_REGIME_RE = re.compile(r"\bR=(?P<regime>[A-Z_]+)", re.IGNORECASE)
_TIMESTAMP_RE = re.compile(
    r"(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<time>\d{2}:\d{2})\s+ART",
    re.IGNORECASE,
)
_SHORT_TIMESTAMP_RE = re.compile(
    r"(?P<date>\d{2}/\d{2})(?!/\d{4})\s+(?P<time>\d{2}:\d{2})\s+ART",
    re.IGNORECASE,
)
_PORTFOLIO_RE = re.compile(
    r"(?:Portfolio:\s*|💼\s*)\$(?P<value>[0-9.]+)\s*ARS",
    re.IGNORECASE,
)
_PLAN_SELL_RE = re.compile(
    r"(?:Plan ventas|Ventas):\s*\$(?P<value>[0-9.]+)\s*ARS",
    re.IGNORECASE,
)
_PLAN_BUY_RE = re.compile(
    r"(?:Plan compras|Compras):\s*\$(?P<value>[0-9.]+)\s*ARS",
    re.IGNORECASE,
)
_FEES_RE = re.compile(
    r"(?:Fees estimados|Fees):\s*\$(?P<value>[0-9.]+)\s*ARS",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ParsedAnalysisCandidate:
    ticker: str
    raw_action: str
    candidate_action: str
    score: float
    regime: str
    current_weight: float | None
    target_weight: float | None


def _plain(report: str) -> str:
    text = html.unescape(str(report or ""))
    text = re.sub(r"<[^>]+>", "", text)
    # Telegram markdown is presentation-only and otherwise breaks compact-line parsing.
    text = text.replace("**", "").replace("`", "")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _money(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return float(value.replace(".", ""))
    except ValueError:
        return 0.0


def _timestamp(text: str) -> datetime:
    match = _TIMESTAMP_RE.search(text)
    if match:
        try:
            local = datetime.strptime(
                f"{match.group('date')} {match.group('time')}",
                "%d/%m/%Y %H:%M",
            ).replace(tzinfo=ART_TZ)
            return local.astimezone(timezone.utc)
        except ValueError:
            pass

    # Compact /analisis uses "CIERRE DE RUEDA — DD/MM HH:MM ART" without year.
    short = _SHORT_TIMESTAMP_RE.search(text)
    if short:
        now_art = datetime.now(ART_TZ)
        try:
            local = datetime.strptime(
                f"{short.group('date')}/{now_art.year} {short.group('time')}",
                "%d/%m/%Y %H:%M",
            ).replace(tzinfo=ART_TZ)
            return local.astimezone(timezone.utc)
        except ValueError:
            pass

    return datetime.now(timezone.utc)


def _normalize_action(raw_action: str, current: float | None, target: float | None) -> str:
    raw = str(raw_action or "").upper().strip()
    if raw in {"SELL_PARTIAL", "REDUCE", "TRIM"}:
        return "REDUCE"
    if raw in {"SELL", "SELL_FULL", "EXIT", "CLOSE"}:
        return "SELL"
    if raw in {"BUY", "BUY_FULL", "BUY_PARTIAL", "ADD"}:
        return "BUY"
    if raw == "WATCH":
        if current is not None and target is not None:
            if target > current + 0.002:
                return "BUY"
            if target < current - 0.002:
                return "REDUCE"
        return "HOLD"
    return "HOLD" if raw == "HOLD" else raw


def parse_analysis_report(
    report: str,
) -> tuple[datetime, list[ParsedAnalysisCandidate], dict[str, float]]:
    text = _plain(report)
    lines = [line.strip() for line in text.splitlines()]
    parsed: list[ParsedAnalysisCandidate] = []

    for index, line in enumerate(lines):
        detailed = _POSITION_RE.match(line)
        compact = None if detailed else _COMPACT_POSITION_RE.match(line)
        if not detailed and not compact:
            continue

        match = detailed or compact
        assert match is not None
        ticker = match.group("ticker").upper()
        raw_action = match.group("action").upper()
        try:
            score = float(match.group("score"))
        except (TypeError, ValueError):
            continue

        if detailed:
            weight_match = _WEIGHT_RE.search(line)
            current = float(weight_match.group("current")) / 100.0 if weight_match else None
            target = float(weight_match.group("target")) / 100.0 if weight_match else None
        else:
            current = float(match.group("current")) / 100.0
            target = float(match.group("target")) / 100.0

        regime = "UNKNOWN"
        inline = _INLINE_REGIME_RE.search(line)
        if inline:
            regime = inline.group("regime").upper()
        elif detailed:
            for lookahead in lines[index + 1 : index + 4]:
                regime_match = _REGIME_RE.search(lookahead)
                if regime_match:
                    regime = regime_match.group("regime").upper()
                    break

        parsed.append(
            ParsedAnalysisCandidate(
                ticker=ticker,
                raw_action=raw_action,
                candidate_action=_normalize_action(raw_action, current, target),
                score=score,
                regime=regime,
                current_weight=current,
                target_weight=target,
            )
        )

    portfolio_match = _PORTFOLIO_RE.search(text)
    sell_match = _PLAN_SELL_RE.search(text)
    buy_match = _PLAN_BUY_RE.search(text)
    fee_match = _FEES_RE.search(text)
    portfolio_ars = _money(portfolio_match.group("value")) if portfolio_match else 0.0
    gross_sell_ars = _money(sell_match.group("value")) if sell_match else 0.0
    gross_buy_ars = _money(buy_match.group("value")) if buy_match else 0.0
    fees_ars = _money(fee_match.group("value")) if fee_match else 0.0

    # Turnover reflects the actually operable rendered plan. WATCH/HOLD optimizer
    # deltas are deliberately excluded from the economic gate.
    if portfolio_ars > 0 and (gross_sell_ars > 0 or gross_buy_ars > 0):
        turnover = max(gross_sell_ars, gross_buy_ars) / portfolio_ars
    else:
        buy_delta = 0.0
        sell_delta = 0.0
        for item in parsed:
            if item.current_weight is None or item.target_weight is None:
                continue
            delta = item.target_weight - item.current_weight
            if item.raw_action in {"BUY", "BUY_FULL", "BUY_PARTIAL", "ADD"}:
                buy_delta += max(0.0, delta)
            elif item.raw_action in {
                "SELL", "SELL_FULL", "SELL_PARTIAL", "REDUCE", "TRIM", "EXIT", "CLOSE"
            }:
                sell_delta += max(0.0, -delta)
        turnover = max(buy_delta, sell_delta)

    gross_traded = gross_sell_ars + gross_buy_ars
    cost_bps = (fees_ars / gross_traded * 10_000.0) if gross_traded > 0 else 0.0
    metrics = {
        "portfolio_ars": portfolio_ars,
        "gross_sell_ars": gross_sell_ars,
        "gross_buy_ars": gross_buy_ars,
        "fees_ars": fees_ars,
        "portfolio_turnover": max(0.0, min(turnover, 1.0)),
        "estimated_cost_bps": max(0.0, cost_bps),
    }
    return _timestamp(text), parsed, metrics


def ingest_analysis_report(
    report: str,
    *,
    store_path: str | Path = DEFAULT_SHADOW_PATH,
    source: str = "telegram-analysis-report",
) -> dict[str, Any]:
    as_of, candidates, metrics = parse_analysis_report(report)
    if not candidates:
        return {"candidate_count": 0, "records_written": 0, "status": "NO_ANALYSIS_ROWS"}

    store = EconomicMetaShadowStore(store_path)
    try:
        existing_rows = store.read_all()
    except Exception:
        existing_rows = []
    existing = {
        (str(row.get("opportunity_id") or ""), str(row.get("policy_name") or ""))
        for row in existing_rows
        if row.get("opportunity_id") and row.get("policy_name")
    }

    stamp = as_of.strftime("%Y%m%dT%H%M%SZ")
    run_id = f"analysis-report-{stamp}"
    written = 0
    for item in candidates:
        opportunity_id = f"analysis:report:{stamp}:{item.ticker}"
        payload = {
            "ticker": item.ticker,
            "candidate_action": item.candidate_action,
            "candidate_score": item.score,
            "as_of": as_of,
            "estimated_cost_bps": metrics["estimated_cost_bps"],
            "portfolio_turnover": metrics["portfolio_turnover"],
            "market_regime": item.regime,
            "expected_edge_vs_hold_bps": None,
            "edge_uncertainty_bps": None,
            "opportunity_id": opportunity_id,
        }
        for record in evaluate_all_preregistered(payload, run_id=run_id):
            key = (opportunity_id, record.policy_name)
            if key in existing:
                continue
            store.append(record)
            existing.add(key)
            written += 1

    return {
        "status": "SHADOW_ONLY",
        "source": source,
        "run_id": run_id,
        "candidate_count": len(candidates),
        "records_written": written,
        "portfolio_turnover": metrics["portfolio_turnover"],
        "estimated_cost_bps": metrics["estimated_cost_bps"],
        "capital_effect": False,
    }


__all__ = [
    "ParsedAnalysisCandidate",
    "ingest_analysis_report",
    "parse_analysis_report",
]
