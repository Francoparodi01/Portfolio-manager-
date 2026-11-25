#!/usr/bin/env python3
"""Read-only, end-to-end data integrity audit for Quantia.

It never writes to the database (every query runs in a READ ONLY transaction), never calls the
attribution sync, and never touches planner, guards or thresholds. For each check it reports
PASS / WARN / FAIL / ERROR / INFO, a count, and a small sample; the full evidence of every
non-PASS check goes to CSV files in --out-dir. Paste summary.md back for review.

What it can and cannot prove: it detects internal inconsistencies (double counting, duplicated
movements, outcomes that do not follow from stored candles, leakage, unexplained price jumps,
mismatch between the /viability report and an independent recomputation). It cannot detect a
value that the broker or the price source delivered wrong from the start.

Usage:  python scripts/audit_data_integrity.py [--days 180] [--out-dir output/data_integrity]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ART = ZoneInfo("America/Argentina/Buenos_Aires")
HORIZONS = (5, 10, 20, 40)
RETURN_TOLERANCE = 1e-4          # absolute difference in a return (0.01 percentage points)
JUMP_THRESHOLD = 0.30            # |daily close-to-close| above this needs a corporate action
SAMPLE_ROWS = 8


@dataclass
class Check:
    check_id: str
    title: str
    status: str = "PASS"          # PASS | WARN | FAIL | ERROR | INFO
    count: int = 0
    detail: str = ""
    sample: list[dict] = field(default_factory=list)
    evidence: Optional[pd.DataFrame] = None


# ---------------------------------------------------------------------------------------------
# Pure logic (unit-tested, no database)
# ---------------------------------------------------------------------------------------------

def recompute_outcome(entry_price: float, decided_at: datetime, decision: str,
                      candle_days: list[tuple[date, float]], now: datetime, horizon_days: int) -> Optional[float]:
    """Independent re-implementation of the system's outcome rule.

    The horizon is CALENDAR days from the decision's Buenos Aires date; the exit is the close of
    the first candle whose date is >= that target day; SELL returns are positive when the price falls.
    """
    if not entry_price:
        return None
    target_day = decided_at.astimezone(ART).date() + timedelta(days=horizon_days)
    if target_day > now.astimezone(ART).date():
        return None
    eligible = [close for day, close in sorted(candle_days, key=lambda item: item[0]) if day >= target_day]
    if not eligible:
        return None
    asset_return = (float(eligible[0]) - float(entry_price)) / float(entry_price)
    return -asset_return if str(decision).upper() == "SELL" else asset_return


def classify_movement_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Group broker movements that look identical and say whether they look like duplicates.

    Rows sharing ticker, type, local day, quantity, price and amount but with different
    external ids are either legitimate partial fills (distinct raw payloads) or duplicates
    (identical payload apart from the id).
    """
    if df.empty:
        return pd.DataFrame(columns=["group", "n", "ids", "classification"])
    data = df.copy()
    data["_day"] = pd.to_datetime(data["executed_at"], utc=True).dt.tz_convert(ART).dt.date
    keys = ["ticker", "movement_type", "_day", "quantity", "price", "amount"]
    rows = []
    for number, (key, group) in enumerate(data.groupby(keys, dropna=False), start=1):
        if len(group) < 2:
            continue
        payloads = group["payload_fingerprint"].nunique(dropna=False)
        synthetic = group["external_movement_id"].astype(str).str.startswith("synthetic:").any()
        real_ids = group.loc[~group["external_movement_id"].astype(str).str.startswith("synthetic:"), "external_movement_id"]
        if synthetic and len(real_ids):
            kind = "SYNTHETIC_AND_REAL"
        elif payloads == 1:
            kind = "LIKELY_DUPLICATE"
        else:
            kind = "POSSIBLE_PARTIAL_FILLS"
        rows.append(dict(group=number, ticker=key[0], movement_type=key[1], day=str(key[2]), quantity=key[3],
                         price=key[4], amount=key[5], n=len(group),
                         ids=",".join(group["external_movement_id"].astype(str)), classification=kind))
    return pd.DataFrame(rows)


def price_jumps(candles: pd.DataFrame, threshold: float = JUMP_THRESHOLD) -> pd.DataFrame:
    """Daily close-to-close moves above the threshold, per ticker."""
    if candles.empty:
        return pd.DataFrame(columns=["ticker", "day", "previous_close", "close", "change"])
    frame = candles.sort_values(["ticker", "day"]).copy()
    frame["previous_close"] = frame.groupby("ticker")["close"].shift(1)
    frame["change"] = frame["close"] / frame["previous_close"] - 1.0
    return frame[frame["change"].abs() >= threshold][["ticker", "day", "previous_close", "close", "change"]].reset_index(drop=True)


def compare_with_report(frame: pd.DataFrame, report: Any, cost: float, scope_mask) -> pd.DataFrame:
    """Recompute n, win rate, net EV and IC with plain numpy and compare with the report."""
    rows = []
    for scope, by_horizon in report.metrics.items():
        subset = frame[scope_mask(frame, scope)]
        for horizon, metric in by_horizon.items():
            gross = pd.to_numeric(subset.get(f"effective_outcome_{horizon}"), errors="coerce")
            data = pd.DataFrame({"gross": gross, "score": pd.to_numeric(subset.get("aligned_final_score"), errors="coerce")}).dropna(subset=["gross"])
            net = data["gross"].to_numpy(dtype=float) - cost
            recomputed = dict(n=int(len(net)),
                              net_ev=float(net.mean()) if len(net) else None,
                              win_rate=float((net > 0).mean()) if len(net) else None)
            pair = data.dropna(subset=["score"])
            ic = None
            if len(pair) >= 5 and pair["score"].std() > 1e-12 and pair["gross"].std() > 1e-12:
                ic = float(np.corrcoef(pair["score"].to_numpy(float), pair["gross"].to_numpy(float))[0, 1])
            for name, mine, theirs in (("n", recomputed["n"], metric.n), ("net_ev", recomputed["net_ev"], metric.net_ev),
                                       ("win_rate", recomputed["win_rate"], metric.win_rate), ("ic_final", ic, metric.ic_final)):
                same = (mine is None and theirs is None) or (
                    mine is not None and theirs is not None and abs(float(mine) - float(theirs)) <= 1e-9)
                rows.append(dict(scope=scope, horizon=horizon, metric=name, report=theirs, recomputed=mine, ok=same))
    return pd.DataFrame(rows)


def mature_missing(decisions: pd.DataFrame, now: datetime) -> pd.DataFrame:
    """Rows whose horizon has already passed but whose outcome is still null."""
    rows = []
    today = now.astimezone(ART).date()
    for horizon in HORIZONS:
        column = f"outcome_{horizon}d"
        if column not in decisions.columns:
            continue
        days = pd.to_datetime(decisions["decided_at"], utc=True).dt.tz_convert(ART).dt.date
        mature = days.map(lambda day: day + timedelta(days=horizon) <= today)
        missing = decisions[mature & decisions[column].isna()]
        rows.append(dict(horizon=f"{horizon}d", mature=int(mature.sum()), missing=int(len(missing)),
                         pct=round(100.0 * len(missing) / max(int(mature.sum()), 1), 1),
                         worst_tickers=",".join(f"{t}:{c}" for t, c in missing["ticker"].value_counts().head(5).items())))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------
# Database checks (read-only)
# ---------------------------------------------------------------------------------------------

async def fetch(conn, sql: str, *params) -> pd.DataFrame:
    return pd.DataFrame([dict(record) for record in await conn.fetch(sql, *params)])


def finish(check: Check, evidence: Optional[pd.DataFrame], fail_when_rows: bool = True, warn_only: bool = False) -> Check:
    if evidence is not None and len(evidence):
        check.count = int(len(evidence))
        check.status = "WARN" if warn_only else ("FAIL" if fail_when_rows else "WARN")
        check.evidence = evidence
        check.sample = evidence.head(SAMPLE_ROWS).astype(str).to_dict("records")
    return check


async def check_attributions(conn, cutoff) -> list[Check]:
    checks = []
    c = Check("A1", "Movement linked to more than one attribution")
    df = await fetch(conn, """SELECT broker_movement_id, COUNT(*) AS links FROM plan_execution_attribution_movements
                              GROUP BY broker_movement_id HAVING COUNT(*) > 1""")
    checks.append(finish(c, df))

    c = Check("A2", "Attribution amount differs from the sum of its linked movements (> 1 ARS)")
    df = await fetch(conn, """SELECT a.id, a.ticker, a.side, a.executed_at, a.executed_amount_ars,
                                     COALESCE(SUM(m.amount_ars), 0) AS linked_sum
                              FROM plan_execution_attributions a
                              LEFT JOIN plan_execution_attribution_movements m ON m.attribution_id = a.id
                              WHERE a.executed_at >= $1
                              GROUP BY a.id HAVING ABS(a.executed_amount_ars - COALESCE(SUM(m.amount_ars), 0)) > 1""", cutoff)
    checks.append(finish(c, df))

    c = Check("A3", "follow_ratio differs from executed / target")
    df = await fetch(conn, """SELECT id, ticker, side, executed_amount_ars, target_amount_ars, follow_ratio
                              FROM plan_execution_attributions
                              WHERE executed_at >= $1 AND target_amount_ars > 0
                                AND ABS(follow_ratio - executed_amount_ars / target_amount_ars) > 0.01""", cutoff)
    checks.append(finish(c, df))

    c = Check("A4", "Eligible for viability but temporally ambiguous")
    df = await fetch(conn, """SELECT id, ticker, side, executed_at, temporal_quality FROM plan_execution_attributions
                              WHERE executed_at >= $1 AND eligible_for_viability AND temporal_quality <> 'CONFIRMED_SEQUENCE'""", cutoff)
    checks.append(finish(c, df))

    c = Check("A5", "Representative plan missing from the bridge, or bridge plan on another ticker/side")
    df = await fetch(conn, """SELECT a.id, a.ticker, a.side, d.ticker AS plan_ticker, d.decision AS plan_decision, p.decision_log_id
                              FROM plan_execution_attributions a
                              LEFT JOIN plan_execution_attribution_plans p
                                     ON p.attribution_id = a.id AND p.is_representative
                              LEFT JOIN decision_log d ON d.id = p.decision_log_id
                              WHERE a.executed_at >= $1
                                AND (p.decision_log_id IS NULL OR d.ticker <> a.ticker
                                     OR (a.side = 'SELL' AND d.decision NOT LIKE 'SELL%')
                                     OR (a.side = 'BUY' AND d.decision NOT LIKE 'BUY%'))""", cutoff)
    checks.append(finish(c, df))
    return checks


async def check_movement_duplicates(conn, cutoff) -> Check:
    check = Check("M1", "Identical broker movements (same ticker/type/day/qty/price/amount) under different ids")
    df = await fetch(conn, """SELECT id, external_movement_id, executed_at, movement_type, ticker, quantity, price, amount,
                                     md5(COALESCE(raw_payload::text, '')) AS payload_fingerprint
                              FROM broker_movements
                              WHERE executed_at >= $1 AND ticker IS NOT NULL AND quantity IS NOT NULL AND price IS NOT NULL""", cutoff)
    groups = classify_movement_duplicates(df)
    if groups.empty:
        return check
    linked = await fetch(conn, "SELECT b.external_movement_id FROM plan_execution_attribution_movements l "
                               "JOIN broker_movements b ON b.id = l.broker_movement_id")
    linked_ids = set(linked["external_movement_id"].astype(str)) if not linked.empty else set()
    groups["linked_to_a_plan"] = groups["ids"].map(lambda ids: any(i in linked_ids for i in ids.split(",")))
    likely = groups[groups["classification"] != "POSSIBLE_PARTIAL_FILLS"]
    check = finish(check, groups, warn_only=likely.empty)
    check.detail = (f"{len(likely)} grupos LIKELY_DUPLICATE/SYNTHETIC_AND_REAL, "
                    f"{len(groups) - len(likely)} POSSIBLE_PARTIAL_FILLS (revisar a mano)")
    return check


async def check_decisions_and_outcomes(conn, cutoff, now) -> list[Check]:
    checks = []
    decisions = await fetch(conn, """SELECT id, decided_at, ticker, decision, decision_type, price_at_decision,
                                            outcome_5d, outcome_10d, outcome_20d, outcome_40d, outcome_filled_at, outcome_basis
                                     FROM decision_log WHERE decided_at >= $1""", cutoff)
    c = Check("D1", "Decision values outside BUY/SELL/HOLD (SELL_PARTIAL/SELL_FULL would be scored as BUY)")
    if decisions.empty:
        c.status, c.detail = "INFO", "sin decisiones en la ventana"
        return [c]
    odd = decisions[~decisions["decision"].astype(str).str.upper().isin(["BUY", "SELL", "HOLD"])]
    odd = odd[odd[["outcome_5d", "outcome_10d", "outcome_20d", "outcome_40d"]].notna().any(axis=1)]
    c.detail = "valores: " + ", ".join(f"{k}={v}" for k, v in decisions["decision"].value_counts().items())
    checks.append(finish(c, odd[["id", "ticker", "decision", "decision_type", "outcome_5d"]]))

    c = Check("D2", "Outcome filled before its horizon matured (leakage) or without any candle at the target")
    leaks = []
    for horizon in HORIZONS:
        col = f"outcome_{horizon}d"
        rows = decisions[decisions[col].notna() & decisions["outcome_filled_at"].notna()]
        for _, row in rows.iterrows():
            decided_day = pd.Timestamp(row["decided_at"]).tz_convert(ART).date()
            filled_day = pd.Timestamp(row["outcome_filled_at"]).tz_convert(ART).date()
            if filled_day < decided_day + timedelta(days=horizon):
                leaks.append(dict(id=row["id"], ticker=row["ticker"], horizon=f"{horizon}d", decided_day=str(decided_day),
                                  filled_day=str(filled_day)))
    checks.append(finish(c, pd.DataFrame(leaks)))
    c = Check("D3", "Mature horizon with null outcome (coverage)")
    cov = mature_missing(decisions, now)
    c.status = "INFO"
    c.detail = "; ".join(f"{r.horizon}: {r.missing}/{r.mature} ({r.pct}%)" for r in cov.itertuples())
    c.evidence = cov
    checks.append(c)

    # --- recompute outcomes from stored candles --------------------------------------------
    c = Check("O1", "Stored outcome differs from recomputation on market_candles (> 0.01 pp)")
    base = decisions[decisions["decision"].astype(str).str.upper().isin(["BUY", "SELL"])
                     & decisions["price_at_decision"].notna() & (decisions["price_at_decision"] > 0)]
    if base.empty:
        c.status, c.detail = "INFO", "sin filas comparables"
        checks.append(c)
        return checks
    tickers = sorted(base["ticker"].str.upper().unique())
    candles = await fetch(conn, """SELECT ticker, (ts AT TIME ZONE 'UTC')::date AS day, close_price AS close, source
                                   FROM market_candles WHERE interval = '1d' AND ticker = ANY($1::text[]) ORDER BY ts""", tickers)
    adjusted = await fetch(conn, """SELECT DISTINCT e.ticker FROM corporate_event_instrument_effects e
                                    JOIN corporate_events ev ON ev.id = e.event_id
                                    WHERE ev.lifecycle_status IN ('CONFIRMED','EFFECTIVE') AND ev.effective_at >= $1""", cutoff - timedelta(days=60))
    adjusted_tickers = set(adjusted["ticker"].str.upper()) if not adjusted.empty else set()
    by_ticker = {t: g for t, g in candles.groupby(candles["ticker"].str.upper())} if not candles.empty else {}
    diffs, checked, ambiguous = [], 0, set()
    for _, row in base.iterrows():
        group = by_ticker.get(str(row["ticker"]).upper())
        if group is None:
            continue
        # one close per day; several different closes on a day make the recomputation ambiguous
        per_day = group.groupby("day")["close"].agg(["min", "max", "first"])
        if (per_day["max"] - per_day["min"]).abs().max() > 1e-6:
            ambiguous.add(str(row["ticker"]).upper())
        days = [(pd.Timestamp(day).date(), float(value)) for day, value in per_day["first"].items()]
        for horizon in HORIZONS:
            stored = row[f"outcome_{horizon}d"]
            if pd.isna(stored):
                continue
            mine = recompute_outcome(float(row["price_at_decision"]), pd.Timestamp(row["decided_at"]).to_pydatetime(),
                                     row["decision"], days, now, horizon)
            checked += 1
            if mine is None or abs(float(stored) - mine) > RETURN_TOLERANCE:
                ticker = str(row["ticker"]).upper()
                diffs.append(dict(id=row["id"], ticker=ticker, horizon=f"{horizon}d", stored=float(stored), recomputed=mine,
                                  diff=None if mine is None else float(stored) - mine,
                                  note="ticker con evento corporativo (ajuste esperable)" if ticker in adjusted_tickers
                                  else ("velas ambiguas ese dia" if ticker in ambiguous else "")))
    unexplained = [d for d in diffs if not d["note"]]
    frame = pd.DataFrame(diffs)
    c.detail = f"{checked} outcomes comparados; {len(diffs)} difieren ({len(unexplained)} sin explicacion)"
    finish(c, frame, warn_only=not unexplained)
    c.count = len(diffs)
    checks.append(c)
    return checks


async def check_candles(conn, cutoff) -> list[Check]:
    checks = []
    c = Check("C1", "Candles with impossible values (<= 0, high < low, close outside the range)")
    df = await fetch(conn, """SELECT ticker, ts, interval, open_price, high_price, low_price, close_price, source FROM market_candles
                              WHERE interval = '1d' AND ts >= $1 AND (close_price <= 0 OR high_price < low_price
                                 OR close_price > high_price * 1.0001 OR close_price < low_price * 0.9999)""", cutoff - timedelta(days=60))
    checks.append(finish(c, df))

    c = Check("C2", "Same ticker and day with different closes")
    df = await fetch(conn, """SELECT ticker, (ts AT TIME ZONE 'UTC')::date AS day, COUNT(*) AS rows, MIN(close_price) AS min_close,
                                     MAX(close_price) AS max_close, ARRAY_AGG(DISTINCT source) AS sources
                              FROM market_candles WHERE interval = '1d' AND ts >= $1
                              GROUP BY ticker, (ts AT TIME ZONE 'UTC')::date
                              HAVING COUNT(*) > 1 AND MAX(close_price) <> MIN(close_price)""", cutoff - timedelta(days=60))
    checks.append(finish(c, df))

    c = Check("C3", f"Daily jump >= {JUMP_THRESHOLD:.0%} with no confirmed corporate action within 5 days")
    candles = await fetch(conn, """SELECT ticker, (ts AT TIME ZONE 'UTC')::date AS day, close_price AS close FROM market_candles
                                   WHERE interval = '1d' AND ts >= $1""", cutoff - timedelta(days=60))
    jumps = price_jumps(candles)
    if not jumps.empty:
        events = await fetch(conn, """SELECT e.ticker, (ev.effective_at AT TIME ZONE 'UTC')::date AS day FROM corporate_event_instrument_effects e
                                      JOIN corporate_events ev ON ev.id = e.event_id
                                      WHERE ev.lifecycle_status IN ('CONFIRMED','EFFECTIVE')""")
        known = {}
        for row in events.itertuples():
            known.setdefault(str(row.ticker).upper(), []).append(row.day)
        jumps = jumps[[not any(abs((pd.Timestamp(r.day).date() - d).days) <= 5 for d in known.get(str(r.ticker).upper(), []))
                       for r in jumps.itertuples()]]
    checks.append(finish(c, jumps))
    return checks


async def check_costs(conn, cutoff, model_cost: float) -> Check:
    check = Check("K1", f"Real fee rate vs the modelled {model_cost:.2%} per operation")
    df = await fetch(conn, """SELECT side, fees_ars / NULLIF(gross_amount_ars, 0) AS rate FROM broker_fills
                              WHERE executed_at >= $1 AND fees_ars IS NOT NULL AND gross_amount_ars > 0""", cutoff)
    check.status = "INFO"
    if df.empty:
        check.detail = "broker_fills sin comisiones registradas: no se puede contrastar el costo"
        return check
    stats = df.groupby("side")["rate"].agg(["count", "median", lambda s: s.quantile(0.9)])
    check.detail = "; ".join(f"{side}: n={int(r['count'])}, mediana={r['median']:.3%}, p90={r.iloc[2]:.3%}" for side, r in stats.iterrows())
    check.detail += " | el modelo agrega ademas slippage; compara solo comisiones+derechos"
    return check


async def check_report(config, cost: float) -> list[Check]:
    from src.analysis.viability_audit import (ACTIVE_ACTIONS, _prepare_frame, _scope_mask,  # noqa: F401
                                              load_viability_decision_log, run_viability_audit_sync)
    raw = await load_viability_decision_log(config)
    checks = []
    c = Check("R1", "Cohorts overlap, or an executed row belongs to no cohort")
    if raw.empty:
        c.status, c.detail = "INFO", "sin filas"
        return [c]
    frame = _prepare_frame(raw, config.horizons)
    masks = {scope: _scope_mask(frame, scope) for scope in ("bot_only", "followed", "manual_only")}
    membership = pd.DataFrame(masks).sum(axis=1)
    status = frame["status"].fillna("").astype(str).str.upper()
    unclassified = frame[(membership == 0) & status.str.startswith("EXECUTED")]
    overlap = frame[membership > 1]
    c.detail = ", ".join(f"{k}={int(v.sum())}" for k, v in masks.items()) + f" | superpuestas={len(overlap)} | ejecutadas sin cohorte={len(unclassified)}"
    evidence = pd.concat([overlap.assign(problem="OVERLAP"), unclassified.assign(problem="UNCLASSIFIED")])
    keep = [col for col in ("id", "ticker", "decision", "source", "status", "decided_at", "problem") if col in evidence.columns]
    checks.append(finish(c, evidence[keep] if len(evidence) else None))

    c = Check("R2", "/viability report differs from an independent recomputation (n, net EV, win rate, IC)")
    report = run_viability_audit_sync(raw, config)
    comparison = compare_with_report(frame, report, cost, _scope_mask)
    wrong = comparison[~comparison["ok"]]
    c.detail = f"{len(comparison)} metricas comparadas; veredicto: {report.verdict}"
    checks.append(finish(c, wrong if len(wrong) else None))
    return checks


# ---------------------------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------------------------

def override_host(url: str, host: Optional[str]) -> str:
    """Return the URL with only its host replaced (user, password, port and database are kept).

    The .env points at the Docker service name (`db`), which only resolves inside the Docker
    network; from the host machine the same database is published on localhost.
    """
    if not host:
        return url
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(url)
    userinfo, at, hostport = parts.netloc.rpartition("@")
    port = f":{parts.port}" if parts.port else ""
    return urlunsplit(parts._replace(netloc=f"{userinfo}{at}{host}{port}"))


def render_summary(checks: list[Check], meta: dict) -> str:
    order = {"FAIL": 0, "ERROR": 1, "WARN": 2, "INFO": 3, "PASS": 4}
    lines = [f"# Data integrity audit — {meta['generated_at']}", "",
             f"Ventana: {meta['days']}d desde {meta['cutoff']} | solo lectura | costo modelado {meta['cost']:.2%}", "",
             "| id | estado | n | chequeo / detalle |", "|---|---|---:|---|"]
    for check in sorted(checks, key=lambda item: (order.get(item.status, 9), item.check_id)):
        detail = f" — {check.detail}" if check.detail else ""
        lines.append(f"| {check.check_id} | {check.status} | {check.count} | {check.title}{detail} |")
    lines += ["", "## Muestras (primeras filas de cada chequeo no-PASS)"]
    for check in checks:
        if check.status in ("FAIL", "WARN", "ERROR") and check.sample:
            lines += ["", f"**{check.check_id}**", "```", *[json.dumps(row, ensure_ascii=False)[:240] for row in check.sample[:5]], "```"]
    lines += ["", "Nota: los horizontes 5/10/20/40D se miden en DIAS CALENDARIO desde la fecha ART de la decision "
              "(primer cierre >= fecha objetivo), no en ruedas."]
    return "\n".join(lines) + "\n"


async def guarded(label: str, coroutine) -> list[Check]:
    try:
        result = await coroutine
        return result if isinstance(result, list) else [result]
    except Exception as exc:  # a missing column or table must not hide the other checks
        return [Check(label, f"Check could not run: {label}", status="ERROR", detail=f"{type(exc).__name__}: {str(exc)[:200]}")]


async def main_async(args) -> int:
    import asyncpg
    from src.analysis.viability_audit import ViabilityAuditConfig
    from src.core.config import get_config

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.days)
    cost = args.cost_bps / 10_000.0
    url = override_host(get_config().database.url, args.db_host)
    dsn = url.replace("postgresql+asyncpg://", "postgresql://")
    config = ViabilityAuditConfig(database_url=url, days=args.days, cost_bps=args.cost_bps, min_sample=args.min_sample)

    checks: list[Check] = []
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(readonly=True):
            checks += await guarded("A", check_attributions(conn, cutoff))
            checks += await guarded("M1", check_movement_duplicates(conn, cutoff))
            checks += await guarded("D", check_decisions_and_outcomes(conn, cutoff, now))
            checks += await guarded("C", check_candles(conn, cutoff))
            checks += await guarded("K1", check_costs(conn, cutoff, cost))
    finally:
        await conn.close()
    checks += await guarded("R", check_report(config, cost))

    out_dir = Path(args.out_dir) / now.strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=False)
    for check in checks:
        if check.evidence is not None and len(check.evidence):
            check.evidence.to_csv(out_dir / f"{check.check_id}.csv", index=False)
    meta = dict(generated_at=now.isoformat(timespec="seconds"), days=args.days, cutoff=cutoff.date().isoformat(), cost=cost)
    (out_dir / "summary.md").write_text(render_summary(checks, meta), encoding="utf-8")
    (out_dir / "report.json").write_text(json.dumps(
        [dict(id=c.check_id, title=c.title, status=c.status, count=c.count, detail=c.detail) for c in checks],
        ensure_ascii=False, indent=2), encoding="utf-8")
    counts = {status: sum(1 for c in checks if c.status == status) for status in ("FAIL", "ERROR", "WARN", "INFO", "PASS")}
    print(json.dumps(dict(out_dir=str(out_dir), **counts), ensure_ascii=False))
    return 1 if counts["FAIL"] or counts["ERROR"] else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only data integrity audit")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--cost-bps", type=float, default=75.0)
    parser.add_argument("--min-sample", type=int, default=30)
    parser.add_argument("--out-dir", default="output/data_integrity")
    parser.add_argument("--db-host", default=None, help="replace only the host of the configured DATABASE_URL (e.g. localhost)")
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
