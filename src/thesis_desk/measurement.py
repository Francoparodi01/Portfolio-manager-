"""Prospective cohorts and paper accounting. Unknown/missing outcomes are never filled with zero."""
from __future__ import annotations

from collections import Counter
from datetime import timedelta
from decimal import Decimal
import json

from src.backtesting.thesis_desk import ART, decimal, instant
from src.core.market_calendar import is_trading_day
from src.thesis_desk.contracts import qualification_error


def session_target(start, sessions):
    day, count = start.astimezone(ART).date(), 0
    while count < sessions:
        day += timedelta(days=1)
        count += bool(is_trading_day(day))
    return start.astimezone(ART).replace(year=day.year, month=day.month, day=day.day)


def observe(store, rows, result, at):
    eligible = {r["ticker"] for r in result.get("catalog", []) if r["status"] == "IDENTIFIED"}
    if result["paper"]["market_state"] == "OPEN":
        for row in rows["intraday_points"]:
            stamp = instant(row["market_ts"])
            if row["ticker"] in eligible and timedelta(0) <= at - stamp <= timedelta(minutes=5):
                store.conn.execute("INSERT OR IGNORE INTO desk_marks VALUES(?,?,?,?)", (row["ticker"], stamp.isoformat(), at.isoformat(), str(row["last_price"])))
    facts = {f["id"]: f for f in result.get("research", {}).get("findings", [])}
    for card in result["watchlist"]:
        if card["pm_decision"] == "WATCHLIST":
            store.conn.execute("INSERT OR IGNORE INTO desk_signals(id,ticker,first_seen) VALUES(?,?,?)", (card["fact_id"], card["ticker"], at.isoformat()))
            fact = facts.get(card["fact_id"])
            if fact:
                store.conn.execute("INSERT OR IGNORE INTO desk_signal_evidence VALUES(?,?)", (card["fact_id"], json.dumps(fact)))
    for sid, ticker, first_seen in store.conn.execute("SELECT id,ticker,first_seen FROM desk_signals WHERE anchor_at IS NULL").fetchall():
        mark = store.conn.execute("SELECT market_at,price FROM desk_marks WHERE ticker=? AND observed_at>=? AND market_at>=? ORDER BY observed_at,market_at LIMIT 1", (ticker, first_seen, first_seen)).fetchone()
        if mark:
            store.conn.execute("UPDATE desk_signals SET anchor_at=?,anchor_price=? WHERE id=?", (*mark, sid))


def outcome_metrics(store, at):
    observed = store.conn.execute("SELECT s.id,s.ticker,s.first_seen,s.anchor_at,s.anchor_price,e.payload FROM desk_signals s LEFT JOIN desk_signal_evidence e ON e.id=s.id ORDER BY s.first_seen,s.id").fetchall()
    signals, exclusions = [], Counter()
    for *signal, payload in observed:
        fact = json.loads(payload) if payload else None
        reason = qualification_error(fact["kind"], fact["quote"]) if fact else "SOURCE_PROVENANCE_UNKNOWN"
        if reason:
            exclusions[reason] += 1
        else:
            signals.append(signal)
    result = []
    for horizon in (5, 20):
        pending_anchor = immature = missing = 0
        values = []
        for _, ticker, _, anchored, initial in signals:
            if not anchored:
                pending_anchor += 1
                continue
            target = session_target(instant(anchored), horizon)
            if at < target:
                immature += 1
                continue
            # First observed tradable quote at/after the same local time, within 60 minutes of the target.
            candidates = store.conn.execute("SELECT market_at,observed_at,price FROM desk_marks WHERE ticker=? ORDER BY market_at", (ticker,)).fetchall()
            marks = [r for r in candidates if target <= instant(r[0]) <= target + timedelta(minutes=60) and instant(r[1]) <= at]
            if not marks:
                missing += 1
                continue
            final = decimal(marks[0][2])
            entry = decimal(initial)
            net = final * Decimal("0.9985") * Decimal("0.994") / (entry * Decimal("1.0015") * Decimal("1.006")) - 1
            values.append(net)
        mature = len(values) + missing
        result.append(dict(horizon_sessions=horizon, cohort=len(signals), pending_anchor=pending_anchor, immature=immature,
                           observed_cohort=len(observed), excluded=sum(exclusions.values()), exclusion_reasons=dict(exclusions),
                           mature=mature, evaluated=len(values), missing=mature - len(values),
                           coverage_pct=str(Decimal(len(values)) / mature * 100) if mature else None,
                           mean_net_hypothetical_return_pct=str(sum(values) / len(values) * 100) if values else None,
                           positive_fraction=str(Decimal(sum(v > 0 for v in values)) / len(values)) if values else None,
                           status="EVALUATED_OBSERVATIONAL" if values else "NOT_EVALUABLE",
                           basis="FIRST_WATCHLIST_PER_THESIS_NOT_FILLS; fixed 0.6% fee + 0.15% slippage each side; not causal alpha"))
    return result


def measure(store, result, state, at):
    history = store.runs() + [result]
    fills = [f for r in history for f in r["paper"]["fills"]]
    cycles = [c for r in history for c in r["paper"]["closed_cycles"]]
    catalog = result.get("catalog", [])
    statuses = {r["ticker"]: r for r in result["source_status"] if r.get("ticker")}
    queued = {r["ticker"] for r in result["research"].get("queue", [])}
    identified = [r for r in catalog if r["status"] == "IDENTIFIED"]
    sourced = [r for r in identified if statuses.get(r["ticker"], {}).get("status") == "CAPTURED" and not statuses.get(r["ticker"], {}).get("truncated")]
    reviewed = [r for r in sourced if r["ticker"] not in queued and r["ticker"] not in {e.get("ticker") for e in result["research"]["errors"]}]
    curve = {}
    for r in history:
        v = r["paper"]["valuation"]
        curve[instant(r["at"]).astimezone(ART).date().isoformat()] = dict(at=r["at"], nav_ars=v["nav_ars"], cash_ars=v["cash_ars"])
    nav, peak = decimal(result["paper"]["valuation"]["nav_ars"]), decimal(state["peak"])
    closed_pnl = [decimal(c["net_pnl_ars"]) for c in cycles]
    blocks = Counter(reason for reasons in result["entry_blocks"].values() for reason in set(reasons))
    return dict(schema="thesis-desk-metrics-v1", at=at.isoformat(), mode="PAPER_ONLY", first_run_at=history[0]["at"], runs=len(history),
                coverage=dict(observed_symbols=len({r["ticker"] for r in catalog}), observed_instruments=len(catalog), identified_equities=len(identified),
                              earnings_calendar_covered=sum("EARNINGS_CALENDAR_UNKNOWN" not in result["entry_blocks"].get(r["ticker"], []) for r in identified),
                              identity_status=dict(Counter(r["status"] for r in catalog)), source_complete=len(sourced), reviewed_equities=len(reviewed),
                              source_coverage_pct=str(Decimal(len(sourced)) / len(identified) * 100) if identified else None,
                              reviewed_coverage_pct=str(Decimal(len(reviewed)) / len(identified) * 100) if identified else None,
                              pending_documents=len(result["research"].get("queue", [])), entry_block_reasons=dict(blocks)),
                paper=dict(nav_ars=str(nav), cash_ars=state["cash"], fills=len(fills), closed_cycles=len(cycles),
                           fees_ars=state["fees"], realized_pnl_ars=state["realized"], open_positions=len(state["positions"]),
                           win_rate=str(Decimal(sum(p > 0 for p in closed_pnl)) / len(cycles)) if cycles else None,
                           total_return_pct=str((nav / decimal(result["initial_cash"]) - 1) * 100) if fills else None,
                           current_drawdown_pct=str((nav / peak - 1) * 100), baseline="CASH_NOMINAL_0_PCT_NO_INTEREST",
                           status="PAPER_OBSERVED_ONLY" if fills else "NO_TRADES_YET", economic_verification="NOT_VERIFIED",
                           quality_flags=result["paper"]["quality_flags"]),
                curve=list(curve.values()), outcomes=outcome_metrics(store, at),
                definitions=dict(coverage_denominator="Identified equities; ETFs and unknown identities reported separately",
                                 source_complete="Captured bounded official feed, not exhaustive knowledge of issuer",
                                 curve="Last observed NAV per local day; not official daily close",
                                 cohort="One first watchlist event per thesis; repeats do not increase denominator"))
