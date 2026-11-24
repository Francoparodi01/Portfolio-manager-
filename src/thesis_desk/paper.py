"""Resume the tested deterministic desk in a local SQLite transaction only."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, time, timedelta
from decimal import Decimal
import json
import sqlite3

from src.backtesting.thesis_desk import ART, POSITIVE, DeskPolicy, ThesisDesk, decimal, instant
from src.core.market_calendar import is_trading_day
from src.thesis_desk.contracts import digest


class PaperStore:
    def __init__(self, path, config):
        self.conn = sqlite3.connect(path, timeout=2)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS desk_state (id INTEGER PRIMARY KEY CHECK(id=1), config_hash TEXT NOT NULL, state TEXT);
            CREATE TABLE IF NOT EXISTS desk_runs (run_id TEXT PRIMARY KEY, created_at TEXT NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS model_cache (key TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS desk_marks (ticker TEXT,market_at TEXT,observed_at TEXT,price TEXT,PRIMARY KEY(ticker,market_at));
            CREATE TABLE IF NOT EXISTS desk_signals (id TEXT PRIMARY KEY,ticker TEXT,first_seen TEXT,anchor_at TEXT,anchor_price TEXT);
            CREATE TABLE IF NOT EXISTS desk_signal_evidence (id TEXT PRIMARY KEY REFERENCES desk_signals(id),payload TEXT NOT NULL);
        """)
        self.conn.execute("BEGIN IMMEDIATE")
        row = self.conn.execute("SELECT config_hash FROM desk_state WHERE id=1").fetchone()
        expected = digest(config)
        if row and row[0] != expected:
            self.conn.rollback()
            self.conn.close()
            raise ValueError("Paper portfolio config is immutable; use a new --state-dir for another experiment")
        if not row:
            self.conn.execute("INSERT INTO desk_state(id,config_hash) VALUES(1,?)", (expected,))
        self.cache_conn = sqlite3.connect(path.with_name("research-cache.sqlite"), timeout=2)
        self.cache_conn.execute("CREATE TABLE IF NOT EXISTS model_cache(key TEXT PRIMARY KEY,payload TEXT NOT NULL)")
        self.cache_conn.executemany("INSERT OR IGNORE INTO model_cache VALUES(?,?)", self.conn.execute("SELECT key,payload FROM model_cache").fetchall())
        self.cache_conn.commit()

    def cached(self, key):
        row = self.cache_conn.execute("SELECT payload FROM model_cache WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def cache(self, key, value):
        self.cache_conn.execute("INSERT OR REPLACE INTO model_cache VALUES(?,?)", (key, json.dumps(value, default=str)))
        self.cache_conn.commit()

    def load(self):
        row = self.conn.execute("SELECT state FROM desk_state WHERE id=1").fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def finish(self, run_id, at, state, result):
        self.conn.execute("UPDATE desk_state SET state=? WHERE id=1", (json.dumps(state, default=str),))
        saved = {k: result[k] for k in ("schema", "at", "mode", "paper", "metrics") if k in result} if "paper" in result else result
        self.conn.execute("INSERT INTO desk_runs VALUES(?,?,?)", (run_id, at.isoformat(), json.dumps(saved, default=str)))
        self.conn.commit()

    def runs(self):
        return [json.loads(r[0]) for r in self.conn.execute("SELECT result FROM desk_runs ORDER BY created_at,run_id")]

    def close(self):
        self.conn.rollback()
        self.conn.close()
        self.cache_conn.close()


def pack(desk, seen_quotes, last_at):
    return dict(schema="desk-paper-state-v1", last_at=last_at.isoformat(), seen_quotes=sorted(seen_quotes),
                invalidation_evidence={section: [r for r in desk.evidence.get(section, []) if r["ticker"] in desk.positions
                                                and (section == "vetoes" or r["kind"] not in POSITIVE)] for section in ("facts", "vetoes")},
                cash=str(desk.cash), peak=str(desk.peak), fees=str(desk.fees), realized=str(desk.realized),
                positions=desk.positions, pending=desk.pending,
                prices={t: [str(price), stamp.isoformat()] for t, (price, stamp) in desk.prices.items()},
                weekly=[dict(year=key[0], week=key[1], count=value) for key, value in desk.weekly.items()],
                used_theses=sorted(desk.used_theses), applied_actions=sorted(desk.applied_actions),
                quality=sorted(desk.quality), braked=desk.braked)


def restore(desk, state):
    if state["schema"] != "desk-paper-state-v1":
        raise ValueError("Unknown paper state schema")
    for section, records in state.get("invalidation_evidence", {}).items():
        if section not in {"facts", "vetoes"}:
            raise ValueError("Unknown persisted invalidation section")
        desk.evidence[section] = list({r["id"]: r for r in desk.evidence.get(section, []) + records}.values())
    for name in ("cash", "peak", "fees", "realized"):
        setattr(desk, name, decimal(state[name]))
    desk.prices = {t: (decimal(value[0]), instant(value[1])) for t, value in state["prices"].items()}
    desk.weekly = Counter({(r["year"], r["week"]): r["count"] for r in state["weekly"]})
    desk.used_theses = {tuple(row) for row in state["used_theses"]}
    desk.applied_actions = set(state["applied_actions"])
    desk.quality = set(state["quality"])
    desk.braked = state["braked"]
    for t, row in state["positions"].items():
        p = dict(row)
        for key in ("basis", "original_cost", "cycle_pnl", "stop"):
            p[key] = decimal(p[key])
        p["opened_at"] = instant(p["opened_at"])
        if type(p["qty"]) is not int or p["qty"] <= 0:
            raise ValueError("Invalid saved quantity")
        desk.positions[t] = p
    for t, row in state["pending"].items():
        p = dict(row)
        for key in ("decision_at", "eligible_at", "expires_at"):
            if key in p:
                p[key] = instant(p[key])
        if p["side"] == "BUY":
            p["zone"] = tuple(decimal(v) for v in p["zone"])
            for key in ("stop_fraction", "weight"):
                p[key] = decimal(p[key])
        desk.pending[t] = p


def advance_paper(rows, evidence, config, state, at, entry_blocks):
    policy = DeskPolicy(variant="RISK_V2", initial_cash=config["initial_cash"])
    desk = ThesisDesk(rows, evidence, policy)
    seen = set()
    if state:
        if at <= instant(state["last_at"]):
            raise ValueError("Paper clock must move forward")
        restore(desk, state)
        seen = set(state["seen_quotes"])
        if at - instant(state["last_at"]) > timedelta(minutes=10) and desk.positions:
            desk.quality.add("UNOBSERVED_INTERVAL_STOPS_NOT_CONTINUOUS")
    if any(instant(r["ts"]) > at for r in rows["intraday_points"]):
        raise ValueError("Future paper observation")
    # Missing research/calendar evidence cancels entries, but never disables exits.
    eligible_ids = {r["id"] for r in evidence["facts"] if r["ticker"] not in entry_blocks}
    desk.pending = {t: p for t, p in desk.pending.items() if p["side"] == "SELL"
                    or (t not in entry_blocks and p["fact"]["id"] in eligible_ids and p["expires_at"] > at)}
    local = at.astimezone(ART)
    opened = is_trading_day(local.date()) and time(10, 30) <= local.time().replace(tzinfo=None) < time(17)
    updates = []
    for effect in rows.get("corporate_effects", []):
        position = desk.positions.get(effect["ticker"])
        if position and instant(effect["effective_at"]) <= position["opened_at"]:
            # A late-ingested split predating the fill is already reflected in its nominal basis.
            desk.applied_actions.add(effect["id"])
    desk.corporate(at)
    for quote in rows["intraday_points"]:
        market_at = instant(quote["market_ts"])
        fresh = timedelta(0) <= at - market_at <= timedelta(minutes=5)
        same_day = market_at.astimezone(ART).date() == local.date()
        if quote["quote_id"] not in seen and opened and fresh and same_day:
            updates.append(quote)
            seen.add(quote["quote_id"])
        elif quote["ticker"] not in desk.prices:
            # Stale data may mark a fictional holding, never execute or qualify an entry.
            desk.prices[quote["ticker"]] = (decimal(quote["last_price"]), market_at)
    if updates:
        # All quotes became observable in this snapshot. Do not backdate to exchange timestamps.
        desk.on_quotes(at, updates)
    if any(t not in desk.prices for t in desk.positions):
        raise ValueError("Missing valuation for an existing paper holding")
    if opened:
        if not updates:
            desk.quality.add("NO_NEW_FRESH_EXECUTABLE_QUOTES")
        # Only admitted facts are presented to entry logic; retain every adverse fact for exits.
        desk.decide(at, min(at + timedelta(minutes=15), datetime.combine(local.date(), time(17), ART)))
    else:
        desk.pending = {t: p for t, p in desk.pending.items() if p["side"] == "SELL"}
    desk.valuation(at)
    result = dict(mode="PAPER_ONLY", market_state="OPEN" if opened else "CLOSED", fills=desk.fills,
                  closed_cycles=desk.cycles, orders=[dict(p) for p in desk.pending.values()],
                  journal=desk.journal, valuation=desk.daily[-1], rejections=dict(desk.rejections),
                  quality_flags=sorted(desk.quality), brake_triggered=desk.braked,
                  economic_verification="NOT_VERIFIED", status="UPDATED" if opened else "WAITING_MARKET_OPEN",
                  source_timestamps={r["ticker"]: r["market_ts"] for r in rows["intraday_points"]})
    return pack(desk, seen, at), result
