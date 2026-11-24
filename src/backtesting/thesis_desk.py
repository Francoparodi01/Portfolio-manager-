"""Offline, causal, thesis-led desk simulation. No broker, network or DB imports.

External facts and universe membership are explicit evidence, never inferred from
prices, ticker names, sentiment or today's constituents. See docs/thesis-desk-backtest.md.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_FLOOR
from itertools import groupby
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from src.core.market_calendar import is_trading_day

ART = ZoneInfo("America/Argentina/Buenos_Aires")
POSITIVE = {"GUIDANCE_RAISED", "GUIDANCE_REAFFIRMED", "DEAL_SIGNED", "DEAL_CLOSED"}
NEGATIVE = {"GUIDANCE_CUT", "DEAL_CANCELLED", "LEGAL_ADVERSE"}


def decimal(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Non-finite numeric input")
    return result


def instant(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Evidence timestamps must include a timezone")
    return result


def official_url(value):
    parsed = urlparse(str(value))
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("Evidence requires an HTTPS source URL")


@dataclass(frozen=True)
class DeskPolicy:
    variant: str = "BASE_V1"
    initial_cash: str = "3000000"
    fee_rate: str = "0.006"
    slippage_rate: str = "0.0015"
    starter_fraction: str = "0.70"
    core_weight: str = "0.08"
    smid_weight: str = "0.04"
    risk_budget: str = "0.015"
    core_stop: str = "0.10"
    smid_stop: str = "0.15"
    zone_band: str = "0.02"
    concentration_cap: str = "0.12"
    drawdown_brake: str = "0.08"
    max_weekly_entries: int = 2
    max_evidence_age_days: int = 14
    technical_sessions: int = 20
    latency_seconds: int = 60

    def __post_init__(self):
        if self.variant not in {"BASE_V1", "RISK_V2"}:
            raise ValueError("Unknown desk variant")
        if decimal(self.initial_cash) <= 0:
            raise ValueError("Initial cash must be positive")
        for key in ("fee_rate", "slippage_rate", "starter_fraction", "core_weight", "smid_weight",
                    "risk_budget", "core_stop", "smid_stop", "zone_band", "concentration_cap", "drawdown_brake"):
            number = decimal(getattr(self, key))
            if not 0 <= number < 1 or (key not in {"fee_rate", "slippage_rate"} and number == 0):
                raise ValueError(f"Invalid {key}")
        for key in ("max_weekly_entries", "max_evidence_age_days", "technical_sessions", "latency_seconds"):
            value = getattr(self, key)
            if type(value) is not int or value <= 0:
                raise ValueError(f"Invalid {key}")


def validate_evidence(data):
    if data.get("schema") != "thesis-desk-evidence-v1":
        raise ValueError("Expected thesis-desk-evidence-v1")
    for section in ("facts", "vetoes", "universe", "risk", "blackouts", "coverage"):
        rows = data.get(section, [])
        if not isinstance(rows, list):
            raise ValueError(f"Invalid {section}")
        seen = set()
        for row in rows:
            if not row.get("id") or row["id"] in seen:
                raise ValueError(f"Missing/duplicate {section} id")
            seen.add(row["id"])
            official_url(row.get("source_url"))
            if section == "coverage":
                if row["domain"] not in {"facts", "universe", "risk", "blackouts"}:
                    raise ValueError("Unknown coverage domain")
                if date.fromisoformat(row["start"]) > date.fromisoformat(row["end"]):
                    raise ValueError("Reversed coverage interval")
                if row["status"] not in {"COMPLETE", "PARTIAL", "UNKNOWN"}:
                    raise ValueError("Invalid coverage status")
                if not row.get("scope"):
                    raise ValueError("Coverage must identify its scope")
                continue
            available = instant(row["available_at"])
            if row.get("knowledge_basis") not in {"ARCHIVED_AT_TIME", "RECONSTRUCTED_PUBLICATION"}:
                raise ValueError("Explicit knowledge_basis required")
            if section in {"facts", "vetoes"}:
                if (section == "facts" and row["kind"] not in POSITIVE | NEGATIVE) or row["source_type"] not in {"SEC", "IR", "CNV"}:
                    raise ValueError("Unsupported official fact")
                host = urlparse(row["source_url"]).hostname
                authority = {"SEC": "sec.gov", "CNV": "cnv.gov.ar"}.get(row["source_type"])
                if authority and host != authority and not host.endswith("." + authority):
                    raise ValueError("Official source type and URL disagree")
                if instant(row["published_at"]) > available or not row.get("ticker") or not row.get("thesis_id"):
                    raise ValueError("Invalid fact chronology or identity")
                if section == "vetoes" and (row["thesis_id"] == "*" or not row.get("quote")
                        or instant(row["published_at"]) <= instant(row["thesis_published_at"])):
                    raise ValueError("A veto must cite a later fact against one specific thesis")
                if section == "facts" and row["kind"] in POSITIVE and not row.get("invalidation"):
                    raise ValueError("A thesis needs a written factual invalidation")
            elif section == "universe":
                if instant(row["asof_at"]) > available:
                    raise ValueError("Universe cannot be known before its snapshot")
                if row["sleeve"] not in {"CORE", "SMID", "EXTENDED"}:
                    raise ValueError("Invalid sleeve")
                if row["currency"] != "ARS" or row["underlying_kind"] not in {"COMMON_STOCK", "DEPOSITARY_RECEIPT"}:
                    raise ValueError("Only explicitly mapped ARS common-stock instruments are supported")
                if not row.get("ticker") or not row.get("asset_type"):
                    raise ValueError("Explicit instrument identity required")
                if row["sleeve"] == "CORE" and row["membership_source"] != "IVV":
                    raise ValueError("CORE requires dated IVV membership")
                if row["sleeve"] == "EXTENDED" and row.get("membership_source") not in {"COMAFI_OFFICIAL_PROGRAM", "CVSA_OFFICIAL_PROGRAM", "BCBA_NATIVE_EQUITY_PANEL"}:
                    raise ValueError("Extended equities require a verified dated catalog")
                if row["sleeve"] == "SMID" and not Decimal("3e9") <= decimal(row["market_cap_usd"]) <= Decimal("70e9"):
                    raise ValueError("SMID market cap outside mandate")
            elif section == "risk":
                if row["state"] not in {"GREEN", "YELLOW", "RED"} or instant(row["valid_until"]) <= available:
                    raise ValueError("Invalid risk state or interval")
            elif instant(row["start_at"]) >= instant(row["end_at"]) or not row.get("ticker"):
                raise ValueError("Invalid blackout")


def sessions_between(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if start > end or (end - start).days > 370:
        raise ValueError("Invalid date window (maximum 370 calendar days)")
    sessions = [start + timedelta(days=i) for i in range((end - start).days + 1)
                if is_trading_day(start + timedelta(days=i))]
    if not sessions:
        raise ValueError("No market sessions in requested window")
    return sessions


def coverage_gaps(evidence, days, variant):
    required = ["facts", "universe"] + (["risk", "blackouts"] if variant == "RISK_V2" else [])
    return {domain: [d.isoformat() for d in days if not any(
        r["domain"] == domain and r["status"] == "COMPLETE"
        and r["scope"] == "DECLARED_UNIVERSE"
        and r["start"] <= d.isoformat() <= r["end"] for r in evidence.get("coverage", []))]
        for domain in required}


class ThesisDesk:
    """Research -> technical zone -> factual veto -> PM quota -> risk -> ledger."""

    def __init__(self, rows, evidence, policy):
        validate_evidence(evidence)
        self.rows, self.evidence, self.policy = rows, evidence, policy
        self.cash = decimal(policy.initial_cash)
        self.positions, self.pending, self.prices = {}, {}, {}
        self.fills, self.cycles, self.journal, self.daily = [], [], [], []
        self.rejections, self.weekly = Counter(), Counter()
        self.used_theses, self.applied_actions = set(), set()
        self.quality = set()
        self.braked = False
        self.peak = self.cash
        self.fees = Decimal(0)
        self.realized = Decimal(0)
        self.bars = defaultdict(list)
        self.identities = defaultdict(set)
        for row in rows.get("daily_candles", []):
            self.bars[row["ticker"]].append(row)
        for row in rows.get("intraday_points", []):
            self.identities[row["ticker"]].add((row["asset_type"], row["currency"]))

    def nav(self):
        return self.cash + sum(p["qty"] * self.prices[t][0] for t, p in self.positions.items())

    def universe(self, ticker, at):
        eligible = [r for r in self.evidence.get("universe", []) if r["ticker"] == ticker
                    and instant(r["available_at"]) <= at
                    and timedelta(0) <= at - instant(r["asof_at"]) <= timedelta(days=self.policy.max_evidence_age_days)]
        return max(eligible, key=lambda r: (instant(r["asof_at"]), instant(r["available_at"]), r["id"]), default=None)

    def risk_state(self, at):
        if self.policy.variant == "BASE_V1":
            return "GREEN"
        rows = [r for r in self.evidence.get("risk", [])
                if instant(r["available_at"]) <= at < instant(r["valid_until"])]
        return max(rows, key=lambda r: (instant(r["available_at"]), r["id"]))["state"] if rows else "UNKNOWN"

    def veto(self, fact, at):
        explicit = any(r["ticker"] == fact["ticker"] and r["thesis_id"] == fact["thesis_id"]
                       and instant(fact["published_at"]) < instant(r["published_at"])
                       and instant(r["available_at"]) <= at for r in self.evidence.get("vetoes", []))
        return explicit or any(r["ticker"] == fact["ticker"] and r["kind"] in NEGATIVE
                   and r["thesis_id"] in {fact["thesis_id"], "*"}
                   and instant(fact["published_at"]) <= instant(r["published_at"])
                   and instant(r["available_at"]) <= at for r in self.evidence.get("facts", []))

    def zone(self, fact, member, at):
        # Anchor to pre-announcement sessions: a rally cannot move its own entry zone.
        anchor = instant(fact["published_at"]).astimezone(ART).date()
        by_day = {}
        for r in self.bars[fact["ticker"]]:
            if (r["asset_type"], r["currency"]) != (member["asset_type"], member["currency"]):
                continue
            if not all(r.get(k) for k in ("bar_end", "available_at", "scraped_at")) or not r.get("is_closed"):
                continue
            if r.get("interval") != "1d" or r.get("adjustment_policy") not in {"RAW", "UNADJUSTED"}:
                continue
            end = instant(r["bar_end"])
            day = end.astimezone(ART).date()
            if day >= anchor or max(end, instant(r["available_at"]), instant(r["scraped_at"])) > at:
                continue
            if not r.get("close_price") or decimal(r["close_price"]) <= 0:
                continue
            priority = ({"COCOS": 0, "TRADINGVIEW_BYMA": 1, "internal_snapshot": 2}.get(r.get("source"), 3),
                        -instant(r["scraped_at"]).timestamp())
            price = decimal(r["close_price"])
            for action in self.rows.get("corporate_effects", []):
                if action["ticker"] != fact["ticker"] or not action["is_active"]:
                    continue
                if not end < instant(action["effective_at"]) <= at:
                    continue
                if self.action_known_at(action) > at or action.get("lifecycle_status") != "EFFECTIVE":
                    return None
                if (action["asset_type"], action["currency"]) != (member["asset_type"], member["currency"]):
                    continue
                factor = decimal(action["price_factor"])
                if factor <= 0:
                    raise ValueError("Invalid corporate price factor")
                price *= factor
            if day not in by_day or priority < by_day[day][0]:
                by_day[day] = (priority, price)
        days = sorted(by_day)[-self.policy.technical_sessions:]
        if len(days) < self.policy.technical_sessions or (anchor - days[-1]).days > 7:
            return None
        mean = sum(by_day[d][1] for d in days) / len(days)
        band = decimal(self.policy.zone_band)
        return mean * (1 - band), mean * (1 + band)

    def weekly_limit(self, at):
        day = at.astimezone(ART).date()
        monday = day - timedelta(days=day.weekday())
        prior_sessions = sum(is_trading_day(monday + timedelta(days=i)) for i in range(day.weekday()))
        return min(self.policy.max_weekly_entries, 1 if prior_sessions < 2 else 2)

    def queue_exit(self, ticker, at, reason, qty=None):
        p = self.positions[ticker]
        current = self.pending.get(ticker)
        if current and current["side"] == "SELL" and current["qty"] >= (qty or p["qty"]):
            return
        self.pending[ticker] = dict(side="SELL", ticker=ticker, qty=qty or p["qty"],
                                    decision_at=at, eligible_at=at + timedelta(seconds=self.policy.latency_seconds),
                                    reason=reason)

    def decide(self, at, expires_at):
        self.pending = {t: o for t, o in self.pending.items()
                        if o["side"] == "SELL" or o["expires_at"] > at}
        nav = self.nav()
        for ticker, p in list(self.positions.items()):
            if self.veto(p["fact"], at):
                self.queue_exit(ticker, at, "FACT_INVALIDATION")
            elif self.policy.variant == "RISK_V2" and p["qty"] * self.prices[ticker][0] > nav * decimal(self.policy.concentration_cap):
                target = int(nav * decimal(self.policy.concentration_cap) / self.prices[ticker][0])
                self.queue_exit(ticker, at, "CONCENTRATION_CAP", p["qty"] - target)
        state = self.risk_state(at)
        if state == "UNKNOWN":
            self.quality.add("MISSING_RISK_STATE_AT_CUTOFF")
        if self.braked or state in {"RED", "UNKNOWN"}:
            self.rejections["DRAWDOWN_BRAKE" if self.braked else "RISK_" + state] += 1
            return
        facts = sorted((r for r in self.evidence.get("facts", []) if r["kind"] in POSITIVE
                        and instant(r["available_at"]) <= at
                        and timedelta(0) <= at - instant(r["published_at"]) <= timedelta(days=self.policy.max_evidence_age_days)),
                       key=lambda r: (-instant(r["published_at"]).timestamp(), r["ticker"], r["id"]))
        for fact in facts:
            t = fact["ticker"]
            key = (t, fact["thesis_id"])
            reason = None
            member = self.universe(t, at)
            if t in self.positions or t in self.pending or key in self.used_theses:
                reason = "POSITION_OR_THESIS_ALREADY_USED"
            elif not member:
                reason = "MISSING_DATED_UNIVERSE"
            elif self.identities[t] != {(member["asset_type"], member["currency"])}:
                reason = "INSTRUMENT_IDENTITY_UNAVAILABLE_OR_AMBIGUOUS"
            elif self.veto(fact, at):
                reason = "FACTUAL_VETO"
            elif t not in self.prices or self.prices[t][1].astimezone(ART).date() != at.astimezone(ART).date() or at - self.prices[t][1] > timedelta(minutes=30):
                reason = "NO_FRESH_QUOTE"
            elif self.policy.variant == "RISK_V2" and any(r["ticker"] in {t, "*"}
                    and instant(r["available_at"]) <= at and instant(r["start_at"]) <= at < instant(r["end_at"])
                    for r in self.evidence.get("blackouts", [])):
                reason = "EVENT_BLACKOUT"
            if reason:
                self.rejections[reason] += 1
                if reason in {"MISSING_DATED_UNIVERSE", "INSTRUMENT_IDENTITY_UNAVAILABLE_OR_AMBIGUOUS", "NO_FRESH_QUOTE"}:
                    self.quality.add(reason)
                continue
            zone = self.zone(fact, member, at)
            if not zone:
                self.rejections["MISSING_PIT_TECHNICAL_HISTORY"] += 1
                self.quality.add("MISSING_PIT_TECHNICAL_HISTORY")
                continue
            price = self.prices[t][0]
            if not zone[0] <= price <= zone[1]:
                self.rejections["OUTSIDE_ENTRY_ZONE"] += 1
                continue
            week = tuple(at.astimezone(ART).date().isocalendar()[:2])
            reserved = sum(o["side"] == "BUY" for o in self.pending.values())
            if self.weekly[week] + reserved >= self.weekly_limit(at):
                self.rejections["WEEKLY_QUOTA"] += 1
                continue
            stop = decimal(self.policy.core_stop if member["sleeve"] == "CORE" else self.policy.smid_stop)
            weight = decimal(self.policy.core_weight if member["sleeve"] == "CORE" else self.policy.smid_weight)
            weight = min(weight, decimal(self.policy.risk_budget) / stop) * decimal(self.policy.starter_fraction)
            if state == "YELLOW":
                weight /= 2
            qty = int((nav * weight / (price * (1 + decimal(self.policy.slippage_rate)) * (1 + decimal(self.policy.fee_rate)))).to_integral_value(rounding=ROUND_FLOOR))
            if qty <= 0:
                self.rejections["INSUFFICIENT_SIZE"] += 1
                continue
            self.pending[t] = dict(side="BUY", ticker=t, qty=qty, decision_at=at,
                                   eligible_at=at + timedelta(seconds=self.policy.latency_seconds), expires_at=expires_at,
                                   fact=fact, zone=zone, stop_fraction=stop, weight=weight, size_state=state,
                                   reason="OFFICIAL_THESIS_IN_ZONE")
            self.journal.append(dict(at=at.isoformat(), ticker=t, action="ON", fact_id=fact["id"],
                                     universe_id=member["id"], risk=state, zone_low=str(zone[0]), zone_high=str(zone[1]), planned_qty=qty))

    def fill(self, order, quote, at):
        t, buy = order["ticker"], order["side"] == "BUY"
        fee_rate, slip = decimal(self.policy.fee_rate), decimal(self.policy.slippage_rate)
        price = quote * (1 + slip if buy else 1 - slip)
        qty = order["qty"]
        if buy:
            state = self.risk_state(at)
            if self.veto(order["fact"], at) or state in {"RED", "UNKNOWN"}:
                self.rejections["ENTRY_VETO_AT_FILL"] += 1
                return False
            if self.policy.variant == "RISK_V2" and any(r["ticker"] in {t, "*"}
                    and instant(r["available_at"]) <= at and instant(r["start_at"]) <= at < instant(r["end_at"])
                    for r in self.evidence.get("blackouts", [])):
                self.rejections["EVENT_BLACKOUT_AT_FILL"] += 1
                return False
            if not order["zone"][0] <= price <= order["zone"][1]:
                return False  # Resting zone order; never chase a gap.
            week = tuple(at.astimezone(ART).date().isocalendar()[:2])
            if self.braked or self.weekly[week] >= self.weekly_limit(at):
                return False
            weight = order["weight"]
            if state == "YELLOW" and order["size_state"] == "GREEN":
                weight /= 2
            qty = min(qty, int(self.cash / (price * (1 + fee_rate))),
                      int(self.nav() * weight / (price * (1 + fee_rate))))
        else:
            qty = min(qty, self.positions[t]["qty"])
        if qty <= 0:
            return False
        gross, pnl = qty * price, None
        fee = gross * fee_rate
        self.fees += fee
        if buy:
            cost = gross + fee
            self.cash -= cost
            self.positions[t] = dict(qty=qty, basis=cost, original_cost=cost, cycle_pnl=Decimal(0),
                                     stop=price * (1 - order["stop_fraction"]), fact=order["fact"], opened_at=at)
            self.used_theses.add((t, order["fact"]["thesis_id"]))
            self.weekly[week] += 1
        else:
            p = self.positions[t]
            cost = p["basis"] * qty / p["qty"]
            pnl = gross - fee - cost
            self.cash += gross - fee
            self.realized += pnl
            p["basis"] -= cost
            p["qty"] -= qty
            p["cycle_pnl"] += pnl
            if p["qty"] == 0:
                self.cycles.append(dict(ticker=t, opened_at=p["opened_at"].isoformat(), closed_at=at.isoformat(),
                                        net_pnl_ars=str(p["cycle_pnl"]), return_pct=str(p["cycle_pnl"] / p["original_cost"] * 100)))
                del self.positions[t]
        assert self.cash >= 0
        self.fills.append(dict(ticker=t, side=order["side"], quantity=qty, decision_at=order["decision_at"].isoformat(),
                               filled_at=at.isoformat(), quote_price=str(quote), fill_price=str(price), fee_ars=str(fee),
                               realized_pnl_ars=None if pnl is None else str(pnl), cash_after=str(self.cash), reason=order["reason"]))
        return True

    def on_quotes(self, at, quotes):
        for row in quotes:
            self.prices[row["ticker"]] = (decimal(row["last_price"]), at)
        self.check_brake(at)
        # Process same-instant sells first. A later sale never funds an earlier buy.
        for row in sorted(quotes, key=lambda r: (self.pending.get(r["ticker"], {}).get("side") != "SELL", r["ticker"])):
            t = row["ticker"]
            order = self.pending.get(t)
            quote_at = instant(row.get("market_ts", at))
            if order and quote_at >= order["eligible_at"] and at >= order["eligible_at"] and (order["side"] == "SELL" or at < order["expires_at"]):
                if self.fill(order, decimal(row["last_price"]), at):
                    del self.pending[t]
            if t in self.positions and decimal(row["last_price"]) <= self.positions[t]["stop"]:
                self.queue_exit(t, at, "STOP")
        self.check_brake(at)

    def check_brake(self, at):
        nav = self.nav()
        self.peak = max(self.peak, nav)
        if self.policy.variant == "RISK_V2" and nav <= self.peak * (1 - decimal(self.policy.drawdown_brake)):
            self.braked = True
            self.pending = {t: o for t, o in self.pending.items() if o["side"] == "SELL"}
            for t in self.positions:
                self.queue_exit(t, at, "DRAWDOWN_BRAKE")

    @staticmethod
    def action_known_at(row):
        return max(instant(row[k]) for k in ("event_created_at", "event_updated_at", "effect_created_at", "effect_updated_at"))

    def corporate(self, at):
        for row in self.rows.get("corporate_effects", []):
            if row["id"] in self.applied_actions or instant(row["effective_at"]) > at or not row["is_active"]:
                continue
            t = row["ticker"]
            known = self.action_known_at(row)
            if known > at:
                if t in self.positions:
                    self.quality.add("CORPORATE_ACTION_NOT_KNOWN_AT_EFFECTIVE_TIME")
                continue
            if row.get("lifecycle_status") != "EFFECTIVE":
                if t in self.positions:
                    self.quality.add("UNCONFIRMED_CORPORATE_ACTION")
                continue
            self.applied_actions.add(row["id"])
            if t not in self.positions:
                continue
            if self.identities[t] != {(row["asset_type"], row["currency"])}:
                self.quality.add("CORPORATE_IDENTITY_CONFLICT")
                continue
            factor = decimal(row["quantity_factor"])
            price_factor = decimal(row["price_factor"])
            basis_factor = decimal(row["cost_basis_factor"])
            if price_factor <= 0 or factor * price_factor != 1 or factor * basis_factor != 1:
                raise ValueError("Corporate action does not preserve value and total cost basis")
            qty = self.positions[t]["qty"] * factor
            if factor <= 0 or qty != int(qty):
                raise ValueError("Unsupported fractional corporate action")
            p = self.positions[t]
            p["qty"] = int(qty)
            p["stop"] *= price_factor
            self.prices[t] = (self.prices[t][0] * price_factor, self.prices[t][1])
            if t in self.pending:
                del self.pending[t]
            self.journal.append(dict(at=at.isoformat(), ticker=t, action="CORPORATE_ADJUSTMENT", effect_id=row["id"], factor=str(factor)))

    def valuation(self, at):
        stale = [t for t in self.positions if self.prices[t][1].astimezone(ART).date() != at.astimezone(ART).date()]
        if stale:
            self.quality.add("STALE_HELD_MARKS")
        nav = self.nav()
        unrealized = sum(p["qty"] * self.prices[t][0] - p["basis"] for t, p in self.positions.items())
        assert abs(nav - decimal(self.policy.initial_cash) - self.realized - unrealized) < Decimal("0.000001")
        row = dict(at=at.isoformat(), nav_ars=str(nav), cash_ars=str(self.cash), open_positions=len(self.positions),
                   exposure_pct=str((nav - self.cash) / nav * 100), stale_tickers=stale)
        self.daily.append(row)


def run_desk(rows, evidence, start, end, policy=None):
    policy = policy or DeskPolicy()
    days = sessions_between(start, end)
    desk = ThesisDesk(rows, evidence, policy)
    quotes, seen = [], {}
    for row in rows.get("intraday_points", []):
        at = instant(row["ts"])
        local = at.astimezone(ART)
        if local.date() not in days or not time(10, 30) <= local.time().replace(tzinfo=None) < time(17):
            continue
        if row["currency"] != "ARS" or decimal(row["last_price"]) <= 0:
            raise ValueError("Invalid market quote")
        key = (at, row["ticker"], row["asset_type"], row["currency"])
        if key in seen:
            if seen[key] != decimal(row["last_price"]):
                raise ValueError("CONFLICTING_RECORDED_QUOTES")
            continue
        seen[key] = decimal(row["last_price"])
        quotes.append((at, row))
    quotes.sort(key=lambda item: item[0])
    quote_days = {at.astimezone(ART).date() for at, _ in quotes}
    missing = [d.isoformat() for d in days if d not in quote_days]
    if missing:
        desk.quality.add("MISSING_MARKET_SESSIONS")
    events = [(at, 0, [r for _, r in group]) for at, group in groupby(quotes, key=lambda item: item[0])]
    for day in days:
        cuts = [datetime.combine(day, time(h), ART) for h in (11, 13, 16, 17)]
        events += [(at, 1, cuts[i + 1]) for i, at in enumerate(cuts[:-1])]
        events.append((cuts[-1], 2, None))
    for at, kind, payload in sorted(events, key=lambda item: (item[0], item[1])):
        desk.corporate(at)
        if kind == 0:
            desk.on_quotes(at, payload)
        elif kind == 1:
            desk.decide(at, payload)
        else:
            desk.valuation(at)
    gaps = {k: v for k, v in coverage_gaps(evidence, days, policy.variant).items() if v}
    if gaps:
        desk.quality.add("INCOMPLETE_EVIDENCE_COVERAGE")
    evaluable = not desk.quality
    peak = decimal(policy.initial_cash)
    max_dd = Decimal(0)
    for row in desk.daily:
        nav = decimal(row["nav_ars"])
        peak = max(peak, nav)
        max_dd = max(max_dd, (1 - nav / peak) * 100)
    metrics = dict(return_pct=str((desk.nav() / decimal(policy.initial_cash) - 1) * 100),
                   max_daily_drawdown_pct=str(max_dd), final_nav_ars=str(desk.nav()),
                   realized_pnl_ars=str(desk.realized), fees_ars=str(desk.fees),
                   closed_cycles=len(desk.cycles), win_rate_pct=(str(sum(decimal(c["net_pnl_ars"]) > 0 for c in desk.cycles)
                       / Decimal(len(desk.cycles)) * 100) if desk.cycles else None))
    knowledge = sorted({r["knowledge_basis"] for section in ("facts", "universe", "risk", "blackouts")
                        for r in evidence.get(section, [])})
    return dict(schema="thesis-desk-result-v1", status="COMPLETE" if evaluable else "NOT_EVALUABLE",
                execution_status="COMPLETED", research_only=True, economic_verification="NOT_VERIFIED",
                evidence_knowledge_bases=knowledge, price_vintage="CURRENT_CAPTURE_NOT_VERIFIED_ORIGINAL_VINTAGE",
                methodology="RETROSPECTIVE_DETERMINISTIC_POLICY_NOT_HISTORICAL_AGENT_REPLAY",
                policy=asdict(policy), requested_start=start, requested_end=end,
                actual_last_session=days[-1].isoformat(), sessions=len(days), cuts=3 * len(days),
                quote_rows=len(quotes), quote_sessions=len(quote_days), missing_quote_sessions=missing,
                coverage_gaps=gaps, quality_flags=sorted(desk.quality), metrics=metrics if evaluable else None,
                # No flat cash performance is published as strategy evidence when inputs are missing.
                accounting=dict(final_cash_ars=str(desk.cash), open_positions=len(desk.positions), fills=len(desk.fills)),
                benchmarks={"cash": dict(return_pct="0", assumption="ARS cash, no interest or inflation adjustment"),
                            "hold": dict(status="NOT_EVALUABLE", reason="NO_PREREGISTERED_FUNDED_REFERENCE_PORTFOLIO")},
                fills=desk.fills, closed_cycles=desk.cycles, journal=desk.journal,
                daily_valuations=desk.daily, rejection_counts=dict(desk.rejections), brake_triggered=desk.braked,
                evidence_counts={k: len(evidence.get(k, [])) for k in ("facts", "universe", "risk", "blackouts")})
