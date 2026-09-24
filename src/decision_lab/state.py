"""As-of reconstruction. Adapters receive this state, never the outcome dataset."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from .models import (
    Dataset,
    Evidence,
    HistoricalState,
    Position,
    QualityAssessment,
    canonical,
    digest,
)


class InsufficientEvidence(ValueError):
    def __init__(self, *reasons: str):
        self.reasons = tuple(reasons)
        super().__init__("; ".join(reasons))


class EvidenceIndex:
    """One parse/hash pass per exported dataset; shared by all walk-forward dates."""

    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self.by_kind = defaultdict(list)
        self.payloads = {}
        self.hashes = {}
        self.sessions = {s.session_id: s for s in dataset.sessions}
        self.ordered_sessions = sorted(dataset.sessions, key=lambda s: s.open_at)
        for e in dataset.records:
            self.by_kind[e.kind].append(e)
            self.payloads[id(e)] = e.payload
            self.hashes[id(e)] = e.content_hash
        for rows in self.by_kind.values():
            rows.sort(key=lambda e: (e.effective_at, e.available_at, e.record_id))

    def payload(self, evidence):
        return self.payloads[id(evidence)]

    def visible(self, kind: str, as_of: datetime, owner: int):
        for e in self.by_kind[kind]:
            if e.owner is not None and e.owner != owner:
                continue
            if (
                kind
                in {
                    "PORTFOLIO",
                    "PLAN",
                    "FEATURES",
                    "CONFIG",
                    "FILL",
                    "HUMAN_COVERAGE",
                    "POLICY",
                }
                and e.owner != owner
            ):
                continue
            if not e.known_at(as_of):
                continue
            # Announced future events are known information. Their effective
            # date is never used to adjust past quantities before effectiveness.
            if (
                kind
                not in {
                    "EVENT",
                    "CORPORATE_ACTION",
                    "ACTION_COVERAGE",
                    "HUMAN_COVERAGE",
                }
                and e.effective_at > as_of
            ):
                continue
            if kind in {"NEWS", "SENTIMENT"}:
                published = self.payload(e).get("published_at")
                if not published or datetime.fromisoformat(published) > as_of:
                    continue
            yield e

    def latest(self, kind, as_of, owner, *, record_id=None):
        rows = [
            e
            for e in self.visible(kind, as_of, owner)
            if record_id is None or e.record_id == record_id
        ]
        return max(
            rows,
            key=lambda e: (
                e.effective_at,
                e.revision_at or e.available_at,
                e.record_id,
            ),
            default=None,
        )

    def bars(
        self,
        cutoff: datetime,
        tickers: set[str],
        *,
        decision_inputs=False,
        session_ids=None,
        owner=None,
    ):
        selected = {}
        for e in self.by_kind["BAR"]:
            if e.owner is not None and e.owner != owner:
                continue
            p = self.payload(e)
            if p.get("ticker") not in tickers or not e.known_at(cutoff):
                continue
            session = self.sessions.get(p.get("session_id"))
            if session is None or session.close_at > cutoff or e.effective_at > cutoff:
                continue
            if session_ids is not None and session.session_id not in session_ids:
                continue
            if decision_inputs and p.get("price_mode") not in {
                "RAW_AS_TRADED",
                "POINT_IN_TIME_ADJUSTED",
            }:
                continue
            if p.get("price_mode") == "POINT_IN_TIME_ADJUSTED":
                adjustment = p.get("adjustment_as_of")
                if not adjustment or datetime.fromisoformat(adjustment) > cutoff:
                    continue
            key = (p["ticker"], session.session_id)
            old = selected.get(key)
            rank = (e.revision_at or e.available_at, e.available_at)
            if old is not None:
                old_rank = (old.revision_at or old.available_at, old.available_at)
                if rank == old_rank and self.hashes[id(e)] != self.hashes[id(old)]:
                    raise InsufficientEvidence(f"AMBIGUOUS_BAR_VINTAGE:{key}")
                if rank < old_rank:
                    continue
            selected[key] = e
        return selected


def build_state(
    index: EvidenceIndex,
    *,
    as_of: datetime,
    owner: int,
    plan_id: str | None = None,
    portfolio_id: str | None = None,
    lookback_sessions: int = 260,
) -> HistoricalState:
    if not owner or as_of.tzinfo is None:
        raise ValueError("explicit owner and timezone-aware as_of required")
    portfolio = index.latest("PORTFOLIO", as_of, owner, record_id=portfolio_id)
    if portfolio is None:
        raise InsufficientEvidence("PORTFOLIO_NOT_KNOWN_AT_T")
    p = index.payload(portfolio)
    try:
        positions = tuple(
            sorted(
                (Position.model_validate(r) for r in p["positions"]),
                key=lambda x: x.ticker,
            )
        )
        cash = Decimal(str(p["cash_ars"]))
        capital = cash + sum((r.quantity * r.mark_ars for r in positions), Decimal(0))
    except (KeyError, TypeError, ValueError) as exc:
        raise InsufficientEvidence("INVALID_PORTFOLIO_PAYLOAD") from exc
    if capital <= 0 or cash < 0 or len({r.ticker for r in positions}) != len(positions):
        raise InsufficientEvidence("INVALID_PORTFOLIO_CAPITAL_OR_DUPLICATE_POSITION")
    selected = [portfolio]
    missing, warnings, assumptions = [], [], list(p.get("assumptions", []))
    universe = index.latest("UNIVERSE", as_of, owner)
    uq = (
        index.payload(universe).get("universe_quality", "UNKNOWN")
        if universe
        else "UNKNOWN"
    )
    if universe:
        selected.append(universe)
    else:
        missing.append("universe_snapshot")
    tickers = {r.ticker for r in positions}
    if universe:
        tickers.update(
            r["ticker"]
            for r in index.payload(universe).get("instruments", [])
            if r.get("enabled") and r.get("operable")
        )
    plan = index.latest("PLAN", as_of, owner, record_id=plan_id)
    if plan:
        selected.append(plan)
        tickers.update(r["ticker"] for r in index.payload(plan).get("orders", []))
        assumptions.extend(index.payload(plan).get("assumptions", []))
    elif plan_id:
        raise InsufficientEvidence("PLAN_NOT_KNOWN_AT_T")
    for kind in ("MACRO", "FX", "CONFIG"):
        row = index.latest(kind, as_of, owner)
        if row:
            selected.append(row)
        else:
            missing.append(kind.lower())
    policy = index.latest("POLICY", as_of, owner)
    if policy:
        selected.append(policy)
    # Feature/aggregated-sentiment history is selected per ticker, not by
    # retrospectively using the last available global row.
    for kind in ("FEATURES", "SENTIMENT"):
        latest = {}
        for row in index.visible(kind, as_of, owner):
            ticker = index.payload(row).get("ticker", "MACRO")
            if ticker in tickers or ticker == "MACRO":
                latest[ticker] = row
        selected.extend(latest.values())
        if not latest:
            missing.append(kind.lower())
    for kind in ("NEWS", "EVENT", "CORPORATE_ACTION", "ACTION_COVERAGE"):
        for row in index.visible(kind, as_of, owner):
            ticker = index.payload(row).get("ticker")
            if ticker is None or ticker in tickers:
                selected.append(row)
    session_ids = {s.session_id for s in index.ordered_sessions if s.close_at <= as_of}
    session_ids = set(
        [s.session_id for s in index.ordered_sessions if s.session_id in session_ids][
            -lookback_sessions:
        ]
    )
    bars = index.bars(
        as_of, tickers, decision_inputs=True, session_ids=session_ids, owner=owner
    )
    selected.extend(bars[k] for k in sorted(bars))
    if not bars:
        missing.append("admissible_price_history")
    if p.get("total_value_ars") is not None and abs(
        Decimal(str(p["total_value_ars"])) - capital
    ) > Decimal("0.05"):
        warnings.append("PORTFOLIO_TOTAL_DIFFERS_FROM_POSITIONS_PLUS_CASH")
    if (as_of - portfolio.effective_at).total_seconds() > 86400:
        warnings.append("PORTFOLIO_OLDER_THAN_24H")
    components = {}
    for row in selected:
        components.setdefault(row.kind, set()).add(row.quality)
    invalid = any(
        e.quality in {"UNSAFE_FOR_REPLAY", "CURRENT_STATE_ONLY"}
        for e in selected
        if e.kind != "BAR"
    )
    approximate = any(e.quality == "APPROXIMATE" for e in selected) or uq in {
        "UNKNOWN",
        "APPROXIMATE",
    }
    level = (
        "INVALID"
        if invalid
        else (
            "LOW"
            if approximate or missing or warnings or assumptions
            else (
                "MEDIUM"
                if uq == "RECONSTRUCTED"
                or any(e.quality == "RECONSTRUCTIBLE" for e in selected)
                else "HIGH"
            )
        )
    )
    quality = QualityAssessment(
        level=level,
        primary_eligible=level == "HIGH",
        components_json=canonical(
            {k: sorted(v) for k, v in sorted(components.items())}
        ),
        missing_fields=tuple(sorted(missing)),
        reconstruction_assumptions=tuple(sorted(set(assumptions))),
        warnings=tuple(warnings),
    )
    selected.sort(
        key=lambda e: (
            e.kind,
            e.record_id,
            e.available_at,
            e.revision_at or e.available_at,
        )
    )
    ih = digest([index.hashes[id(e)] for e in selected])
    state_id = digest([as_of.isoformat(), owner, ih, index.dataset.calendar_version])
    # Opportunity identity excludes strategy/plan and future outcomes. Version
    # comparisons require the entire state_id as well as this common unit.
    opportunity_id = digest([owner, as_of.isoformat(), index.hashes[id(portfolio)]])
    feature_ids = [e.record_id for e in selected if e.kind == "FEATURES"]
    return HistoricalState(
        as_of=as_of,
        owner=owner,
        state_id=state_id,
        opportunity_id=opportunity_id,
        records=tuple(selected),
        positions=positions,
        cash_ars=cash,
        capital_base_ars=capital,
        portfolio_snapshot_id=portfolio.record_id,
        market_snapshot_id=digest([index.hashes[id(bars[k])] for k in sorted(bars)]),
        feature_snapshot_id=digest(feature_ids) if feature_ids else None,
        universe_snapshot_id=universe.record_id if universe else None,
        universe_quality=uq,
        input_hash=ih,
        quality=quality,
    )


def require_kind(state: HistoricalState, kind: str) -> list[Evidence]:
    rows = [e for e in state.records if e.kind == kind]
    if not rows:
        raise InsufficientEvidence(f"MISSING_{kind}")
    return rows
