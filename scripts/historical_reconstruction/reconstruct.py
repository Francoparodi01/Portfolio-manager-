"""Pure historical reconstruction logic for Quantia.

This module never connects to the database. It classifies already-extracted
formal-plan evidence and delegates price-return math to the audited dashboard
calculator from PR #16.

Important lineage rule: execution_plans + order_intents are the source of truth
for a recorded formal plan. decision_log is compatibility/audit evidence. A
corrupted legacy decision_log link is therefore reported as a lineage defect,
but it does not erase an otherwise self-contained formal plan signal.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable

CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW", "UNRECOVERABLE")
PRIMARY_LEVELS = {"HIGH", "MEDIUM"}

REASON_IMMUTABLE_CAPTURE = "IMMUTABLE_PLAN_CAPTURE"
REASON_CAPTURE_OWNER_PROOF = "IMMUTABLE_CAPTURE_OWNER_PROOF"
REASON_EXPLICIT_OWNER = "EXPLICIT_OWNER"
REASON_LEGACY_OWNER = "LEGACY_OWNER_INFERRED"
REASON_OWNER_MISMATCH = "OWNER_MISMATCH"
REASON_DECISION_MISSING = "DECISION_LINK_MISSING"
REASON_DECISION_OWNER = "BROKEN_DECISION_OWNER_LINK"
REASON_DECISION_SOURCE = "BROKEN_DECISION_DOMAIN_LINK"
REASON_TICKER_MISMATCH = "BROKEN_DECISION_TICKER_LINK"
REASON_RUN_MISMATCH = "MUTATED_CROSS_RUN_DECISION_LINK"
REASON_REUSED_DECISION = "DECISION_LINK_REUSED_ACROSS_PLANS"
REASON_SUPERSEDED = "DECISION_SUPERSEDED"
REASON_ROW_UPDATED = "ROW_UPDATED_AFTER_CREATION"


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _text(value: Any) -> str:
    return str(value or "").strip().lower()


def is_evaluable_intent(row: dict[str, Any]) -> bool:
    """Only formal, feasible, executable BUY/SELL intents are episodes."""
    return (
        _text(row.get("plan_source")) == "execution_plan"
        and row.get("feasible") is True
        and row.get("is_executable") is True
        and row.get("was_blocked") is False
        and _upper(row.get("side")) in {"BUY", "SELL"}
        and bool(_upper(row.get("ticker")))
    )


def _changed_after_creation(created, updated) -> bool:
    if not created or not updated:
        return False
    return updated > created


def decision_link_reuse(rows: Iterable[dict[str, Any]]) -> dict[int, set[str]]:
    links: dict[int, set[str]] = defaultdict(set)
    for row in rows:
        decision_id = row.get("decision_log_id")
        if decision_id is not None:
            links[int(decision_id)].add(str(row.get("plan_id")))
    return links


def classify_episode(
    row: dict[str, Any],
    *,
    requested_owner: int,
    legacy_owner_verified: bool,
    immutable_plan_ids: set[str] | None = None,
    reused_links: dict[int, set[str]] | None = None,
) -> dict[str, Any]:
    """Classify one formal-plan intent without inventing historical evidence.

    Confidence is about whether the recorded PLAN SIGNAL can be defended:

    HIGH: matching immutable capture exists for the requested owner.
    MEDIUM: explicit requested owner on a self-contained formal plan/order intent.
    LOW: strict single-owner NULL inference is required.
    UNRECOVERABLE: the formal plan itself cannot be attributed to the owner or
    is not an evaluable formal signal.

    decision_log is never used to decide ticker/side/outcome for the episode.
    Its contradictions are lineage diagnostics, not reasons to discard an intact
    execution_plan/order_intent record.
    """
    immutable_plan_ids = immutable_plan_ids or set()
    reused_links = reused_links or {}
    reasons: list[str] = []

    if not is_evaluable_intent(row):
        return {
            "confidence": "UNRECOVERABLE",
            "primary_eligible": False,
            "decision_link_status": "NOT_APPLICABLE",
            "reason_codes": ["NOT_EVALUABLE_FORMAL_SIGNAL"],
        }

    plan_id = str(row.get("plan_id"))
    immutable = plan_id in immutable_plan_ids
    plan_owner = row.get("plan_owner_chat_id")

    if immutable:
        # immutable_plan_ids is loaded from decision_lab_plan_captures filtered by
        # owner_chat_id=requested_owner, so the capture itself is owner proof.
        reasons.extend([REASON_IMMUTABLE_CAPTURE, REASON_CAPTURE_OWNER_PROOF])
        owner_confidence = "CAPTURE_PROVEN"
    elif plan_owner == requested_owner:
        reasons.append(REASON_EXPLICIT_OWNER)
        owner_confidence = "EXPLICIT"
    elif plan_owner is None and legacy_owner_verified:
        reasons.append(REASON_LEGACY_OWNER)
        owner_confidence = "LEGACY_INFERRED"
    else:
        return {
            "confidence": "UNRECOVERABLE",
            "primary_eligible": False,
            "decision_link_status": "NOT_TRUSTED",
            "reason_codes": [REASON_OWNER_MISMATCH],
        }

    decision_link_status = "HEALTHY"
    decision_id = row.get("decision_log_id")
    if decision_id is None or row.get("decision_exists") is False:
        decision_link_status = "MISSING"
        reasons.append(REASON_DECISION_MISSING)
    else:
        broken = False
        decision_owner = row.get("decision_owner_chat_id")
        if decision_owner not in (None, requested_owner):
            broken = True
            reasons.append(REASON_DECISION_OWNER)

        decision_source = _text(row.get("decision_source"))
        if decision_source and decision_source != "execution_plan":
            broken = True
            reasons.append(REASON_DECISION_SOURCE)

        decision_ticker = _upper(row.get("decision_ticker"))
        if decision_ticker and decision_ticker != _upper(row.get("ticker")):
            broken = True
            reasons.append(REASON_TICKER_MISMATCH)

        plan_run = row.get("plan_run_id")
        decision_run = row.get("decision_run_id")
        if plan_run and decision_run and str(plan_run) != str(decision_run):
            broken = True
            reasons.append(REASON_RUN_MISMATCH)

        plans = reused_links.get(int(decision_id), set())
        if len(plans) > 1:
            broken = True
            reasons.append(REASON_REUSED_DECISION)

        if row.get("superseded_by_id") is not None:
            broken = True
            reasons.append(REASON_SUPERSEDED)

        if broken:
            decision_link_status = "BROKEN"

    if (
        _changed_after_creation(row.get("created_at"), row.get("plan_updated_at"))
        or _changed_after_creation(row.get("intent_created_at"), row.get("intent_updated_at"))
    ):
        # Diagnostic only: persistence can legitimately update timestamps.
        reasons.append(REASON_ROW_UPDATED)

    if immutable:
        confidence = "HIGH"
    elif owner_confidence == "EXPLICIT":
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "confidence": confidence,
        "primary_eligible": confidence in PRIMARY_LEVELS,
        "decision_link_status": decision_link_status,
        "reason_codes": sorted(set(reasons)),
    }


def reconstruct_episodes(
    rows: Iterable[dict[str, Any]],
    *,
    requested_owner: int,
    legacy_owner_verified: bool,
    immutable_plan_ids: set[str] | None = None,
) -> dict[str, Any]:
    rows = [dict(r) for r in rows]
    eligible = [r for r in rows if is_evaluable_intent(r)]
    reuse = decision_link_reuse(eligible)
    episodes = []
    excluded = len(rows) - len(eligible)
    for row in eligible:
        quality = classify_episode(
            row,
            requested_owner=requested_owner,
            legacy_owner_verified=legacy_owner_verified,
            immutable_plan_ids=immutable_plan_ids,
            reused_links=reuse,
        )
        episodes.append(
            {
                "plan_id": str(row.get("plan_id")),
                "intent_id": row.get("intent_id"),
                "decision_log_id": row.get("decision_log_id"),
                "created_at": row.get("created_at"),
                "ticker": _upper(row.get("ticker")),
                "side": _upper(row.get("side")),
                **quality,
            }
        )

    confidence = Counter(e["confidence"] for e in episodes)
    reasons = Counter(code for e in episodes for code in e["reason_codes"])
    link_status = Counter(e["decision_link_status"] for e in episodes)
    return {
        "episodes": episodes,
        "confidence_counts": {
            level: confidence.get(level, 0) for level in CONFIDENCE_LEVELS
        },
        "decision_link_status_counts": dict(sorted(link_status.items())),
        "reason_counts": dict(sorted(reasons.items())),
        "raw_intents": len(rows),
        "evaluable_intents": len(eligible),
        "excluded_non_evaluable": excluded,
        "reused_decision_links": sum(1 for plans in reuse.values() if len(plans) > 1),
    }


def rows_for_confidence(
    raw_rows: Iterable[dict[str, Any]],
    episodes: Iterable[dict[str, Any]],
    allowed: set[str],
) -> list[dict[str, Any]]:
    by_intent = {e["intent_id"]: e for e in episodes}
    result = []
    for raw in raw_rows:
        episode = by_intent.get(raw.get("intent_id"))
        if not episode or episode["confidence"] not in allowed:
            continue
        result.append(
            {
                "plan_id": raw.get("plan_id"),
                "run_id": raw.get("plan_run_id"),
                "created_at": raw.get("created_at"),
                "source": raw.get("plan_source"),
                "feasible": raw.get("feasible"),
                "intent_id": raw.get("intent_id"),
                "ticker": raw.get("ticker"),
                "side": raw.get("side"),
                "is_executable": raw.get("is_executable"),
                "was_blocked": raw.get("was_blocked"),
            }
        )
    return result


def attach_outcomes(
    episodes: list[dict[str, Any]], outcome_result: dict[str, Any]
) -> list[dict[str, Any]]:
    by_intent = {s["intent_id"]: s for s in outcome_result.get("signals", [])}
    merged = []
    for episode in episodes:
        signal = by_intent.get(episode["intent_id"])
        item = dict(episode)
        if signal:
            item["returns"] = signal["returns"]
            item["outcome_details"] = signal["details"]
            item["entry_date"] = signal["entry_date"]
        else:
            item["returns"] = {}
            item["outcome_details"] = {}
        merged.append(item)
    return merged


def reconstruction_summary(report: dict[str, Any]) -> str:
    rec = report["reconstruction"]
    conf = rec["confidence_counts"]
    primary = report["outcomes"]["primary"]["metrics"]
    low = report["outcomes"]["low"]["metrics"]

    def metric_line(label: str, metrics: dict[str, Any]) -> str:
        parts = []
        for horizon in ("5", "10", "20", "40"):
            m = metrics[horizon]
            avg = "N/D" if m["mean_pct"] is None else f'{m["mean_pct"]:.2f}%'
            parts.append(f"{horizon}D n={m['n']} EV={avg}")
        return f"- {label}: " + " | ".join(parts)

    link_counts = rec.get("decision_link_status_counts", {})
    return "\n".join(
        [
            "# Historical Reconstruction v1",
            "",
            f"- Modo: {report['mode']}",
            f"- Owner scope: {report['owner_scope']}",
            f"- HIGH: {conf['HIGH']}",
            f"- MEDIUM: {conf['MEDIUM']}",
            f"- LOW: {conf['LOW']}",
            f"- UNRECOVERABLE: {conf['UNRECOVERABLE']}",
            f"- Intenciones no evaluables: {rec['excluded_non_evaluable']}",
            f"- Links de decision reutilizados: {rec['reused_decision_links']}",
            f"- Decision links HEALTHY: {link_counts.get('HEALTHY', 0)}",
            f"- Decision links BROKEN: {link_counts.get('BROKEN', 0)}",
            f"- Decision links MISSING: {link_counts.get('MISSING', 0)}",
            "",
            "## Outcomes recalculados desde precios crudos",
            metric_line("PRIMARY (HIGH+MEDIUM)", primary),
            metric_line("LOW (separado, no primario)", low),
            "",
            "## Invariantes",
            "- No usa outcome_* ni executable_outcome_* historicos.",
            "- La señal formal sale de execution_plans + order_intents; decision_log es evidencia auxiliar.",
            "- Un decision_log cross-run/reutilizado se marca BROKEN, pero no borra un plan formal autocontenido.",
            "- Radar/optimizer no entran en la muestra formal.",
            "- LOW nunca entra en metricas primarias.",
            "- UNRECOVERABLE no recibe outcomes primarios.",
            "- El proceso no modifica PostgreSQL.",
        ]
    ) + "\n"
