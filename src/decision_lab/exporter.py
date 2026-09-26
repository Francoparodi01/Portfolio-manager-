"""Bulk read-only export of legacy evidence. No sync, downloads or DB mutations.

Unknown revisions/adjustments remain APPROXIMATE. This exporter cannot create
historical macro, universe or corporate-action vintages that were never stored.
"""

from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone, time
from decimal import Decimal
import json
from pathlib import Path

from src.agentic.read_only import connect_read_only
from src.core.market_calendar import is_trading_day
from .models import Dataset, Evidence, Session, canonical, digest

ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc


def sessions_between(start, end):
    calendar = json.loads(
        (ROOT / "config/market_holidays_ar.json").read_text(encoding="utf-8")
    )
    years = {int(c["date"][:4]) for c in calendar["closures"]}
    if any(y not in years for y in range(start.year, end.year + 1)):
        raise ValueError("market calendar does not cover requested years")
    result = []
    day = start.date()
    while day <= end.date():
        if is_trading_day(day):
            # Existing BYMA daily convention: 10:30-17:00 ART (UTC-3).
            result.append(
                Session(
                    session_id=day.isoformat(),
                    open_at=datetime.combine(day, time(13, 30), UTC),
                    close_at=datetime.combine(day, time(20), UTC),
                )
            )
        day += timedelta(days=1)
    return tuple(result), digest(calendar)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value or {}


def _clean_features(value):
    # Persist only the already-frozen feature snapshot and run metadata, never
    # decision_log's postdecision status, executions or outcome fields.
    if isinstance(value, dict):
        return {
            k: _clean_features(v)
            for k, v in value.items()
            if not k.startswith(
                ("outcome_", "executable_outcome_", "forward_return", "future_return")
            )
            and k not in {"was_correct", "closed_at", "label_timeout"}
        }
    if isinstance(value, list):
        return [_clean_features(v) for v in value]
    return value


def captured_records(capture):
    """Convert a prospective immutable capture without rejoining mutable rows."""
    p = _json(capture["payload"])
    at = datetime.fromisoformat(p["captured_at"])
    owner = p["owner"]
    raw = p["portfolio"]
    if any(r.get("currency") != "ARS" for r in raw.get("positions", [])):
        raise ValueError("NON_ARS_CAPTURE_REQUIRES_FX_MARKS")
    sid = capture["capture_hash"] + ":portfolio"
    portfolio = {
        "cash_ars": raw["cash_ars"],
        "positions": [
            {
                "ticker": r["ticker"],
                "quantity": r["quantity"],
                "mark_ars": r["current_price"],
                "lot_size": 1,
                "sector": r.get("sector"),
            }
            for r in raw.get("positions", [])
        ],
        "assumptions": ["LOT_ONE_BYMA_CONVENTION"],
    }
    plan = p["plan"]
    orders = []
    decisions = {r["ticker"]: r for r in plan["decisions"]}
    for field in ("sell_orders", "buy_orders", "blocked_orders"):
        for o in plan[field]:
            d = decisions.get(o["ticker"], {})
            orders.append(
                {
                    "ticker": o["ticker"],
                    "side": o["side"],
                    "quantity": o.get("quantity_est"),
                    "reference_price": o.get("reference_price"),
                    "target_amount_ars": o["amount_ars"],
                    "executable": field != "blocked_orders",
                    "blocked": field == "blocked_orders",
                    "action": o["action"],
                    "current_weight": d.get("current_weight"),
                    "target_weight": d.get("target_weight"),
                    "reason": o.get("reason", ""),
                    "restriction": o.get("block_code"),
                    "priority": o.get("priority", 0),
                    "funded_by": o.get("funded_by", []),
                }
            )
    pid = capture["capture_hash"] + ":plan"
    payload = {
        "complete": True,
        "orders": orders,
        "cash_before": plan["cash_before"],
        "cash_precision": "ROUND_HALF_EVEN_WHOLE_ARS",
        "feasible": plan["feasible"],
        "portfolio_snapshot_id": sid,
        "decisions": plan["decisions"],
        "segments": {"planner_guard": plan["gate"]},
        "historical_strategy_version": "captured-code:" + digest(p["code_hashes"]),
        "assumptions": ["CAPTURE_IS_RECORDED_PLAN_NOT_FULL_POLICY_INPUT_SET"],
    }
    result = []

    def put(kind, rid, value, effective=at):
        result.append(
            Evidence(
                kind=kind,
                record_id=rid,
                effective_at=effective,
                available_at=at,
                source="immutable_formal_plan_capture_v1",
                quality="POINT_IN_TIME_SAFE",
                owner=owner,
                payload_json=canonical(_clean_features(value)),
            )
        )

    put("PORTFOLIO", sid, portfolio, datetime.fromisoformat(raw["scraped_at"]))
    put("PLAN", pid, payload)
    macro = p.get("macro")
    if macro:
        put("MACRO", capture["capture_hash"] + ":macro", macro)
        if macro.get("ccl") is not None:
            put(
                "FX",
                capture["capture_hash"] + ":fx",
                {"ccl": macro["ccl"], "basis": "CAPTURED_INPUT"},
            )
    for r in p.get("signals", []):
        put(
            "FEATURES",
            capture["capture_hash"] + ":" + r["ticker"],
            {"ticker": r["ticker"], "snapshot": r},
        )
    return result, {"as_of": at.isoformat(), "plan_id": pid, "portfolio_id": sid}


async def export_legacy(
    dsn,
    *,
    owner,
    start,
    end,
    evaluated_as_of,
    legacy_single_owner=False,
    candle_source="TRADINGVIEW_BYMA",
):
    if (
        not owner
        or any(t.tzinfo is None for t in (start, end, evaluated_as_of))
        or end < start
        or end > evaluated_as_of
    ):
        raise ValueError("explicit owner and ordered aware cutoffs required")
    conn = await connect_read_only(dsn, command_timeout=60)
    records = []
    warnings = []
    requests = []
    rejected = []

    def add(kind, rid, effective, available, payload, private=False, revision=None):
        records.append(
            Evidence(
                kind=kind,
                record_id=str(rid),
                effective_at=effective,
                available_at=available,
                revision_at=revision,
                source="legacy_postgresql_export_v1",
                quality="APPROXIMATE",
                owner=owner if private else None,
                payload_json=canonical(payload),
            )
        )

    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            owners = await conn.fetch(
                """SELECT DISTINCT owner_chat_id FROM portfolio_snapshots WHERE owner_chat_id IS NOT NULL
                UNION SELECT DISTINCT owner_chat_id FROM decision_log WHERE owner_chat_id IS NOT NULL
                UNION SELECT DISTINCT owner_chat_id FROM broker_fills WHERE owner_chat_id IS NOT NULL
                UNION SELECT DISTINCT owner_chat_id FROM execution_plans WHERE owner_chat_id IS NOT NULL"""
            )
            if legacy_single_owner and {r["owner_chat_id"] for r in owners} != {owner}:
                raise ValueError(
                    "legacy owner inference requires exactly the requested verified owner"
                )
            if legacy_single_owner:
                warnings.append("LEGACY_OWNER_INFERRED_EXCLUDED_FROM_PRIMARY")
            captured_plan_ids = set()
            if await conn.fetchval(
                "SELECT to_regclass('public.decision_lab_plan_captures')"
            ):
                captures = await conn.fetch(
                    "SELECT * FROM decision_lab_plan_captures WHERE owner_chat_id=$1 AND captured_at BETWEEN $2 AND $3 ORDER BY captured_at,capture_hash",
                    owner,
                    start,
                    min(end, evaluated_as_of),
                )
                for capture in captures:
                    try:
                        rows, request = captured_records(capture)
                    except (ValueError, KeyError, TypeError) as exc:
                        rejected.append(
                            {
                                "capture_hash": capture["capture_hash"],
                                "reason": str(exc),
                            }
                        )
                        continue
                    records.extend(rows)
                    requests.append(request)
                    captured_plan_ids.add(capture["plan_id"])
            plans = await conn.fetch(
                """SELECT * FROM execution_plans WHERE created_at >= $1 AND created_at <= $2
                AND (owner_chat_id=$3 OR ($4 AND owner_chat_id IS NULL)) ORDER BY created_at,id""",
                start,
                end,
                owner,
                legacy_single_owner,
            )
            plans = [r for r in plans if str(r["id"]) not in captured_plan_ids]
            ids = [r["id"] for r in plans]
            orders = await conn.fetch(
                "SELECT * FROM order_intents WHERE execution_plan_id=ANY($1::uuid[]) ORDER BY execution_plan_id,sequence_no,id",
                ids,
            )
            decision_ids = sorted(
                {
                    r["decision_log_id"]
                    for r in orders
                    if r["decision_log_id"] is not None
                }
            )
            decisions = await conn.fetch(
                """SELECT id,ticker,decided_at,owner_chat_id,run_id,decision,final_score,confidence,vix_at_decision,regime,
                current_weight,target_weight,layers FROM decision_log WHERE id=ANY($1::bigint[])
                AND (owner_chat_id=$2 OR ($3 AND owner_chat_id IS NULL))""",
                decision_ids,
                owner,
                legacy_single_owner,
            )
            snapshots = await conn.fetch(
                """SELECT p.snapshot_id,p.scraped_at,p.created_at,p.owner_chat_id,p.cash_ars,p.total_value_ars,r.payload
                FROM portfolio_snapshots p JOIN raw_snapshots r ON r.snapshot_id=p.snapshot_id AND r.scraped_at=p.scraped_at
                WHERE p.scraped_at >= $1 AND p.scraped_at <= $2
                AND (p.owner_chat_id=$3 OR ($4 AND p.owner_chat_id IS NULL)) ORDER BY p.scraped_at,p.snapshot_id""",
                start - timedelta(days=7),
                end,
                owner,
                legacy_single_owner,
            )
            order_map = defaultdict(list)
            for o in orders:
                order_map[o["execution_plan_id"]].append(dict(o))
            decision_map = {r["id"]: dict(r) for r in decisions}
            snapshot_map = {}
            tickers = {
                r["ticker"]
                for e in records
                if e.kind == "PORTFOLIO"
                for r in e.payload["positions"]
            }
            tickers.update(
                r["ticker"]
                for e in records
                if e.kind == "PLAN"
                for r in e.payload["orders"]
            )
            for s in snapshots:
                raw = _json(s["payload"])
                positions = []
                valid = True
                for p in raw.get("positions", []):
                    if (
                        p.get("currency") != "ARS"
                        or p.get("quantity") is None
                        or not p.get("current_price")
                        or p["current_price"] <= 0
                    ):
                        valid = False
                        break
                    positions.append(
                        {
                            "ticker": p["ticker"],
                            "quantity": str(p["quantity"]),
                            "mark_ars": str(p["current_price"]),
                            "lot_size": "1",
                            "sector": p.get("sector"),
                        }
                    )
                if not valid:
                    continue
                available = max(s["created_at"], s["scraped_at"])
                payload = {
                    "positions": positions,
                    "cash_ars": str(s["cash_ars"]),
                    "reported_total_value_ars": str(s["total_value_ars"]),
                    "assumptions": [
                        "LEGACY_SNAPSHOT_OVERWRITABLE_NO_REVISION_HISTORY",
                        "LOT_ONE_BYMA_CONVENTION",
                    ],
                }
                if s["owner_chat_id"] is None:
                    payload["assumptions"].append("LEGACY_OWNER_INFERRED")
                snapshot_map[str(s["snapshot_id"])] = (s, payload, available)
            used_snapshots = set()
            for plan in plans:
                pid = str(plan["id"])
                source_orders = order_map[plan["id"]]
                freeze = max(
                    [
                        plan["created_at"],
                        plan["updated_at"],
                        *(o["created_at"] for o in source_orders),
                        *(o["updated_at"] for o in source_orders),
                    ]
                )
                if freeze > evaluated_as_of:
                    rejected.append(
                        {
                            "plan_id": pid,
                            "reason": "PLAN_VERSION_NOT_AVAILABLE_AT_CUTOFF",
                        }
                    )
                    continue
                links = [decision_map.get(o["decision_log_id"]) for o in source_orders]
                layers = [_json(d["layers"]) for d in links if d]
                contexts = [x.get("run_context", {}) for x in layers]
                if any(
                    d and d.get("run_id") and d["run_id"] != plan["run_id"]
                    for d in links
                ) or any(
                    c.get("run_id") and c["run_id"] != str(plan["run_id"])
                    for c in contexts
                ):
                    rejected.append(
                        {"plan_id": pid, "reason": "MUTATED_CROSS_RUN_DECISION_LINK"}
                    )
                    continue
                freeze = max([freeze, *[d["decided_at"] for d in links if d]])
                if freeze > evaluated_as_of:
                    rejected.append(
                        {
                            "plan_id": pid,
                            "reason": "DECISION_EVIDENCE_NOT_AVAILABLE_AT_CUTOFF",
                        }
                    )
                    continue
                portfolio_stamps = {
                    x.get("portfolio_snapshot_id")
                    for x in contexts
                    if x.get("portfolio_snapshot_id")
                }
                possible = [
                    (sid, s, p, a)
                    for sid, (s, p, a) in snapshot_map.items()
                    if a <= freeze
                    and (
                        not portfolio_stamps
                        or str(s["snapshot_id"]) in portfolio_stamps
                        or s["scraped_at"].isoformat() in portfolio_stamps
                    )
                ]
                # No hindsight matching on returns or prices. Exact recorded
                # timestamp first; otherwise last prior snapshot, cash checked below.
                if not possible:
                    rejected.append(
                        {"plan_id": pid, "reason": "NO_RECORDED_PORTFOLIO_LINK"}
                    )
                    continue
                sid, s, p, available = max(possible, key=lambda x: x[1]["scraped_at"])
                if abs(
                    Decimal(p["cash_ars"]).quantize(Decimal(1))
                    - Decimal(str(plan["cash_before"]))
                ) > Decimal("0.05"):
                    rejected.append(
                        {"plan_id": pid, "reason": "PLAN_PORTFOLIO_CASH_MISMATCH"}
                    )
                    continue
                rows = []
                decision_rows = []
                failed = None
                for o, d in zip(source_orders, links):
                    if d is None and o["decision_log_id"] is not None:
                        failed = "ORDER_DECISION_OWNER_OR_LINK_MISSING"
                        break
                    if (
                        o["is_executable"]
                        and not o["was_blocked"]
                        and (o["quantity_est"] is None or o["reference_price"] is None)
                    ):
                        failed = "ORDER_QUANTITY_OR_PRICE_MISSING"
                        break
                    meta = _json(o["metadata"])
                    rows.append(
                        {
                            "ticker": o["ticker"],
                            "side": o["side"],
                            "quantity": (
                                str(o["quantity_est"])
                                if o["quantity_est"] is not None
                                else None
                            ),
                            "reference_price": (
                                str(o["reference_price"])
                                if o["reference_price"] is not None
                                else None
                            ),
                            "target_amount_ars": str(o["amount_ars"]),
                            "executable": o["is_executable"],
                            "blocked": o["was_blocked"],
                            "action": o["action"],
                            "current_weight": (
                                float(d["current_weight"])
                                if d and d["current_weight"] is not None
                                else None
                            ),
                            "target_weight": (
                                float(d["target_weight"])
                                if d and d["target_weight"] is not None
                                else None
                            ),
                            "reason": o["reason"] or "",
                            "restriction": o["block_code"],
                            "priority": o["priority"] or 0,
                            "funded_by": meta.get("funded_by", []),
                        }
                    )
                    if d:
                        decision_rows.append(
                            {
                                "ticker": d["ticker"],
                                "action": d["decision"],
                                "score": d["final_score"],
                                "conviction": d["confidence"],
                                "macro_regime": d["regime"],
                            }
                        )
                if failed:
                    rejected.append({"plan_id": pid, "reason": failed})
                    continue
                used_snapshots.add(sid)
                assumptions = ["RECORDED_PROPOSAL_NOT_HISTORICAL_STRATEGY_REEXECUTION"]
                if not portfolio_stamps:
                    assumptions.append("LAST_PRIOR_PORTFOLIO_WITH_MATCHED_CASH")
                if plan["owner_chat_id"] is None:
                    assumptions.append("LEGACY_OWNER_INFERRED")
                add(
                    "PLAN",
                    pid,
                    plan["created_at"],
                    freeze,
                    {
                        "complete": True,
                        "cash_before": plan["cash_before"],
                        "cash_precision": "ROUND_HALF_EVEN_WHOLE_ARS",
                        "feasible": plan["feasible"],
                        "portfolio_snapshot_id": sid,
                        "orders": rows,
                        "decisions": decision_rows,
                        "gate": plan["gate"],
                        "source_warnings": _json(plan["warnings"]),
                        "historical_strategy_version": (
                            contexts[0].get("strategy_version", "UNKNOWN")
                            if contexts
                            else "UNKNOWN"
                        ),
                        "segments": {"planner_guard": plan["gate"]},
                        "assumptions": assumptions,
                    },
                    True,
                    freeze,
                )
                requests.append(
                    {"as_of": freeze.isoformat(), "plan_id": pid, "portfolio_id": sid}
                )
                tickers.update(r["ticker"] for r in rows)
                for d, layer in zip([x for x in links if x], layers):
                    if layer.get("feature_snapshot"):
                        rid = str(d["id"])
                        if not any(
                            r.kind == "FEATURES" and r.record_id == rid for r in records
                        ):
                            add(
                                "FEATURES",
                                rid,
                                d["decided_at"],
                                freeze,
                                {
                                    "ticker": d["ticker"],
                                    "snapshot": _clean_features(
                                        layer["feature_snapshot"]
                                    ),
                                },
                                True,
                            )
            for sid in sorted(used_snapshots):
                s, p, a = snapshot_map[sid]
                add("PORTFOLIO", sid, s["scraped_at"], a, p, True)
                tickers.update(x["ticker"] for x in p["positions"])
            candles = await conn.fetch(
                """SELECT * FROM market_candles WHERE ticker=ANY($1::text[]) AND ts >= $2 AND ts <= $3
                AND scraped_at <= $3 AND source=$4 AND interval='1d' AND currency='ARS' ORDER BY ts,ticker""",
                sorted(tickers),
                start - timedelta(days=360),
                evaluated_as_of,
                candle_source,
            )
            # UTC date is the stored DAILY label, including internal_snapshot's
            # 00:00Z. Converting midnight to ART would incorrectly shift a day.
            calendar_start = min([start, *[r["ts"] for r in candles]])
            calendar_start = max(calendar_start, datetime(start.year, 1, 1, tzinfo=UTC))
            calendar_end = min(
                evaluated_as_of + timedelta(days=75),
                datetime(evaluated_as_of.year, 12, 31, 23, tzinfo=UTC),
            )
            sessions, calendar_version = sessions_between(calendar_start, calendar_end)
            session_map = {s.session_id: s for s in sessions}
            for r in candles:
                session = session_map.get(r["ts"].date().isoformat())
                if session is None:
                    continue
                add(
                    "BAR",
                    f"{r['ticker']}:{r['ts'].isoformat()}:{r['source']}",
                    session.close_at,
                    max(r["scraped_at"], session.close_at),
                    {
                        "ticker": r["ticker"],
                        "session_id": session.session_id,
                        "open": r["open_price"],
                        "high": r["high_price"],
                        "low": r["low_price"],
                        "close": r["close_price"],
                        "volume": r["volume"],
                        "price_mode": "UNKNOWN",
                        "provider": r["source"],
                    },
                    revision=r["scraped_at"],
                )
    finally:
        await conn.close()
    dataset = Dataset(
        records=tuple(records), sessions=sessions, calendar_version=calendar_version
    )
    return dataset, {
        "schema": "decision-lab-export-v1",
        "owner": owner,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "evaluated_as_of": evaluated_as_of.isoformat(),
        "requests": requests,
        "rejected": rejected,
        "warnings": warnings,
        "source_counts": dict(sorted(Counter(e.kind for e in records).items())),
        "candle_source": candle_source,
        "missing_sources": [
            "MACRO_VINTAGES",
            "FX_VINTAGES",
            "UNIVERSE_HISTORY",
            "COMPLETE_CORPORATE_ACTIONS",
            "CONFIG_HISTORY",
        ],
        "dataset_hash": digest(dataset),
    }
