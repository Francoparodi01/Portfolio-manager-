from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.read_only import connect_read_only
from src.agentic.tools import ToolContext, ToolRegistry, read_only_dsn


_REGRESSION_MODES = {"signal", "optimizer", "execution", "blocked", "all"}
_QUALITY_MODES = {"strict", "relaxed", "all"}


def _normalized_decision(value: Any) -> str:
    raw = str(value or "").upper().strip()
    if raw in {"BUY", "BUY_FULL", "BUY_PARTIAL", "BUY_REBALANCE", "ADD", "ACCUMULATE"}:
        return "ACCUMULATE"
    if raw in {"SELL", "SELL_FULL", "SELL_PARTIAL", "EXIT", "CLOSE", "REDUCE"}:
        return "REDUCE"
    return raw or "HOLD"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def register_audit_tools(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    """Register additional read-only evidence and statistical audit tools."""

    async def persisted_decision_evidence(arguments: dict[str, Any]) -> ToolObservation:
        """Read the latest persisted formal analysis instead of recomputing it.

        This is intentionally optimized for conversational status checks. It
        preserves the original run timestamp so callers can reason about
        freshness without paying the cost of scripts/run_analysis.py.
        """
        started = time.monotonic()
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=20)
        try:
            owner = int(context.owner_chat_id or 0)
            legacy = bool(context.legacy_single_owner)
            candidates: list[tuple[str, datetime]] = []

            decision_latest = await conn.fetchrow(
                """
                SELECT run_id::text AS run_id, MAX(decided_at) AS as_of
                FROM decision_log
                WHERE run_id IS NOT NULL
                  AND (owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL))
                  AND COALESCE(source, layers->>'source') = 'execution_plan'
                GROUP BY run_id
                ORDER BY MAX(decided_at) DESC
                LIMIT 1
                """,
                owner,
                legacy,
            )
            if decision_latest and decision_latest["run_id"] and decision_latest["as_of"]:
                candidates.append((str(decision_latest["run_id"]), decision_latest["as_of"]))

            try:
                hold_latest = await conn.fetchrow(
                    """
                    SELECT run_id::text AS run_id, MAX(observed_at) AS as_of
                    FROM position_hold_observations
                    WHERE run_id IS NOT NULL
                      AND (owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL))
                    GROUP BY run_id
                    ORDER BY MAX(observed_at) DESC
                    LIMIT 1
                    """,
                    owner,
                    legacy,
                )
            except Exception:
                hold_latest = None
            if hold_latest and hold_latest["run_id"] and hold_latest["as_of"]:
                candidates.append((str(hold_latest["run_id"]), hold_latest["as_of"]))

            latest_snapshot_as_of = await conn.fetchval(
                """
                SELECT MAX(scraped_at)
                FROM portfolio_snapshots
                WHERE owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL)
                """,
                owner,
                legacy,
            )

            if not candidates:
                payload = {
                    "schema_version": "persisted-decision-evidence-v1",
                    "evidence_source": "persisted_latest_run",
                    "status": "missing",
                    "evaluated_at": None,
                    "snapshot_as_of": None,
                    "latest_portfolio_snapshot_as_of": (
                        latest_snapshot_as_of.isoformat() if latest_snapshot_as_of else None
                    ),
                    "signals": [],
                    "warnings": ["No persisted formal decision run is available."],
                }
                return ToolObservation(
                    tool_name="get_persisted_decision_evidence",
                    arguments=arguments,
                    ok=True,
                    content=json.dumps(payload, ensure_ascii=False),
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                )

            run_id, evaluated_at = max(candidates, key=lambda item: item[1])
            if evaluated_at.tzinfo is None:
                evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)

            # Bind the decision run to the portfolio snapshot that existed when
            # that run was evaluated. Never label today's latest account snapshot
            # as the source snapshot of an older decision run.
            run_snapshot_as_of = await conn.fetchval(
                """
                SELECT MAX(scraped_at)
                FROM portfolio_snapshots
                WHERE (owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL))
                  AND scraped_at <= $3
                """,
                owner,
                legacy,
                evaluated_at,
            )

            by_ticker: dict[str, dict[str, Any]] = {}
            decision_rows = await conn.fetch(
                """
                SELECT
                    ticker,
                    decision,
                    decision_type,
                    final_score,
                    layers,
                    status,
                    block_reason,
                    decided_at AS as_of
                FROM decision_log
                WHERE run_id=$1::uuid
                  AND (owner_chat_id=$2 OR ($3::boolean AND owner_chat_id IS NULL))
                  AND COALESCE(source, layers->>'source') = 'execution_plan'
                ORDER BY decided_at, id
                """,
                run_id,
                owner,
                legacy,
            )
            for raw in decision_rows:
                row = dict(raw)
                ticker = str(row.get("ticker") or "").upper().strip()
                if not ticker:
                    continue
                layers = _json_object(row.get("layers"))
                semantic_action = (
                    layers.get("action")
                    or row.get("decision")
                    or row.get("decision_type")
                )
                reason = (
                    layers.get("reason")
                    or layers.get("block_reason")
                    or row.get("block_reason")
                )
                by_ticker[ticker] = {
                    "ticker": ticker,
                    "decision": _normalized_decision(semantic_action),
                    "final_score": row.get("final_score"),
                    "status": str(row.get("status") or "UNKNOWN").upper(),
                    "reason": str(reason).strip() if reason else None,
                    "as_of": row.get("as_of").isoformat() if row.get("as_of") else None,
                    "source_kind": "decision_log",
                }

            try:
                hold_rows = await conn.fetch(
                    """
                    SELECT ticker, action, final_score, status, reason_primary, observed_at AS as_of
                    FROM position_hold_observations
                    WHERE run_id=$1::uuid
                      AND (owner_chat_id=$2 OR ($3::boolean AND owner_chat_id IS NULL))
                    ORDER BY observed_at, id
                    """,
                    run_id,
                    owner,
                    legacy,
                )
            except Exception:
                hold_rows = []
            for raw in hold_rows:
                row = dict(raw)
                ticker = str(row.get("ticker") or "").upper().strip()
                if not ticker or ticker in by_ticker:
                    continue
                by_ticker[ticker] = {
                    "ticker": ticker,
                    "decision": _normalized_decision(row.get("action")),
                    "final_score": row.get("final_score"),
                    "status": str(row.get("status") or "OBSERVED").upper(),
                    "reason": str(row.get("reason_primary") or "").strip() or None,
                    "as_of": row.get("as_of").isoformat() if row.get("as_of") else None,
                    "source_kind": "position_hold_observations",
                }

            age_seconds = max(0.0, (datetime.now(timezone.utc) - evaluated_at).total_seconds())
            payload = {
                "schema_version": "persisted-decision-evidence-v1",
                "evidence_source": "persisted_latest_run",
                "status": "observed",
                "analysis_run_id": run_id,
                "evaluated_at": evaluated_at.isoformat(),
                "decision_run_age_seconds": round(age_seconds, 1),
                "snapshot_as_of": run_snapshot_as_of.isoformat() if run_snapshot_as_of else None,
                "latest_portfolio_snapshot_as_of": (
                    latest_snapshot_as_of.isoformat() if latest_snapshot_as_of else None
                ),
                "signals": [by_ticker[key] for key in sorted(by_ticker)],
                "warnings": [
                    "Persisted evidence: the chat did not recompute the full analysis pipeline for this status request."
                ],
            }
            content = json.dumps(payload, ensure_ascii=False, default=str)
            return ToolObservation(
                tool_name="get_persisted_decision_evidence",
                arguments=arguments,
                ok=True,
                content=content[: context.output_limit_chars],
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="get_persisted_decision_evidence",
                arguments=arguments,
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            await conn.close()

    registry.register(
        ToolSpec(
            name="get_persisted_decision_evidence",
            description=(
                "Read the most recent persisted formal Quantia decision run directly from PostgreSQL. "
                "Fast/read-only status evidence with explicit timestamps; it does not recompute signals or execute orders."
            ),
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            read_only=True,
            timeout_seconds=20,
        ),
        persisted_decision_evidence,
    )

    async def bot_follow_pnl(arguments: dict[str, Any]) -> ToolObservation:
        """Compute the bot plan-level hypothetical directional PnL read-only.

        This mirrors the bot side of Decision Ledger without running schema
        migrations, human-fill matching, radar or unrelated reports. It is the
        bounded source for questions such as 'what if I had followed the bot?'.
        """
        started = time.monotonic()
        days = max(1, min(365, int(arguments.get("days", 90))))
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=20)
        try:
            row = await conn.fetchrow(
                """
                WITH plans AS (
                    SELECT
                        GREATEST(
                            ABS(COALESCE(
                                NULLIF(layers->>'amount_ars', '')::numeric,
                                NULLIF(executed_amount_ars, 0),
                                theoretical_amount_ars,
                                0
                            )),
                            1
                        )::double precision AS target_amount_ars,
                        COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                        COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                        COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d
                    FROM decision_log
                    WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                      AND (owner_chat_id=$2 OR ($3::boolean AND owner_chat_id IS NULL))
                      AND COALESCE(source, layers->>'source') = 'execution_plan'
                      AND COALESCE(run_intent, 'formal_plan') = 'formal_plan'
                      AND COALESCE(metric_scope, 'planner_audit') IN ('planner_audit', 'primary')
                      AND status IN ('APPROVED', 'EXECUTED')
                      AND decision_type = 'executable'
                      AND decision IN ('BUY', 'SELL')
                      AND price_at_decision IS NOT NULL
                )
                SELECT
                    COUNT(*)::int AS plans_total,
                    COUNT(outcome_5d)::int AS plans_closed_5d,
                    COUNT(outcome_10d)::int AS plans_closed_10d,
                    COUNT(outcome_20d)::int AS plans_closed_20d,
                    SUM(target_amount_ars * outcome_5d) AS bot_pnl_5d_ars,
                    SUM(target_amount_ars * outcome_10d) AS bot_pnl_10d_ars,
                    SUM(target_amount_ars * outcome_20d) AS bot_pnl_20d_ars
                FROM plans
                """,
                days,
                int(context.owner_chat_id or 0),
                bool(context.legacy_single_owner),
            )
            values = dict(row or {})
            payload = {
                "schema_version": "bot-follow-pnl-v1",
                "source": "decision_log_formal_plans",
                "mode": "PRODUCTION_OBSERVATION",
                "as_of": datetime.now(timezone.utc).isoformat(),
                "lookback_days": days,
                "plans_total": int(values.get("plans_total") or 0),
                "plans_closed_5d": int(values.get("plans_closed_5d") or 0),
                "plans_closed_10d": int(values.get("plans_closed_10d") or 0),
                "plans_closed_20d": int(values.get("plans_closed_20d") or 0),
                "bot_pnl_5d_ars": values.get("bot_pnl_5d_ars"),
                "bot_pnl_10d_ars": values.get("bot_pnl_10d_ars"),
                "bot_pnl_20d_ars": values.get("bot_pnl_20d_ars"),
                "scope": "FORMAL_PLAN_DIRECTIONAL_GROSS_PLAN_LEVEL_NOT_DEDUPLICATED",
                "limitations": [
                    "Hypothetical bot PnL, not realized account PnL.",
                    "Gross directional result before fees/slippage.",
                    "Plan-level rows can repeat recommendations across runs; do not treat horizons as additive.",
                ],
            }
            content = json.dumps(payload, ensure_ascii=False, default=str)
            return ToolObservation(
                tool_name="get_bot_follow_pnl",
                arguments={"days": days},
                ok=True,
                content=content[: context.output_limit_chars],
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="get_bot_follow_pnl",
                arguments={"days": days},
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            await conn.close()

    registry.register(
        ToolSpec(
            name="get_bot_follow_pnl",
            description=(
                "Read-only hypothetical PnL of following Quantia formal executable bot plans over a bounded lookback. "
                "Returns separate 5D/10D/20D gross plan-level outcomes and mature sample counts; never executes trades."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 90}
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=20,
        ),
        bot_follow_pnl,
    )

    async def regression_audit(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.regression_audit import (
            RegressionAuditConfig,
            render_regression_audit_compact,
            run_regression_audit,
        )

        started = time.monotonic()
        mode = str(arguments.get("mode") or "optimizer").lower().strip()
        if mode not in _REGRESSION_MODES:
            return ToolObservation(
                tool_name="get_regression_audit",
                arguments=arguments,
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"unsupported regression mode: {mode}",
            )
        report_obj = await run_regression_audit(
            RegressionAuditConfig(
                database_url=read_only_dsn(context.database_url),
                days=int(arguments.get("days", 180)),
                min_n=int(arguments.get("min_n", 12)),
                cost_bps=float(arguments.get("cost_bps", 75)),
                mode=mode,
                target_mode="directional",
            )
        )
        payload = {
            "source": "regression_audit",
            "mode": "RESEARCH",
            "generated_at": report_obj.generated_at.astimezone(timezone.utc).isoformat(),
            "audit_mode": report_obj.mode,
            "rows_loaded": report_obj.rows_loaded,
            "rows_usable": report_obj.rows_usable,
            "warnings": report_obj.warnings,
            "report": render_regression_audit_compact(report_obj),
        }
        return ToolObservation(
            tool_name="get_regression_audit",
            arguments=arguments,
            ok=True,
            content=json.dumps(payload, ensure_ascii=False, default=str)[: context.output_limit_chars],
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    registry.register(
        ToolSpec(
            name="get_regression_audit",
            description=(
                "Run Quantia's statistical regression audit over persisted decision outcomes. "
                "Research/read-only: it does not generate orders or modify planner thresholds."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 730, "default": 180},
                    "mode": {
                        "type": "string",
                        "enum": ["signal", "optimizer", "execution", "blocked", "all"],
                        "default": "optimizer",
                    },
                    "min_n": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 12},
                    "cost_bps": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 75},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=120,
        ),
        regression_audit,
    )

    async def calibration(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.dcl.report_generator import render_calibration_report
        from src.analysis.dcl.run_calibration import run_calibration_cycle

        started = time.monotonic()
        quality_mode = str(arguments.get("quality_mode") or "relaxed").lower().strip()
        if quality_mode not in _QUALITY_MODES:
            return ToolObservation(
                tool_name="get_calibration_audit",
                arguments=arguments,
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"unsupported quality mode: {quality_mode}",
            )
        report_obj = await run_calibration_cycle(
            read_only_dsn(context.database_url),
            days=int(arguments.get("days", 180)),
            owner_chat_id=context.owner_chat_id,
            quality_mode=quality_mode,
            min_n=int(arguments.get("min_n", 20)),
            dry_run=True,
        )
        payload = {
            "source": "decision_calibration_layer",
            "mode": "RESEARCH",
            "dry_run": True,
            "lookback_days": report_obj.lookback_days,
            "status": report_obj.status,
            "quality_mode": report_obj.quality_mode,
            "quality_counts": report_obj.quality_counts,
            "report": render_calibration_report(report_obj),
        }
        return ToolObservation(
            tool_name="get_calibration_audit",
            arguments=arguments,
            ok=True,
            content=json.dumps(payload, ensure_ascii=False, default=str)[: context.output_limit_chars],
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    registry.register(
        ToolSpec(
            name="get_calibration_audit",
            description=(
                "Run the Decision Calibration Layer audit in dry-run/read-only mode. "
                "It reports IC, EV, confidence and sample quality but applies no configuration changes."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 730, "default": 180},
                    "quality_mode": {
                        "type": "string",
                        "enum": ["strict", "relaxed", "all"],
                        "default": "relaxed",
                    },
                    "min_n": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 20},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=120,
        ),
        calibration,
    )
    return registry
