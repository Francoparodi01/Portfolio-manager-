from __future__ import annotations

import json
import time
from datetime import timezone
from typing import Any

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.tools import ToolContext, ToolRegistry, read_only_dsn


_REGRESSION_MODES = {"signal", "optimizer", "execution", "blocked", "all"}
_QUALITY_MODES = {"strict", "relaxed", "all"}


def register_audit_tools(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    """Register statistical audits that are demonstrably read-only."""

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
