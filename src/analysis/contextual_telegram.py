"""Pure contracts and rendering for /analisis_contextual.

The command displays a productive pipeline result plus E2 context.  Context is
never fed back into synthesis, optimizer, planner or execution.
"""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import html
import json
import math
from typing import Any, Mapping


SHADOW_MODE = "SHADOW_ONLY"
SHADOW_PLAN_SOURCE = "execution_plan"
SHADOW_PLAN_PAYLOAD_VERSION = "execution-plan-v2-immutable"


class ContextualShadowSafetyError(RuntimeError):
    """Raised when the experimental command cannot prove its safety contract."""


def _value(value: Any) -> Any:
    return getattr(value, "value", value)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    value = _value(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def payload_hash(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False, default=str,
    )
    return sha256(encoded.encode("utf-8")).hexdigest()


def _decision_payload(decision: Any) -> dict[str, Any]:
    return {
        "ticker": str(getattr(decision, "ticker", "") or "").upper(),
        "action": str(_value(getattr(decision, "action", "UNKNOWN"))),
        "current_weight": _number(getattr(decision, "current_weight", None)),
        "target_weight": _number(getattr(decision, "target_weight", None)),
        "delta_weight": _number(getattr(decision, "delta_weight", None)),
        "score": _number(getattr(decision, "score", None)),
        "conviction": _number(getattr(decision, "conviction", None)),
        "portfolio_intent": str(
            _value(getattr(decision, "portfolio_intent", "UNKNOWN"))
        ),
    }


def _order_payload(order: Any) -> dict[str, Any]:
    return {
        "ticker": str(getattr(order, "ticker", "") or "").upper(),
        "side": str(_value(getattr(order, "side", "UNKNOWN"))),
        "action": str(_value(getattr(order, "action", "UNKNOWN"))),
        "amount_ars": _number(getattr(order, "amount_ars", None)),
        "theoretical_ars": _number(getattr(order, "theoretical_ars", None)),
        "quantity_est": _number(getattr(order, "quantity_est", None)),
        "reference_price": _number(getattr(order, "reference_price", None)),
        "status": str(_value(getattr(order, "status", "UNKNOWN"))),
        "block_code": getattr(order, "block_code", None),
    }


def productive_view(runtime: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze only productive fields before contextual evidence is attached."""
    plan = runtime.get("execution_plan")
    results = list(runtime.get("results") or [])
    signals = [
        {
            "ticker": str(getattr(item, "ticker", "") or "").upper(),
            "decision": str(getattr(item, "decision", "UNKNOWN") or "UNKNOWN"),
            "final_score": _number(getattr(item, "final_score", None)),
            "conviction": _number(
                getattr(item, "conviction", getattr(item, "confidence", None))
            ),
        }
        for item in results
    ]
    if plan is None:
        return {
            "signals": signals,
            "decisions": [],
            "orders": {"sell_orders": [], "buy_orders": [], "blocked_orders": []},
            "cash": {
                "cash_before": _number(runtime.get("cash_ars")),
                "gross_sell_ars": 0.0,
                "net_sell_ars": 0.0,
                "gross_buy_ars": 0.0,
                "fee_sell_ars": 0.0,
                "fee_buy_ars": 0.0,
                "cash_after": _number(runtime.get("cash_ars")),
            },
            "gate": "UNKNOWN",
            "feasible": False,
        }
    return {
        "signals": signals,
        "decisions": [
            _decision_payload(item) for item in (getattr(plan, "decisions", []) or [])
        ],
        "orders": {
            name: [_order_payload(item) for item in (getattr(plan, name, []) or [])]
            for name in ("sell_orders", "buy_orders", "blocked_orders")
        },
        "cash": {
            key: _number(getattr(plan, key, None))
            for key in (
                "cash_before", "gross_sell_ars", "net_sell_ars", "gross_buy_ars",
                "fee_sell_ars", "fee_buy_ars", "cash_after",
            )
        },
        "gate": str(getattr(plan, "gate", "UNKNOWN") or "UNKNOWN"),
        "feasible": bool(getattr(plan, "feasible", False)),
    }


def assert_runtime_is_shadow_only(runtime: Mapping[str, Any]) -> None:
    if runtime.get("no_persist") is not True:
        raise ContextualShadowSafetyError("FAIL_CLOSED:PRODUCTIVE_PIPELINE_PERSISTENCE_ENABLED")
    if str(runtime.get("run_intent") or "") != "exploratory":
        raise ContextualShadowSafetyError("FAIL_CLOSED:RUN_INTENT_IS_NOT_EXPLORATORY")
    if not str(runtime.get("analysis_run_id") or ""):
        raise ContextualShadowSafetyError("FAIL_CLOSED:RUN_ID_MISSING")


def assert_neutral(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, bool]:
    before_copy = deepcopy(dict(before))
    after_copy = deepcopy(dict(after))
    checks = {
        "scores": before_copy.get("signals") == after_copy.get("signals"),
        "signals": [item.get("decision") for item in before_copy.get("signals", [])]
        == [item.get("decision") for item in after_copy.get("signals", [])],
        "decisions": before_copy.get("decisions") == after_copy.get("decisions"),
        "order_intents": before_copy.get("orders") == after_copy.get("orders"),
        "quantities": [
            item.get("quantity_est")
            for values in before_copy.get("orders", {}).values()
            for item in values
        ] == [
            item.get("quantity_est")
            for values in after_copy.get("orders", {}).values()
            for item in values
        ],
        "cash": before_copy.get("cash") == after_copy.get("cash"),
    }
    if not all(checks.values()):
        raise ContextualShadowSafetyError("FAIL_CLOSED:CONTEXT_CHANGED_PRODUCTIVE_VIEW")
    return checks


def assert_shadow_plan_envelope(row: Mapping[str, Any], order_intent_count: int) -> None:
    expected_zero = (
        "gross_sell_ars", "fee_sell_ars", "net_sell_ars",
        "gross_buy_ars", "fee_buy_ars",
    )
    valid = (
        str(row.get("source")) == SHADOW_PLAN_SOURCE
        and str(row.get("gate")) == SHADOW_MODE
        and str(row.get("authority_mode")) == SHADOW_MODE
        and row.get("affects_analysis") is False
        and row.get("affects_execution") is False
        and row.get("feasible") is False
        and str(row.get("payload_version")) == SHADOW_PLAN_PAYLOAD_VERSION
        and all((_number(row.get(name)) or 0.0) == 0.0 for name in expected_zero)
        and _number(row.get("cash_before")) == _number(row.get("cash_after"))
        and int(order_intent_count) == 0
    )
    if not valid:
        raise ContextualShadowSafetyError("FAIL_CLOSED:INVALID_SHADOW_PLAN_ENVELOPE")


def _money(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "UNKNOWN"
    return "$" + f"{number:,.0f}".replace(",", ".")


def _metric(value: Any, *, percent: bool = False) -> str:
    number = _number(value)
    if number is None:
        return "UNKNOWN"
    return f"{number:+.1%}" if percent else f"{number:.2f}"


def _escape(value: Any) -> str:
    return html.escape(str(value if value is not None else "UNKNOWN"))


def _productive_action(asset: Mapping[str, Any]) -> str:
    return str(asset.get("productive_action") or asset.get("signal") or "UNKNOWN")


def _icon(action: str) -> str:
    action = action.upper()
    if action in {"BUY", "ACCUMULATE"}:
        return "🟢"
    if action in {"SELL", "SELL_FULL", "SELL_PARTIAL", "REDUCE"}:
        return "🔴"
    if action in {"HOLD", "WATCH"}:
        return "🟡"
    return "⚪"


def render_contextual_telegram(payload: Mapping[str, Any]) -> str:
    assets = list(payload.get("assets") or [])
    lines = [
        "🧪 <b>ANÁLISIS CONTEXTUAL — SHADOW</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"Portfolio: <b>{_money(payload.get('portfolio_value_ars'))}</b>",
        f"Cash: <b>{_money(payload.get('cash_ars'))}</b>",
        f"Cutoff: <code>{_escape(payload.get('cutoff'))}</code>",
        f"Run: <code>{_escape(payload.get('run_id'))}</code>",
        f"Evidence: <b>{int(payload.get('evidence_count') or 0)} observaciones</b>",
        f"Context confidence: <b>{_escape(payload.get('context_confidence') or 'UNKNOWN')}</b>",
        "",
        "━━━ <b>PLAN PRODUCTIVO</b> ━━━",
    ]
    for asset in assets:
        action = _productive_action(asset)
        context = dict(asset.get("context") or {})
        components = dict(context.get("components") or {})
        relative = dict(components.get("relative_strength") or {})
        general = dict(relative.get("general") or {})
        windows = dict(general.get("windows") or {})
        confidence = dict(components.get("context_confidence") or {})
        invalidators = list(context.get("invalidators") or [])
        missingness = list(context.get("missingness") or []) + list(
            asset.get("missingness") or []
        )
        lines.extend([
            "",
            f"{_icon(action)} <b>{_escape(asset.get('ticker'))}</b>",
            f"Productivo: <b>{_escape(action)}</b>",
            f"Score: <code>{_metric(asset.get('score'))}</code>",
            f"Conviction: <code>{_metric(asset.get('conviction'))}</code>",
            "",
            "<b>Contexto [SHADOW]</b>",
            f"Daily: <b>{_escape(components.get('trend_daily'))}</b>",
            f"Weekly: <b>{_escape(components.get('trend_weekly'))}</b>",
            f"RVOL: <code>{_metric(components.get('rvol'))}</code>",
            f"Volume: <b>{_escape(components.get('volume_state'))}</b>",
            f"Breakout: <b>{_escape(components.get('breakout_state'))}</b>",
            f"RS20: <code>{_metric((windows.get('20') or {}).get('relative_price_change'), percent=True)}</code>",
            f"RS60: <code>{_metric((windows.get('60') or {}).get('relative_price_change'), percent=True)}</code>",
            f"RS120: <code>{_metric((windows.get('120') or {}).get('relative_price_change'), percent=True)}</code>",
            f"Confidence: <b>{_escape(confidence.get('status'))}</b>",
            "",
            "<b>Invalidadores:</b>",
        ])
        if invalidators:
            for item in invalidators:
                status = str(item.get("status") or "UNKNOWN")
                reason = str(item.get("reason") or "SIN_MOTIVO")
                lines.append(f"• <b>{_escape(status)}</b> · {_escape(reason)}")
        else:
            lines.append("• <b>UNKNOWN</b> · NO_CONTEXTUAL_SNAPSHOT")
        if missingness:
            lines.append("<b>UNKNOWN / faltantes preservados:</b>")
            lines.extend(f"• {_escape(reason)}" for reason in sorted(set(missingness)))
        lines.extend(["", "Impacto contextual: <b>NONE · SHADOW ONLY</b>"])

    lines.extend([
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Capture: <code>{_escape(payload.get('capture_id'))}</code>",
        f"Plan evidence: <code>{_escape(payload.get('plan_id'))}</code>",
        "<code>SHADOW_ONLY · affects_analysis=false · affects_execution=false</code>",
    ])
    return "\n".join(lines)


__all__ = [
    "ContextualShadowSafetyError",
    "SHADOW_MODE",
    "SHADOW_PLAN_PAYLOAD_VERSION",
    "SHADOW_PLAN_SOURCE",
    "assert_neutral",
    "assert_runtime_is_shadow_only",
    "assert_shadow_plan_envelope",
    "payload_hash",
    "productive_view",
    "render_contextual_telegram",
]
