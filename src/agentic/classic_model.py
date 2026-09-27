from __future__ import annotations

import json
import re
import unicodedata
from typing import Any

from .contracts import AgentDecision, ToolSpec
from .model import OllamaAgentModel


def _plain(text: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", str(text or "").lower())
        if not unicodedata.combining(char)
    )


def _is_bot_directional_history_goal(goal: str) -> bool:
    text = _plain(goal)
    bot_scope = "bot" in text and any(
        term in text
        for term in ("decision", "resultado", "rind", "performance", "win", "acierto", "ev", "gan")
    )
    direction_scope = ("buy" in text and "sell" in text) or (
        any(term in text for term in ("separa", "separado", "direccion"))
        and any(term in text for term in ("buy", "sell", "compra", "venta"))
    )
    return bot_scope and direction_scope


def _directional_arguments(goal: str) -> dict[str, Any]:
    text = _plain(goal)
    horizon_match = re.search(r"\b(5|10|20|40)\s*(?:d|dias|sesiones)\b", text)
    lookback_match = re.search(r"\bultimos?\s+(\d{1,3})\s+dias\b", text)
    return {
        "days": max(1, min(730, int(lookback_match.group(1)) if lookback_match else 180)),
        "horizon": int(horizon_match.group(1)) if horizon_match else 20,
        "cost_bps": 150,
    }


def _percent(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value) * 100:+.2f}%"
    except (TypeError, ValueError):
        return "N/D"


def _ratio(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "N/D"


def _directional_final(history: list[dict[str, Any]]) -> AgentDecision:
    payload: dict[str, Any] | None = None
    for item in reversed(history):
        observation = item.get("observation") or {}
        decision = item.get("decision") or {}
        tool = observation.get("tool") or observation.get("tool_name") or decision.get("tool") or decision.get("tool_name")
        if tool != "get_bot_directional_outcomes" or not observation.get("ok"):
            continue
        try:
            parsed = json.loads(str(observation.get("content") or ""))
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, dict):
            payload = parsed
            break

    if not payload:
        return AgentDecision(
            kind="final",
            answer="No pude obtener evidencia histórica direccional suficiente del bot. No completo BUY/SELL con supuestos.",
            rationale="La tool histórica no devolvió evidencia utilizable.",
            answer_origin="classic_deterministic",
            objective_status="INSUFFICIENT",
        )

    sides = payload.get("sides") if isinstance(payload.get("sides"), dict) else {}
    lines = [
        f"Histórico del bot · últimos {int(payload.get('lookback_days') or 0)} días · {int(payload.get('horizon_days') or 0)}D",
        f"Costo research aplicado: {float(payload.get('cost_bps') or 0) / 100:.2f}% por episodio.",
    ]
    observed_sides = 0
    for side in ("BUY", "SELL"):
        row = sides.get(side) if isinstance(sides.get(side), dict) else {}
        n = int(row.get("n_episodes") or 0)
        if n:
            observed_sides += 1
        win = row.get("win_rate_net")
        win_text = f"{float(win) * 100:.1f}%" if win is not None else "N/D"
        lines.append(
            f"{side}: n={n} episodios · win neto {win_text} · EV bruto {_percent(row.get('mean_gross_return'))} · "
            f"EV neto {_percent(row.get('mean_net_return'))} · mediana neta {_percent(row.get('median_net_return'))} · "
            f"PF {_ratio(row.get('profit_factor_net'))} · calidad {row.get('quality') or 'N/D'}."
        )

    lines.extend(
        [
            "Las recomendaciones consecutivas del mismo ticker y dirección cuentan como un solo episodio; HOLD, ausencia o cambio de dirección lo cortan.",
            "Esto es retorno direccional histórico del plan del bot, no PnL realizado de la cuenta y no es DVA contra HOLD.",
        ]
    )
    status = "EXPLAINED" if observed_sides == 2 else "PARTIAL" if observed_sides else "INSUFFICIENT"
    return AgentDecision(
        kind="final",
        answer="\n".join(lines),
        rationale="Evidencia histórica owner-scoped calculada sin síntesis LLM.",
        answer_origin="classic_deterministic",
        objective_status=status,
    )


class ClassicAgentModel(OllamaAgentModel):
    """Generic Quantia agent plus deterministic routes for bounded evidence queries."""

    async def decide(
        self,
        *,
        goal: str,
        tools: list[ToolSpec],
        history: list[dict[str, Any]],
        step_no: int,
        max_steps: int,
        force_final: bool = False,
    ) -> AgentDecision:
        if _is_bot_directional_history_goal(goal):
            available = {tool.name for tool in tools}
            if "get_bot_directional_outcomes" in available:
                attempted = any(
                    ((item.get("decision") or {}).get("tool") or (item.get("decision") or {}).get("tool_name"))
                    == "get_bot_directional_outcomes"
                    and item.get("observation") is not None
                    for item in history
                )
                if not attempted and not force_final:
                    return AgentDecision(
                        kind="tool",
                        tool_name="get_bot_directional_outcomes",
                        arguments=_directional_arguments(goal),
                        rationale="Consulta histórica BUY/SELL resuelta con evidencia read-only owner-scoped.",
                    )
                return _directional_final(history)
        return await super().decide(
            goal=goal,
            tools=tools,
            history=history,
            step_no=step_no,
            max_steps=max_steps,
            force_final=force_final,
        )


__all__ = ["ClassicAgentModel"]
