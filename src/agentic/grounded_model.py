from __future__ import annotations

import asyncio
import json
import unicodedata
from typing import Any

from .answer import evidence_decision
from .contracts import AgentDecision, AgentModelError, ToolSpec, validate_answer
from .diagnostics import question_plan
from .model import OllamaAgentModel


_DECISION_LAB_TOOLS = {
    "get_decision_value_added",
    "compare_plan_vs_hold",
    "get_decision_counterfactuals",
    "get_replay_evidence_quality",
    "get_similar_historical_episodes",
}
_REAL_PNL_TOOLS = {"get_decision_ledger", "get_analytics_v2"}


def _plain(text: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFKD", str(text or "").lower())
        if not unicodedata.combining(char)
    )


def _is_capability_question(goal: str) -> bool:
    text = _plain(goal)
    mentions_tools = any(term in text for term in ("herramient", "tools", "capacidades", "capabilities"))
    asks_availability = any(
        term in text
        for term in ("dispon", "tenes", "tienes", "podes", "puedes", "que hay", "cuales")
    )
    return mentions_tools and asks_availability


def _successful_tools(history: list[dict[str, Any]]) -> set[str]:
    found: set[str] = set()
    for item in history:
        observation = item.get("observation") or {}
        if not observation.get("ok"):
            continue
        decision = item.get("decision") or {}
        name = str(
            observation.get("tool")
            or observation.get("tool_name")
            or decision.get("tool")
            or decision.get("tool_name")
            or ""
        )
        if name:
            found.add(name)
    return found


def _attempted_tools(history: list[dict[str, Any]]) -> set[str]:
    attempted: set[str] = set()
    for item in history:
        if item.get("observation") is None:
            continue
        decision = item.get("decision") or {}
        observation = item.get("observation") or {}
        name = str(
            decision.get("tool")
            or decision.get("tool_name")
            or observation.get("tool")
            or observation.get("tool_name")
            or ""
        )
        if name:
            attempted.add(name)
    return attempted


def _provenance_suffix(history: list[dict[str, Any]]) -> str:
    tools: list[str] = []
    sql_hashes: list[str] = []
    for item in history:
        observation = item.get("observation") or {}
        if not observation.get("ok"):
            continue
        decision = item.get("decision") or {}
        name = str(
            observation.get("tool")
            or observation.get("tool_name")
            or decision.get("tool")
            or decision.get("tool_name")
            or ""
        )
        if name and name not in tools:
            tools.append(name)
        if name == "query_quantia_sql":
            try:
                payload = json.loads(str(observation.get("content") or ""))
            except (TypeError, ValueError):
                payload = {}
            digest = str(payload.get("query_sha256") or "") if isinstance(payload, dict) else ""
            if digest:
                sql_hashes.append(digest[:12])
    suffix = "Fuentes auditadas: " + ", ".join(tools) + "."
    if sql_hashes:
        suffix += " SQL query hash: " + ", ".join(sql_hashes) + "."
    return suffix


def _verified_synthesis(goal: str, answer: str, history: list[dict[str, Any]]) -> AgentDecision:
    tools = _successful_tools(history)
    if not tools:
        raise AgentModelError("grounded synthesis requires at least one successful evidence source")

    text = validate_answer(answer)
    lower = text.lower()

    if "dva" in lower and not (tools & _DECISION_LAB_TOOLS):
        return evidence_decision(goal, history)

    real_pnl_claim = any(
        phrase in lower
        for phrase in (
            "pnl real",
            "pnl realizado",
            "ganancia real",
            "ganó realmente",
            "gano realmente",
        )
    )
    if real_pnl_claim and not (tools & _REAL_PNL_TOOLS):
        return evidence_decision(goal, history)

    causal_claim = any(
        phrase in lower
        for phrase in (
            "demuestra que",
            "garantiza",
            "causó",
            "causo",
            "es la causa",
        )
    )
    if causal_claim and "query_quantia_sql" in tools and not (tools & _DECISION_LAB_TOOLS):
        return evidence_decision(goal, history)

    tagged = text + "\n\n" + _provenance_suffix(history)
    return AgentDecision(
        kind="final",
        answer=tagged,
        rationale="Síntesis LLM limitada a evidencia observada, con guards semánticos y provenance runtime.",
        confidence=None,
        answer_origin="grounded_llm_v1",
        objective_status="EXPLAINED",
    )


class GroundedQuantiaAgentModel(OllamaAgentModel):
    """Dynamic controller grounded in Quantia's checked-in operating contract.

    Open-ended questions are planned by the LLM. Decision Lab remains a
    deterministic route because its counterfactual semantics must not drift.
    """

    def __init__(self, *, project_context: str = "", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_context = str(project_context or "")[:24000]

    def _system_prompt(
        self,
        tools: list[ToolSpec],
        step_no: int,
        max_steps: int,
        force_final: bool,
    ) -> str:
        base = super()._system_prompt(tools, step_no, max_steps, force_final)
        grounded = self.project_context or "No project context files were loaded. Fail closed on undefined financial semantics."
        return (
            base
            + "\n\nPROJECT OPERATING CONTEXT (authoritative instructions and semantic definitions):\n"
            + grounded
            + "\n\nDYNAMIC PLANNING RULES:\n"
              "- For open-ended analytical questions, decide the evidence plan yourself; do not require a hardcoded intent route.\n"
              "- Use inspect_quantia_schema when exact live columns are uncertain.\n"
              "- Use query_quantia_sql for exploratory SELECT analysis that is not a canonical metric.\n"
              "- Use search_quantia_docs for architecture, metric definitions and implementation rationale grounded in repository documentation.\n"
              "- Prefer canonical deterministic tools for PnL, PLAN-vs-HOLD/DVA, current portfolio state, or canonical episode methodology.\n"
              "- A failed SQL query is evidence of a bad query/schema assumption, not evidence that the financial value is zero. Repair by inspecting schema or choosing another valid source.\n"
              "- Never request a write capability. Never output SQL as if it had executed unless a successful tool observation contains its result.\n"
              "- Final answers may interpret observed evidence, but must not invent numbers, redefine canonical metrics, claim causality from exploratory SQL, or convert missing values to zero.\n"
              "- Keep the final answer in Spanish unless the user explicitly asks otherwise.\n"
        )

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
        if _is_capability_question(goal):
            lines = [
                f"- {tool.name}: {' '.join(tool.description.split())[:240]}"
                for tool in tools
            ]
            answer = "Herramientas disponibles en este run:\n" + (
                "\n".join(lines) if lines else "- No hay herramientas registradas."
            )
            return AgentDecision(
                kind="final",
                answer=answer,
                rationale="Respuesta derivada del registro runtime de herramientas; no requiere evidencia de mercado.",
                confidence=1.0,
                answer_origin="runtime_capabilities_v1",
                objective_status="EXPLAINED",
            )

        plan = question_plan(goal, self.conversation_context)
        if plan.intent.startswith("decision_lab"):
            return await super().decide(
                goal=goal,
                tools=tools,
                history=history,
                step_no=step_no,
                max_steps=max_steps,
                force_final=force_final,
            )

        # High-confidence canonical routes are bootstrapped deterministically before
        # handing control back to the dynamic planner. This prevents the controller
        # from trying to synthesize a portfolio/explanation answer with zero evidence,
        # while still allowing it to choose additional tools after the required facts
        # have been observed.
        if not force_final and plan.required_tools:
            available = {tool.name for tool in tools}
            attempted = _attempted_tools(history)
            for name in plan.required_tools:
                if name in available and name not in attempted:
                    return AgentDecision(
                        kind="tool",
                        tool_name=name,
                        arguments={},
                        rationale=f"Fuente canónica requerida por la política {plan.intent} antes de la síntesis dinámica.",
                    )

        messages = [
            {
                "role": "system",
                "content": self._system_prompt(tools, step_no, max_steps, force_final),
            },
            {
                "role": "user",
                "content": (
                    "Goal:\n"
                    + goal.strip()
                    + "\nRecent user questions, context only, not verified market facts:\n"
                    + json.dumps(self.conversation_context, ensure_ascii=False)
                    + "\n\nUse only evidence obtained from the allowed tools and observations."
                ),
            },
            *self._history_messages(history),
            {
                "role": "user",
                "content": (
                    "Retomá el objetivo original:\n"
                    + goal.strip()
                    + "\nElegí una sola herramienta si falta evidencia. Si ya alcanza, devolvé kind='final' con una síntesis breve basada exclusivamente en las observaciones."
                ),
            },
        ]
        payload = {
            "model": self.name,
            "messages": messages,
            "stream": False,
            "format": "json",
            # The controller only needs a compact JSON action. Disabling model
            # thinking avoids spending the response budget on a hidden reasoning
            # channel and is supported by current Ollama thinking-capable models.
            "think": False,
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.num_predict,
                "num_ctx": self.context_tokens,
            },
        }

        last_error: Exception | None = None
        for attempt in range(2):
            try:
                content = await asyncio.wait_for(self._call(payload), timeout=self.timeout_seconds)
                value = self._extract_json(content)
                kind = str(value.get("kind") or value.get("type") or "").strip().lower()
                if kind == "final":
                    if not _successful_tools(history):
                        raise AgentModelError("model attempted final before gathering evidence")
                    return _verified_synthesis(goal, value.get("answer"), history)
                if force_final:
                    raise AgentModelError("model requested a tool during forced finalization")
                return AgentDecision.from_mapping(value)
            except Exception as exc:
                last_error = exc
                if attempt == 0:
                    payload["messages"] = [
                        *messages,
                        {
                            "role": "user",
                            "content": (
                                "The previous controller output was invalid or insufficiently grounded. "
                                "Return exactly one valid JSON object. If there is no successful observation yet, "
                                "you MUST choose exactly one allowed tool instead of finalizing. "
                                "When forced to finalize, use only successful observations."
                            ),
                        },
                    ]
        if force_final and _successful_tools(history):
            return evidence_decision(goal, history)
        raise AgentModelError(f"grounded agent model failed: {last_error}")


__all__ = ["GroundedQuantiaAgentModel"]