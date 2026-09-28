from __future__ import annotations

from typing import Any

from .contracts import ToolSpec
from .model import OllamaAgentModel


class GroundedQuantiaAgentModel(OllamaAgentModel):
    """Dynamic controller grounded in Quantia's checked-in operating contract.

    The LLM chooses evidence/tools for open-ended questions. Deterministic canonical
    routes in the shared diagnostics policy remain available for metrics whose
    definition must not drift.
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
              "- Prefer canonical deterministic tools for PnL, PLAN-vs-HOLD/DVA, current portfolio state, or canonical episode methodology.\n"
              "- A failed SQL query is evidence of a bad query/schema assumption, not evidence that the financial value is zero. Repair by inspecting schema or choosing another valid source.\n"
              "- Never request a write capability. Never output SQL as if it had executed unless a successful tool observation contains its result.\n"
              "- Keep the final answer in Spanish unless the user explicitly asks otherwise.\n"
        )


__all__ = ["GroundedQuantiaAgentModel"]
