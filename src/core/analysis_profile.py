"""Shared production contract for complete analysis and its measurements."""
from __future__ import annotations

ANALYSIS_FULL_PROFILE = "analisis_full_operational_v1"
ANALYSIS_FULL_HOURS_ART = (11, 13, 16)


def full_analysis_args(owner_chat_id: int, *, persist: bool = True,
                       notify: bool = False) -> list[str]:
    """Keep full radar, optimizer and sentiment; only the engine permits writes."""
    owner = int(owner_chat_id)
    if owner <= 0:
        raise ValueError("Complete analysis requires a positive owner_chat_id")
    args = ["scripts/run_analysis.py", "--no-llm"]
    if not notify:
        args.append("--no-telegram")
    args.extend(["--owner-chat-id", str(owner)])
    if persist:
        args.extend(["--run-intent", "formal_plan"])
    else:
        args.extend(["--no-persist", "--run-intent", "exploratory"])
    return args
