"""Shared production contract for complete analysis and its measurements."""
from __future__ import annotations

ANALYSIS_FULL_PROFILE = "analisis_full_operational_v2"
ANALYSIS_FULL_HOURS_ART = (11, 13, 16)


def full_analysis_args(owner_chat_id: int, *, persist: bool = True,
                       notify: bool = False) -> list[str]:
    """Optimizer y sentiment completos, sin radar (el radar vive solo en /radar).

    v2 (27/11/2026): el radar dejó de correr en /analisis_full y no aporta
    compras al plan. Solo el motor habilita escrituras.
    """
    owner = int(owner_chat_id)
    if owner <= 0:
        raise ValueError("Complete analysis requires a positive owner_chat_id")
    args = ["scripts/run_analysis.py", "--no-llm", "--no-radar"]
    if not notify:
        args.append("--no-telegram")
    args.extend(["--owner-chat-id", str(owner)])
    if persist:
        args.extend(["--run-intent", "formal_plan"])
    else:
        args.extend(["--no-persist", "--run-intent", "exploratory"])
    return args
