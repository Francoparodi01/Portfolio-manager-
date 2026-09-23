#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agentic import (
    AgentOrchestrator,
    AgentRunStore,
    OllamaAgentModel,
    ToolContext,
    build_default_registry,
)
from src.agentic.jev import build_routed_agent_model, router_mode_from_env
from src.agentic.orchestrator import default_max_steps
from src.core.config import get_config


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Quantia's bounded read-only agentic loop."
    )
    parser.add_argument("--goal", required=True, help="Analytical objective for the agent.")
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--model", default=os.getenv("QUANTIA_AGENT_MODEL", "qwen2.5:3b"))
    parser.add_argument("--max-steps", type=int, default=default_max_steps())
    parser.add_argument("--json", action="store_true", help="Print the complete trace as JSON.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow a manual run even when QUANTIA_AGENT_ENABLED is false.",
    )
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    enabled = _bool_env("QUANTIA_AGENT_ENABLED", False)
    if not enabled and not args.force:
        print(
            "Agentic loop disabled. Set QUANTIA_AGENT_ENABLED=true or use --force for a manual run.",
            file=sys.stderr,
        )
        return 2

    cfg = get_config()
    owner_chat_id = args.owner_chat_id
    if owner_chat_id is None and cfg.multiuser_enabled:
        configured = str(cfg.scraper.telegram_chat_id or "").strip()
        if configured.isdigit():
            owner_chat_id = int(configured)

    timeout_seconds = float(os.getenv("QUANTIA_AGENT_TOOL_TIMEOUT_SECONDS", "600"))
    output_limit = int(os.getenv("QUANTIA_AGENT_TOOL_OUTPUT_CHARS", "18000"))
    require_audit = _bool_env("QUANTIA_AGENT_REQUIRE_AUDIT", True)
    router_mode = router_mode_from_env()

    context = ToolContext(
        database_url=cfg.database.url,
        owner_chat_id=owner_chat_id,
        repo_root=str(ROOT),
        output_limit_chars=output_limit,
        tool_timeout_seconds=timeout_seconds,
    )
    registry = build_default_registry(context)
    base_model = OllamaAgentModel(model=args.model)
    model = build_routed_agent_model(base_model)
    store = AgentRunStore(cfg.database.url) if cfg.database.url else None
    orchestrator = AgentOrchestrator(
        model=model,
        registry=registry,
        store=store,
        max_steps=args.max_steps,
        max_identical_calls=1,
        require_audit=require_audit,
    )

    result = await orchestrator.run(
        goal=args.goal,
        owner_chat_id=owner_chat_id,
        metadata={
            "trigger": "cli",
            "agent_version": "quantia-agent-v1-jev-router",
            "read_only": True,
            "router_mode": router_mode,
            "jev_model": os.getenv("QUANTIA_JEV_MODEL", "jev-latest") if router_mode != "off" else None,
        },
    )

    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(result.answer)
        print(
            f"\n[agent run={result.run_id} status={result.status} "
            f"steps={len(result.steps)} stop={result.stop_reason} "
            f"router={router_mode} audit={result.audit_persisted}]"
        )

    return 0 if result.status in {"COMPLETE", "LIMIT_REACHED"} else 1


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
