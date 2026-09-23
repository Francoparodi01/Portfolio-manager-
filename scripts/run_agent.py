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
from src.agentic.orchestrator import default_max_steps
from src.agentic.tools import verify_single_owner
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
    parser.add_argument("--output-json", type=Path, help="Write the full trace to a new file.")
    parser.add_argument("--timeout-seconds", type=int, default=600, help="Total loop budget, 1..1800 seconds.")
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
    configured = str(getattr(cfg.scraper, "telegram_chat_id", "") or "").strip()
    if owner_chat_id is None:
        if cfg.multiuser_enabled:
            raise ValueError("--owner-chat-id is required in multiuser mode")
        if configured.lstrip("-").isdigit():
            owner_chat_id = int(configured)
    if not owner_chat_id:
        raise ValueError("an account owner is required")
    legacy_single_owner = (not cfg.multiuser_enabled and configured == str(owner_chat_id)
                           and await verify_single_owner(cfg.database.url, owner_chat_id))

    timeout_seconds = float(os.getenv("QUANTIA_AGENT_TOOL_TIMEOUT_SECONDS", "600"))
    output_limit = int(os.getenv("QUANTIA_AGENT_TOOL_OUTPUT_CHARS", "18000"))
    require_audit = _bool_env("QUANTIA_AGENT_REQUIRE_AUDIT", True)

    context = ToolContext(
        database_url=cfg.database.url,
        owner_chat_id=owner_chat_id,
        repo_root=str(ROOT),
        output_limit_chars=output_limit,
        tool_timeout_seconds=timeout_seconds,
        legacy_single_owner=legacy_single_owner,
    )
    registry = build_default_registry(context)
    model = OllamaAgentModel(model=args.model)
    store = AgentRunStore(cfg.database.url) if cfg.database.url else None
    orchestrator = AgentOrchestrator(
        model=model,
        registry=registry,
        store=store,
        max_steps=args.max_steps,
        max_identical_calls=1,
        require_audit=require_audit,
    )

    timeout_seconds = getattr(args, "timeout_seconds", 600)
    if not 1 <= timeout_seconds <= 1800:
        raise ValueError("total timeout must be between 1 and 1800 seconds")
    try:
        result = await asyncio.wait_for(
            orchestrator.run(
                goal=args.goal,
                owner_chat_id=owner_chat_id,
                metadata={
                    "trigger": "cli",
                    "agent_version": "quantia-agent-v1",
                    "read_only": True,
                    "legacy_single_owner": legacy_single_owner,
                },
            ),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        print("El agente agotó el tiempo disponible; las herramientas fueron canceladas.", file=sys.stderr)
        return 1

    output_json = getattr(args, "output_json", None)
    if output_json:
        with Path(output_json).open("x", encoding="utf-8") as stream:
            json.dump(result.to_dict(), stream, ensure_ascii=False, indent=2)

    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(result.answer)
        print(
            f"\n[agent run={result.run_id} status={result.status} "
            f"steps={len(result.steps)} stop={result.stop_reason} "
            f"audit={result.audit_persisted}]"
        )

    return 0 if result.status in {"COMPLETE", "LIMIT_REACHED"} else 1


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
