"""Run the offline Qwen narrative lab against a JSON evidence packet.

This script does not connect to the database and does not publish anything. It
either prints the prompt or calls a local Ollama endpoint and prints the
validated JSON result.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.llm_narratives import (
    DEFAULT_MODEL,
    DEFAULT_OLLAMA_URL,
    DEFAULT_TIMEOUT_SECONDS,
    build_decision_explanation_prompt,
    build_market_report_prompt,
    explain_decision_with_ollama,
    generate_market_narrative_with_ollama,
)


def _load_packet(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_task(packet: dict, requested: str) -> str:
    if requested != "auto":
        return requested
    packet_type = str(packet.get("packet_type") or "").strip()
    if packet_type in {"market_report", "decision_evidence"}:
        return packet_type
    raise SystemExit("Cannot infer task: packet_type must be market_report or decision_evidence")


async def _run(args: argparse.Namespace) -> dict:
    packet = _load_packet(Path(args.packet))
    task = _resolve_task(packet, args.task)
    if args.print_prompt:
        prompt = (
            build_market_report_prompt(packet)
            if task == "market_report"
            else build_decision_explanation_prompt(packet)
        )
        return {"task": task, "prompt": prompt}

    if task == "market_report":
        result = await generate_market_narrative_with_ollama(
            packet,
            model=args.model,
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        result = await explain_decision_with_ollama(
            packet,
            model=args.model,
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout_seconds,
        )
    return result.to_dict()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Offline Qwen narrative lab")
    parser.add_argument("packet", help="Path to a MarketReportPacket or DecisionEvidencePacket JSON")
    parser.add_argument(
        "--task",
        choices=["auto", "market_report", "decision_evidence"],
        default="auto",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--print-prompt", action="store_true")
    args = parser.parse_args()

    output = asyncio.run(_run(args))
    print(json.dumps(output, ensure_ascii=False, indent=2))
