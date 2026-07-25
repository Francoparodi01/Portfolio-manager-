"""
Benchmark representative Quantia output paths.

The benchmark is read-only. It measures renderer, HTTP, and CLI output latency,
response size, line count, and process status. CLI targets can be executed on
the host or through the live scheduler container.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import subprocess
import sys
import time
import types
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.core.output_perf import summarize_measurements


CLI_TARGETS: dict[str, list[str]] = {
    "timeline_2d_json": ["scripts/run_decision_timeline.py", "--days", "2", "--limit", "5", "--json"],
    "performance_30d": ["scripts/run_performance.py", "--days", "30", "--no-telegram"],
    "ledger_30d": ["scripts/run_decision_ledger.py", "--days", "30", "--no-telegram"],
    "override_30d": ["scripts/run_override_audit.py", "--days", "30", "--no-telegram"],
}

HTTP_TARGETS: dict[str, tuple[str, bool]] = {
    "monitor_index": ("/", False),
    "monitor_health": ("/api/health", True),
    "monitor_override_audit_7d": ("/api/override-audit?days=7", True),
}


def _load_env_token() -> str:
    token = os.getenv("MONITOR_API_TOKEN", "")
    env_file = ROOT / ".env"
    if token or not env_file.exists():
        return token
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("MONITOR_API_TOKEN="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _install_telegram_stubs() -> None:
    if "telegram" in sys.modules:
        return
    telegram_module = types.ModuleType("telegram")
    telegram_module.InlineKeyboardButton = object
    telegram_module.InlineKeyboardMarkup = object
    telegram_module.Update = object

    constants_module = types.ModuleType("telegram.constants")
    constants_module.ParseMode = SimpleNamespace(HTML="HTML")

    error_module = types.ModuleType("telegram.error")
    error_module.BadRequest = Exception

    ext_module = types.ModuleType("telegram.ext")
    ext_module.Application = object
    ext_module.CallbackQueryHandler = object
    ext_module.CommandHandler = object
    ext_module.ContextTypes = SimpleNamespace(DEFAULT_TYPE=object)
    ext_module.MessageHandler = object
    ext_module.filters = SimpleNamespace(TEXT=object(), COMMAND=object())

    sys.modules.setdefault("telegram", telegram_module)
    sys.modules.setdefault("telegram.constants", constants_module)
    sys.modules.setdefault("telegram.error", error_module)
    sys.modules.setdefault("telegram.ext", ext_module)


def _measure_callable(name: str, fn: Callable[[], str], runs: int) -> dict[str, object]:
    durations: list[float] = []
    sizes: list[int] = []
    line_counts: list[int] = []
    for _ in range(runs):
        started = time.perf_counter()
        text = fn()
        durations.append((time.perf_counter() - started) * 1000.0)
        sizes.append(len(text.encode("utf-8")))
        line_counts.append(len(text.splitlines()))
    return summarize_measurements(
        name=name,
        kind="renderer",
        durations_ms=durations,
        sizes_bytes=sizes,
        line_counts=line_counts,
        statuses=["ok"],
        query_count=0,
    )


def measure_renderers(runs: int) -> list[dict[str, object]]:
    _install_telegram_stubs()
    bot = importlib.import_module("scripts.telegram_bot")
    return [
        _measure_callable("telegram_menu_text", bot.menu_text, runs),
        _measure_callable("telegram_help_text", bot.help_text, runs),
    ]


def measure_http(name: str, base_url: str, token: str, runs: int) -> dict[str, object]:
    path, needs_token = HTTP_TARGETS[name]
    durations: list[float] = []
    sizes: list[int] = []
    statuses: list[int] = []
    headers = {"X-API-Token": token} if needs_token and token else {}
    for _ in range(runs):
        request = urllib.request.Request(f"{base_url.rstrip('/')}{path}", headers=headers)
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
                statuses.append(int(response.status))
        except urllib.error.HTTPError as exc:
            body = exc.read()
            statuses.append(int(exc.code))
        durations.append((time.perf_counter() - started) * 1000.0)
        sizes.append(len(body))
    query_count = 1 if name == "monitor_health" else None
    return summarize_measurements(
        name=name,
        kind="http",
        durations_ms=durations,
        sizes_bytes=sizes,
        statuses=statuses,
        query_count=query_count,
    )


def measure_cli(name: str, runs: int, *, docker: bool) -> dict[str, object]:
    durations: list[float] = []
    sizes: list[int] = []
    line_counts: list[int] = []
    statuses: list[int] = []
    target = CLI_TARGETS[name]
    use_docker = docker and shutil.which("docker") is not None
    command = (
        ["docker", "compose", "exec", "-T", "scheduler", "python", *target]
        if use_docker
        else [sys.executable, *target]
    )
    for _ in range(runs):
        started = time.perf_counter()
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
            check=False,
        )
        durations.append((time.perf_counter() - started) * 1000.0)
        output = (result.stdout or "") + (result.stderr or "")
        sizes.append(len(output.encode("utf-8")))
        line_counts.append(len(output.splitlines()))
        statuses.append(int(result.returncode))
    return summarize_measurements(
        name=name,
        kind="docker_cli" if use_docker else "cli",
        durations_ms=durations,
        sizes_bytes=sizes,
        line_counts=line_counts,
        statuses=statuses,
        query_count=None,
    )


def render_table(rows: list[dict[str, object]]) -> str:
    header = "| target | kind | runs | median ms | p95 ms | max ms | bytes | lines | status |"
    sep = "|---|---:|---:|---:|---:|---:|---:|---:|---|"
    lines = [header, sep]
    for row in rows:
        status = ",".join(str(value) for value in row.get("status") or [])
        display_row = {**row, "status_text": status}
        lines.append(
            "| {name} | {kind} | {runs} | {median_ms} | {p95_ms} | {max_ms} | "
            "{bytes_avg} | {lines_avg} | {status_text} |".format(
                **display_row,
            )
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Quantia output paths")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--monitor-url", default=os.getenv("MONITOR_URL", "http://localhost:8010"))
    parser.add_argument("--include-http", action="store_true")
    parser.add_argument("--include-cli", action="store_true")
    parser.add_argument("--docker-cli", action="store_true")
    parser.add_argument("--target", action="append", choices=sorted(CLI_TARGETS))
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows: list[dict[str, object]] = []
    rows.extend(measure_renderers(max(1, args.runs)))

    if args.include_http:
        token = _load_env_token()
        for name in HTTP_TARGETS:
            rows.append(measure_http(name, args.monitor_url, token, max(1, args.runs)))

    if args.include_cli:
        targets = args.target or ["timeline_2d_json", "performance_30d", "override_30d"]
        for name in targets:
            rows.append(measure_cli(name, max(1, args.runs), docker=bool(args.docker_cli)))

    print(json.dumps(rows, indent=2, ensure_ascii=False) if args.json else render_table(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
