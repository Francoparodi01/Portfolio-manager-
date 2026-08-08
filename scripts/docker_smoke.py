"""Non-destructive Docker Compose smoke checks for Cocos Copilot."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class SmokeResult:
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(f"No se pudo ejecutar {command[0]}: {exc}") from exc


def _request_json(url: str, token: str | None, timeout: float) -> dict:
    headers = {"Accept": "application/json"}
    if token:
        headers["X-API-Token"] = token
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"{url}: {exc}") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{url}: respuesta no JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{url}: payload inesperado")
    return payload


def _running_services_from_json(raw: str) -> set[str]:
    services: set[str] = set()
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if not isinstance(row, dict):
                continue
            state = str(row.get("State", "")).lower()
            service = str(row.get("Service", "")).strip()
            if state == "running" and service:
                services.add(service)
    return services


def run_smoke(
    *,
    required_services: Sequence[str],
    api_base: str,
    api_token: str | None,
    with_local_db: bool,
    with_frontend: bool,
    frontend_url: str,
    docker_bin: str = "docker",
    timeout: float = 8.0,
) -> SmokeResult:
    result = SmokeResult()

    config = _run([docker_bin, "compose", "config", "--quiet"])
    if config.returncode != 0:
        result.failures.append(
            f"docker compose config: {config.stderr.strip() or config.returncode}"
        )
        return result
    result.checks.append("Compose config valido")

    ps = _run([docker_bin, "compose", "ps", "--services", "--status", "running"])
    if ps.returncode != 0:
        fallback = _run([docker_bin, "compose", "ps", "--format", "json"])
        if fallback.returncode != 0:
            result.failures.append(
                "docker compose ps: "
                f"{fallback.stderr.strip() or ps.stderr.strip() or fallback.returncode}"
            )
            return result
        try:
            running = _running_services_from_json(fallback.stdout)
        except json.JSONDecodeError:
            result.failures.append("docker compose ps --format json devolvio JSON invalido")
            return result
        result.warnings.append(
            "docker compose ps filtrado fallo; se uso el inventario JSON como fallback"
        )
    else:
        running = {line.strip() for line in ps.stdout.splitlines() if line.strip()}
    expected = set(required_services)
    if with_local_db:
        expected.add("db")
    if with_frontend:
        expected.add("frontend")
    missing = sorted(expected - running)
    if missing:
        result.failures.append(f"Servicios no running: {', '.join(missing)}")
    else:
        result.checks.append(f"Servicios running: {', '.join(sorted(expected))}")

    if with_local_db and "db" in running:
        ready = _run(
            [
                docker_bin,
                "compose",
                "--profile",
                "localdb",
                "exec",
                "-T",
                "db",
                "pg_isready",
                "-U",
                os.getenv("POSTGRES_USER", "portfolio"),
                "-d",
                os.getenv("POSTGRES_DB", "portfolio"),
            ]
        )
        if ready.returncode != 0:
            result.failures.append("PostgreSQL local no responde a pg_isready")
        else:
            result.checks.append("PostgreSQL local listo")

    api_base = api_base.rstrip("/")
    api_path = "/api/health" if api_token else "/api/auth/status"
    try:
        payload = _request_json(f"{api_base}{api_path}", api_token, timeout)
        result.checks.append(f"Monitor API responde en {api_path}")
        if api_token:
            database_ok = bool((payload.get("database") or {}).get("ok"))
            if not database_ok:
                result.failures.append("Monitor API responde, pero database.ok=false")
            else:
                result.checks.append("Monitor API confirma database.ok=true")
        else:
            result.warnings.append(
                "MONITOR_API_TOKEN no disponible: se valido autenticacion, no /api/health"
            )
    except RuntimeError as exc:
        result.failures.append(str(exc))

    if with_frontend:
        try:
            request = Request(frontend_url, headers={"Accept": "text/html"})
            with urlopen(request, timeout=timeout) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
            result.checks.append("Frontend responde HTTP 200")
        except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
            result.failures.append(f"Frontend {frontend_url}: {exc}")

    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke no destructivo de Docker Compose.")
    parser.add_argument(
        "--services",
        default="scheduler,telegram_bot,monitor_api",
        help="Servicios obligatorios separados por coma.",
    )
    parser.add_argument(
        "--api-base",
        default=f"http://127.0.0.1:{os.getenv('MONITOR_API_PORT', '8010')}",
    )
    parser.add_argument("--token-env", default="MONITOR_API_TOKEN")
    parser.add_argument("--with-local-db", action="store_true")
    parser.add_argument("--with-frontend", action="store_true")
    parser.add_argument(
        "--frontend-url",
        default=f"http://127.0.0.1:{os.getenv('FRONTEND_PORT', '5173')}",
    )
    parser.add_argument("--docker-bin", default="docker")
    parser.add_argument("--timeout", type=float, default=8.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    services = [item.strip() for item in args.services.split(",") if item.strip()]
    try:
        result = run_smoke(
            required_services=services,
            api_base=args.api_base,
            api_token=os.getenv(args.token_env) or None,
            with_local_db=args.with_local_db,
            with_frontend=args.with_frontend,
            frontend_url=args.frontend_url,
            docker_bin=args.docker_bin,
            timeout=args.timeout,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    for check in result.checks:
        print(f"OK: {check}")
    for warning in result.warnings:
        print(f"WARN: {warning}")
    for failure in result.failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
