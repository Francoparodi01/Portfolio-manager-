from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from scripts import docker_smoke, postgres_maintenance


def test_restore_requires_exact_database_confirmation(tmp_path: Path):
    backup = tmp_path / "db.dump"
    backup.write_bytes(b"dump")

    with pytest.raises(postgres_maintenance.MaintenanceError, match="Restore cancelado"):
        postgres_maintenance.restore_backup(
            backup,
            confirmed_database="otra_base",
            database="portfolio",
        )


def test_create_backup_verifies_before_replacing(monkeypatch, tmp_path: Path):
    calls = []

    def _run(command, *, stdin=None, stdout=None):
        calls.append(command)
        if stdout is not None:
            assert isinstance(stdout, io.BufferedWriter)
            stdout.write(b"valid-custom-dump")
        return postgres_maintenance.subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr(postgres_maintenance, "_run_binary", _run)
    output = tmp_path / "portfolio.dump"

    created = postgres_maintenance.create_backup(output)

    assert created == output.resolve()
    assert output.read_bytes() == b"valid-custom-dump"
    assert any("pg_dump" in command for command in calls)
    assert any("pg_restore" in command for command in calls)


def test_docker_smoke_detects_missing_service(monkeypatch):
    responses = iter(
        [
            docker_smoke.subprocess.CompletedProcess([], 0, "", ""),
            docker_smoke.subprocess.CompletedProcess(
                [], 0, "scheduler\nmonitor_api\n", ""
            ),
        ]
    )
    monkeypatch.setattr(docker_smoke, "_run", lambda _command: next(responses))
    monkeypatch.setattr(
        docker_smoke,
        "_request_json",
        lambda *_args: {"authenticated": False},
    )

    result = docker_smoke.run_smoke(
        required_services=["scheduler", "telegram_bot", "monitor_api"],
        api_base="http://127.0.0.1:8010",
        api_token=None,
        with_local_db=False,
        with_frontend=False,
        frontend_url="http://127.0.0.1:5173",
    )

    assert not result.ok
    assert result.failures == ["Servicios no running: telegram_bot"]
    assert result.warnings


def test_docker_smoke_uses_authenticated_health(monkeypatch):
    responses = iter(
        [
            docker_smoke.subprocess.CompletedProcess([], 0, "", ""),
            docker_smoke.subprocess.CompletedProcess(
                [], 0, "scheduler\ntelegram_bot\nmonitor_api\n", ""
            ),
        ]
    )
    monkeypatch.setattr(docker_smoke, "_run", lambda _command: next(responses))
    monkeypatch.setattr(
        docker_smoke,
        "_request_json",
        lambda url, token, _timeout: {
            "database": {"ok": url.endswith("/api/health") and token == "secret"}
        },
    )

    result = docker_smoke.run_smoke(
        required_services=["scheduler", "telegram_bot", "monitor_api"],
        api_base="http://127.0.0.1:8010",
        api_token="secret",
        with_local_db=False,
        with_frontend=False,
        frontend_url="http://127.0.0.1:5173",
    )

    assert result.ok
    assert "Monitor API confirma database.ok=true" in result.checks


def test_docker_smoke_falls_back_to_compose_json(monkeypatch):
    service_rows = "\n".join(
        json.dumps({"Service": service, "State": "running"})
        for service in ("scheduler", "telegram_bot", "monitor_api")
    )
    responses = iter(
        [
            docker_smoke.subprocess.CompletedProcess([], 0, "", ""),
            docker_smoke.subprocess.CompletedProcess([], 1, "", "stale metadata"),
            docker_smoke.subprocess.CompletedProcess([], 0, service_rows, ""),
        ]
    )
    monkeypatch.setattr(docker_smoke, "_run", lambda _command: next(responses))
    monkeypatch.setattr(
        docker_smoke,
        "_request_json",
        lambda *_args: {"authenticated": False},
    )

    result = docker_smoke.run_smoke(
        required_services=["scheduler", "telegram_bot", "monitor_api"],
        api_base="http://127.0.0.1:8010",
        api_token=None,
        with_local_db=False,
        with_frontend=False,
        frontend_url="http://127.0.0.1:5173",
    )

    assert result.ok
    assert any("fallback" in warning for warning in result.warnings)
