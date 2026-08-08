"""Backup, verify and restore the local Compose PostgreSQL database.

The script streams binary dumps directly between Docker and the host. It never
prints database passwords or DSNs, and restore requires an explicit database
name confirmation.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Sequence


DEFAULT_SERVICE = "db"
DEFAULT_USER = "portfolio"
DEFAULT_DATABASE = "portfolio"


class MaintenanceError(RuntimeError):
    pass


def _compose_command(
    args: Sequence[str],
    *,
    docker_bin: str = "docker",
    profile: str = "localdb",
) -> list[str]:
    command = [docker_bin, "compose"]
    if profile:
        command.extend(["--profile", profile])
    command.extend(args)
    return command


def _run_binary(
    command: Sequence[str],
    *,
    stdin: BinaryIO | None = None,
    stdout: BinaryIO | int | None = None,
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            list(command),
            stdin=stdin,
            stdout=stdout if stdout is not None else subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        raise MaintenanceError(f"No se pudo ejecutar {command[0]}: {exc}") from exc


def _error_text(proc: subprocess.CompletedProcess[bytes]) -> str:
    return (proc.stderr or b"").decode("utf-8", errors="replace").strip()


def _assert_success(proc: subprocess.CompletedProcess[bytes], action: str) -> None:
    if proc.returncode == 0:
        return
    detail = _error_text(proc) or f"exit code {proc.returncode}"
    raise MaintenanceError(f"{action}: {detail}")


def verify_backup(
    backup_path: Path,
    *,
    service: str = DEFAULT_SERVICE,
    docker_bin: str = "docker",
    profile: str = "localdb",
) -> None:
    backup_path = backup_path.resolve()
    if not backup_path.is_file() or backup_path.stat().st_size == 0:
        raise MaintenanceError(f"Backup inexistente o vacio: {backup_path}")

    command = _compose_command(
        ["exec", "-T", service, "pg_restore", "--list"],
        docker_bin=docker_bin,
        profile=profile,
    )
    with backup_path.open("rb") as source:
        proc = _run_binary(command, stdin=source)
    _assert_success(proc, "Verificacion de backup fallida")


def create_backup(
    output_path: Path,
    *,
    service: str = DEFAULT_SERVICE,
    user: str = DEFAULT_USER,
    database: str = DEFAULT_DATABASE,
    docker_bin: str = "docker",
    profile: str = "localdb",
) -> Path:
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_name(f"{output_path.name}.partial")

    command = _compose_command(
        [
            "exec",
            "-T",
            service,
            "pg_dump",
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--username",
            user,
            "--dbname",
            database,
        ],
        docker_bin=docker_bin,
        profile=profile,
    )

    try:
        with partial_path.open("wb") as target:
            proc = _run_binary(command, stdout=target)
        _assert_success(proc, "Backup de PostgreSQL fallido")
        verify_backup(
            partial_path,
            service=service,
            docker_bin=docker_bin,
            profile=profile,
        )
        os.replace(partial_path, output_path)
    except Exception:
        partial_path.unlink(missing_ok=True)
        raise

    return output_path


def restore_backup(
    backup_path: Path,
    *,
    confirmed_database: str,
    service: str = DEFAULT_SERVICE,
    user: str = DEFAULT_USER,
    database: str = DEFAULT_DATABASE,
    docker_bin: str = "docker",
    profile: str = "localdb",
) -> None:
    if confirmed_database != database:
        raise MaintenanceError(
            "Restore cancelado: --confirm-database debe coincidir exactamente "
            f"con {database!r}"
        )

    backup_path = backup_path.resolve()
    verify_backup(
        backup_path,
        service=service,
        docker_bin=docker_bin,
        profile=profile,
    )
    command = _compose_command(
        [
            "exec",
            "-T",
            service,
            "pg_restore",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--single-transaction",
            "--username",
            user,
            "--dbname",
            database,
        ],
        docker_bin=docker_bin,
        profile=profile,
    )
    with backup_path.open("rb") as source:
        proc = _run_binary(command, stdin=source)
    _assert_success(proc, "Restore de PostgreSQL fallido")


def _default_output() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("outputs") / "backups" / f"portfolio-{stamp}.dump"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mantiene la base PostgreSQL local de Docker Compose."
    )
    parser.add_argument("--service", default=DEFAULT_SERVICE)
    parser.add_argument("--user", default=os.getenv("POSTGRES_USER", DEFAULT_USER))
    parser.add_argument(
        "--database", default=os.getenv("POSTGRES_DB", DEFAULT_DATABASE)
    )
    parser.add_argument("--profile", default="localdb")
    parser.add_argument("--docker-bin", default="docker")

    subparsers = parser.add_subparsers(dest="action", required=True)
    backup = subparsers.add_parser("backup", help="Crea y verifica un dump custom.")
    backup.add_argument("--output", type=Path, default=None)

    verify = subparsers.add_parser("verify", help="Valida que el dump sea legible.")
    verify.add_argument("backup", type=Path)

    restore = subparsers.add_parser(
        "restore", help="Restaura el dump sobre la base indicada."
    )
    restore.add_argument("backup", type=Path)
    restore.add_argument("--confirm-database", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    common = {
        "service": args.service,
        "docker_bin": args.docker_bin,
        "profile": args.profile,
    }
    try:
        if args.action == "backup":
            output = create_backup(
                args.output or _default_output(),
                user=args.user,
                database=args.database,
                **common,
            )
            print(f"Backup verificado: {output}")
        elif args.action == "verify":
            verify_backup(args.backup, **common)
            print(f"Backup valido: {args.backup.resolve()}")
        else:
            restore_backup(
                args.backup,
                confirmed_database=args.confirm_database,
                user=args.user,
                database=args.database,
                **common,
            )
            print(f"Restore completado sobre la base {args.database!r}.")
    except MaintenanceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
