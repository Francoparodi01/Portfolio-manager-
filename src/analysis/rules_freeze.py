"""Freeze of the decision rules for a prospective (holdout) evaluation.

The rules are every ``src`` module reachable by import from the decision entry
point (``scripts/run_analysis.py``) plus the JSON policies in ``config/``.
Hashes are taken over LF-normalized bytes, so a Windows CRLF checkout does not
look like a rule change. Environment variables, the database state and
external models (LLM, data providers) are not covered: the check says so.
"""

from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
ENTRY_POINTS = ("scripts/run_analysis.py",)
MANIFEST_PATH = ROOT / "config" / "decision_rules_freeze.json"
VERSION = "decision-rules-freeze-v1"
NOT_COVERED = (
    "variables de entorno (.env)",
    "estado de la base (pesos, calibraciones guardadas)",
    "modelos externos (LLM, proveedores de datos)",
)


def _module_path(root: Path, module: str) -> Optional[Path]:
    base = root.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _imports(path: Path, root: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    package = ".".join(path.relative_to(root).with_suffix("").parts)
    if path.name == "__init__.py":
        package = package.rsplit(".", 1)[0]
    else:
        package = package.rsplit(".", 1)[0] if "." in package else ""
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = package.split(".") if package else []
                parts = parts[: len(parts) - (node.level - 1)] if node.level > 1 else parts
                base = ".".join(parts + ([node.module] if node.module else []))
            else:
                base = node.module or ""
            found.add(base)
            found.update(f"{base}.{alias.name}" for alias in node.names)
    return {name for name in found if name == "src" or name.startswith("src.")}


def decision_rule_files(root: Path = ROOT) -> list[str]:
    pending = [root / entry for entry in ENTRY_POINTS]
    seen: set[Path] = set()
    while pending:
        path = pending.pop()
        if path in seen or not path.is_file():
            continue
        seen.add(path)
        for module in _imports(path, root):
            parts = module.split(".")
            for i in range(1, len(parts) + 1):  # packages' __init__ run on import too
                target = _module_path(root, ".".join(parts[:i]))
                if target is not None and target not in seen:
                    pending.append(target)
    files = {p.relative_to(root).as_posix() for p in seen}
    files.update(p.relative_to(root).as_posix() for p in (root / "config").glob("*.json")
                 if p.name != MANIFEST_PATH.name)
    return sorted(files)


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def current_hashes(root: Path = ROOT) -> dict[str, str]:
    return {name: file_digest(root / name) for name in decision_rule_files(root)}


def _git_commit(root: Path) -> Optional[str]:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True,
                             capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


def build_manifest(root: Path = ROOT, *, frozen_at: Optional[datetime] = None, note: str = "") -> dict:
    files = current_hashes(root)
    frozen_at = frozen_at or datetime.now(tz=timezone.utc)
    return {
        "version": VERSION,
        "frozen_at": frozen_at.astimezone(timezone.utc).isoformat(),
        "git_commit": _git_commit(root),
        "entry_points": list(ENTRY_POINTS),
        "files": files,
        "tree_sha256": hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(),
        "not_covered": list(NOT_COVERED),
        "note": note,
    }


def load_manifest(path: Path = MANIFEST_PATH) -> Optional[dict]:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def check_manifest(manifest: dict, root: Path = ROOT) -> dict:
    frozen = manifest.get("files") or {}
    now = current_hashes(root)
    changed = sorted(k for k in frozen.keys() & now.keys() if frozen[k] != now[k])
    added = sorted(now.keys() - frozen.keys())
    removed = sorted(frozen.keys() - now.keys())
    return {
        "status": "FROZEN_OK" if not (changed or added or removed) else "FREEZE_BROKEN",
        "frozen_at": manifest.get("frozen_at"),
        "changed": changed,
        "added": added,
        "removed": removed,
    }


def freeze_note(check: dict) -> str:
    if check["status"] == "FROZEN_OK":
        return "reglas intactas desde el congelamiento"
    touched = check["changed"] + check["added"] + check["removed"]
    return (
        f"REGLAS MODIFICADAS despues del congelamiento ({len(touched)} archivos: "
        + ", ".join(touched[:3])
        + ("..." if len(touched) > 3 else "")
        + "): la cohorte prospectiva deja de ser un holdout limpio"
    )
