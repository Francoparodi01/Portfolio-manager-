"""
src/core/credentials.py — Credenciales encriptadas por usuario (Fernet/AES-128).

Almacena un dict {chat_id -> {cocos_user, cocos_pass, mfa_timeout}}
encriptado en /app/secrets/.secrets con clave en /app/secrets/.secret_key.

El directorio /app/secrets/ debe ser un volumen Docker nombrado (secrets_data)
para persistir entre reinicios y tener permisos de escritura correctos.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

# ── Rutas ─────────────────────────────────────────────────────────────────────
SECRETS_DIR  = Path(os.environ.get("SECRETS_DIR", "/app/secrets"))
SECRETS_FILE = SECRETS_DIR / ".secrets"
KEY_FILE     = SECRETS_DIR / ".secret_key"


def _ensure_dir() -> None:
    """Crea el directorio de secrets si no existe, con permisos 700."""
    try:
        SECRETS_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    except PermissionError as e:
        raise RuntimeError(
            f"No se puede crear/acceder a {SECRETS_DIR}. "
            f"Verificá que el volumen 'secrets_data' está montado en docker-compose.yml. "
            f"Error: {e}"
        ) from e


def _get_or_create_key() -> bytes:
    _ensure_dir()
    if KEY_FILE.is_file():
        return KEY_FILE.read_bytes()
    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    KEY_FILE.write_bytes(key)
    KEY_FILE.chmod(0o600)
    return key


def _fernet():
    from cryptography.fernet import Fernet
    return Fernet(_get_or_create_key())


def load_all() -> dict:
    _ensure_dir()
    if not SECRETS_FILE.is_file():
        return {}
    try:
        raw = _fernet().decrypt(SECRETS_FILE.read_bytes())
        return json.loads(raw)
    except Exception:
        return {}


def save_all(data: dict) -> None:
    _ensure_dir()
    raw = json.dumps(data).encode()
    SECRETS_FILE.write_bytes(_fernet().encrypt(raw))
    SECRETS_FILE.chmod(0o600)


def get_user(chat_id: int) -> dict:
    return load_all().get(str(chat_id), {})


def set_user(chat_id: int, updates: dict) -> None:
    data = load_all()
    key  = str(chat_id)
    if key not in data:
        data[key] = {}
    data[key].update(updates)
    save_all(data)


def delete_user(chat_id: int) -> None:
    data = load_all()
    data.pop(str(chat_id), None)
    save_all(data)


def is_configured(chat_id: int) -> bool:
    u = get_user(chat_id)
    return bool(u.get("cocos_user") and u.get("cocos_pass"))


def mask(s: str) -> str:
    if not s:
        return "—"
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]