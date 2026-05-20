"""Credential encryption helpers for future multi-user onboarding."""
from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover - exercised in lean local envs
    Fernet = None

    class InvalidToken(Exception):
        pass


class CredentialKeyMissing(RuntimeError):
    """Raised when encrypted credential support is requested without a key."""


@dataclass(frozen=True)
class UserCredentials:
    username: str
    password: str


class CredentialCipher:
    """
    Central encryption boundary for user credentials.

    The key is process configuration, not user data. Later rotations can use
    `credentials_key_version` from `bot_users` without changing call sites.
    """

    def __init__(self, key: str):
        if not key:
            raise CredentialKeyMissing("APP_ENCRYPTION_KEY no configurada")
        if Fernet is None:
            raise RuntimeError("cryptography no instalado")
        self._fernet = Fernet(key.encode("ascii"))

    @classmethod
    def from_env(cls) -> "CredentialCipher":
        return cls(os.environ.get("APP_ENCRYPTION_KEY", "").strip())

    def encrypt(self, value: str) -> str:
        if value is None:
            raise ValueError("No se puede cifrar None")
        return self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

    def decrypt(self, token: str) -> str:
        if not token:
            raise ValueError("Token cifrado vacio")
        try:
            return self._fernet.decrypt(token.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("Token cifrado invalido") from exc

    def encrypt_credentials(self, credentials: UserCredentials) -> tuple[str, str]:
        return self.encrypt(credentials.username), self.encrypt(credentials.password)

    def decrypt_credentials(self, username_token: str, password_token: str) -> UserCredentials:
        return UserCredentials(
            username=self.decrypt(username_token),
            password=self.decrypt(password_token),
        )
