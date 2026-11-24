import asyncio
from contextlib import asynccontextmanager

import pytest

fernet = pytest.importorskip("cryptography.fernet")
Fernet = fernet.Fernet

from src.collector.db import PortfolioDatabase
from src.core.credentials import CredentialCipher, UserCredentials


class _Conn:
    def __init__(self):
        self.execute_args = None
        self.row = None

    async def execute(self, *args):
        self.execute_args = args

    async def fetchrow(self, *_args):
        return self.row


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def test_bot_user_credentials_are_stored_encrypted():
    conn = _Conn()
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)
    cipher = CredentialCipher(Fernet.generate_key().decode())

    asyncio.run(
        db.upsert_bot_user_credentials(
            chat_id=123,
            credentials=UserCredentials("franco@example.com", "secreto"),
            cipher=cipher,
            telegram_username="franco",
            display_name="Franco",
        )
    )

    params = conn.execute_args[1:]
    assert "franco@example.com" not in params
    assert "secreto" not in params
    assert cipher.decrypt(params[3]) == "franco@example.com"
    assert cipher.decrypt(params[4]) == "secreto"


def test_bot_user_credentials_ignore_plaintext_legacy_columns():
    conn = _Conn()
    cipher = CredentialCipher(Fernet.generate_key().decode())
    user_token, pass_token = cipher.encrypt_credentials(
        UserCredentials("user@example.com", "clave")
    )
    conn.row = {
        "cocos_user_ciphertext": user_token,
        "cocos_pass_ciphertext": pass_token,
        "cocos_user": "legacy@example.com",
        "cocos_pass": "legacy",
    }
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)

    credentials = asyncio.run(
        db.get_bot_user_credentials(chat_id=123, cipher=cipher)
    )

    assert credentials == UserCredentials("user@example.com", "clave")
