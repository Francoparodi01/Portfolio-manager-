import pytest

fernet = pytest.importorskip("cryptography.fernet")
Fernet = fernet.Fernet

from src.core.credentials import CredentialCipher, CredentialKeyMissing, UserCredentials


def test_credential_cipher_round_trip():
    cipher = CredentialCipher(Fernet.generate_key().decode())
    encrypted_user, encrypted_pass = cipher.encrypt_credentials(
        UserCredentials(username="franco@example.com", password="secreto")
    )

    assert encrypted_user != "franco@example.com"
    assert encrypted_pass != "secreto"
    assert cipher.decrypt_credentials(encrypted_user, encrypted_pass) == UserCredentials(
        username="franco@example.com",
        password="secreto",
    )


def test_credential_cipher_requires_key(monkeypatch):
    monkeypatch.delenv("APP_ENCRYPTION_KEY", raising=False)

    with pytest.raises(CredentialKeyMissing):
        CredentialCipher.from_env()
