import importlib.util
from pathlib import Path
import sys
from types import ModuleType

import pytest

from src.technical_desk.owner_scope import resolve_owner_chat_id


def test_single_admin_and_duplicate_values_are_unambiguous():
    assert resolve_owner_chat_id({"ADMIN_CHAT_IDS": " 123 ; 123 "}) == 123


def test_explicit_owner_must_be_a_member():
    assert resolve_owner_chat_id({"ADMIN_CHAT_IDS": "123,456"}, "456") == 456
    with pytest.raises(ValueError, match="REQUESTED_OWNER_NOT_ADMIN") as error:
        resolve_owner_chat_id({"ADMIN_CHAT_IDS": "123,456"}, "987654")
    assert "987654" not in str(error.value)
    assert "123" not in str(error.value)


def test_multiple_admins_without_selection_fail_closed():
    with pytest.raises(ValueError, match="EXPLICIT_OWNER_CHAT_ID_REQUIRED"):
        resolve_owner_chat_id({"ADMIN_CHAT_IDS": "123;456"})


@pytest.mark.parametrize("environment", [{}, {"ADMIN_CHAT_IDS": ""}, {"ADMIN_CHAT_IDS": "  "},
                                             {"TELEGRAM_CHAT_ID": "123"}])
def test_no_fallback_to_telegram_destination(environment):
    with pytest.raises(ValueError, match="ADMIN_CHAT_IDS_REQUIRED"):
        resolve_owner_chat_id(environment, "123")


@pytest.mark.parametrize("value", ["123,secret", "123,,456", "0", "-123", "1.0", "１２３", "9223372036854775808"])
def test_invalid_admin_configuration_is_not_silently_filtered(value):
    with pytest.raises(ValueError, match="INVALID_OWNER_CONFIGURATION") as error:
        resolve_owner_chat_id({"ADMIN_CHAT_IDS": value})
    assert value not in str(error.value)


@pytest.mark.parametrize("requested", [True, 1.5, [], "-1", "secret"])
def test_invalid_requested_owner_is_redacted(requested):
    with pytest.raises(ValueError, match="INVALID_OWNER_CONFIGURATION"):
        resolve_owner_chat_id({"ADMIN_CHAT_IDS": "123"}, requested)


def _audit_module():
    path = Path(__file__).resolve().parents[1] / "scripts/audit_technical_desk_stage0b.py"
    spec = importlib.util.spec_from_file_location("audit_technical_desk_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stdin_transport_uses_shipped_selector_and_explicit_admin_before_connect(monkeypatch):
    # Exercise the actual serialized bootstrap without Docker or a database.
    script = _audit_module()
    fake = ModuleType("asyncpg")
    calls = []

    async def connect(*args, **kwargs):
        calls.append(kwargs)
        raise RuntimeError("TEST_CONNECTION_BOUNDARY")

    fake.connect = connect
    monkeypatch.setitem(sys.modules, "asyncpg", fake)
    monkeypatch.setitem(sys.modules, "src.technical_desk.owner_scope", ModuleType("stale_selector"))
    monkeypatch.setattr(sys, "argv", ["-", "--capture", "--owner-chat-id", "456"])
    monkeypatch.setenv("ADMIN_CHAT_IDS", "123,456")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("DATABASE_URL", "postgresql://test.invalid/test")
    with pytest.raises(RuntimeError, match="TEST_CONNECTION_BOUNDARY"):
        exec(script.container_capture_source(), {})
    assert len(calls) == 1
    assert calls[0]["server_settings"]["default_transaction_read_only"] == "on"


def test_stdin_transport_rejects_ambiguous_admins_before_database_import(monkeypatch):
    script = _audit_module()
    monkeypatch.setitem(sys.modules, "asyncpg", None)
    monkeypatch.setitem(sys.modules, "src.technical_desk.owner_scope", ModuleType("stale_selector"))
    monkeypatch.setattr(sys, "argv", ["-", "--capture"])
    monkeypatch.setenv("ADMIN_CHAT_IDS", "123,456")
    with pytest.raises(ValueError, match="EXPLICIT_OWNER_CHAT_ID_REQUIRED"):
        exec(script.container_capture_source(), {})
