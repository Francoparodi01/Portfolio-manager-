"""Fail-closed owner selection for offline audit helpers; no environment reads."""
from __future__ import annotations

from collections.abc import Mapping
import re


def _positive_chat_id(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("INVALID_OWNER_CONFIGURATION")
    raw = str(value).strip()
    if not re.fullmatch(r"[0-9]+", raw):
        raise ValueError("INVALID_OWNER_CONFIGURATION")
    number = int(raw)
    if not 0 < number <= 9223372036854775807:
        raise ValueError("INVALID_OWNER_CONFIGURATION")
    return number


def resolve_owner_chat_id(environment: Mapping[str, str], requested_owner: object = None) -> int:
    """Use ADMIN_CHAT_IDS only, requiring an explicit member when ambiguous.

    Errors intentionally include neither configured nor requested identifiers.
    The caller supplies its environment; this module never loads a .env file.
    """
    raw = environment.get("ADMIN_CHAT_IDS")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("ADMIN_CHAT_IDS_REQUIRED")
    admins = {_positive_chat_id(item) for item in raw.replace(";", ",").split(",")}
    if requested_owner is not None:
        selected = _positive_chat_id(requested_owner)
        if selected not in admins:
            raise ValueError("REQUESTED_OWNER_NOT_ADMIN")
        return selected
    if len(admins) != 1:
        raise ValueError("EXPLICIT_OWNER_CHAT_ID_REQUIRED")
    return next(iter(admins))
