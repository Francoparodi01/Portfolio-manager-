import asyncio
import os

import pytest

from scripts.validate_contextual_telegram import validate


def test_contextual_telegram_shadow_guard_in_postgres():
    url = os.environ.get("QUANTIA_CONTEXTUAL_T1_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set QUANTIA_CONTEXTUAL_T1_TEST_DATABASE_URL to isolated loopback PostgreSQL")
    evidence = asyncio.run(validate(url))
    assert evidence["result"] == "PASS"
    assert evidence["migration_idempotent"] is True
    assert evidence["productive_order_contract_unchanged"] is True
    assert evidence["shadow_order_intent_blocked"] is True
    assert evidence["nonzero_shadow_plan_blocked"] is True
