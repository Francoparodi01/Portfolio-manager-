import asyncio
import os

import pytest

from scripts.validate_contextual_g1 import validate_g1


def test_new_contextual_run_roundtrips_all_e1_contracts_in_timescale():
    url = os.environ.get("QUANTIA_CONTEXTUAL_G1_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set QUANTIA_CONTEXTUAL_G1_TEST_DATABASE_URL to isolated loopback TimescaleDB")
    evidence = asyncio.run(validate_g1(url))
    assert evidence["result"] == "PASS"
    assert evidence["gaps"] == []
    assert evidence["quality"]["bar_count"] == 260
    assert evidence["analysis"]["feature_snapshot"]["schema_version"] == "feature_snapshot_v3"
    assert evidence["analysis"]["contextual_snapshot"]["mode"] == "SHADOW_ONLY"
    assert evidence["analysis"]["feature_snapshot"]["payload"]["contextual_market_shadow"]["snapshot_id"] == evidence["analysis"]["contextual_snapshot"]["snapshot_id"]
    assert evidence["plan"]["intent_executable"] is False
    assert evidence["lineage"]["owner"] == 910001
