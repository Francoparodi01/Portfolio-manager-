import asyncio
import os

import pytest

from scripts.validate_contextual_g2 import validate_g2


def test_g2_migration_and_shadow_roundtrip_in_disposable_timescale():
    url = os.environ.get("QUANTIA_CONTEXTUAL_G2_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set QUANTIA_CONTEXTUAL_G2_TEST_DATABASE_URL to isolated loopback TimescaleDB")
    evidence = asyncio.run(validate_g2(url))
    assert evidence["result"] == "PASS"
    assert evidence["migration"]["applied_twice"] is True
    assert evidence["migration"]["legacy_unknown_preserved"] is True
    assert all(evidence["migration"]["append_only"].values())
    assert evidence["roundtrip"]["feature_schema"] == "feature_snapshot_v3"
    assert evidence["roundtrip"]["context_confidence"]["value"] >= .8
    assert evidence["pit"]["all_available_at_lte_cutoff"] is True
    assert evidence["pit"]["all_scraped_at_lte_cutoff"] is True
    assert evidence["pit"]["closed_bars_have_bar_end"] is True
    assert evidence["pit"]["future_bars"] == 0
    assert all(evidence["non_regression"].values())
    assert evidence["orders_executed"] == 0
