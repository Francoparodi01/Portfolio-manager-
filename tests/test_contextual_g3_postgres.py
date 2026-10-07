import asyncio
import os

import pytest

from scripts.validate_contextual_g3 import validate_g3


def test_g3_append_only_asof_and_determinism_in_disposable_timescale():
    url = os.environ.get("QUANTIA_CONTEXTUAL_G3_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set QUANTIA_CONTEXTUAL_G3_TEST_DATABASE_URL to isolated loopback TimescaleDB")

    evidence = asyncio.run(validate_g3(url))
    assert evidence["append_only"]["result"] == "PASS"
    assert all(evidence["append_only"]["correction"].values())
    assert all(evidence["append_only"]["immutability"].values())
    assert evidence["asof"]["result"] == "PASS"
    assert evidence["asof"]["before_selects_a"] is True
    assert evidence["asof"]["after_selects_b"] is True
    assert evidence["asof"]["past_replay_unchanged_after_correction"] is True
    assert evidence["asof"]["aborted_capture_excluded"] is True
    assert evidence["asof"]["future_complete_event_excluded"] is True
    assert evidence["asof"]["observation_selected_after_complete_event"] is True
    assert evidence["determinism"]["same_replay_twice"] is True
    assert evidence["determinism"]["past_snapshot_unchanged_after_future_correction"] is True
    assert evidence["determinism"]["persisted_snapshot_reread_equal"] is True
    assert evidence["lifecycle"]["complete_capture"]["state"] == "COMPLETE"
    assert evidence["lifecycle"]["aborted_capture"]["state"] == "ABORTED"
    assert evidence["non_regression"]["result"] == "PASS"
    assert all(evidence["non_regression"]["equal"].values())
    assert evidence["non_regression"]["orders_executed"] == 0
