"""A deliberately broken test feature must trip the real prefix assertion."""
from copy import deepcopy

import pytest

from src.technical_desk.causality import assert_prefix_causal
from tests.test_technical_desk_features import bars, compute


def leaky_feature(rows, timeframe="D"):
    # Test-only mutation: the next close is unavailable at snapshot k.
    result = deepcopy(compute(rows, timeframe))
    for k, snapshot in enumerate(result["snapshots"]):
        snapshot["deliberately_broken_next_close"] = rows[k + 1]["close"] if k + 1 < len(rows) else None
    return result


@pytest.mark.parametrize("timeframe", ["D", "W"])
def test_prefix_check_accepts_causal_features(timeframe):
    source = bars(35)
    assert assert_prefix_causal(source, lambda rows: compute(rows, timeframe), range(35)) == list(range(35))


@pytest.mark.parametrize("timeframe", ["D", "W"])
def test_prefix_check_rejects_a_deliberate_future_feature(timeframe):
    with pytest.raises(AssertionError, match="PREFIX_CAUSALITY_VIOLATION k=0 fields=deliberately_broken_next_close"):
        assert_prefix_causal(bars(35), lambda rows: leaky_feature(rows, timeframe), range(35))


def test_prefix_check_rejects_invalid_cut():
    with pytest.raises(ValueError, match="INVALID_PREFIX_CUT"):
        assert_prefix_causal(bars(3), compute, [3])
