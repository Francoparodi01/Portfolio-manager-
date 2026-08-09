from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.analysis.intraday_range_shadow import build_intraday_range_shadow


ART = ZoneInfo("America/Argentina/Buenos_Aires")


def _sample(hour: int, minute: int, price: float) -> dict:
    return {
        "ts": datetime(2026, 8, 10, hour, minute, tzinfo=ART),
        "last_price": price,
    }


def test_complete_intraday_range_tracks_observed_extremes_and_close_location():
    result = build_intraday_range_shadow(
        [
            _sample(10, 40, 100.0),
            _sample(12, 0, 104.0),
            _sample(16, 40, 98.0),
            _sample(17, 2, 103.0),
        ],
        previous_close=100.0,
    )

    assert result is not None
    assert result.observed_high == 104.0
    assert result.observed_low == 98.0
    assert result.close_price == 103.0
    assert result.sample_count == 4
    assert result.observed_slots == (
        "POST_OPEN",
        "MIDDAY",
        "PRE_CLOSE",
        "POST_CLOSE",
    )
    assert result.slot_coverage == 1.0
    assert result.range_pct_vs_previous_close == pytest.approx(0.06)
    assert result.close_location == pytest.approx(5 / 6)
    assert result.state == "CLOSE_NEAR_HIGH"
    assert result.quality_status == "COMPLETE"
    assert result.eligible_for_evaluation is True


def test_partial_intraday_range_is_persistable_but_not_evaluable():
    result = build_intraday_range_shadow(
        [_sample(10, 40, 100.0), _sample(17, 2, 99.0)],
        previous_close=100.0,
    )

    assert result is not None
    assert result.quality_status == "PARTIAL_SLOTS"
    assert result.slot_coverage == 0.5
    assert result.eligible_for_evaluation is False


def test_intraday_range_marks_price_basis_break_as_outlier():
    result = build_intraday_range_shadow(
        [
            _sample(10, 40, 100.0),
            _sample(12, 0, 1.0),
            _sample(16, 40, 101.0),
            _sample(17, 2, 102.0),
        ],
        previous_close=100.0,
    )

    assert result is not None
    assert result.quality_status == "OUTLIER"
    assert result.eligible_for_evaluation is False
