from datetime import date, timedelta
from decimal import Decimal, localcontext

import pytest

from src.technical_desk.volume_scan import scan_volume


def rows(values):
    return [dict(session_date=(date(2026, 1, 1)+timedelta(days=i)).isoformat(), volume=value)
            for i, value in enumerate(values)]


@pytest.mark.parametrize("before,after", [("100", "300"), ("300", "100")])
def test_exact_threefold_boundaries_are_review_candidates(before, after):
    result = scan_volume(rows([before]*10+[after]*10))
    check = result["checks"][10]
    with localcontext() as context:
        context.prec = 34
        assert check["post_pre_ratio"] == Decimal(after) / Decimal(before)
    assert check["level_jump"] is True
    assert check["flag"] == "CANDIDATE_FOR_REVIEW"
    assert check["observed_at"] == "2026-01-20"
    assert result["flagged_dates"] == ["2026-01-11"]
    assert result["flagged_count"] == 1
    assert result["status_counts"] == {"INSUFFICIENT_WINDOW": 19, "EVALUATED": 1}
    assert result["use_in_causal_features"] is False
    assert all(check["use_in_causal_features"] is False for check in result["checks"])


@pytest.mark.parametrize("before,after", [("100", "299.9999"), ("300", "100.0001")])
def test_near_but_below_threshold_is_not_marked(before, after):
    result = scan_volume(rows([before]*10+[after]*10))
    assert result["flagged_dates"] == []
    assert result["checks"][10]["level_jump"] is False


def test_post_window_includes_candidate_date_and_pre_excludes_it():
    result = scan_volume(rows([100]*10+[600]*10))
    check = result["checks"][10]
    assert check["pre_median"] == 100
    assert check["post_median"] == 600
    assert check["post_pre_ratio"] == 6


def test_no_partial_windows_and_future_use_is_explicit():
    data = rows([100]*30)
    full = scan_volume(data)
    truncated = scan_volume(data[:15])
    assert full["checks"][10]["status"] == "EVALUATED"
    assert truncated["checks"][10]["status"] == "INSUFFICIENT_WINDOW"
    assert truncated["checks"][10]["post_pre_ratio"] is None
    assert full["temporal_basis"] == "RETROSPECTIVE"
    assert full["checks"][10]["observed_at"] > full["checks"][10]["session_date"]
    assert full["affects_analysis"] is full["affects_execution"] is False


@pytest.mark.parametrize("values,status", [([None]*10+[100]*10, "MISSING_VOLUME"),
                                         ([0]*10+[100]*10, "ZERO_MEDIAN_NOT_EVALUABLE"),
                                         ([100]*10+[0]*10, "ZERO_MEDIAN_NOT_EVALUABLE")])
def test_missing_and_zero_medians_are_explicit(values, status):
    result = scan_volume(rows(values))
    assert result["checks"][10]["status"] == status
    assert result["checks"][10]["post_pre_ratio"] is None
    assert result["checks"][10]["level_jump"] is None
    assert result["flagged_count"] == 0


@pytest.mark.parametrize("kwargs", [dict(window=9), dict(window=True), dict(threshold="2.99"),
                                    dict(threshold="NaN"), dict(threshold="bad")])
def test_versioned_policy_cannot_be_tuned_in_place(kwargs):
    with pytest.raises(ValueError, match="VOLUME_SCAN_POLICY_MISMATCH"):
        scan_volume(rows([100]*30), **kwargs)


@pytest.mark.parametrize("value", [-1, "NaN", "Infinity", "bad", True])
def test_invalid_volumes_fail_closed(value):
    with pytest.raises(ValueError, match="INVALID_VOLUME"):
        scan_volume(rows([100]*10+[value]+[100]*10))


def test_duplicate_or_unsorted_dates_are_rejected():
    data = rows([100]*30)
    data[10]["session_date"] = data[9]["session_date"]
    with pytest.raises(ValueError, match="SESSION_DATES_NOT_STRICTLY_INCREASING"):
        scan_volume(data)


def test_empty_input_has_explicit_empty_counts():
    result = scan_volume([])
    assert result["rows_scanned"] == result["flagged_count"] == 0
    assert result["flagged_dates"] == result["checks"] == []


def test_decimal_calculation_does_not_depend_on_callers_context():
    data = rows([Decimal("300.00001")]*10+[Decimal("100.00001")]*10)
    expected = scan_volume(data)
    with localcontext() as context:
        context.prec = 4
        assert scan_volume(data) == expected
