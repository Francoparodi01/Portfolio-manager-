from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest

from src.technical_desk.evidence_capture import canonical
from src.technical_desk.features import compute_features
from src.technical_desk.stage0c import terminal_atr


POLICY = json.loads((Path(__file__).resolve().parents[1]/"docs/technical-desk-stage1a-policy.json").read_text())


def bars(count=65):
    output = []
    for index in range(count):
        start = datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(days=index)
        close = Decimal(100 + ((index % 12) if index % 24 < 12 else 12-(index%12)))
        output.append(dict(time=start.isoformat(),closed_at=(start+timedelta(hours=20)).isoformat(),
                           open=close,close=close,high=close+2,low=close-2,fact_hash=str(index)))
    return output


def compute(rows, timeframe="D"):
    return compute_features(rows,timeframe=timeframe,tick_size=Decimal('1'),policy=POLICY)


@pytest.mark.parametrize("timeframe",["D","W"])
def test_every_prefix_reconstructs_the_same_bar_features_without_future_data(timeframe):
    source = bars()
    full = compute(source,timeframe)
    for k in range(len(source)):
        prefix = compute(source[:k+1],timeframe)
        assert canonical(full["snapshots"][k]) == canonical(prefix["snapshots"][-1])
        assert all(p["index"]+2 <= k and p["pivot_time"] < p["confirmed_at"] <= source[k]["closed_at"]
                   for p in prefix["confirmed_pivots"])
    changed_future = deepcopy(source)
    for row in changed_future[30:]:
        for field in ("open","high","low","close"):
            row[field] *= 100
    assert canonical(compute(changed_future,timeframe)["snapshots"][:30]) == canonical(full["snapshots"][:30])


def test_open_extreme_stays_candidate_until_two_later_closed_bars():
    rows = bars(8)
    rows[4].update(open=Decimal(120),close=Decimal(120),high=Decimal(122),low=Decimal(118))
    at_pivot = compute(rows[:5])["snapshots"][-1]
    candidate = next(p for p in at_pivot["open_extrema"] if p["index"]==4 and p["kind"]=="HIGH")
    assert candidate["status"]=="CANDIDATE" and candidate["confirmed_at"] is None
    assert not any(p["index"]==4 for p in compute(rows[:6])["confirmed_pivots"])
    confirmed = next(p for p in compute(rows[:7])["confirmed_pivots"] if p["index"]==4)
    assert confirmed["confirmed_index"] == 6 and confirmed["confirmed_at"] == rows[6]["closed_at"]


def test_equal_high_ties_choose_earliest_and_dual_extreme_is_ambiguous():
    rows = bars(9)
    for index in (3,4):
        rows[index]["high"] = Decimal(130)
    pivots = compute(rows)["confirmed_pivots"]
    assert [p["index"] for p in pivots if p["kind"]=="HIGH"] == [3]
    rows[3]["low"] = Decimal(50)
    result = compute(rows)
    assert result["ambiguous_extrema"][0]["index"] == 3
    assert not any(p["index"]==3 for p in result["confirmed_pivots"])


def test_wilder_atr_matches_frozen_v3_and_weekly_sma200_is_explicitly_missing():
    rows = bars(57)
    result = compute(rows,"W")
    assert result["snapshots"][-1]["indicators"]["atr"]["value"] == terminal_atr(rows)["value"]
    assert result["missing_features"]["sma_200"] == "NEEDS_200_COMPLETE_BARS"
    assert result["missing_features"]["relative_strength"] == "NO_COMPATIBLE_ARS_BENCHMARK"
    assert result["snapshots"][18]["indicators"]["sma_20"]["value"] is None
    assert result["snapshots"][19]["indicators"]["sma_20"]["value"] is not None


def test_flat_series_has_rsi50_macd0_and_full_window_warmups():
    rows = bars(35)
    for row in rows:
        row.update(open=Decimal(10),high=Decimal(11),low=Decimal(9),close=Decimal(10))
    snapshots = compute(rows)["snapshots"]
    assert snapshots[13]["indicators"]["rsi"]["value"] is None
    assert snapshots[14]["indicators"]["rsi"]["value"] == 50
    assert snapshots[24]["indicators"]["macd"]["value"] is None
    assert snapshots[25]["indicators"]["macd"]["value"] == 0
    assert snapshots[32]["indicators"]["macd_signal"]["value"] is None
    assert snapshots[33]["indicators"]["macd_signal"]["value"] == 0


def test_breakout_requires_two_closes_against_the_original_range():
    rows = bars(23)
    for row in rows[:20]:
        row.update(open=Decimal(10),high=Decimal(11),low=Decimal(9),close=Decimal(10))
    for row in rows[20:]:
        row.update(open=Decimal(12),high=Decimal(14),low=Decimal(10),close=Decimal(12))
    result = compute(rows)
    assert result["snapshots"][20]["breakout_confirmed"] is None
    event = result["snapshots"][21]["breakout_confirmed"]
    assert event["threshold"]==12 and event["range"]["end_index"]==19 and event["closes"]==2
    rows[21]["close"] = Decimal(11)
    assert compute(rows)["snapshots"][21]["breakout_confirmed"] is None


def test_fibonacci_uses_only_confirmed_swing_anchors_and_fixed_ratios():
    result = compute(bars())
    known = {p["id"]:p for p in result["confirmed_pivots"]}
    assert result["swings"]
    for swing in result["swings"]:
        start,end = known[swing["start_id"]],known[swing["end_id"]]
        assert swing["fibonacci"]["retracements"]["0.5"] == (start["price"]+end["price"])/2
        assert swing["fibonacci"]["extensions"]["1"] == end["price"]
        assert swing["confirmed_at"] == end["confirmed_at"]


def test_unclosed_or_out_of_order_bars_are_rejected():
    rows=bars(5)
    rows[-1]["closed_at"] = rows[-1]["time"]
    with pytest.raises(ValueError,match="INVALID_CLOSED_OHLC_BAR"):
        compute(rows)
    with pytest.raises(ValueError,match="BARS_MUST_BE_CHRONOLOGICAL"):
        compute(list(reversed(bars(5))))


def test_timestamp_comparison_uses_instants_not_lexicographic_offsets():
    rows=bars(1)
    rows[0].update(time="2026-01-01T12:00:00+00:00",closed_at="2026-01-01T13:00:00+02:00")
    with pytest.raises(ValueError,match="INVALID_CLOSED_OHLC_BAR"):
        compute(rows)
    rows=bars(2)
    rows[1]["time"]="2026-01-01T02:00:00+02:00"  # Same instant as first start.
    with pytest.raises(ValueError,match="BARS_MUST_BE_CHRONOLOGICAL"):
        compute(rows)


@pytest.mark.parametrize("field",["time","closed_at"])
def test_naive_bar_time_is_rejected(field):
    rows=bars(1)
    rows[0][field]=rows[0][field].replace("+00:00","")
    with pytest.raises(ValueError,match="AWARE_BAR_TIMESTAMPS_REQUIRED"):
        compute(rows)
