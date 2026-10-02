from datetime import datetime, timedelta, timezone
from server import compute


def test_next_open_cost_dedup_and_sell():
    t = datetime(2026, 1, 5, 12, tzinfo=timezone.utc)
    base = dict(created_at=t, feasible=True, is_executable=True,
                was_blocked=False, ticker='ABC', side='BUY', run_id=None)
    rows = [dict(base, intent_id=1), dict(base, intent_id=2, created_at=t+timedelta(hours=1))]
    candles = [dict(ticker='ABC', ts=t+timedelta(days=i), open_price=100,
                    close_price=100+2*i) for i in range(1, 13)]
    buy = compute(rows, candles, cost_bps=75)
    assert buy['quality']['unique_signals'] == 1
    assert buy['quality']['same_day_duplicates'] == 1
    assert buy['metrics']['5']['mean_pct'] == 9.25
    assert buy['metrics']['20']['pending'] == 1
    sell = compute([dict(base, intent_id=1, side='SELL')], candles, cost_bps=75)
    assert sell['metrics']['5']['mean_pct'] == -10.75


def test_conflicting_exit_is_missing_not_zero():
    t = datetime(2026, 1, 5, 12, tzinfo=timezone.utc)
    row = dict(created_at=t, intent_id=1, feasible=True, is_executable=True,
               was_blocked=False, ticker='ABC', side='BUY', run_id=None)
    candles = [dict(ticker='ABC', ts=t+timedelta(days=i), open_price=100,
                    close_price=100+2*i) for i in range(1, 7)]
    candles.append(dict(candles[4], close_price=999))
    result = compute([row], candles)
    assert result['quality']['conflicting_candle_days'] == 1
    assert result['metrics']['5']['n'] == 0
    assert result['metrics']['5']['pending'] == 1
