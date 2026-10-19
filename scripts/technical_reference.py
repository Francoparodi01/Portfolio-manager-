"""Independent scalar reference calculations, with explicit Quantia conventions.

No pandas rolling/ewm, production helpers, data access or signal policies.
These references validate arithmetic, not predictive usefulness.
"""
from math import fsum, isfinite, sqrt


def mean(values):
    return fsum(values) / len(values)


def sma(values, period):
    if len(values) < period or not all(isfinite(v) for v in values[-period:]):
        return None
    return mean(values[-period:])


def ema(values, span):
    """First-price seed, alpha=2/(span+1), adjust=False, complete inputs."""
    if not values or not all(isfinite(v) for v in values):
        raise ValueError('EMA reference needs complete finite inputs')
    alpha = 2 / (span + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append(alpha * value + (1 - alpha) * result[-1])
    return result


def rsi(values, period=14, *, percentage=False):
    """Rolling arithmetic means; flat=50 is an explicit convention, not 0/0."""
    if len(values) <= period:
        return None
    window = values[-period-1:]
    if not all(isfinite(v) and v > 0 for v in window):
        return None
    changes = [(b/a-1) if percentage else b-a for a,b in zip(window,window[1:])]
    gains = fsum(max(x,0) for x in changes)
    losses = fsum(max(-x,0) for x in changes)
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    return 100 - 100 / (1 + gains/losses)


def true_ranges(high, low, close):
    return [h-l if i == 0 else max(h-l, abs(h-close[i-1]), abs(l-close[i-1]))
            for i,(h,l) in enumerate(zip(high,low))]


def bollinger(values, period=20, *, ddof=1):
    if len(values) < period:
        return None
    window = values[-period:]
    middle = mean(window)
    sd = sqrt(fsum((v-middle)**2 for v in window)/(period-ddof))
    return middle+2*sd, middle, middle-2*sd


def indicators(frame):
    """Reference for accepted complete positive OHLC frames, without mutation."""
    close = frame.Close.tolist(); high=frame.High.tolist(); low=frame.Low.tolist()
    vol = frame.Volume.tolist() if 'Volume' in frame else [float('nan')]*len(close)
    fast,slow = ema(close,12),ema(close,26)
    line = [a-b for a,b in zip(fast,slow)]; signal=ema(line,9)
    hist=[a-b for a,b in zip(line,signal)]
    upper,middle,lower = bollinger(close)
    recent = vol[-20:]
    ratio = recent[-1]/mean(recent) if all(isfinite(v) and v>0 for v in recent) else None
    if 'IsClosed' in frame and any(v is False or v == False for v in frame.IsClosed if v is not None):
        ratio = None
    obv = None; obv_mean = None
    if ratio is not None and all(isfinite(v) and v>0 for v in vol):
        running=0.0; totals=[0.0]
        for i in range(1,len(close)):
            running += vol[i]*(1 if close[i]>close[i-1] else -1 if close[i]<close[i-1] else 0)
            totals.append(running)
        obv=totals[-1]; obv_mean=mean(totals[-20:])
    return {'sma_20':sma(close,20),'sma_50':sma(close,50),'sma_200':sma(close,200),
        'ema_12':fast[-1],'ema_26':slow[-1],'rsi_14':rsi(close),
        'macd_line':line[-1],'macd_signal':signal[-1],'macd_hist':hist[-1],
        'macd_hist_prev':hist[-2],'bb_upper':upper,'bb_middle':middle,'bb_lower':lower,
        'atr_14':mean(true_ranges(high,low,close)[-14:]),'vol_ratio':ratio,'obv':obv,'obv_sma20':obv_mean}
