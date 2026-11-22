"""Versioned deterministic features over closed bars; no I/O or model calls."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, localcontext

from src.technical_desk.evidence_capture import content_hash


VERSION = "technical-desk-features-v1"


def number(value):
    if isinstance(value, (bool, float)):
        raise ValueError("EXACT_DECIMAL_REQUIRED")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("FINITE_NUMBER_REQUIRED")
    return result


def metric(value, reason="INSUFFICIENT_HISTORY"):
    return dict(value=value, status="AVAILABLE" if value is not None else "NOT_AVAILABLE",
                reason=None if value is not None else reason)


def _moment(value):
    if not isinstance(value, str):
        raise ValueError("AWARE_BAR_TIMESTAMPS_REQUIRED")
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("AWARE_BAR_TIMESTAMPS_REQUIRED")
    return moment


def _ema(values, period):
    result, seed, prior = [], [], None
    alpha = Decimal(2) / (period + 1)
    for value in values:
        if value is None:
            result.append(None)
            continue
        if prior is None:
            seed.append(value)
            if len(seed) == period:
                prior = sum(seed) / period
        else:
            prior = value * alpha + prior * (1-alpha)
        result.append(prior)
    return result


def _indicator_series(bars, policy):
    cfg = policy["indicators"]
    closes = [bar["close"] for bar in bars]
    smas = {window: [sum(closes[k-window+1:k+1])/window if k+1 >= window else None
                     for k in range(len(bars))] for window in cfg["sma_windows"]}
    fast, slow = (_ema(closes, cfg[key]) for key in ("macd_fast", "macd_slow"))
    macd = [a-b if a is not None and b is not None else None for a,b in zip(fast,slow)]
    signal = _ema(macd, cfg["macd_signal"])
    atr, rsi, tr_seed, gains, losses = [], [], [], [], []
    prior_atr = avg_gain = avg_loss = None
    ap, rp = cfg["atr_period"], cfg["rsi_period"]
    for k,bar in enumerate(bars):
        tr = bar["high"] - bar["low"]
        if k:
            tr = max(tr, abs(bar["high"]-closes[k-1]), abs(bar["low"]-closes[k-1]))
            change = closes[k]-closes[k-1]
            gain, loss = max(change, Decimal(0)), max(-change, Decimal(0))
            if avg_gain is None:
                gains.append(gain)
                losses.append(loss)
                if len(gains) == rp:
                    avg_gain, avg_loss = sum(gains)/rp, sum(losses)/rp
            else:
                avg_gain, avg_loss = (avg_gain*(rp-1)+gain)/rp, (avg_loss*(rp-1)+loss)/rp
        if prior_atr is None:
            tr_seed.append(tr)
            if len(tr_seed) == ap:
                prior_atr = sum(tr_seed)/ap
        else:
            prior_atr = (prior_atr*(ap-1)+tr)/ap
        atr.append(prior_atr)
        value = None
        if avg_gain is not None:
            value = (Decimal(50) if avg_gain == avg_loss == 0 else
                     Decimal(100) if avg_loss == 0 else
                     100 - 100/(1+avg_gain/avg_loss))
        rsi.append(value)
    output = []
    for k in range(len(bars)):
        row = {f"sma_{window}": metric(values[k], f"NEEDS_{window}_COMPLETE_BARS") for window,values in smas.items()}
        row.update(rsi=metric(rsi[k],f"NEEDS_{rp+1}_CLOSES"), atr=metric(atr[k],f"NEEDS_{ap}_COMPLETE_BARS"),
            macd=metric(macd[k],f"NEEDS_{cfg['macd_slow']}_COMPLETE_BARS"),
            macd_signal=metric(signal[k],f"NEEDS_{cfg['macd_slow']+cfg['macd_signal']-1}_COMPLETE_BARS"),
            macd_histogram=metric(macd[k]-signal[k] if signal[k] is not None else None),
            relative_strength=metric(None,"NO_COMPATIBLE_ARS_BENCHMARK"))
        output.append(row)
    return output


def _extrema(bars, index, left, observed_end):
    if index < left:
        return []
    prior, after = bars[index-left:index], bars[index+1:observed_end+1]
    kinds = []
    if all(bars[index]["high"] > r["high"] for r in prior) and all(bars[index]["high"] >= r["high"] for r in after):
        kinds.append("HIGH")
    if all(bars[index]["low"] < r["low"] for r in prior) and all(bars[index]["low"] <= r["low"] for r in after):
        kinds.append("LOW")
    return kinds


def _pivot(bars, index, kind, timeframe, confirmed_index=None):
    row = bars[index]
    pivot = dict(index=index, kind=kind, price=row[kind.lower()], timeframe=timeframe,
                 pivot_time=row["time"], confirmed_at=None if confirmed_index is None else bars[confirmed_index]["closed_at"],
                 confirmed_index=confirmed_index, status="CANDIDATE" if confirmed_index is None else "CONFIRMED")
    pivot["id"] = content_hash(dict(version=VERSION,index=index,kind=kind,price=pivot["price"],
                                    timeframe=timeframe,pivot_time=pivot["pivot_time"]))
    return pivot


def _structure(chain, tick, tolerance):
    output = {}
    for kind, labels in (("HIGH",("HH","LH","EH")),("LOW",("HL","LL","EL"))):
        same = [p for p in chain if p["kind"] == kind]
        if len(same) < 2:
            output[kind.lower()] = dict(status="NOT_AVAILABLE",reason="NEEDS_TWO_CONFIRMED_SAME_KIND_PIVOTS")
        else:
            prior, latest = same[-2:]
            delta = latest["price"]-prior["price"]
            label = labels[2] if abs(delta) <= tick*tolerance else labels[0] if delta > 0 else labels[1]
            output[kind.lower()] = dict(status="AVAILABLE",label=label,anchor_ids=[prior["id"],latest["id"]])
    return output


def _swing(first, second, fibonacci):
    delta = second["price"]-first["price"]
    return dict(start_id=first["id"],end_id=second["id"],start_index=first["index"],end_index=second["index"],
                confirmed_at=second["confirmed_at"],delta=delta,
                direction="UP" if delta>0 else "DOWN" if delta<0 else "FLAT",
                fibonacci=dict(version=fibonacci["version"],
                    retracements={r:second["price"]-delta*number(r) for r in fibonacci["retracements"]},
                    extensions={r:first["price"]+delta*number(r) for r in fibonacci["extensions"]}))


def compute_features(rows, *, timeframe, tick_size, policy):
    if timeframe not in {"D","W"} or policy["version"] != VERSION:
        raise ValueError("UNSUPPORTED_FEATURE_CONTRACT")
    tick = number(tick_size)
    if tick <= 0:
        raise ValueError("POSITIVE_TICK_REQUIRED")
    bars = []
    prior_start = prior_close = None
    for row in rows:
        bar = dict(row)
        bar.update({key:number(row[key]) for key in ("open","high","low","close")})
        starts, closes = _moment(bar["time"]), _moment(bar["closed_at"])
        if (bar["low"] > min(bar["open"],bar["close"]) or bar["high"] < max(bar["open"],bar["close"])
                or bar["low"] <= 0 or starts >= closes):
            raise ValueError("INVALID_CLOSED_OHLC_BAR")
        if bars and (prior_start >= starts or prior_close > closes):
            raise ValueError("BARS_MUST_BE_CHRONOLOGICAL")
        bars.append(bar)
        prior_start, prior_close = starts, closes
    with localcontext() as ctx:
        ctx.prec = policy["decimal_precision"]
        return _compute(bars,timeframe,tick,policy)


def _compute(bars,timeframe,tick,policy):
    left, right = policy["pivots"]["left_bars"], policy["pivots"]["right_bars"]
    if type(left) is not int or type(right) is not int or min(left,right)<1:
        raise ValueError("POSITIVE_PIVOT_WINDOWS_REQUIRED")
    indicators = _indicator_series(bars,policy)
    confirmed, chain, ambiguous, snapshots, breakouts = [], [], [], [], []
    pending = None
    range_cfg = policy["ranges"]
    lookback, persistence = range_cfg["lookback_bars"], range_cfg["persistence_closes"]
    for k,bar in enumerate(bars):
        index = k-right
        if index >= left:
            kinds = _extrema(bars,index,left,k)
            if len(kinds) == 2:
                ambiguous.append(dict(index=index,pivot_time=bars[index]["time"],known_at=bar["closed_at"],reason="DUAL_EXTREME"))
            elif kinds:
                pivot = _pivot(bars,index,kinds[0],timeframe,k)
                confirmed.append(pivot)
                if not chain or chain[-1]["kind"] != pivot["kind"]:
                    chain.append(pivot)
                elif (pivot["price"]>chain[-1]["price"] if pivot["kind"]=="HIGH" else pivot["price"]<chain[-1]["price"]):
                    chain[-1] = pivot
        candidates = []
        for i in range(max(left,k-right+1),k+1):
            kinds = _extrema(bars,i,left,k)
            for kind in kinds:
                candidate = _pivot(bars,i,kind,timeframe)
                candidate.update(right_closed_bars_remaining=right-(k-i),
                                 ambiguity="DUAL_EXTREME" if len(kinds)==2 else None)
                candidates.append(candidate)
        range_value = None
        if k >= lookback:
            reference = bars[k-lookback:k]
            range_value = dict(low=min(r["low"] for r in reference),high=max(r["high"] for r in reference),
                               start_index=k-lookback,end_index=k-1,known_at=bar["closed_at"])
        breakout_event = None
        if pending is not None:
            still = bar["close"] >= pending["threshold"] if pending["direction"]=="UP" else bar["close"] <= pending["threshold"]
            if still:
                pending = dict(pending,closes=pending["closes"]+1)
                if pending["closes"] >= persistence:
                    breakout_event = dict(pending,status="CONFIRMED",confirmed_at=bar["closed_at"],confirmed_index=k)
                    breakouts.append(breakout_event)
                    pending = None
            else:
                pending = None
        if pending is None and breakout_event is None and range_value is not None:
            buffer = tick*range_cfg["breakout_threshold_ticks"]
            direction = "UP" if bar["close"] >= range_value["high"]+buffer else "DOWN" if bar["close"] <= range_value["low"]-buffer else None
            if direction:
                threshold = range_value["high"]+buffer if direction=="UP" else range_value["low"]-buffer
                pending = dict(direction=direction,threshold=threshold,range=range_value,start_index=k,
                               first_close_at=bar["closed_at"],closes=1,status="CANDIDATE")
                if persistence == 1:
                    breakout_event = dict(pending,status="CONFIRMED",confirmed_at=bar["closed_at"],confirmed_index=k)
                    breakouts.append(breakout_event)
                    pending = None
        snapshots.append(dict(index=k,time=bar["time"],known_at=bar["closed_at"],indicators=indicators[k],
            confirmed_pivot_ids=[p["id"] for p in confirmed],active_chain_ids=[p["id"] for p in chain],
            open_extrema=candidates,structure=_structure(chain,tick,policy["structure"]["equal_tolerance_ticks"]),
            latest_swing=_swing(chain[-2],chain[-1],policy["fibonacci"]) if len(chain)>=2 else None,
            range=range_value,breakout_candidate=pending,breakout_confirmed=breakout_event))
    swings = [_swing(a,b,policy["fibonacci"]) for a,b in zip(chain,chain[1:])]
    windows = [[p["id"] for p in chain[i:i+5]] for i in range(max(0,len(chain)-4))]
    return dict(version=VERSION,timeframe=timeframe,mode="SHADOW_ONLY",affects_analysis=False,affects_execution=False,
        price_tick=tick,policy_hash=content_hash(policy),snapshots=snapshots,confirmed_pivots=confirmed,
        active_alternating_pivots=chain,ambiguous_extrema=ambiguous,swings=swings,
        five_pivot_windows=windows,confirmed_breakouts=breakouts,
        missing_features={name:metric_["reason"] for name,metric_ in indicators[-1].items() if metric_["value"] is None} if indicators else {"all":"NO_CLOSED_BARS"})
