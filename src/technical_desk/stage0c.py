"""Offline SHADOW_ONLY data contract. No database, network or model dispatch."""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, localcontext
from statistics import median

from src.technical_desk.evidence_capture import content_hash
from src.technical_desk.packet_data import build_candidate


DATA_VERSION = "technical-desk-data-v3"
VOLUME_BASIS = "RELATIVE_UNVERIFIED"


def _volume(row):
    value = row.get("volume")
    if value is None:
        return None
    number = Decimal(str(value))
    if not number.is_finite() or number < 0:
        raise ValueError("INVALID_VOLUME")
    return number


def relative_volume_features(rows, window):
    """Trailing D median excludes today's volume and all future observations."""
    if type(window) is not int or window < 2:
        raise ValueError("INVALID_RELATIVE_VOLUME_WINDOW")
    values = [_volume(row) for row in rows]
    output = []
    for index, value in enumerate(values):
        previous = values[max(0, index-window):index]
        base = None
        if len(previous) < window:
            state = "WARMUP"
        elif value is None or any(v is None for v in previous):
            state = "MISSING_VOLUME"
        else:
            base = median(previous)
            state = "AVAILABLE" if base > 0 else "ZERO_DENOMINATOR"
        with localcontext() as ctx:
            ctx.prec = 34
            ratio = value / base if state == "AVAILABLE" else None
        output.append(dict(relative_volume=ratio, status=state,
                           window_sessions=window, volume_basis=VOLUME_BASIS,
                           previous_session_dates=[r["session_date"] for r in rows[max(0, index-window):index]]))
    return output


def adjustment_volume_checks(rows, events, *, instrument_id, window, threshold):
    """Retrospective diagnostic only; does not rescale data or create features."""
    if type(window) is not int or window < 2:
        raise ValueError("INVALID_ADJUSTMENT_WINDOW")
    threshold = Decimal(str(threshold))
    if not threshold.is_finite() or threshold <= 1:
        raise ValueError("INVALID_ADJUSTMENT_THRESHOLD")
    checks = []
    for event in events:
        if event["instrument_id"] != instrument_id:
            continue
        check = dict(event=deepcopy(event), volume_basis=VOLUME_BASIS,
                     window_sessions_each_side=window, jump_factor_threshold=threshold,
                     action="REPORT_ONLY_NO_AUTOMATIC_RESCALE", level_jump=None,
                     post_pre_ratio=None, symmetric_factor=None)
        if not rows or event["date"] < rows[0]["session_date"] or event["date"] > rows[-1]["session_date"]:
            check["status"] = "OUTSIDE_PACKET_HISTORY"
        else:
            pre = [r for r in rows if r["session_date"] < event["date"]][-window:]
            post = [r for r in rows if r["session_date"] >= event["date"]][:window]
            values = [_volume(r) for r in pre + post]
            if len(pre) != window or len(post) != window:
                check["status"] = "INSUFFICIENT_WINDOW"
            elif any(v is None for v in values):
                check["status"] = "MISSING_VOLUME"
            else:
                before, after = median(values[:window]), median(values[window:])
                check.update(pre_median=before, post_median=after,
                             pre_dates=[r["session_date"] for r in pre],
                             post_dates=[r["session_date"] for r in post])
                if before <= 0 or after <= 0:
                    check["status"] = "ZERO_MEDIAN_NOT_EVALUABLE"
                else:
                    with localcontext() as ctx:
                        ctx.prec = 34
                        ratio = after / before
                        factor = max(ratio, before / after)
                    check.update(status="EVALUATED", post_pre_ratio=ratio,
                                 symmetric_factor=factor, level_jump=factor >= threshold)
        checks.append(check)
    return checks


def terminal_atr(rows, window=14, precision=34):
    if type(window) is not int or window < 2:
        raise ValueError("INVALID_ATR_WINDOW")
    ranges, prior_close = [], None
    with localcontext() as ctx:
        ctx.prec = precision
        for row in rows:
            high, low, close = (Decimal(str(row[key])) for key in ("high", "low", "close"))
            ranges.append(max(high-low, abs(high-prior_close), abs(low-prior_close))
                          if prior_close is not None else high-low)
            prior_close = close
        if len(ranges) < window:
            return dict(value=None, status="INSUFFICIENT_HISTORY", window_bars=window)
        value = sum(ranges[:window]) / Decimal(window)
        for true_range in ranges[window:]:
            value = (value * (window-1) + true_range) / window
    return dict(value=value if value > 0 else None,
                status="AVAILABLE" if value > 0 else "ZERO_ATR_NOT_EVALUABLE",
                window_bars=window, method="WILDER_SMA_SEED", decimal_precision=precision,
                input_fact_hashes=[row["fact_hash"] for row in rows])


def build_stage0c_packet(capture, calendar, *, original_start, cutoff, source_hash, policy, adjustments):
    if (policy["version"] != "technical-desk-stage0c-policy-v1"
            or policy["operating_mode"] != "SHADOW_ONLY"
            or policy["volume_basis"] != VOLUME_BASIS
            or policy["weekly_absolute_volume"] != "PROHIBITED"
            or policy["relative_volume_denominator"] != "MEDIAN_PREVIOUS_D_SESSIONS_EXCLUDING_CURRENT"
            or policy["atr"]["method"] != "WILDER_SMA_SEED"):
        raise ValueError("UNSUPPORTED_STAGE0C_POLICY")
    ticker = capture["ticker"]
    override = policy["history_start_override"].get(ticker)
    if override is not None and override < original_start:
        raise ValueError("OVERRIDE_CANNOT_EXTEND_UNCAPTURED_HISTORY")
    packet = build_candidate(capture, calendar, start_date=override or original_start,
                             cutoff=cutoff, source_hash=source_hash)
    packet.update(schema=DATA_VERSION, operating_mode="SHADOW_ONLY", affects_analysis=False,
                  affects_execution=False, policy_hash=content_hash(policy),
                  original_history_start=original_start, history_start_override=override,
                  history_start_override_reason=policy["history_start_override_reason"] if override else None,
                  volume_basis=VOLUME_BASIS, volume_quality_status="PARTIAL",
                  evaluation_cohorts=deepcopy(policy["evaluation_cohorts"]))
    features = relative_volume_features(packet["daily"], policy["relative_volume_window_sessions"])
    remap = {}
    for row, feature in zip(packet["daily"], features):
        parent = row.pop("fact_hash")
        row.update(parent_fact_hash=parent, relative_volume_feature=feature, volume_basis=VOLUME_BASIS)
        row["fact_hash"] = content_hash(row)
        remap[parent] = row["fact_hash"]
    for week in packet["weekly"]:
        parent = week.pop("fact_hash")
        week.update(parent_fact_hash=parent, component_hashes=[remap[h] for h in week["component_hashes"]],
                    volume=None, volume_unit=None, volume_status="ABSOLUTE_WEEKLY_VOLUME_PROHIBITED")
        week["fact_hash"] = content_hash(week)
    config = policy["adjustment_check"]
    checks = adjustment_volume_checks(packet["daily"], adjustments["events"],
              instrument_id=packet["instrument_id"], window=config["window_sessions_each_side"],
              threshold=config["jump_factor_threshold"])
    packet["adjustment_check"] = dict(source_hash=content_hash(adjustments),
        coverage_status=adjustments["coverage_status"], checks=checks,
        status="EVENTS_REPORTED" if checks else "NO_KNOWN_EVENTS_NOT_EVIDENCE_OF_NONE",
        interpretation="RETROSPECTIVE_DIAGNOSTIC_NOT_A_CAUSAL_FEATURE",
        policy=deepcopy(config))
    atr = policy["atr"]
    packet["atr_by_timeframe"] = {tf: terminal_atr(packet[name], atr["window_bars"], atr["decimal_precision"])
                                  for tf, name in (("D", "daily"), ("W", "weekly"))}
    quality = packet["quality"]
    accepted = {"VOLUME_UNIT_UNKNOWN", "VOLUME_ADJUSTMENT_UNKNOWN"}
    quality["accepted_limitations"] = sorted(accepted)
    quality["blockers"] = [b for b in quality["blockers"] if b not in accepted]
    available = sum(row["status"] == "AVAILABLE" for row in features)
    invalid = sum(row["status"] not in {"AVAILABLE", "WARMUP"} for row in features)
    if not available or invalid or features[-1]["status"] != "AVAILABLE":
        quality["blockers"].append("RELATIVE_VOLUME_INCOMPLETE")
    if not quality["price_fresh"]:
        quality["blockers"].append("PRICE_NOT_FRESH_AT_FROZEN_CUTOFF")
    quality.update(status="PARTIAL" if quality["blockers"] else "READY",
                   ready=not quality["blockers"], ready_scope="SHADOW_ONLY_RELATIVE_VOLUME",
                   volume_basis=VOLUME_BASIS, volume_quality_status="PARTIAL",
                   relative_volume_available=available, relative_volume_warmup=len(features)-available-invalid,
                   relative_volume_invalid=invalid)
    return packet


def role_input(packet, role):
    """Allowlisted role view: raw absolute volume never reaches Wyckoff or peers."""
    if role not in {"elliott", "wyckoff", "structure", "momentum", "moderator"}:
        raise ValueError("UNKNOWN_ROLE")
    daily = []
    for row in packet["daily"]:
        item = {key: row[key] for key in ("session_date", "open", "high", "low", "close", "fact_hash")}
        if role == "wyckoff":
            item["relative_volume_feature"] = deepcopy(row["relative_volume_feature"])
        daily.append(item)
    weekly = [{key: row[key] for key in ("monday", "last_session", "open", "high", "low", "close", "fact_hash")}
              for row in packet["weekly"]]
    return dict(role=role, instrument_id=packet["instrument_id"], operating_mode="SHADOW_ONLY",
                volume_basis=VOLUME_BASIS if role == "wyckoff" else "NOT_USED",
                evaluation_cohort="WYCKOFF_RELATIVE_UNVERIFIED" if role == "wyckoff" else "OTHER_ROLES",
                weekly_volume_policy="NO_ABSOLUTE_AGGREGATION_D_RELATIVE_CONTEXT_ONLY",
                daily=daily, weekly=weekly)
