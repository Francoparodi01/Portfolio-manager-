"""Build inspectable D/W candidate packets without upgrading unknown source facts."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import re
from zoneinfo import ZoneInfo

from src.technical_desk.evidence_capture import content_hash


DATA_VERSION = "technical-desk-data-v1"


def covered(day, calendar):
    return any(item["start"] <= day <= item["end"] for item in calendar["verified_ranges"])


def expected_dates(start, end, calendar):
    closures = {item["date"] for item in calendar["closures"] if item.get("trading") is False}
    special = {item["date"]: item["trading"] for item in calendar.get("special_sessions", [])}
    cursor, finish = date.fromisoformat(start), date.fromisoformat(end)
    expected, unknown = [], []
    while cursor <= finish:
        label = cursor.isoformat()
        if cursor.weekday() < 5:
            if not covered(label, calendar):
                unknown.append(label)
            elif label not in closures and special.get(label, True):
                expected.append(label)
        cursor += timedelta(days=1)
    return expected, unknown


def last_bar_close_proof(bar, capture, calendar, cutoff):
    """Use a documented regular session plus the exchange's later close guard.

    The guard conservatively waits through BYMA's closing-price phase and the
    source's reported delay. It does not relabel the requested regular series
    as an extended-session series or claim an explicit provider final flag.
    """
    meta = capture["source_metadata"]
    zone = ZoneInfo(calendar["timezone"])
    session_day = datetime.fromisoformat(bar["bar_start"]).astimezone(zone).date()
    label = session_day.isoformat()
    rules = [r for r in calendar.get("hours_rules", [])
             if r["effective_from"] <= label <= r["verified_through"]]
    if not rules or not covered(label, calendar):
        return None
    provider_session = meta.get("session", "")
    boundaries = re.findall(r"#(\d{8})/", provider_session)
    current_hours = provider_session.rsplit("/", 1)[-1]
    if not re.fullmatch(r"\d{4}-\d{4}", current_hours):
        return None
    if boundaries and label < datetime.strptime(boundaries[-1], "%Y%m%d").date().isoformat():
        return None  # historical schedules require their own verified adapter
    rule = max(rules, key=lambda r: r["effective_from"])
    provider_close = current_hours.split("-")[1]
    if provider_close != rule["regular_close_local"].replace(":", ""):
        return None
    if meta.get("timezone") != calendar["timezone"] or meta.get("subsession_id") != "regular":
        return None
    delay = meta.get("delay")
    if type(delay) is not int or not 0 <= delay <= 86400:
        return None
    end = time.fromisoformat(rule["closing_price_phase_end_local"])
    safe_after = datetime.combine(session_day, end, zone) + timedelta(seconds=delay)
    observed = datetime.fromisoformat(capture["available_at"])
    if observed < safe_after or cutoff < safe_after:
        return None
    expected, _ = expected_dates(label, label, calendar)
    if label not in expected:
        return None
    return dict(rule="DOCUMENTED_REGULAR_SESSION_PLUS_EXCHANGE_CLOSE_AND_SOURCE_DELAY",
                regular_close_local=rule["regular_close_local"],
                exchange_guard_local=rule["closing_price_phase_end_local"],
                source_delay_seconds=delay, safe_after=safe_after.astimezone(timezone.utc).isoformat(),
                observed_after_guard=True, explicit_provider_final_flag=False,
                includes_postmarket=False)


def build_candidate(capture, calendar, *, start_date, cutoff, source_hash):
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("AWARE_CUTOFF_REQUIRED")
    if content_hash(capture) != source_hash or not capture.get("download_complete"):
        raise ValueError("SOURCE_CAPTURE_HASH_OR_COMPLETION_INVALID")
    if datetime.fromisoformat(capture["available_at"]) > cutoff:
        raise ValueError("SOURCE_AFTER_CUTOFF")
    if datetime.fromisoformat(calendar["captured_at"]) > cutoff:
        raise ValueError("CALENDAR_AFTER_CUTOFF")
    ticker, meta = capture["ticker"], capture["source_metadata"]
    if (capture["requested_symbol"] != f"BYMA:{ticker}" or meta.get("name") != ticker
            or meta.get("exchange") != "BYMA" or meta.get("currency_code") != "ARS"
            or meta.get("type") != "dr" or capture.get("requested_session") != "regular"
            or capture.get("requested_adjustment") != "splits" or capture.get("interval") != "1D"):
        raise ValueError("SOURCE_INSTRUMENT_OR_CONTRACT_MISMATCH")
    zone = ZoneInfo(calendar["timezone"])
    calendar_hash = content_hash(calendar)
    last_expected = calendar["last_expected_complete_session"]
    local_cutoff_day = cutoff.astimezone(zone).date().isoformat()
    if last_expected > local_cutoff_day:
        raise ValueError("CALENDAR_EXPECTED_SESSION_AFTER_CUTOFF")
    following_day = (date.fromisoformat(last_expected) + timedelta(days=1)).isoformat()
    later_sessions, calendar_uncovered_after_last = expected_dates(following_day, local_cutoff_day, calendar)
    expected, unknown_expected = expected_dates(start_date, last_expected, calendar)
    daily, excluded = [], []
    seen = set()
    for raw in capture["bars"]:
        if datetime.fromisoformat(raw["bar_start"]) > datetime.fromisoformat(capture["available_at"]):
            raise ValueError("SOURCE_BAR_AFTER_AVAILABILITY")
        label = datetime.fromisoformat(raw["bar_start"]).astimezone(zone).date().isoformat()
        if label < start_date:
            continue
        if label in seen:
            raise ValueError("DUPLICATE_SESSION_LABEL")
        seen.add(label)
        if label > last_expected:
            excluded.append(dict(session_date=label, reason="AFTER_EXPECTED_LAST_COMPLETE_SESSION"))
            continue
        row = dict(raw)
        close_proof = None
        if raw["close_status"] == "CONFIRMED_BY_NEXT_PROVIDER_BAR":
            if datetime.fromisoformat(raw["close_confirmed_at"]) <= datetime.fromisoformat(capture["available_at"]):
                close_proof = dict(rule="NEXT_PROVIDER_BAR", confirmed_at=raw["close_confirmed_at"])
        else:
            close_proof = last_bar_close_proof(raw, capture, calendar, cutoff)
        if close_proof is None:
            excluded.append(dict(session_date=label, reason="BAR_CLOSE_UNCONFIRMED"))
            continue
        row.update(session_date=label, instrument_id=f"BYMA:CEDEAR:{ticker}:ARS",
                   available_at=capture["available_at"], volume_unit=None,
                   volume_unit_status="UNKNOWN", volume_adjustment_status="UNKNOWN",
                   calendar_status="VERIFIED_CURRENT_SOURCE" if covered(label, calendar) else "UNKNOWN",
                   calendar_hash=calendar_hash, source_capture_hash=source_hash,
                   close_proof=close_proof)
        row["fact_hash"] = content_hash(row)
        daily.append(row)
    daily.sort(key=lambda r: r["session_date"])
    observed_dates = {r["session_date"] for r in daily}
    by_week = defaultdict(list)
    for row in daily:
        parsed = date.fromisoformat(row["session_date"])
        by_week[(parsed - timedelta(days=parsed.weekday())).isoformat()].append(row)
    weekly, excluded_weeks = [], []
    for monday, rows in sorted(by_week.items()):
        friday = (date.fromisoformat(monday) + timedelta(days=4)).isoformat()
        sessions, unknown = expected_dates(monday, friday, calendar)
        if monday < start_date or friday > last_expected or unknown or {r["session_date"] for r in rows} != set(sessions):
            excluded_weeks.append(dict(monday=monday, reason="WEEK_PARTIAL_OR_CALENDAR_UNKNOWN"))
            continue
        week = dict(monday=monday, last_session=rows[-1]["session_date"],
                    open=rows[0]["open"], high=max(Decimal(str(r["high"])) for r in rows),
                    low=min(Decimal(str(r["low"])) for r in rows), close=rows[-1]["close"],
                    volume=None, volume_unit=None, volume_status="UNIT_UNKNOWN_NOT_AGGREGATED",
                    available_at=max(r["available_at"] for r in rows),
                    component_hashes=[r["fact_hash"] for r in rows])
        week["fact_hash"] = content_hash(week)
        weekly.append(week)
    missing = sorted(set(expected) - observed_dates)
    unexpected = sorted(d for d in observed_dates if covered(d, calendar) and d not in expected)
    unknown_rows = [r["session_date"] for r in daily if r["calendar_status"] == "UNKNOWN"]
    blockers = ["VOLUME_UNIT_UNKNOWN", "VOLUME_ADJUSTMENT_UNKNOWN"]
    if missing:
        blockers.append("MISSING_CLOSED_SESSIONS")
    if unexpected:
        blockers.append("UNEXPECTED_SESSION_LABELS")
    if unknown_expected or unknown_rows:
        blockers.append("CALENDAR_COVERAGE_INCOMPLETE")
    if calendar_uncovered_after_last:
        blockers.append("CALENDAR_NOT_COVERING_CURRENT_CUTOFF")
    if later_sessions:
        # A frozen last-complete-session label cannot certify a later session.
        # Require a refreshed calendar audit, including its intraday close rule.
        blockers.append("CALENDAR_LAST_COMPLETE_SESSION_NOT_REVALIDATED")
    if not weekly:
        blockers.append("NO_COMPLETE_WEEKLY_BARS")
    packet = dict(schema=DATA_VERSION, ticker=ticker, instrument_id=f"BYMA:CEDEAR:{ticker}:ARS",
        cutoff=cutoff.isoformat(), history_start=start_date, source_capture_hash=source_hash,
        calendar_hash=calendar_hash, requested_session="regular", requested_price_adjustment="splits",
        original_vintage=False, daily=daily, weekly=weekly, excluded=excluded, excluded_weeks=excluded_weeks,
        quality=dict(status="PARTIAL", ready=False, blockers=blockers,
                     latest_closed_date=daily[-1]["session_date"] if daily else None,
                     expected_last_session=last_expected, missing_sessions=missing,
                     unexpected_sessions=unexpected, calendar_unknown_observations=len(unknown_rows),
                     calendar_unknown_expected_weekdays=len(unknown_expected),
                     price_fresh=not missing and not calendar_uncovered_after_last and not later_sessions and bool(daily) and daily[-1]["session_date"] == last_expected,
                     volume_unit_status="UNKNOWN", daily_rows=len(daily), weekly_rows=len(weekly)))
    return packet
