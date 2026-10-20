"""Independent BYMADATA evidence. No price substitution or trading decisions."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp

from src.core.market_calendar import is_trading_day

ART = ZoneInfo("America/Argentina/Buenos_Aires")
ROOT = Path(__file__).resolve().parents[2]
BASE = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/"
HISTORY = BASE + "bnown/seriesHistoricas/especies"
POLICY = ROOT / "config/radar_market_policy.json"


def aware(value):
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Timezone required")
    return stamp


def number(value):
    if value is None or isinstance(value, bool):
        raise ValueError("Missing numeric evidence")
    try:
        n = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid numeric evidence") from exc
    if not n.is_finite():
        raise ValueError("Nonfinite numeric evidence")
    return n


def session_end(session: date):
    policy = json.loads(POLICY.read_text(encoding="utf8"))
    if not policy["valid_from"] <= session.isoformat() <= policy["valid_through"]:
        raise ValueError("Calendar not verified for requested date")
    if not is_trading_day(session):
        raise ValueError("Not a BYMA trading day")
    rule = next(r for r in policy["sessions"] if r["from"] <= session.isoformat() <= r["through"])
    return datetime.combine(session, time.fromisoformat(rule["end"]), ART)


def eligible_session(now: datetime):
    """Only today's session, after the official close plus both feed delays."""
    local = aware(now).astimezone(ART)
    try:
        end = session_end(local.date())
    except ValueError:
        return None
    return local.date() if local >= end + timedelta(minutes=30) else None


def latest_due_session(now: datetime):
    """Also used by readers: a missed daily run makes the previous run stale."""
    local = aware(now).astimezone(ART)
    policy = json.loads(POLICY.read_text(encoding="utf8"))
    if not policy["valid_from"] <= local.date().isoformat() <= policy["valid_through"]:
        return None
    for offset in range(15):
        day = local.date() - timedelta(days=offset)
        try:
            end = session_end(day)
        except ValueError:
            continue
        if local >= end + timedelta(minutes=30):
            return day
    return None


def validate_candles(payload, target_session=None):
    rows = payload["candles"]
    meta = payload["evidence"]["symbol_metadata"]
    if not rows or meta.get("exchange") != "BYMA" or meta.get("currency_code") != "ARS":
        raise ValueError("Unverified market/currency")
    days = []
    for row in rows:
        if (row.get("ticker") != meta.get("name") or row.get("currency") != "ARS"
                or row.get("venue") != "BYMA" or row.get("interval") != "1d"):
            raise ValueError("Mixed candle identity")
        days.append(aware(row["ts"]).astimezone(ART).date())
        o, h, l, c = (number(row[k]) for k in ("open_price", "high_price", "low_price", "close_price"))
        if min(o, h, l, c) <= 0 or l > min(o, c) or h < max(o, c) or l > h:
            raise ValueError("Invalid OHLC geometry")
        if number(row["volume"]) < 0:
            raise ValueError("Negative volume")
    if days != sorted(set(days)):
        raise ValueError("Duplicate or unordered daily candles")
    if target_session and days[-1] != target_session:
        raise ValueError("Latest candle is not the requested session")
    return days[-1]


def assess_close(payload, envelope, session: date, cutoff):
    """An undated live quote alone can never corroborate a historical close."""
    result = dict(independent_close=None, confirmed_close=None, volume_comparison="UNKNOWN",
                  close_comparison="UNKNOWN", settlement_verified=False,
                  independent_source="BYMADATA", missing_data=[])
    missing = result["missing_data"]
    try:
        end = session_end(session)
        tv_received = aware(payload["evidence"]["received_at"])
        delay = number(payload["evidence"]["symbol_metadata"].get("delay"))
        if delay < 0 or tv_received < end + timedelta(seconds=float(delay)):
            raise ValueError("Primary feed still provisional")
        if tv_received > aware(cutoff):
            raise ValueError("Future primary retrieval")
        if not envelope or envelope.get("source") != "BYMADATA":
            raise ValueError("Independent source unavailable")
        received = aware(envelope["received_at"])
        if received < end + timedelta(minutes=20) or received > aware(cutoff):
            raise ValueError("Independent feed time invalid")
        request = envelope["request"]
        if envelope.get("raw_response") is not None:
            raw = envelope["raw_response"].encode("utf8")
            if (hashlib.sha256(raw).hexdigest() != envelope["response_sha256"]
                    or json.loads(raw) != envelope["response"]):
                raise ValueError("Independent response checksum mismatch")
        ticker = payload["evidence"]["symbol_metadata"]["name"]
        if (envelope["url"] != HISTORY or request["symbol"] != ticker
                or request["fromDate"] != session.isoformat() or request["toDate"] != session.isoformat()
                or request["maturity"] != "2" or envelope.get("status") != 200):
            raise ValueError("Independent request identity invalid")
        rows = envelope["response"].get("data", [])
        if len(rows) != 1 or rows[0].get("fecha", "").split(" ")[0] != session.isoformat():
            raise ValueError("Missing or ambiguous dated official close")
        row, quote = rows[0], envelope.get("instrument", {})
        if (quote.get("symbol") != ticker or quote.get("market") != "BYMA"
                or quote.get("denominationCcy") != "ARS" or quote.get("settlementType") != "2"):
            raise ValueError("Independent currency/settlement unknown")
        expected_type = "CD" if payload["candles"][-1]["asset_type"] == "CEDEAR" else "CS"
        if quote.get("securityType") != expected_type:
            raise ValueError("Independent instrument type mismatch")
        close, volume = number(row["precioCierre"]), number(row["volumenNominal"])
        if close <= 0 or volume < 0:
            raise ValueError("Invalid official close/volume")
        last = payload["candles"][-1]
        result["independent_close"] = dict(price=float(close), session=session.isoformat(),
            source="BYMADATA", url=HISTORY, retrieved_at=received.isoformat(),
            currency="ARS", settlement="T+1", volume=float(volume), volume_unit="nominales",
            field="precioCierre", response_sha256=envelope["response_sha256"])
        result["close_comparison"] = "MATCH" if close == number(last["close_price"]) else "DIFFERENT"
        result["volume_comparison"] = "MATCH" if volume == number(last["volume"]) else "DIFFERENT"
        # Identical prices/volumes do NOT establish a settlement identity.
        result["settlement_verified"] = payload["evidence"]["symbol_metadata"].get("settlement_type") == "2"
        if not result["settlement_verified"]:
            missing.append("plazo de liquidación de la serie TradingView no declarado")
        if result["close_comparison"] != "MATCH":
            missing.append("precio final de vela distinto del cierre BYMADATA T+1")
        if result["volume_comparison"] != "MATCH":
            missing.append("volumen distinto de BYMADATA T+1")
        if result["close_comparison"] == "MATCH" and result["settlement_verified"]:
            result["confirmed_close"] = dict(result["independent_close"], confirmation="BYMADATA_TV_SAME_IDENTITY")
    except (ValueError, KeyError, TypeError) as exc:
        missing.append(str(exc))
    return result


async def collect_close_bundle(output: Path, tickers: list[str], session: date):
    """Public read endpoints only; preserve dated responses and failed attempts."""
    output.mkdir(parents=True, exist_ok=False)
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "Quantia market evidence/1.0", "Referer": "https://open.bymadata.com.ar/",
               "Options": "renta-variable"}
    quotes = {}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as client:
        for endpoint in ("cedears", "leading-equity", "general-equity"):
            async with client.post(BASE + endpoint, json={"excludeZeroPxAndQty": False, "T0": False,
                                                         "T1": True, "T2": False}) as response:
                raw = await response.text()
                data = json.loads(raw) if response.status == 200 else {}
                (output / f"_instruments-{endpoint}.json").write_text(json.dumps(dict(
                    url=BASE+endpoint, received_at=datetime.now(timezone.utc).isoformat(),
                    status=response.status, response=data)), encoding="utf8")
                for row in data if isinstance(data, list) else data.get("data", []):
                    key = row.get("symbol")
                    if row.get("settlementType") == "2" and row.get("denominationCcy") == "ARS":
                        if key in quotes and quotes[key] != row:
                            quotes[key] = {}  # ambiguous identity, fail closed
                        else:
                            quotes[key] = row
        semaphore = asyncio.Semaphore(3)

        async def one(ticker):
            request = dict(symbol=ticker, maturity="2", fromDate=session.isoformat(),
                           toDate=session.isoformat(), homogeneous="1", period="1")
            item = dict(source="BYMADATA", url=HISTORY, request=request, instrument=quotes.get(ticker, {}))
            async with semaphore:
                try:
                    async with client.post(HISTORY, json=request) as response:
                        raw = await response.read()
                        item.update(status=response.status, response_sha256=hashlib.sha256(raw).hexdigest(),
                                    raw_response=raw.decode("utf8"), response=json.loads(raw))
                except Exception as exc:
                    item.update(status=None, response={}, error=type(exc).__name__)
                item["received_at"] = datetime.now(timezone.utc).isoformat()
                (output / f"{ticker}.json").write_text(json.dumps(item, ensure_ascii=False), encoding="utf8")
                await asyncio.sleep(.2)
        await asyncio.gather(*(one(t) for t in sorted(set(tickers))))
