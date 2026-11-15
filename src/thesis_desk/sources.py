"""Bounded public-source retrieval plus SELECT-only Quantia market snapshots."""
from __future__ import annotations

import asyncio
import csv
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
import io
import json
import re
import subprocess
from urllib.parse import urljoin
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import httpx

from src.backtesting.thesis_desk import ART, decimal, instant
from src.core.market_calendar import is_trading_day
from src.thesis_desk.contracts import check_url, digest, dump, normalized, now

IVV_URL = "https://www.ishares.com/us/products/239726/ishares-core-s-p-500-etf/latest-holdings.csv"
VIX_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?range=1mo&interval=1d"
EMPLOYMENT_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
NY = ZoneInfo("America/New_York")


class SourceUnavailable(RuntimeError):
    pass


class Collector:
    def __init__(self, directory, client, *, max_bytes=4 * 1024 * 1024):
        self.directory, self.client = directory, client
        self.max_bytes = max_bytes
        self.receipts = []
        (directory / "raw").mkdir(parents=True, exist_ok=True)

    async def fetch(self, url, allowed_hosts):
        original = url
        for _ in range(4):
            check_url(url, allowed_hosts)
            async with self.client.stream("GET", url, headers={"User-Agent": "Mozilla/5.0 QuantiaPaperResearch/1.0"}) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    url = urljoin(url, response.headers.get("location", ""))
                    continue
                if response.status_code != 200:
                    raise SourceUnavailable(f"HTTP_{response.status_code}")
                chunks, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise SourceUnavailable("SOURCE_SIZE_LIMIT")
                    chunks.append(chunk)
            body = b"".join(chunks)
            fetched = now()
            identity = digest(body)
            path = self.directory / "raw" / (identity + ".bin")
            if not path.exists():
                path.write_bytes(body)
            receipt = dict(requested_url=original, source_url=url, fetched_at=fetched.isoformat(), sha256=identity,
                           bytes=len(body), raw_path=str(path.resolve()))
            self.receipts.append(receipt)
            return body, receipt
        raise SourceUnavailable("REDIRECT_LIMIT")


def ivv_members(content, instruments, receipt):
    rows = list(csv.reader(io.StringIO(content.decode("utf-8-sig"))))
    dates = [r[1] for r in rows if len(r) > 1 and r[0].strip() == "Fund Holdings as of"]
    header = next((i for i, r in enumerate(rows) if r and r[0] == "Ticker"), None)
    if len(dates) != 1 or header is None:
        raise ValueError("IVV_SCHEMA_OR_DATE_UNAVAILABLE")
    day = datetime.strptime(dates[0], "%b %d, %Y").date()
    fetched = instant(receipt["fetched_at"])
    asof = datetime.combine(day, time.min, NY)
    if not timedelta(0) <= fetched - asof <= timedelta(days=14):
        raise ValueError("IVV_MEMBERSHIP_STALE_OR_FUTURE")
    holdings = {r["Ticker"]: r for r in csv.DictReader(io.StringIO("\n".join(",".join('"' + c.replace('"', '""') + '"' for c in row) for row in rows[header:])))}
    result = []
    for instrument in instruments:
        holding = holdings.get(instrument["underlying"])
        if not holding or holding.get("Asset Class") != "Equity":
            continue
        result.append(dict(id=digest([instrument["ticker"], receipt["sha256"]]), ticker=instrument["ticker"],
                           asset_type=instrument["asset_type"], currency="ARS", underlying_kind="COMMON_STOCK", sleeve="CORE",
                           membership_source="IVV", source_url=receipt["source_url"], source_hash=receipt["sha256"],
                           asof_at=asof.isoformat(), available_at=fetched.isoformat(), knowledge_basis="ARCHIVED_AT_TIME"))
    return result


def document_text(body):
    soup = BeautifulSoup(body, "html.parser")
    for tag in soup.find_all(["script", "style", "nav", "footer", "header", "form"]):
        tag.decompose()
    article = soup.select_one(".article-body, .news-content, .body-copy, .module_news-detail, article, main") or soup
    return normalized(article.get_text(" ", strip=True))


def document_date(body):
    soup = BeautifulSoup(body, "html.parser")
    for selector in ('meta[property="article:published_time"]', 'meta[name="date"]', 'time[datetime]'):
        tag = soup.select_one(selector)
        if tag:
            value = tag.get("content") or tag.get("datetime")
            try:
                return instant(value).isoformat(), "TIMESTAMP"
            except ValueError:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
                    return datetime.combine(datetime.fromisoformat(value).date(), time.min, NY).isoformat(), "DAY_START_BOUND_FOR_AGE"
    match = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(20\d{2})\b", normalized(soup.get_text(" ", strip=True)))
    if not match:
        raise ValueError("OFFICIAL_PUBLICATION_DATE_UNKNOWN")
    day = datetime.strptime(match.group(), "%B %d, %Y").date()
    return datetime.combine(day, time.min, NY).isoformat(), "DAY_START_BOUND_FOR_AGE"


async def issuer_documents(collector, instrument, config):
    body, receipt = await collector.fetch(instrument["source_url"], instrument["allowed_hosts"])
    cutoff = instant(receipt["fetched_at"]) - timedelta(days=config["lookback_days"])
    links = []
    if instrument["source_kind"] == "rss":
        if b"<!DOCTYPE" in body.upper() or b"<!ENTITY" in body.upper():
            raise ValueError("UNSAFE_XML")
        for item in ET.fromstring(body).findall(".//item"):
            url, title, date_text = item.findtext("link"), item.findtext("title"), item.findtext("pubDate")
            if not all((url, title, date_text)):
                continue
            try:
                check_url(url, instrument["allowed_hosts"])
                published = parsedate_to_datetime(date_text)
                instant(published)
            except ValueError:
                continue
            if cutoff <= published <= instant(receipt["fetched_at"]):
                links.append((url, title, published.isoformat(), "TIMESTAMP"))
    else:
        soup = BeautifulSoup(body, "html.parser")
        for link in soup.find_all("a", href=True):
            url = urljoin(instrument["source_url"], link["href"])
            if "/news/press-release/" in url:
                check_url(url, instrument["allowed_hosts"])
                links.append((url, normalized(link.get_text(" ", strip=True)), None, None))
    links = list({row[0]: row for row in links}.values())
    documents, errors = [], []
    for url, title, published, precision in links[:config["max_documents_per_issuer"]]:
        try:
            body, detail = await collector.fetch(url, instrument["allowed_hosts"])
            if published is None:
                published, precision = document_date(body)
            if not cutoff <= instant(published) <= instant(detail["fetched_at"]):
                continue
            text = document_text(body)
            if len(text) < 200:
                raise ValueError("EMPTY_OFFICIAL_DOCUMENT")
            # Ignore page nonces/navigation changes while retaining the exact raw artifact hash.
            documents.append(dict(id=digest([instrument["ticker"], url, published, text]), ticker=instrument["ticker"], title=title,
                                  source_url=url, published_at=published, publication_precision=precision,
                                  available_at=detail["fetched_at"], sha256=detail["sha256"], text=text))
        except (httpx.HTTPError, ValueError, SourceUnavailable) as exc:
            errors.append(dict(url=url, error_type=type(exc).__name__))
    return documents, dict(ticker=instrument["ticker"], status="PARTIAL" if errors else "CAPTURED",
                           documents=len(documents), discovered=len(links), truncated=len(links) > config["max_documents_per_issuer"],
                           errors=errors, completeness="BOUNDED_OFFICIAL_FEED_NOT_EXHAUSTIVE_IR_OR_SEC")


def employment_events(content, receipt):
    text = content.decode("utf-8-sig").replace("\r\n ", "").replace("\n ", "")
    rows = []
    for block in text.split("BEGIN:VEVENT")[1:]:
        block = block.split("END:VEVENT", 1)[0]
        if not re.search(r"SUMMARY:.*Employment Situation", block, re.I):
            continue
        match = re.search(r"DTSTART(?:;TZID=([^:\r\n]+))?:(\d{8}T\d{6})(Z?)", block)
        if not match:
            continue
        # BLS publishes this custom VTIMEZONE identifier for US daylight/standard time.
        zone = {"US-Eastern": "America/New_York"}.get(match[1], match[1] or "America/New_York")
        stamp = datetime.strptime(match[2], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc if match[3] else ZoneInfo(zone))
        rows.append(dict(id=digest(["employment", stamp.isoformat()]), ticker="*", start_at=(stamp - timedelta(hours=24)).isoformat(),
                         end_at=(stamp + timedelta(hours=1)).isoformat(), event_at=stamp.isoformat(), source_url=receipt["source_url"],
                         available_at=receipt["fetched_at"], knowledge_basis="ARCHIVED_AT_TIME"))
    if not any(instant(receipt["fetched_at"]) <= instant(r["event_at"]) <= instant(receipt["fetched_at"]) + timedelta(days=45) for r in rows):
        raise ValueError("EMPLOYMENT_CALENDAR_COVERAGE_UNKNOWN")
    return rows


def vix_signal(content, receipt, config):
    data = json.loads(content, parse_float=Decimal)["chart"]["result"][0]
    received = instant(receipt["fetched_at"])
    points = [(datetime.fromtimestamp(ts, timezone.utc), decimal(price)) for ts, price in
              zip(data["timestamp"], data["indicators"]["quote"][0]["close"]) if price is not None
              and datetime.fromtimestamp(ts, timezone.utc).astimezone(NY).date() < received.astimezone(NY).date()]
    stamp, value = max(points, key=lambda p: p[0])
    if received - stamp > timedelta(days=5) or value <= 0:
        raise ValueError("VIX_STALE_OR_INVALID")
    state = "RED" if value >= decimal(config["vix_red"]) else "YELLOW" if value >= decimal(config["vix_yellow"]) else "GREEN"
    return dict(id=digest([receipt["sha256"], state]), state=state, available_at=received.isoformat(),
                valid_until=(received + timedelta(hours=24)).isoformat(), source_url=receipt["source_url"],
                knowledge_basis="ARCHIVED_AT_TIME", vix=str(value), observation_at=stamp.isoformat(),
                rule="PREVIOUS_COMPLETED_SESSION_VIX_FIXED_THRESHOLDS")


def capture_market(config, container, user, database, directory):
    symbols = ",".join("'" + i["ticker"] + "'" for i in config["instruments"])
    # Input symbols are validated by validate_config before this function is called.
    if any(not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", i["ticker"]) for i in config["instruments"]):
        raise ValueError("Invalid market-query instrument")
    queries = {
        "identities": f"SELECT ticker,asset_type,currency,source_ticker FROM issuer_instruments WHERE is_active AND ticker IN ({symbols})",
        "quotes": f"""SELECT DISTINCT ON (ticker,asset_type,currency) ticker,asset_type,currency,ts,last_price
            FROM market_prices WHERE ticker IN ({symbols}) AND currency='ARS' AND ts<=now()
            ORDER BY ticker,asset_type,currency,ts DESC""",
        "conflicts": f"""SELECT ticker,ts,asset_type,currency FROM market_prices
            WHERE ticker IN ({symbols}) AND currency='ARS' AND ts>=now()-interval '180 days' AND ts<=now()
            GROUP BY ticker,ts,asset_type,currency HAVING count(DISTINCT last_price)>1 LIMIT 20""",
        "closes": f"""SELECT DISTINCT ON (ticker,asset_type,currency,(ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date)
            ticker,asset_type,currency,ts,last_price FROM market_prices WHERE ticker IN ({symbols}) AND currency='ARS'
            AND ts >= now()-interval '180 days' AND ts < date_trunc('day',now() AT TIME ZONE 'America/Argentina/Buenos_Aires') AT TIME ZONE 'America/Argentina/Buenos_Aires'
            AND (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= '16:30'
            AND (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < '17:00'
            ORDER BY ticker,asset_type,currency,(ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,ts DESC""",
        "earnings": f"""SELECT ticker,event_date,source_url,created_at,updated_at,source_published_at FROM issuer_event_observations
            WHERE ticker IN ({symbols}) AND event_type='EARNINGS' AND lifecycle_status IN ('ANNOUNCED','CONFIRMED')
            AND event_date>=CURRENT_DATE AND event_date<=CURRENT_DATE+120 AND updated_at>=now()-interval '7 days'
            ORDER BY ticker,event_date,id""",
        "corporate_effects": f"""SELECT i.id,i.ticker,i.asset_type,i.currency,i.quantity_factor,i.price_factor,i.cost_basis_factor,i.is_active,
            e.lifecycle_status,e.effective_at,e.created_at AS event_created_at,e.updated_at AS event_updated_at,
            i.created_at AS effect_created_at,i.updated_at AS effect_updated_at FROM corporate_events e
            JOIN corporate_event_instrument_effects i ON i.event_id=e.id WHERE i.ticker IN ({symbols})
            AND e.effective_at>=now()-interval '180 days' AND e.effective_at<=now() ORDER BY e.effective_at,i.id""",
    }
    sql = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout='30000'; SET LOCAL timezone='UTC';\n"
    sql += "\n".join("SELECT jsonb_build_object('name','" + name + "','rows',COALESCE(jsonb_agg(q),'[]'::jsonb)) FROM (" + query + ") q;" for name, query in queries.items())
    sql += "\nCOMMIT;\n"
    (directory / "market.sql").write_text(sql, encoding="utf-8")
    command = ["docker", "exec", "-i", container, "psql", "-U", user, "-d", database, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"]
    result = subprocess.run(command, input=sql.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
    if result.returncode:
        raise SourceUnavailable("READ_ONLY_MARKET_CAPTURE_FAILED")
    rows = [json.loads(line, parse_float=Decimal) for line in result.stdout.decode("utf-8").splitlines() if line.strip()]
    if {r["name"] for r in rows} != set(queries):
        raise ValueError("Incomplete market snapshot")
    payload = dict(observations=rows, captured_at=now().isoformat(), transaction_read_only=True)
    dump(directory / "market.json", payload)
    return payload


def normalize_market(payload, instruments):
    observed = instant(payload["captured_at"])
    raw = {r["name"]: r["rows"] for r in payload["observations"]}
    if raw.get("conflicts"):
        raise ValueError("CONFLICTING_RECORDED_QUOTES")
    identities = {(r["ticker"], r["asset_type"], r["currency"], r["source_ticker"]) for r in raw["identities"]}
    for i in instruments:
        expected = (i["ticker"], i["asset_type"], i["currency"], i["underlying"])
        matches = {r for r in identities if r[0] == i["ticker"]}
        if matches != {expected} and i.get("status") != "IDENTIFIED":
            if i.get("status") in {"IDENTITY_UNKNOWN", "IDENTITY_CONFLICT", "EXCLUDED_ETF"}:
                continue
            raise ValueError("Instrument mapping missing or conflicting: " + i["ticker"])
    allowed = {(i["ticker"], i["asset_type"], i["currency"]) for i in instruments}
    if any((r["ticker"], r["asset_type"], r["currency"]) not in allowed for r in raw["quotes"]):
        raise ValueError("Market quote identity conflicts with configured instrument")
    bars, quotes, blackouts = [], [], []
    ambiguous = {i["ticker"] for i in instruments if i.get("status") == "IDENTITY_CONFLICT"}
    for row in raw["closes"]:
        if row["ticker"] in ambiguous:
            continue
        stamp = instant(row["ts"])
        if not is_trading_day(stamp.astimezone(ART).date()) or decimal(row["last_price"]) <= 0:
            continue
        bars.append(dict(ticker=row["ticker"], asset_type=row["asset_type"], currency=row["currency"],
                         ts=row["ts"], bar_end=row["ts"], available_at=observed.isoformat(), scraped_at=observed.isoformat(),
                         is_closed=True, interval="1d", adjustment_policy="RAW", close_price=str(row["last_price"]),
                         source="DESK_RECORDED_LATE_SESSION_QUOTE", basis="LAST_RECORDED_QUOTE_16_30_TO_17_NOT_OFFICIAL_CLOSE"))
    for row in raw["quotes"]:
        if row["ticker"] in ambiguous:
            continue
        stamp = instant(row["ts"])
        if stamp > observed or decimal(row["last_price"]) <= 0:
            raise ValueError("Invalid recorded market quote")
        quotes.append(dict(row, ts=observed.isoformat(), market_ts=stamp.isoformat(), quote_id=digest(row),
                           fresh=observed - stamp <= timedelta(minutes=5)))
    for row in raw["earnings"]:
        day = datetime.fromisoformat(row["event_date"]).date()
        event = datetime.combine(day, time.min, ART)
        blackouts.append(dict(id=digest([row["ticker"], row["event_date"], row["source_url"]]), ticker=row["ticker"],
                              start_at=(event - timedelta(days=1)).isoformat(), end_at=(event + timedelta(days=1)).isoformat(),
                              available_at=observed.isoformat(), source_url=row["source_url"], knowledge_basis="ARCHIVED_AT_TIME"))
    return dict(daily_candles=bars, intraday_points=quotes, corporate_effects=raw["corporate_effects"]), blackouts, sorted({r["ticker"] for r in raw["earnings"]})


async def collect_public(config, directory):
    documents, statuses, universe, risk, blackouts = [], [], [], [], []
    async with httpx.AsyncClient(timeout=25, follow_redirects=False, trust_env=False) as client:
        collector = Collector(directory, client)
        for instrument in config["instruments"]:
            try:
                found, status = await issuer_documents(collector, instrument, config)
                documents.extend(found)
                statuses.append(status)
            except (httpx.HTTPError, ValueError, SourceUnavailable, ET.ParseError) as exc:
                statuses.append(dict(ticker=instrument["ticker"], status="SOURCE_BLOCKED", error_type=type(exc).__name__))
        for name, url, hosts in [("IVV", IVV_URL, ["www.ishares.com"]), ("VIX", VIX_URL, ["query1.finance.yahoo.com"]),
                                 ("EMPLOYMENT", EMPLOYMENT_URL, ["www.bls.gov", "bls.gov"])]:
            try:
                content, receipt = await collector.fetch(url, hosts)
                if name == "IVV":
                    universe = ivv_members(content, config["instruments"], receipt)
                elif name == "VIX":
                    risk = [vix_signal(content, receipt, config)]
                else:
                    blackouts = employment_events(content, receipt)
                statuses.append(dict(source=name, status="CAPTURED"))
            except (httpx.HTTPError, ValueError, SourceUnavailable, KeyError, IndexError) as exc:
                statuses.append(dict(source=name, status="SOURCE_BLOCKED", error_type=type(exc).__name__))
        return dict(documents=documents, universe=universe, risk=risk, blackouts=blackouts,
                    source_status=statuses, receipts=collector.receipts, observed_at=now().isoformat())
