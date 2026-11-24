"""Archive dated public evidence; downloading a filing does not approve a thesis.

Publication time, observation time and historical instrument identity stay separate.
No model, broker, database writes or inferred membership are used by this module.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import csv
from datetime import datetime, time, timedelta
import io
import json
from pathlib import Path
import re
import time as clock
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, NavigableString
import httpx

from src.backtesting.thesis_desk import instant
from src.thesis_desk.contracts import check_url, digest, dump, normalized, now
from src.thesis_desk.sources import Collector, NY, SourceUnavailable, document_text

SEC_HOSTS = ["data.sec.gov", "www.sec.gov", "sec.gov"]
VIX_HISTORY = "https://cdn-api.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
IVV_DOCUMENT = "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document"
FORMS = {"8-K", "8-K/A", "6-K", "6-K/A"}


class Archive(Collector):
    """Resumable disk receipts plus a bounded RAM cache and shared request limiter."""
    def __init__(self, directory, client, memory_mb=512):
        super().__init__(directory, client, max_bytes=32 * 1024 * 1024)
        self.memory_limit = memory_mb * 1024 * 1024
        self.memory = OrderedDict()
        self.memory_bytes = self.memory_peak = self.memory_hits = self.disk_hits = self.requests = 0
        self.locks = {}
        self.rate_lock, self.connections = asyncio.Lock(), asyncio.Semaphore(4)
        self.last_request = 0.0
        (directory / "receipts").mkdir(parents=True, exist_ok=True)

    def remember(self, key, body, receipt):
        if len(body) > self.memory_limit:
            return
        while self.memory_bytes + len(body) > self.memory_limit and self.memory:
            _, (old, _) = self.memory.popitem(last=False)
            self.memory_bytes -= len(old)
        self.memory[key] = (body, receipt)
        self.memory_bytes += len(body)
        self.memory_peak = max(self.memory_peak, self.memory_bytes)

    async def fetch(self, url, allowed_hosts, *, download=None):
        check_url(url, allowed_hosts)
        async with self.locks.setdefault(url, asyncio.Lock()):
            if url in self.memory:
                self.memory_hits += 1
                self.memory.move_to_end(url)
                return self.memory[url]
            receipt_path = self.directory / "receipts" / (digest(url) + ".json")
            if receipt_path.exists():
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                check_url(receipt["source_url"], allowed_hosts)
                path = Path(receipt["raw_path"]).resolve()
                if receipt["requested_url"] != url or not path.is_relative_to((self.directory / "raw").resolve()):
                    raise ValueError("Historical receipt path or URL mismatch")
                body = path.read_bytes()
                if digest(body) != receipt["sha256"]:
                    raise ValueError("Historical source integrity failure")
                self.disk_hits += 1
            else:
                async with self.connections:
                    async with self.rate_lock:
                        await asyncio.sleep(max(0, 0.3 - (clock.monotonic() - self.last_request)))
                        self.last_request = clock.monotonic()
                    self.requests += 2 if download else 1
                    if download:
                        body = await download(self.client)
                        if len(body) > self.max_bytes:
                            raise SourceUnavailable("SOURCE_SIZE_LIMIT")
                        identity = digest(body)
                        path = self.directory / "raw" / (identity + ".bin")
                        if not path.exists():
                            path.write_bytes(body)
                        receipt = dict(requested_url=url, source_url=url, fetched_at=now().isoformat(), sha256=identity,
                            bytes=len(body), raw_path=str(path.resolve()), request_method="POST_PUBLIC_DOWNLOAD")
                    else:
                        for attempt in range(3):
                            try:
                                body, receipt = await super().fetch(url, allowed_hosts)
                                break
                            except (httpx.HTTPError, SourceUnavailable) as exc:
                                transient = isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)) or str(exc) in {
                                    "HTTP_429", "HTTP_502", "HTTP_503", "HTTP_504"}
                                if not transient or attempt == 2:
                                    raise
                                self.requests += 1
                                await asyncio.sleep(attempt + 1)
                dump(receipt_path, receipt)
            self.remember(url, body, receipt)
            return body, receipt

    def stats(self):
        return dict(memory_limit_mb=self.memory_limit // 1024**2, memory_peak_bytes=self.memory_peak,
                    memory_hits=self.memory_hits, disk_hits=self.disk_hits, network_requests=self.requests,
                    model_calls=0, network_concurrency=4, request_spacing_seconds=0.3)


def filing_rows(columns):
    required = {"form", "accessionNumber", "primaryDocument", "acceptanceDateTime", "filingDate"}
    if not required <= columns.keys() or any(not isinstance(columns[k], list) for k in required):
        raise ValueError("SEC filing columns missing")
    if len({len(columns[k]) for k in required}) != 1:
        raise ValueError("SEC filing column lengths differ")
    return [dict(zip(sorted(required), row)) for row in zip(*(columns[k] for k in sorted(required)))]


def extracted_text(body):
    if body.startswith(b"%PDF"):
        import pymupdf
        with pymupdf.open(stream=body, filetype="pdf") as doc:
            return normalized(" ".join(page.get_text() for page in doc))
    return document_text(body)


def archived_document(instrument, body, receipt, published, title, source_type, **extra):
    text = extracted_text(body)
    if len(text) < 120:
        raise ValueError("Historical document has insufficient extractable text")
    if published is not None and instant(published) > instant(receipt["fetched_at"]):
        raise ValueError("Future publication")
    return dict(id=digest([instrument["ticker"], receipt["source_url"], published, text]), ticker=instrument["ticker"],
                title=title, source_type=source_type, source_url=receipt["source_url"], source_hash=receipt["sha256"],
                sha256=receipt["sha256"], published_at=published, observed_at=receipt["fetched_at"],
                available_at=receipt["fetched_at"], historical_publication_at=published,
                knowledge_basis="RECONSTRUCTED_PUBLICATION" if published else "UNKNOWN_PUBLICATION_TIME",
                identity_basis="CURRENT_CATALOG_DISCOVERY_NOT_HISTORICAL_MEMBERSHIP",
                raw_path=receipt["raw_path"], text=text, classification_status="PENDING_RESEARCH", **extra)


async def sec_history(archive, instrument, begin, end):
    cik = instrument["sec_cik"]
    if not re.fullmatch(r"\d{10}", cik):
        raise ValueError("Invalid SEC CIK")
    raw, receipt = await archive.fetch(f"https://data.sec.gov/submissions/CIK{cik}.json", SEC_HOSTS)
    submissions = json.loads(raw)
    if str(submissions["cik"]).zfill(10) != cik:
        raise ValueError("SEC issuer mismatch")
    rows = filing_rows(submissions["filings"]["recent"])
    metadata_urls = [receipt["source_url"]]
    for older in submissions["filings"].get("files", []):
        if older["filingFrom"] <= end.date().isoformat() and older["filingTo"] >= begin.date().isoformat():
            if not re.fullmatch(r"CIK\d+-submissions-\d+\.json", older["name"]):
                raise ValueError("Unexpected SEC history filename")
            raw, rec = await archive.fetch("https://data.sec.gov/submissions/" + older["name"], SEC_HOSTS)
            rows.extend(filing_rows(json.loads(raw)))
            metadata_urls.append(rec["source_url"])
    selected = {}
    for row in rows:
        if row["form"] in FORMS and begin <= instant(row["acceptanceDateTime"]) <= end:
            previous = selected.setdefault(row["accessionNumber"], row)
            if previous != row:
                raise ValueError("Conflicting SEC accession metadata")
    documents, errors, filings = [], [], []
    for row in sorted(selected.values(), key=lambda r: r["acceptanceDateTime"]):
        accession, primary = row["accessionNumber"], row["primaryDocument"]
        if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession) or not re.fullmatch(r"[A-Za-z0-9_.-]+", primary):
            raise ValueError("Invalid SEC document identity")
        base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
        url = base + primary
        filings.append(dict(row, source_url=url))
        try:
            raw, rec = await archive.fetch(url, SEC_HOSTS)
            soup = BeautifulSoup(raw, "html.parser")
            exhibits = sorted({urljoin(url, a["href"]).split("#", 1)[0] for a in soup.select("a[href]")
                               if re.search(r"(?:ex(?:hibit)?[\s_-]*99|991|99\.\d+)", a["href"] + " " + a.get_text(), re.I)
                               and urljoin(url, a["href"]).startswith(base)})
            for doc_url in dict.fromkeys([url] + exhibits):
                try:
                    content, source = (raw, rec) if doc_url == url else await archive.fetch(doc_url, SEC_HOSTS)
                    documents.append(archived_document(instrument, content, source, row["acceptanceDateTime"],
                                     row["form"] + " " + accession, "SEC", accession=accession,
                                     acceptance_metadata_urls=metadata_urls, publication_precision="SEC_ACCEPTANCE_TIMESTAMP"))
                except (ValueError, httpx.HTTPError, SourceUnavailable, ImportError) as exc:
                    errors.append(dict(source_url=doc_url, error=str(exc)[:180], error_type=type(exc).__name__))
        except (ValueError, httpx.HTTPError, SourceUnavailable) as exc:
            errors.append(dict(source_url=url, error=str(exc)[:180], error_type=type(exc).__name__))
    return dict(ticker=instrument["ticker"], sec_cik=cik, status="PARTIAL" if errors else "ARCHIVED",
                scope="SEC_8K_6K_AMENDMENTS_AND_ALL_LINKED_EX99_NOT_EXHAUSTIVE_ISSUER_NEWS",
                metadata_urls=metadata_urls, filings=filings, documents=documents, errors=errors)


def vix_history(raw, receipt, begin, end):
    values = {}
    for row in csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))):
        day = datetime.strptime(row["DATE"], "%m/%d/%Y").date()
        if not begin <= day <= end:
            continue
        from decimal import Decimal
        close = Decimal(row["CLOSE"])
        if not close.is_finite() or close <= 0 or day in values:
            raise ValueError("Invalid or duplicate VIX close")
        values[day] = dict(date=day.isoformat(), close=str(close), source_url=receipt["source_url"],
                           source_hash=receipt["sha256"], observed_at=receipt["fetched_at"],
                           publication_timestamp=None, vintage="CURRENT_DOWNLOAD_OF_HISTORICAL_DAILY_VALUES")
    if not values:
        raise ValueError("VIX source does not cover the requested period")
    return [values[d] for d in sorted(values)]


def employment_history(raw, receipt, begin, end):
    soup = BeautifulSoup(raw, "html.parser")
    result = []
    for table in soup.select("table.release-list"):
        footer = None
        for node in table.next_elements:
            if table in node.parents:
                continue
            if getattr(node, "name", None) == "table":
                break
            if isinstance(node, NavigableString) and "Last Modified Date" in node:
                footer = node
                break
        # BLS puts the label in <strong> and the date in its next text node.
        # Do not consume the enclosing <p>: malformed yearly HTML nests later months there.
        footer_text = str(footer or "")
        if footer and footer.parent.name == "strong":
            for sibling in footer.parent.next_siblings:
                if not isinstance(sibling, NavigableString):
                    break
                footer_text += str(sibling)
        match = re.search(r"Last Modified Date:\s*([A-Za-z]+ \d{1,2}, \d{4})", footer_text)
        modified = datetime.strptime(match[1], "%B %d, %Y").date() if match else None
        for row in table.select("tbody tr"):
            cells = [c.get_text(" ", strip=True) for c in row.select("td")]
            if len(cells) != 3 or not cells[2].startswith("Employment Situation for"):
                continue
            stamp = datetime.strptime(cells[0] + " " + cells[1], "%A, %B %d, %Y %I:%M %p").replace(tzinfo=NY)
            if not begin <= stamp.date() <= end:
                continue
            result.append(dict(id=digest([receipt["source_url"], stamp.isoformat()]), ticker="*", event_at=stamp.isoformat(),
                               start_at=(stamp - timedelta(hours=24)).isoformat(), end_at=(stamp + timedelta(hours=1)).isoformat(),
                               available_at=datetime.combine(modified + timedelta(days=1), time.min, NY).isoformat() if modified else None,
                               schedule_last_modified=modified.isoformat() if modified else None,
                               availability_basis="END_OF_OFFICIAL_LAST_MODIFIED_DAY_BOUND",
                               knowledge_basis="RECONSTRUCTED_PUBLICATION", source_url=receipt["source_url"],
                               source_hash=receipt["sha256"], observed_at=receipt["fetched_at"], event_type="US_EMPLOYMENT"))
    if not result:
        raise ValueError("Employment calendar has no matching releases")
    return result


def holdings_url(day):
    return IVV_DOCUMENT + "?" + urlencode(dict(appType="PRODUCT_PAGE", appSubType="ISHARES", targetSite="us-ishares",
        locale="en_US", portfolioId="239726", userType="individual", asOfDate=day.strftime("%Y%m%d"), component="holdings"))


def historical_holdings(raw, receipt, requested):
    rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"))))
    dates = [r[1] for r in rows if len(r) > 1 and r[0].strip() == "Fund Holdings as of"]
    header = next((i for i, r in enumerate(rows) if r and r[0] == "Ticker"), None)
    if len(dates) != 1 or header is None or datetime.strptime(dates[0], "%b %d, %Y").date() != requested:
        raise ValueError("IVV historical response date does not match request; latest-data fallback rejected")
    records = [dict(zip(rows[header], r)) for r in rows[header + 1:] if len(r) == len(rows[header])]
    equities = [r for r in records if r.get("Asset Class") == "Equity"]
    if not equities or len({r["Ticker"] for r in equities}) != len(equities):
        raise ValueError("Empty or duplicate IVV holdings")
    return dict(asof=requested.isoformat(), equities=equities, source_url=receipt["source_url"],
                source_hash=receipt["sha256"], observed_at=receipt["fetched_at"],
                publication_timestamp=None, historical_local_instrument_mapping="NOT_YET_VERIFIED")
