"""Free official-source sweep across every recorded equity, with reusable source receipts."""
from __future__ import annotations

import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from src.backtesting.thesis_desk import instant
from src.collector.issuer_event_sources import _cnv_datetime
from src.thesis_desk.catalog import COMAFI, CAJA, SEC_DIRECTORY, BOLSAR, BOLSAR_GENERAL, comafi_catalog, caja_catalog, native_catalog, resolve_catalog, name_key
from src.thesis_desk.contracts import check_url, digest, normalized, now
from src.thesis_desk.sources import Collector, SourceUnavailable, IVV_URL, VIX_URL, EMPLOYMENT_URL, ivv_members, vix_signal, employment_events, document_text, issuer_documents
from src.thesis_desk.earnings import collect_earnings

CNV = "https://www.cnv.gov.ar/sitioWeb/HechosRelevantes"
SEC_HOSTS = ["www.sec.gov", "data.sec.gov", "sec.gov"]


class CachedCollector(Collector):
    def __init__(self, directory, client, store):
        super().__init__(directory, client)
        self.store = store
        self.last_request = 0.0

    async def fetch(self, url, allowed_hosts, ttl=14400):
        check_url(url, allowed_hosts)
        key = "source:" + digest(url)
        cached = self.store.cached(key)
        if cached and now() - instant(cached["fetched_at"]) < timedelta(seconds=ttl):
            path = Path(cached["raw_path"]).resolve()
            if path.is_relative_to(self.directory.parent.parent.resolve()) and path.exists():
                body = path.read_bytes()
                if digest(body) == cached["sha256"]:
                    destination = self.directory / "raw" / (cached["sha256"] + ".bin")
                    if not destination.exists():
                        try:
                            os.link(path, destination)
                        except OSError:
                            destination.write_bytes(body)
                    receipt = dict(cached, raw_path=str(destination.resolve()), cache_hit=True, reused_at=now().isoformat())
                    self.receipts.append(receipt)
                    return body, receipt
        # Below SEC's 10 requests/second limit; one collector also bounds other public sources.
        await asyncio.sleep(max(0, 0.26 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        body, receipt = await super().fetch(url, allowed_hosts)
        self.store.cache(key, receipt)
        return body, receipt


def make_document(instrument, body, receipt, published, title, source_type):
    text = document_text(body)
    if len(text) < 120 or instant(published) > instant(receipt["fetched_at"]):
        raise ValueError("Official document empty or future dated")
    return dict(id=digest([instrument["ticker"], receipt["source_url"], published, text]), ticker=instrument["ticker"], title=title,
                source_type=source_type, source_url=receipt["source_url"], published_at=published,
                publication_precision="TIMESTAMP", available_at=receipt["fetched_at"], sha256=receipt["sha256"], text=text)


async def sec_documents(collector, instrument, config):
    url = "https://data.sec.gov/submissions/CIK" + instrument["sec_cik"] + ".json"
    content, receipt = await collector.fetch(url, SEC_HOSTS)
    data = json.loads(content)
    if str(data["cik"]).zfill(10) != instrument["sec_cik"]:
        raise ValueError("SEC issuer mismatch")
    recent = data["filings"]["recent"]
    rows = [dict(zip(recent, values)) for values in zip(*recent.values())]
    cutoff = now() - timedelta(days=config["lookback_days"])
    selected = [r for r in rows if r["form"] in {"8-K", "6-K"} and r.get("acceptanceDateTime")
                and cutoff <= instant(r["acceptanceDateTime"]) <= now()]
    selected.sort(key=lambda r: r["acceptanceDateTime"], reverse=True)
    documents, errors = [], []
    limit = config["max_documents_per_issuer"]
    for row in selected[:limit]:
        accession = row["accessionNumber"].replace("-", "")
        if not re.fullmatch(r"\d+", accession) or "/" in row["primaryDocument"]:
            raise ValueError("Invalid SEC accession")
        base = "https://www.sec.gov/Archives/edgar/data/" + str(int(instrument["sec_cik"])) + "/" + accession + "/"
        primary = base + row["primaryDocument"]
        try:
            content, doc_receipt = await collector.fetch(primary, SEC_HOSTS, ttl=30 * 86400)
            docs = [(content, doc_receipt)]
            soup = BeautifulSoup(content, "html.parser")
            links = sorted({urljoin(primary, a["href"]) for a in soup.select("a[href]")
                            if re.search(r"(?:ex(?:hibit)?[-_]?99|ex99|991|99\.1).*\.html?", a["href"], re.I)})
            valid_links = [u for u in links if u.startswith(base)]
            for link in valid_links[:2]:
                docs.append(await collector.fetch(link, SEC_HOSTS, ttl=30 * 86400))
            if len(valid_links) > 2:
                errors.append("EXHIBIT_LIMIT")
            for raw, rec in docs:
                documents.append(make_document(instrument, raw, rec, row["acceptanceDateTime"], row["form"] + " " + row["accessionNumber"], "SEC"))
        except (httpx.HTTPError, ValueError, SourceUnavailable) as exc:
            errors.append(type(exc).__name__)
    return documents, dict(ticker=instrument["ticker"], status="PARTIAL" if errors else "CAPTURED", documents=len(documents),
                           discovered=len(selected), truncated=len(selected) > limit, errors=errors,
                           checked_at=receipt["fetched_at"], completeness="SEC_RECENT_8K_6K_WITH_UP_TO_TWO_EXHIBITS_PER_FILING")


def cnv_rows(body):
    rows = []
    for tr in BeautifulSoup(body, "html.parser").select("tr"):
        cells = [normalized(c.get_text(" ", strip=True)) for c in tr.select("td")]
        link = tr.select_one("a[href]")
        stamp = _cnv_datetime(cells[0]) if cells else None
        if len(cells) >= 4 and stamp and link:
            rows.append(dict(entity=cells[1], title=cells[2], published_at=stamp.isoformat(), source_url=urljoin(CNV, link["href"])))
    # The public feed can repeat a presentation. Deduplicate before applying issuer limits.
    return list({(r["entity"], r["source_url"], r["published_at"]): r for r in rows}.values())


async def local_documents(collector, instrument, rows, config):
    name = instrument.get("cnv_name")
    if not name:
        # A quote lookup supplies a name candidate only. CNV must match it exactly after punctuation normalization.
        symbol = instrument["ticker"] + ".BA"
        url = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?range=1d&interval=1d"
        body, _ = await collector.fetch(url, ["query1.finance.yahoo.com"], ttl=86400)
        meta = json.loads(body)["chart"]["result"][0]["meta"]
        if meta.get("symbol") != symbol or meta.get("instrumentType") != "EQUITY":
            raise ValueError("Local issuer lookup identity mismatch")
        name = meta.get("longName") or meta.get("shortName")
    matching = [r for r in rows if name and name_key(r["entity"]) == name_key(name)]
    documents, errors = [], []
    for row in matching[:config["max_documents_per_issuer"]]:
        try:
            raw, receipt = await collector.fetch(row["source_url"], ["www.cnv.gov.ar", "aif2.cnv.gov.ar", "aif.cnv.gov.ar"], ttl=86400)
            # Presentation headings alone do not establish the substance of an agreement or guidance.
            doc = make_document(instrument, raw, receipt, row["published_at"], row["title"], "CNV")
            if len(doc["text"]) < 500:
                raise ValueError("CNV_DOCUMENT_BODY_UNAVAILABLE")
            documents.append(doc)
        except (httpx.HTTPError, ValueError, SourceUnavailable) as exc:
            errors.append(type(exc).__name__)
    return documents, dict(ticker=instrument["ticker"], status="PARTIAL", documents=len(documents), errors=errors,
                           truncated=len(matching) > config["max_documents_per_issuer"],
                           completeness="CNV_PUBLIC_FEED_NO_EXHAUSTIVE_HISTORICAL_COVERAGE", issuer_match=bool(matching))


async def collect_all(config, directory, market, store):
    documents, statuses, deposits, native = [], [], [], []
    sec, risk, blackouts, ivv, cnv = {}, [], [], [], []
    async with httpx.AsyncClient(timeout=25, follow_redirects=False, trust_env=False) as client:
        collector = CachedCollector(directory, client, store)
        for name, url, parser in [("COMAFI", COMAFI, comafi_catalog), ("CVSA", CAJA, caja_catalog),
                                  ("BCBA_LEADING", BOLSAR, native_catalog), ("BCBA_GENERAL", BOLSAR_GENERAL, native_catalog)]:
            try:
                raw, receipt = await collector.fetch(url, [urlparse(url).hostname], ttl=86400)
                values = parser(raw, receipt)
                (native if name.startswith("BCBA") else deposits).extend(values)
                statuses.append(dict(source=name, status="CAPTURED", rows=len(values)))
            except (httpx.HTTPError, ValueError, SourceUnavailable, KeyError) as exc:
                statuses.append(dict(source=name, status="SOURCE_BLOCKED", error_type=type(exc).__name__))
        try:
            raw, _ = await collector.fetch(SEC_DIRECTORY, SEC_HOSTS, ttl=86400)
            sec = json.loads(raw)
        except (httpx.HTTPError, ValueError, SourceUnavailable) as exc:
            statuses.append(dict(source="SEC_DIRECTORY", status="SOURCE_BLOCKED", error_type=type(exc).__name__))
        catalog = resolve_catalog(market, deposits, native, sec, config["instruments"])
        identified = [r for r in catalog if r["status"] == "IDENTIFIED"]
        for name, url, hosts in [("IVV", IVV_URL, ["www.ishares.com"]), ("VIX", VIX_URL, ["query1.finance.yahoo.com"]),
                                 ("EMPLOYMENT", EMPLOYMENT_URL, ["www.bls.gov", "bls.gov"]), ("CNV", CNV, ["www.cnv.gov.ar"])]:
            try:
                raw, receipt = await collector.fetch(url, hosts, ttl=21600)
                if name == "IVV":
                    ivv = ivv_members(raw, identified, receipt)
                elif name == "VIX":
                    risk = [vix_signal(raw, receipt, config)]
                elif name == "EMPLOYMENT":
                    blackouts = employment_events(raw, receipt)
                else:
                    cnv = cnv_rows(raw)
                statuses.append(dict(source=name, status="CAPTURED", checked_at=receipt["fetched_at"]))
            except (httpx.HTTPError, ValueError, SourceUnavailable, KeyError, IndexError) as exc:
                statuses.append(dict(source=name, status="SOURCE_BLOCKED", error_type=type(exc).__name__))
        by_ticker = {r["ticker"]: r for r in ivv}
        universe = []
        for item in identified:
            if item["ticker"] in by_ticker:
                universe.append(dict(by_ticker[item["ticker"]], underlying_kind=item["underlying_kind"]))
            else:
                universe.append(dict(id=digest([item["ticker"], item["source_hash"]]), ticker=item["ticker"], asset_type=item["asset_type"],
                                     currency="ARS", underlying_kind=item["underlying_kind"], sleeve="EXTENDED", membership_source=item["identity_basis"],
                                     source_url=item["source_url"], asof_at=item["available_at"], available_at=item["available_at"],
                                     knowledge_basis="ARCHIVED_AT_TIME"))
        for index, item in enumerate(catalog):
            if item["status"] != "IDENTIFIED":
                statuses.append(dict(ticker=item["ticker"], status=item["status"], documents=0))
                continue
            try:
                if item.get("sec_cik"):
                    found, status = await sec_documents(collector, item, config)
                elif item.get("ir_override"):
                    found, status = await issuer_documents(collector, item["ir_override"], config)
                elif item["asset_type"] == "ACCION":
                    found, status = await local_documents(collector, item, cnv, config)
                else:
                    found, status = [], dict(ticker=item["ticker"], status="OFFICIAL_SOURCE_UNMAPPED", documents=0)
                documents.extend(found)
                statuses.append(status)
            except (httpx.HTTPError, ValueError, SourceUnavailable, KeyError, IndexError, TypeError) as exc:
                statuses.append(dict(ticker=item["ticker"], status="SOURCE_BLOCKED", documents=0, error_type=type(exc).__name__))
            if (index + 1) % 25 == 0:
                print(json.dumps(dict(stage="OFFICIAL_SOURCE_SWEEP", processed=index + 1, total=len(catalog), documents=len(documents))), flush=True)
        earnings_covered = []
        try:
            events, earnings_covered, status = await collect_earnings(store, directory, catalog)
            blackouts.extend(events)
            statuses.append(status)
        except Exception as exc:
            statuses.append(dict(source="EARNINGS", status="SOURCE_BLOCKED", error_type=type(exc).__name__))
        return dict(documents=documents, universe=universe, risk=risk, blackouts=blackouts, source_status=statuses, earnings_covered=earnings_covered,
                    receipts=collector.receipts, catalog=catalog, observed_at=now().isoformat(),
                    coverage_basis=market["basis"], source_cache_hits=sum(bool(r.get("cache_hit")) for r in collector.receipts))
