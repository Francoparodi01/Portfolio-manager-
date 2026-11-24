"""Discover the entire recorded equity market; resolve identities from dated public catalogs."""
from __future__ import annotations

from collections import Counter
import json
import re
import subprocess
import unicodedata
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from src.thesis_desk.contracts import digest, dump, normalized, now

COMAFI = "https://www.comafi.com.ar/custodiaglobal/json/apps/getproducts.aspx"
CAJA = "https://cajadevalores.com.ar/Servicios/Cedears"
SEC_DIRECTORY = "https://www.sec.gov/files/company_tickers_exchange.json"
BOLSAR = "https://bolsar.info/lideres.php"
BOLSAR_GENERAL = "https://bolsar.info/paneles.php?panel=2&titulo=Panel%20General"


def name_key(value):
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().upper()
    return re.sub(r"[^A-Z0-9]", "", value)


def read_market_universe(container, user, database, directory, held=()):
    sql = """BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='30000';
SELECT jsonb_build_object('instruments', (
 SELECT coalesce(jsonb_agg(q),'[]'::jsonb) FROM (
  SELECT ticker,asset_type,currency,max(ts) AS last_market_at,count(*) AS quote_count
  FROM market_prices WHERE currency='ARS' AND asset_type IN ('CEDEAR','ACCION') AND ts<=now()
  GROUP BY ticker,asset_type,currency) q), 'registry', (
 SELECT coalesce(jsonb_agg(q),'[]'::jsonb) FROM (
  SELECT i.ticker,i.asset_type,i.currency,i.source_ticker,r.issuer_name,r.cnv_entity_name,r.sec_cik
  FROM issuer_instruments i JOIN issuer_registry r USING(issuer_id) WHERE i.is_active AND r.is_active) q));
COMMIT;"""
    (directory / "universe.sql").write_text(sql, encoding="utf-8")
    response = subprocess.run(["docker", "exec", "-i", container, "psql", "-U", user, "-d", database,
                               "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"],
                              input=sql.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    if response.returncode:
        raise RuntimeError("Universe read-only capture failed")
    result = json.loads(response.stdout.decode("utf-8"))
    result.update(captured_at=now().isoformat(), basis="ALL_RECORDED_ARS_EQUITY_SYMBOLS_NO_RECENCY_SURVIVOR_FILTER")
    observed = {i["ticker"] for i in result["instruments"]}
    if set(held) - observed:
        raise ValueError("Held instrument missing from the market catalog")
    dump(directory / "market-universe.json", result)
    return result


def comafi_catalog(body, receipt):
    products = json.loads(body)["products"]
    if not products:
        raise ValueError("Empty depositary catalog")
    rows = []
    for item in products:
        ticker = normalized(item["name"]).upper()
        description = BeautifulSoup(item.get("description", ""), "html.parser")
        origin = next((normalized(li.get_text(" ", strip=True)).split(":", 1)[-1].strip()
                       for li in description.select("li") if "ticker en mercado de origen" in li.get_text().lower()), None)
        market = next((normalized(li.get_text(" ", strip=True)).split(":", 1)[-1].strip()
                       for li in description.select("li") if "mercadodevalorsubyacente" in re.sub(r"\s+", "", li.get_text()).lower()), None)
        if item["section"] == "Cedear ETF" and "ADR" in item.get("keys", "").upper():
            kind = "UNKNOWN"  # Contradictory catalog fields are not proof that an issuer is an ETF.
        elif item["section"] == "Cedear ETF" or "ETF" in item.get("keys", "").upper():
            kind = "ETF"
        elif "ADR" in item.get("keys", "").upper():
            kind = "DEPOSITARY_RECEIPT"
        elif "ordinaria" in item.get("keys", "").lower():
            kind = "COMMON_STOCK"
        else:
            kind = "UNKNOWN"
        rows.append(dict(ticker=ticker, underlying=origin, underlying_kind=kind, issuer_market=market, name=normalized(item.get("summary", ticker)),
                         isin=item.get("tech", "").strip(), website=item.get("legal"), source_url=receipt["source_url"],
                         source_hash=receipt["sha256"], available_at=receipt["fetched_at"], identity_basis="COMAFI_OFFICIAL_PROGRAM"))
    return rows


def caja_catalog(body, receipt):
    rows = []
    for table in BeautifulSoup(body, "html.parser").select("table"):
        header = table.select_one("tr")
        headers = [normalized(x.get_text(" ", strip=True)) for x in header.select("th,td")] if header else []
        if "Símbolo BYMA" not in headers or "Ticker en Mercado de Origen" not in headers:
            continue
        kind = "ETF" if any("ETF" in h for h in headers[:1]) else "COMMON_STOCK"
        for tr in table.select("tr"):
            cells = tr.select("th,td")
            if len(cells) != len(headers) or tr.select("th") == cells:
                continue
            record = dict(zip(headers, [normalized(c.get_text(" ", strip=True)) for c in cells]))
            ticker = record.get("Símbolo BYMA")
            if not ticker:
                continue
            rows.append(dict(ticker=ticker, underlying=record["Ticker en Mercado de Origen"], underlying_kind=kind,
                             issuer_market=next((v for k, v in record.items() if k.lower().startswith("mercado")), None),
                             name=record.get(headers[0], ticker), source_url=receipt["source_url"],
                             source_hash=receipt["sha256"], available_at=receipt["fetched_at"], identity_basis="CVSA_OFFICIAL_PROGRAM"))
    if not rows:
        raise ValueError("Unrecognized Caja de Valores catalog")
    return rows


def native_catalog(body, receipt):
    symbols = {c.get("data-order") for c in BeautifulSoup(body, "html.parser").select("td[data-order]")
               if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", c.get("data-order", ""))}
    return [dict(ticker=t, underlying=t, underlying_kind="COMMON_STOCK", name=t, source_url=receipt["source_url"],
                 source_hash=receipt["sha256"], available_at=receipt["fetched_at"], identity_basis="BCBA_NATIVE_EQUITY_PANEL") for t in symbols]


def resolve_catalog(market, deposits, native, sec, overrides):
    by_symbol = {}
    for row in sec.get("data", []):
        record = dict(zip(sec["fields"], row))
        by_symbol.setdefault(record["ticker"], []).append(record)
    override = {r["ticker"]: r for r in overrides}
    registry = {r["ticker"]: r for r in market["registry"]}
    records = []
    for observation in market["instruments"]:
        t = observation["ticker"]
        candidates = [r for r in (deposits if observation["asset_type"] == "CEDEAR" else native) if r["ticker"] == t]
        identities = {(r["underlying"], r["underlying_kind"]) for r in candidates}
        record = dict(observation, status="IDENTITY_UNKNOWN", underlying=None, underlying_kind="UNKNOWN", name=t)
        if len(identities) == 1:
            record.update(candidates[0])
            record["status"] = "EXCLUDED_ETF" if record["underlying_kind"] == "ETF" else (
                "IDENTIFIED" if record["underlying_kind"] in {"COMMON_STOCK", "DEPOSITARY_RECEIPT"} and record["underlying"] else "IDENTITY_UNKNOWN")
        elif len(identities) > 1:
            record["status"] = "IDENTITY_CONFLICT"
        underlying = (record.get("underlying") or "").replace("/", "-").replace(".", "-")
        us_market = re.match(r"NYSE|NASDAQ|OTC|NEW YORK|CBOE", str(record.get("issuer_market") or "").upper())
        matches = by_symbol.get(underlying, []) if observation["asset_type"] == "CEDEAR" and us_market else []
        if len({r["cik"] for r in matches}) == 1:
            record["sec_cik"] = str(matches[0]["cik"]).zfill(10)
            record["sec_name"] = matches[0]["name"]
        known = registry.get(t, {})
        record["cnv_name"] = known.get("cnv_entity_name")
        if t in override:
            record["ir_override"] = override[t]
        records.append(record)
    if len({(r["ticker"], r["asset_type"], r["currency"]) for r in records}) != len(records):
        raise ValueError("Duplicate universe instrument")
    # A ticker collision is never allowed to mix two securities in the paper ledger.
    counts = Counter(r["ticker"] for r in records)
    programs = {}
    for r in records:
        if r["status"] == "IDENTIFIED" and r["asset_type"] == "CEDEAR":
            programs.setdefault((r["underlying"], r.get("issuer_market")), set()).add(r["ticker"])
    for r in records:
        if counts[r["ticker"]] > 1:
            r["status"] = "IDENTITY_CONFLICT"
        if r["asset_type"] == "CEDEAR" and r["status"] == "IDENTIFIED":
            shared = programs.get((r["underlying"], r.get("issuer_market")), set())
            if len(shared) > 1:
                # Do not guess which program is correct or treat aliases as independent exposure.
                r.update(status="IDENTITY_CONFLICT", identity_conflict="SHARED_UNDERLYING_PROGRAM_REQUIRES_VERIFICATION",
                         conflicting_tickers=sorted(shared))
    return records
