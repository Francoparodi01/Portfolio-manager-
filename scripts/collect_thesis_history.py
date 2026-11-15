"""Recover dated official documents for thesis research, without any LLM or trading.

python -m scripts.collect_thesis_history --source-run outputs/thesis-desk-all-local/runs/RUN \
    --start 2026-09-01 --end 2026-10-31 --output outputs/thesis-history-sep-oct-2026
Run the same command again to reuse verified raw receipts and completed issuer archives.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import csv
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path

import httpx

from src.backtesting.thesis_desk import instant, sessions_between, validate_evidence
from src.thesis_desk.contracts import digest, dump, now
from src.thesis_desk.historical import (Archive, NY, VIX_HISTORY, archived_document, employment_history,
                                       historical_holdings, holdings_url, sec_history, vix_history)
from src.thesis_desk.archive_reuse import reuse_downloads
from src.thesis_desk.historical_cnv import CNV_HOSTS, cnv_attachments, public_blob_loader


def load_source_run(directory):
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("code_changed_during_run"):
        raise ValueError("Source run changed code during execution")
    path = directory / "result.json"
    if digest(path.read_bytes()) != manifest["artifacts"]["result.json"]:
        raise ValueError("Source catalog integrity failure")
    return json.loads(path.read_text(encoding="utf-8"))["catalog"], manifest


def write_checkpoint(path, value):
    tmp = path.with_suffix(".tmp")
    dump(tmp, dict(sha256=digest(value), result=value))
    tmp.replace(path)


def read_checkpoint(path, output):
    saved = json.loads(path.read_text(encoding="utf-8"))
    previous = saved["result"]
    if digest(previous) != saved["sha256"]:
        raise ValueError("Issuer checkpoint integrity failure")
    for doc in previous.get("documents", []):
        raw_path = Path(doc["raw_path"]).resolve()
        if not raw_path.is_relative_to((output / "raw").resolve()) or digest(raw_path.read_bytes()) != doc["source_hash"]:
            raise ValueError("Issuer archive integrity failure")
    return previous


async def collect(args):
    sessions = sessions_between(args.start, args.end)
    first, last = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if first.year != last.year:
        raise ValueError("Use one calendar year per archive")
    begin = datetime.combine(first - timedelta(days=14), time.min, NY)
    end = datetime.combine(last, time.max, NY)
    catalog, source_manifest = load_source_run(args.source_run)
    equities = [r for r in catalog if r["status"] == "IDENTIFIED"]
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    (output / "issuers").mkdir(exist_ok=True)
    root = Path(__file__).resolve().parents[1]
    code = [root / p for p in ("scripts/collect_thesis_history.py", "src/thesis_desk/historical.py", "src/thesis_desk/sources.py",
                             "src/thesis_desk/historical_cnv.py", "src/thesis_desk/archive_reuse.py")]
    policy = dict(schema="thesis-history-archive-v1", start=args.start, end=args.end,
                  publication_lookback_start=begin.isoformat(), source_run=source_manifest["run_id"],
                  catalog_sha256=digest(catalog), code_sha256={str(p): digest(p.read_bytes()) for p in code},
                  market_capture_sha256=digest(args.market_capture.read_bytes()) if args.market_capture else None,
                  reused_archive_manifest_sha256=digest((args.reuse_archive / "manifest.json").read_bytes()) if args.reuse_archive else None,
                  mode="PUBLIC_SOURCE_ARCHIVE_NO_MODEL_NO_TRADES", current_catalog_not_historical_universe=True)
    policy_file = output / "policy.json"
    if policy_file.exists() and json.loads(policy_file.read_text(encoding="utf-8")) != policy:
        raise ValueError("Historical archive policy/code changed; use a new output directory")
    dump(policy_file, policy)
    if args.reuse_archive:
        print(json.dumps(dict(stage="REUSED_VERIFIED_DOWNLOADS", receipts=reuse_downloads(args.reuse_archive, output))), flush=True)
    statuses, issuer_results, macro = [], [], {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=15), trust_env=False) as client:
        archive = Archive(output, client, args.memory_cache_mb)
        for name, url, hosts in (("VIX", VIX_HISTORY, ["cdn-api.cboe.com", "cdn.cboe.com"]),
                                 ("BLS", f"https://www.bls.gov/schedule/{first.year}/home.htm", ["www.bls.gov"])):
            try:
                raw, receipt = await archive.fetch(url, hosts)
                macro[name] = (vix_history(raw, receipt, first - timedelta(days=10), last) if name == "VIX"
                               else employment_history(raw, receipt, first, last))
                statuses.append(dict(source=name, status="ARCHIVED", records=len(macro[name])))
            except (ValueError, httpx.HTTPError, RuntimeError) as exc:
                macro[name] = []
                statuses.append(dict(source=name, status="SOURCE_BLOCKED", error_type=type(exc).__name__, error=str(exc)[:180]))
        dump(output / "macro.json", macro)
        snapshots = []
        dates = sorted({first - timedelta(days=1)} | {d for d in sessions if d.weekday() == 4})
        for day in dates:
            try:
                raw, receipt = await archive.fetch(holdings_url(day), ["www.blackrock.com", "www.ishares.com"])
                snapshot = historical_holdings(raw, receipt, day)
                snapshots.append(snapshot)
                statuses.append(dict(source="IVV", date=day.isoformat(), status="ARCHIVED", equities=len(snapshot["equities"])))
            except (ValueError, httpx.HTTPError, RuntimeError) as exc:
                statuses.append(dict(source="IVV", date=day.isoformat(), status="SOURCE_BLOCKED", error_type=type(exc).__name__, error=str(exc)[:180]))
        dump(output / "ivv-snapshots.json", snapshots)
        print(json.dumps(dict(stage="HISTORICAL_MACRO_AND_IVV", vix_rows=len(macro["VIX"]),
                              employment_events=len(macro["BLS"]), holdings_snapshots=len(snapshots))), flush=True)
        semaphore = asyncio.Semaphore(4)

        async def issuer(instrument):
            async with semaphore:
                path = output / "issuers" / (digest(instrument["ticker"]) + ".json")
                if path.exists():
                    previous = read_checkpoint(path, output)
                    if previous["status"] in {"ARCHIVED", "NO_SEC_MAPPING"}:
                        issuer_results.append(previous)
                        return
                if not instrument.get("sec_cik"):
                    result = dict(ticker=instrument["ticker"], status="NO_SEC_MAPPING", documents=[], filings=[], errors=[])
                else:
                    try:
                        result = await sec_history(archive, instrument, begin, end)
                    except (ValueError, httpx.HTTPError, RuntimeError) as exc:
                        result = dict(ticker=instrument["ticker"], status="SOURCE_BLOCKED", documents=[], filings=[],
                                      errors=[dict(error_type=type(exc).__name__, error=str(exc)[:180])])
                write_checkpoint(path, result)
                issuer_results.append(result)
                count = len(issuer_results)
                if count % 10 == 0 or count == len(equities):
                    print(json.dumps(dict(stage="HISTORICAL_ISSUERS", processed=count, total=len(equities),
                                          filings=sum(len(r["filings"]) for r in issuer_results),
                                          documents=sum(len(r["documents"]) for r in issuer_results))), flush=True)

        await asyncio.gather(*(issuer(r) for r in equities))
        issuer_results.sort(key=lambda r: r["ticker"])
        cnv = []
        if args.market_capture:
            capture = json.loads(args.market_capture.read_text(encoding="utf-8"))
            events = next(o["rows"] for o in capture["observations"] if o["name"] == "issuer_events")
            mapped = {r["ticker"]: r for r in equities}
            seen = set()
            for event in events:
                url, ticker = event.get("source_url"), event["ticker"]
                if event["source"] != "CNV" or event["event_type"] not in {"RELEVANT_FACT", "EARNINGS"} or ticker not in mapped or (ticker, url) in seen:
                    continue
                seen.add((ticker, url))
                try:
                    if not isinstance(url, str):
                        raise ValueError("CNV source URL missing")
                    published = event.get("source_published_at")
                    if published and not begin <= instant(published) <= end:
                        continue
                    raw, receipt = await archive.fetch(url, CNV_HOSTS)
                    attachments = cnv_attachments(raw, url) if not raw.startswith(b"%PDF") else []
                    if not attachments:
                        cnv.append(archived_document(mapped[ticker], raw, receipt, published, event["title"], "CNV",
                            local_event_id=event["id"], publication_precision="SOURCE_TIMESTAMP" if published else "UNKNOWN"))
                    for attachment in attachments:
                        try:
                            content, source = await archive.fetch(attachment["source_url"], CNV_HOSTS,
                                download=public_blob_loader(attachment, archive.max_bytes))
                            cnv.append(archived_document(mapped[ticker], content, source, published, attachment["filename"], "CNV",
                                local_event_id=event["id"], publication_precision="SOURCE_TIMESTAMP" if published else "UNKNOWN",
                                presentation_url=url, presentation_hash=receipt["sha256"], attachment_metadata=attachment))
                        except (ValueError, httpx.HTTPError, RuntimeError, ImportError) as exc:
                            statuses.append(dict(source="CNV", ticker=ticker, status="SOURCE_BLOCKED", source_url=attachment["source_url"],
                                error_type=type(exc).__name__, error=str(exc)[:180]))
                except (ValueError, httpx.HTTPError, RuntimeError, ImportError) as exc:
                    statuses.append(dict(source="CNV", ticker=ticker, status="SOURCE_BLOCKED", source_url=url,
                                         error_type=type(exc).__name__, error=str(exc)[:180]))
        dump(output / "cnv-documents.json", cnv)
        documents = sorted([d for result in issuer_results for d in result["documents"]] + cnv, key=lambda d: d["id"])
        if len({d["id"] for d in documents}) != len(documents):
            raise ValueError("Duplicate historical document identity")
        dump(output / "documents.json", documents)
        with (output / "coverage.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["ticker", "sec_cik", "sec_status", "filings", "sec_documents",
                                                       "cnv_documents", "retrieval_errors", "classification_status"])
            writer.writeheader()
            cnv_counts = Counter(d["ticker"] for d in cnv)
            for result in issuer_results:
                writer.writerow(dict(ticker=result["ticker"], sec_cik=result.get("sec_cik"), sec_status=result["status"],
                    filings=len(result["filings"]), sec_documents=len(result["documents"]), cnv_documents=cnv_counts[result["ticker"]],
                    retrieval_errors=len(result["errors"]) + sum(s.get("ticker") == result["ticker"] and s.get("source") == "CNV" for s in statuses),
                    classification_status="PENDING_RESEARCH"))
        # Calendar reconstruction is usable as partial evidence. No invented thesis,
        # historical mapping, close-publication timestamp or earnings coverage is filled in.
        blackouts = [e for e in macro["BLS"] if e["available_at"] and instant(e["available_at"]) <= instant(e["start_at"])]
        evidence = dict(schema="thesis-desk-evidence-v1", facts=[], universe=[], risk=[], blackouts=blackouts,
                        coverage=[dict(id="employment-only", domain="blackouts", start=args.start, end=args.end,
                            status="PARTIAL", scope="DECLARED_UNIVERSE", source_url=f"https://www.bls.gov/schedule/{first.year}/home.htm",
                            detail="Employment releases only; issuer earnings schedule remains incomplete")])
        validate_evidence(evidence)
        dump(output / "evidence.json", evidence)
        dates_seen = [d["published_at"] for d in documents if d["published_at"]]
        summary = dict(at=now().isoformat(), start=args.start, end=args.end, identified_equities=len(equities),
                       sec_mapped=sum(bool(r.get("sec_cik")) for r in equities), issuer_status=dict(Counter(r["status"] for r in issuer_results)),
                       filings=sum(len(r["filings"]) for r in issuer_results), documents=len(documents),
                       document_sources=dict(Counter(d["source_type"] for d in documents)),
                       documents_with_publication_time=len(dates_seen), first_publication=min(dates_seen, default=None),
                       documents_in_window=sum(first <= instant(d).astimezone(NY).date() <= last for d in dates_seen),
                       documents_in_lookback=sum(instant(d).astimezone(NY).date() < first for d in dates_seen),
                       documents_without_publication_time=len(documents) - len(dates_seen),
                       last_publication=max(dates_seen, default=None), vix_daily_rows=len(macro["VIX"]),
                       ivv_snapshots=len(snapshots), employment_blackouts=len(blackouts), statuses=statuses, runtime=archive.stats(),
                       backtest_status="NOT_EVALUABLE", limitations=["DOCUMENTS_REQUIRE_CLASSIFICATION_AND_REVIEW",
                         "CURRENT_LOCAL_INSTRUMENT_MAPPING_NOT_HISTORICAL_PROOF", "VIX_ORIGINAL_PUBLICATION_TIMESTAMPS_UNKNOWN",
                         "ISSUER_EARNINGS_CALENDAR_INCOMPLETE", "ORIGINAL_PRICE_LINEAGE_UNCHANGED",
                         "SEC_FORMS_AND_LINKED_EXHIBITS_NOT_ALL_ISSUER_NEWS"])
        dump(output / "summary.json", summary)
    files = [p for p in output.rglob("*") if p.is_file() and p.name != "manifest.json"]
    dump(output / "manifest.json", dict(completed_at=now().isoformat(), policy=policy,
        artifacts={str(p.relative_to(output)): digest(p.read_bytes()) for p in files},
        code_changed_during_run=[str(p) for p in code if digest(p.read_bytes()) != policy["code_sha256"][str(p)]],
        market_capture_sha256=digest(args.market_capture.read_bytes()) if args.market_capture else None))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, required=True)
    parser.add_argument("--start", default="2026-09-01")
    parser.add_argument("--end", default="2026-10-31")
    parser.add_argument("--market-capture", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--memory-cache-mb", type=int, default=512)
    parser.add_argument("--reuse-archive", type=Path, help="Reuse raw downloads from a completed, hash-verified archive")
    args = parser.parse_args()
    if not 16 <= args.memory_cache_mb <= 4096:
        parser.error("memory-cache-mb must be between 16 and 4096")
    result = asyncio.run(collect(args))
    print(json.dumps({k: v for k, v in result.items() if k not in {"statuses", "limitations"}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
