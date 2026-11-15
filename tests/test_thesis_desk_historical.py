"""Historical retrieval must preserve dates, source identity and missingness."""
import asyncio
from datetime import date, datetime
import json

import httpx
import pytest

from scripts.collect_thesis_history import read_checkpoint, write_checkpoint
from src.thesis_desk.contracts import digest
from src.thesis_desk.historical import (Archive, NY, archived_document, employment_history,
    filing_rows, historical_holdings, sec_history, vix_history)
from src.thesis_desk.sources import SourceUnavailable


def receipt(url="https://www.sec.gov/example", body=b""):
    return dict(source_url=url, requested_url=url, fetched_at="2026-11-12T12:00:00+00:00",
                sha256=digest(body), raw_path="unused")


def columns(*rows):
    keys = ("form", "accessionNumber", "primaryDocument", "acceptanceDateTime", "filingDate")
    return {key: [row[i] for row in rows] for i, key in enumerate(keys)}


def test_archive_reuses_memory_and_verified_disk_and_rejects_tampering(tmp_path):
    async def check():
        calls = []
        def respond(request):
            calls.append(str(request.url))
            return httpx.Response(200, content=b"official source")
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            archive = Archive(tmp_path, client, memory_mb=1)
            url = "https://www.sec.gov/example"
            first = await archive.fetch(url, ["www.sec.gov"])
            assert await archive.fetch(url, ["www.sec.gov"]) == first
            second = Archive(tmp_path, client, memory_mb=1)
            assert await second.fetch(url, ["www.sec.gov"]) == first
            assert len(calls) == 1 and archive.stats()["memory_hits"] == 1
            assert second.stats()["disk_hits"] == 1 and second.stats()["model_calls"] == 0
            from pathlib import Path
            Path(first[1]["raw_path"]).write_bytes(b"changed")
            with pytest.raises(ValueError, match="integrity"):
                await Archive(tmp_path, client).fetch(url, ["www.sec.gov"])
    asyncio.run(check())


def test_archive_size_bound_and_lru_memory_bound(tmp_path):
    async def check():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"x" * 12))) as client:
            archive = Archive(tmp_path, client, memory_mb=1)
            archive.max_bytes = 10
            with pytest.raises(SourceUnavailable, match="SIZE_LIMIT"):
                await archive.fetch("https://www.sec.gov/example", ["www.sec.gov"])
            archive.memory_limit = 10
            archive.remember("a", b"123456", {})
            archive.remember("b", b"123456", {})
            assert list(archive.memory) == ["b"] and archive.memory_peak == 6
    asyncio.run(check())


def test_sec_old_files_amendments_full_exhibits_and_future_exclusion():
    async def check():
        recent = columns(("8-K", "0000000001-26-000002", "current.htm", "2026-10-01T12:00:00Z", "2026-10-01"),
                         ("8-K", "0000000001-26-000003", "future.htm", "2026-11-01T12:00:00Z", "2026-11-01"))
        old = columns(("8-K/A", "0000000001-26-000001", "old.htm", "2026-08-25T12:00:00Z", "2026-08-25"))
        metadata = dict(cik="1", filings=dict(recent=recent, files=[dict(name="CIK0000000001-submissions-001.json",
                       filingFrom="2025-01-01", filingTo="2026-09-01")]))
        seen = []
        class FakeArchive:
            async def fetch(self, url, hosts):
                seen.append(url)
                if url.endswith("CIK0000000001.json"):
                    body = json.dumps(metadata).encode()
                elif "submissions-001" in url:
                    body = json.dumps(old).encode()
                else:
                    body = ("<p>" + "Official historical company disclosure. " * 6 + "</p>" +
                            ("".join(f'<a href="ex99{i}.htm">EX-99.{i}</a>' for i in range(1, 4)) +
                             '<a href="attachment.htm">Exhibit 99.4</a><a href="#99.1">See exhibit</a>' +
                             '<a href="https://evil.example/ex99.htm">EX-99</a>' if url.endswith("current.htm") else "")).encode()
                return body, receipt(url, body)
        result = await sec_history(FakeArchive(), {"ticker": "AAA", "sec_cik": "0000000001"},
                                   datetime(2026, 8, 18, tzinfo=NY), datetime(2026, 10, 31, 23, 59, tzinfo=NY))
        assert result["status"] == "ARCHIVED" and len(result["filings"]) == 2
        assert len(result["documents"]) == 6
        assert not any("evil" in url or "future" in url for url in seen)
        doc = result["documents"][0]
        assert doc["published_at"] == "2026-08-25T12:00:00Z"
        assert doc["available_at"] == receipt()["fetched_at"]
        assert doc["classification_status"] == "PENDING_RESEARCH"
        assert doc["identity_basis"] == "CURRENT_CATALOG_DISCOVERY_NOT_HISTORICAL_MEMBERSHIP"
    asyncio.run(check())


def test_sec_rejects_wrong_issuer_and_ragged_metadata():
    class WrongIssuer:
        async def fetch(self, url, hosts):
            return b'{"cik": 2}', receipt(url)
    with pytest.raises(ValueError, match="issuer mismatch"):
        asyncio.run(sec_history(WrongIssuer(), {"ticker": "A", "sec_cik": "0000000001"}, None, None))
    malformed = columns()
    malformed["form"] = ["8-K"]
    with pytest.raises(ValueError, match="lengths differ"):
        filing_rows(malformed)


def calendar_table(day):
    return f'<table class="release-list"><tbody><tr><td>{day}</td><td>08:30 AM</td><td>Employment Situation for prior month</td></tr></tbody></table>'


def test_employment_split_footer_timezones_and_no_borrowing_next_month_date():
    html = (calendar_table("Friday, September 4, 2026") + '<p class="update"><strong>Last Modified Date: </strong>February 18, 2026'
            + calendar_table("Friday, October 2, 2026") + '<p><strong>Last Modified Date: </strong>February 18, 2026')
    result = employment_history(html, receipt(), date(2026, 9, 1), date(2026, 10, 31))
    assert len(result) == 2
    assert result[0]["available_at"] == "2026-02-19T00:00:00-05:00"
    assert result[1]["event_at"] == "2026-10-02T08:30:00-04:00"
    missing = calendar_table("Friday, September 4, 2026") + calendar_table("Friday, October 2, 2026") + '<p>Last Modified Date: February 18, 2026</p>'
    result = employment_history(missing, receipt(), date(2026, 9, 1), date(2026, 10, 31))
    assert result[0]["available_at"] is None
    assert result[1]["available_at"] is not None


def test_ivv_exact_historical_date_required_and_no_implicit_local_mapping():
    raw = b'Fund Holdings as of,"Sep 30, 2026"\nTicker,Asset Class\nAAA,Equity\nUSD,Cash\n'
    result = historical_holdings(raw, receipt(), date(2026, 9, 30))
    assert len(result["equities"]) == 1 and result["publication_timestamp"] is None
    assert result["historical_local_instrument_mapping"] == "NOT_YET_VERIFIED"
    with pytest.raises(ValueError, match="fallback rejected"):
        historical_holdings(raw, receipt(), date(2026, 9, 4))
    with pytest.raises(ValueError, match="duplicate"):
        historical_holdings(raw + b'AAA,Equity\n', receipt(), date(2026, 9, 30))


@pytest.mark.parametrize("close", ["NaN", "Infinity", "-1", "0"])
def test_vix_rejects_invalid_closes(close):
    with pytest.raises(ValueError):
        vix_history(f"DATE,CLOSE\n09/01/2026,{close}\n".encode(), receipt(), date(2026, 9, 1), date(2026, 10, 31))


def test_vix_date_filter_and_no_invented_availability():
    raw = b'DATE,CLOSE\n08/01/2026,15\n09/01/2026,20\n11/01/2026,22\n'
    rows = vix_history(raw, receipt(), date(2026, 9, 1), date(2026, 10, 31))
    assert len(rows) == 1 and rows[0]["date"] == "2026-09-01"
    assert rows[0]["publication_timestamp"] is None
    with pytest.raises(ValueError, match="duplicate"):
        vix_history(raw + b'09/01/2026,21\n', receipt(), date(2026, 9, 1), date(2026, 10, 31))


def test_pdf_extraction_and_unknown_publication_stays_unknown():
    import pymupdf
    with pymupdf.open() as pdf:
        pdf.new_page().insert_textbox(pymupdf.Rect(30, 30, 500, 500), "Official company historical disclosure. " * 10)
        raw = pdf.tobytes()
    doc = archived_document({"ticker": "A"}, raw, receipt(body=raw), None, "Report", "CNV")
    assert len(doc["text"]) > 120 and doc["published_at"] is None
    assert doc["knowledge_basis"] == "UNKNOWN_PUBLICATION_TIME"
    with pytest.raises(ValueError, match="Future publication"):
        archived_document({"ticker": "A"}, raw, receipt(body=raw), "2027-01-01T00:00:00Z", "Report", "CNV")


def test_checkpoint_detects_changed_metadata_and_raw(tmp_path):
    (tmp_path / "raw").mkdir()
    raw = tmp_path / "raw" / "sample.bin"
    raw.write_bytes(b"source")
    value = dict(ticker="AAA", status="ARCHIVED", documents=[dict(raw_path=str(raw), source_hash=digest(b"source"))])
    path = tmp_path / "issuer.json"
    write_checkpoint(path, value)
    assert read_checkpoint(path, tmp_path) == value
    saved = json.loads(path.read_text())
    saved["result"]["ticker"] = "BBB"
    path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match="checkpoint integrity"):
        read_checkpoint(path, tmp_path)
    write_checkpoint(path, value)
    raw.write_bytes(b"changed")
    with pytest.raises(ValueError, match="archive integrity"):
        read_checkpoint(path, tmp_path)


def cnv_page(blob_host="https://blob.cnv.gov.ar/BlobWebService.svc/"):
    item = dict(guid="9218508c-6725-4844-c79e-9c10db879edb", nombreArchivo="Informe.pdf", hash="publisher-opaque-hash")
    model = '<modeloDatos><propiedad uploader="attachments">' + json.dumps([item]) + '</propiedad></modeloDatos>'
    return '<title>Official presentation</title><script>var presentation=\'' + model + "'; var blobHost = \"" + blob_host + '\";</script>'


def test_cnv_extracts_attachments_without_executing_scripts_or_assuming_hash_algorithm():
    from src.thesis_desk.historical_cnv import cnv_attachments
    items = cnv_attachments(cnv_page(), "https://aif2.cnv.gov.ar/Presentations/publicview/example")
    assert len(items) == 1 and items[0]["filename"] == "Informe.pdf"
    assert items[0]["source_url"].startswith("https://blob.cnv.gov.ar/")
    assert items[0]["publisher_hash_basis"] == "UNSPECIFIED_BY_SOURCE_NOT_ASSUMED_SHA256"
    with pytest.raises(ValueError):
        cnv_attachments(cnv_page("https://evil.example/"), "https://aif2.cnv.gov.ar/example")
    with pytest.raises(ValueError, match="metadata"):
        cnv_attachments("<p>Cargando informacion</p>", "https://aif2.cnv.gov.ar/example")


def test_cnv_public_download_keeps_temporary_grant_out_of_metadata():
    from src.thesis_desk.historical_cnv import cnv_attachments, public_blob_loader
    attachment = cnv_attachments(cnv_page(), "https://aif2.cnv.gov.ar/Presentations/publicview/example")[0]
    requests = []
    def respond(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"valetKeyData": "temporary-public-download-grant"})
        assert request.url.host == "blob.cnv.gov.ar"
        assert request.content == b"ValetKey=temporary-public-download-grant"
        return httpx.Response(200, content=b"%PDF fake fixture")
    async def check():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            assert await public_blob_loader(attachment, 100)(client) == b"%PDF fake fixture"
            with pytest.raises(SourceUnavailable, match="SIZE_LIMIT"):
                await public_blob_loader(attachment, 5)(client)
    asyncio.run(check())
    assert "temporary-public" not in json.dumps(attachment)
    assert len(requests) == 4


def test_cnv_rejects_html_instead_of_pdf():
    from src.thesis_desk.historical_cnv import cnv_attachments, public_blob_loader
    attachment = cnv_attachments(cnv_page(), "https://aif2.cnv.gov.ar/Presentations/publicview/example")[0]
    async def check():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200,
                json={"valetKeyData": "public"}) if r.method == "GET" else httpx.Response(200, content=b"<p>error</p>"))) as client:
            with pytest.raises(ValueError, match="not a supported PDF"):
                await public_blob_loader(attachment, 100)(client)
    asyncio.run(check())


def test_archive_rebuild_verifies_originals_and_preserves_observation_date(tmp_path):
    from src.thesis_desk.archive_reuse import reuse_downloads
    from src.thesis_desk.contracts import dump
    source, target = tmp_path / "old", tmp_path / "new"
    (source / "raw").mkdir(parents=True)
    (source / "receipts").mkdir()
    raw = source / "raw" / "original.bin"
    raw.write_bytes(b"official")
    rec = dict(receipt(body=b"official"), raw_path=str(raw))
    path = source / "receipts" / "source.json"
    dump(path, rec)
    dump(source / "manifest.json", dict(code_changed_during_run=[], artifacts={
        str(raw.relative_to(source)): digest(raw.read_bytes()), str(path.relative_to(source)): digest(path.read_bytes())}))
    assert reuse_downloads(source, target) == 1
    copied = json.loads((target / "receipts" / "source.json").read_text())
    assert copied["fetched_at"] == rec["fetched_at"] and copied["sha256"] == rec["sha256"]
    assert str(target.resolve()) in copied["raw_path"]
    assert reuse_downloads(source, target) == 1
    raw.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="raw integrity"):
        reuse_downloads(source, target)


def test_collector_exports_partial_evidence_without_promoting_downloads(tmp_path, monkeypatch):
    from argparse import Namespace
    import scripts.collect_thesis_history as runner
    original_client = httpx.AsyncClient
    def respond(request):
        if request.url.host == "cdn-api.cboe.com":
            return httpx.Response(200, content=b'DATE,CLOSE\n09/01/2026,20\n')
        if request.url.host == "www.bls.gov":
            html = calendar_table("Friday, September 4, 2026") + '<p><strong>Last Modified Date: </strong>February 18, 2026</p>'
            return httpx.Response(200, text=html)
        day = datetime.strptime(request.url.params["asOfDate"], "%Y%m%d").strftime("%b %d, %Y")
        return httpx.Response(200, text=f'Fund Holdings as of,"{day}"\nTicker,Asset Class\nAAA,Equity\n')
    monkeypatch.setattr(runner.httpx, "AsyncClient", lambda **kw: original_client(transport=httpx.MockTransport(respond), **kw))
    monkeypatch.setattr(runner, "load_source_run", lambda p: ([dict(ticker="AAA", status="IDENTIFIED")], {"run_id": "fixture"}))
    args = Namespace(start="2026-09-01", end="2026-09-04", source_run=tmp_path / "source", output=tmp_path / "archive",
                     memory_cache_mb=16, market_capture=None, reuse_archive=None)
    summary = asyncio.run(runner.collect(args))
    evidence = json.loads((args.output / "evidence.json").read_text())
    assert evidence["facts"] == evidence["universe"] == evidence["risk"] == []
    assert len(evidence["blackouts"]) == 1
    assert all(row["status"] == "PARTIAL" for row in evidence["coverage"])
    assert summary["runtime"]["model_calls"] == 0 and summary["backtest_status"] == "NOT_EVALUABLE"
    assert summary["issuer_status"] == {"NO_SEC_MAPPING": 1}
    manifest = json.loads((args.output / "manifest.json").read_text())
    assert manifest["code_changed_during_run"] == []
    assert digest((args.output / "documents.json").read_bytes()) == manifest["artifacts"]["documents.json"]


def test_transient_http_failure_retries_but_does_not_cache_error_page(tmp_path):
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(503, content=b"unavailable") if len(calls) == 1 else httpx.Response(200, content=b"official")
    async def check():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            archive = Archive(tmp_path, client)
            raw, rec = await archive.fetch("https://www.sec.gov/example", ["www.sec.gov"])
            assert raw == b"official" and rec["sha256"] == digest(raw)
            assert archive.stats()["network_requests"] == 2
            assert len(list((tmp_path / "raw").glob("*"))) == 1
    asyncio.run(check())


def test_public_download_loader_cached_without_storing_grant(tmp_path):
    from src.thesis_desk.historical_cnv import cnv_attachments, public_blob_loader
    attachment = cnv_attachments(cnv_page(), "https://aif2.cnv.gov.ar/Presentations/publicview/example")[0]
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"valetKeyData": "ephemeral-public-grant"}) if request.method == "GET" else httpx.Response(200, content=b"%PDF fixture")
    async def check():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            archive = Archive(tmp_path, client)
            load = public_blob_loader(attachment, archive.max_bytes)
            first = await archive.fetch(attachment["source_url"], ["blob.cnv.gov.ar"], download=load)
            assert first[1]["request_method"] == "POST_PUBLIC_DOWNLOAD"
            assert await Archive(tmp_path, client).fetch(attachment["source_url"], ["blob.cnv.gov.ar"], download=load) == first
            assert len(calls) == 2
            assert "ephemeral-public-grant" not in json.dumps(first[1])
            assert archive.stats()["network_requests"] == 2
    asyncio.run(check())
