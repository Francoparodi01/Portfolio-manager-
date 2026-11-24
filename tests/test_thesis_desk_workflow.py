"""Contracts, data boundaries, source adapters and persistent paper replay."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path

import httpx
import pytest

from tests.test_thesis_desk_backtest import at, fixture, quote
from scripts.run_thesis_desk import replay, review_runtime, run_once
from src.backtesting.thesis_desk import instant
from src.thesis_desk.agents import DeskAgents
from src.thesis_desk.contracts import digest, validate_config, validate_research, validate_review, validate_selection
from src.thesis_desk.paper import PaperStore, advance_paper
from src.thesis_desk.sources import Collector, SourceUnavailable, employment_events, issuer_documents, ivv_members, normalize_market, vix_signal
from src.thesis_desk.workflow import assemble_evidence, research_book

ROOT = Path(__file__).resolve().parents[1]


def config():
    value = json.loads((ROOT / "config/thesis_desk.json").read_text(encoding="utf-8"))
    value["instruments"] = [dict(value["instruments"][0], ticker="AAA", underlying="AAA")]
    return value


def document():
    return dict(id="doc", ticker="AAA", title="Official release", source_url="https://ir.amd.com/release",
                published_at=at("2026-08-31"), available_at=at("2026-08-31", "11:00:01"), sha256="abc",
                text="The company raises full-year revenue guidance after strong customer demand. A signed agreement is binding.")


def proposal():
    return dict(findings=[dict(document_id="doc", kind="GUIDANCE_RAISED", quote="raises full-year revenue guidance",
                              thesis="La empresa elevó su proyección de ingresos.", invalidation="Un recorte oficial posterior de la guía.")])


def review(finding_id, verdict="SUPPORTED"):
    return dict(reviews=[dict(finding_id=finding_id, verdict=verdict, reason="El texto respalda el cambio de guía.",
                             counterevidence_document_id=None, counterevidence_quote=None)])


def paper_inputs():
    rows, evidence = fixture()
    rows["intraday_points"] = [dict(quote(), market_ts=at(clock="10:59:00"), ts=at(), quote_id="q1", fresh=True)]
    return rows, evidence


@pytest.mark.parametrize("change", [
    lambda c: c.update(mode="LIVE"),
    lambda c: c["instruments"][0].update(source_url="http://localhost/private"),
    lambda c: c["instruments"][0].update(ticker="AAA';DELETE"),
    lambda c: c["instruments"][0].update(currency="USD"),
    lambda c: c.update(vix_yellow="35", vix_red="25"),
    lambda c: c["llm"].update(model="nvidia/nemotron-3.5-super-vl"),
    lambda c: c["llm"].update(cost_policy="PAID_ALLOWED"),
])
def test_config_rejects_execution_currency_url_and_sql_changes(change):
    value = config()
    change(value)
    with pytest.raises(ValueError):
        validate_config(value)


def test_grounded_finding_identity_and_current_knowledge():
    result = validate_research(proposal(), [document()], "AAA", instant(at()))
    assert result[0]["ticker"] == "AAA" and result[0]["available_at"] == at()
    assert result[0]["source_url"] == document()["source_url"]
    changed = proposal()
    changed["findings"][0]["thesis"] = "Nueva redacción de la misma tesis oficial."
    assert result[0]["thesis_id"] == validate_research(changed, [document()], "AAA", instant(at()))[0]["thesis_id"]


@pytest.mark.parametrize("change", [
    lambda r: r["findings"][0].update(quote="An invented revenue growth quote"),
    lambda r: r["findings"][0].update(document_id="invented-source"),
    lambda r: r["findings"][0].update(quantity=100),
    lambda r: r["findings"].append(dict(r["findings"][0])),
])
def test_research_cannot_invent_quote_source_or_order(change):
    result = proposal()
    change(result)
    with pytest.raises(ValueError):
        validate_research(result, [document()], "AAA", instant(at()))


def test_future_source_rejected():
    doc = document()
    doc["available_at"] = at("2026-09-02")
    with pytest.raises(ValueError, match="Future"):
        validate_research(proposal(), [doc], "AAA", instant(at()))


def test_contrarian_veto_requires_actual_later_evidence():
    findings = validate_research(proposal(), [document()], "AAA", instant(at()))
    response = review(findings[0]["id"], "FACT_VETO")
    with pytest.raises(ValueError, match="counterevidence"):
        validate_review(response, findings, [document()])
    response["reviews"][0].update(counterevidence_document_id="doc", counterevidence_quote="raises full-year revenue guidance")
    with pytest.raises(ValueError, match="later"):
        validate_review(response, findings, [document()])
    later = dict(document(), id="later", published_at=at(), text="The company cut full-year revenue guidance following a lost customer.")
    response["reviews"][0].update(counterevidence_document_id="later", counterevidence_quote="cut full-year revenue guidance")
    assert validate_review(response, findings, [document(), later])
    with pytest.raises(ValueError, match="another issuer"):
        validate_review(response, findings, [dict(later, ticker="BBB")])
    with pytest.raises(ValueError, match="Every finding"):
        validate_review({"reviews": []}, findings, [document()])


def test_pm_cannot_introduce_names_or_sizes():
    candidates = [{"id": "x"}]
    valid = {"watchlist": [{"finding_id": "x", "decision": "WATCHLIST", "reason": "Tesis oficial revisada."}]}
    assert validate_selection(valid, candidates)
    valid["watchlist"][0]["quantity"] = 10
    with pytest.raises(ValueError):
        validate_selection(valid, candidates)


def test_ivv_requires_dated_equity_and_exact_symbol():
    text = b'Fund Holdings as of,"Aug 31, 2026"\nTicker,Asset Class\nAAA,Equity\nBBB,Cash\n'
    receipt = dict(fetched_at=at(), sha256="csv", source_url="https://www.ishares.com/holdings.csv")
    members = ivv_members(text, config()["instruments"], receipt)
    assert len(members) == 1 and members[0]["ticker"] == "AAA"
    with pytest.raises(ValueError, match="STALE"):
        ivv_members(text, config()["instruments"], dict(receipt, fetched_at=at("2026-10-01")))
    with pytest.raises(ValueError, match="SCHEMA"):
        ivv_members(b'Ticker,Asset Class\nAAA,Equity\n', config()["instruments"], receipt)


def test_source_redirect_cannot_exfiltrate_to_unapproved_host(tmp_path):
    calls = []
    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://attacker.example/collect"})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = Collector(tmp_path, client)
            with pytest.raises(ValueError, match="official hosts"):
                await collector.fetch("https://ir.amd.com/source", ["ir.amd.com"])
    asyncio.run(run())
    assert len(calls) == 1


def test_source_http_error_has_no_success_receipt(tmp_path):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(403))) as client:
            collector = Collector(tmp_path, client)
            with pytest.raises(SourceUnavailable, match="HTTP_403"):
                await collector.fetch("https://ir.amd.com/source", ["ir.amd.com"])
            assert not collector.receipts
    asyncio.run(run())


def test_document_version_uses_text_not_ir_page_nonce():
    class Source:
        nonce = 0
        async def fetch(self, url, hosts):
            if url.endswith("/rss"):
                body = b'<rss><channel><item><link>https://ir.amd.com/release</link><title>Release</title><pubDate>Mon, 31 Aug 2026 11:00:00 -0300</pubDate></item></channel></rss>'
            else:
                self.nonce += 1
                body = ('<html><article>' + document()["text"] * 5 + '</article><!--nonce:' + str(self.nonce) + '--></html>').encode()
            return body, dict(fetched_at=at(), sha256=digest(body), source_url=url)
    async def run():
        source = Source()
        first, _ = await issuer_documents(source, config()["instruments"][0], config())
        second, _ = await issuer_documents(source, config()["instruments"][0], config())
        assert first[0]["id"] == second[0]["id"] and first[0]["sha256"] != second[0]["sha256"]
    asyncio.run(run())


@pytest.mark.parametrize("zone", ["America/New_York", "US-Eastern"])
def test_employment_calendar_preserves_timezone_and_known_time(zone):
    body = ('BEGIN:VCALENDAR\nBEGIN:VEVENT\nSUMMARY:Employment Situation\nDTSTART;TZID=' + zone + ':20260904T083000\nEND:VEVENT\nEND:VCALENDAR').encode()
    rows = employment_events(body, dict(fetched_at=at(), source_url="https://www.bls.gov/feed"))
    assert rows[0]["event_at"] == "2026-09-04T08:30:00-04:00"
    assert instant(rows[0]["event_at"]) - instant(rows[0]["start_at"]) == timedelta(hours=24)


def test_vix_excludes_incomplete_current_day():
    previous = int(instant(at("2026-08-31", "10:00:00")).timestamp())
    current = int(instant(at("2026-09-01", "10:00:00")).timestamp())
    body = json.dumps({"chart": {"result": [{"timestamp": [previous, current], "indicators": {"quote": [{"close": [26, 99]}]}}]}}).encode()
    signal = vix_signal(body, dict(fetched_at=at(), sha256="vix", source_url="https://query1.finance.yahoo.com/vix"), config())
    assert signal["state"] == "YELLOW" and signal["vix"] == "26"


def test_persistent_paper_waits_for_next_quote_and_deduplicates():
    rows, evidence = paper_inputs()
    state, first = advance_paper(rows, evidence, config(), None, instant(at()), {})
    assert not first["fills"] and first["orders"][0]["side"] == "BUY"
    state = json.loads(json.dumps(state, default=str))
    state, duplicate = advance_paper(rows, evidence, config(), state, instant(at(clock="11:02:00")), {})
    assert not duplicate["fills"]
    rows["intraday_points"] = [dict(quote(clock="11:03:00"), market_ts=at(clock="11:03:00"), quote_id="q2", fresh=True)]
    state = json.loads(json.dumps(state, default=str))
    state, bought = advance_paper(rows, evidence, config(), state, instant(at(clock="11:03:00")), {})
    assert bought["fills"][0]["side"] == "BUY"
    state = json.loads(json.dumps(state, default=str))
    rows["intraday_points"] = [dict(quote(85, clock="11:04:00"), market_ts=at(clock="11:04:00"), quote_id="q3", fresh=True)]
    state, stopped = advance_paper(rows, evidence, config(), state, instant(at(clock="11:04:00")), {})
    assert not stopped["fills"] and stopped["orders"][0]["reason"] == "STOP"
    state = json.loads(json.dumps(state, default=str))
    rows["intraday_points"] = [dict(quote(80, clock="11:05:00"), market_ts=at(clock="11:05:00"), quote_id="q4", fresh=True)]
    state, sold = advance_paper(rows, evidence, config(), state, instant(at(clock="11:05:00")), {})
    assert sold["fills"][0]["side"] == "SELL" and len(sold["closed_cycles"]) == 1


def test_paper_data_block_cancels_pending_entry():
    rows, evidence = paper_inputs()
    state, _ = advance_paper(rows, evidence, config(), None, instant(at()), {})
    state = json.loads(json.dumps(state, default=str))
    rows["intraday_points"] = [dict(quote(clock="11:02:00"), market_ts=at(clock="11:02:00"), quote_id="q2", fresh=True)]
    evidence["facts"] = []
    _, result = advance_paper(rows, evidence, config(), state, instant(at(clock="11:02:00")), {"AAA": ["CALENDAR_UNKNOWN"]})
    assert not result["orders"] and not result["fills"]


@pytest.mark.parametrize("after_hours", [False, True])
def test_cited_contrarian_veto_exits_existing_paper_position_despite_entry_blocks(after_hours):
    rows, evidence = paper_inputs()
    state, _ = advance_paper(rows, evidence, config(), None, instant(at()), {})
    rows["intraday_points"] = [dict(quote(clock="11:03:00"), market_ts=at(clock="11:03:00"), quote_id="q2")]
    state, bought = advance_paper(rows, evidence, config(), json.loads(json.dumps(state, default=str)), instant(at(clock="11:03:00")), {})
    assert bought["fills"][0]["side"] == "BUY"
    fact = state["positions"]["AAA"]["fact"]
    doc = dict(document(), id="adverse", published_at=at(clock="11:03:30"), available_at=at(clock="11:03:40"),
               text="The company cuts its full-year guidance after a contract cancellation.")
    checked = review(fact["id"], "FACT_VETO")["reviews"][0]
    checked.update(counterevidence_document_id=doc["id"], counterevidence_quote="company cuts its full-year guidance")
    research = dict(findings=[fact], reviews=[checked], admitted=[], selection=[], errors=[])
    bundle = dict(documents=[doc], universe=evidence["universe"], risk=evidence["risk"], blackouts=[], source_status=[])
    updated, blocks, _ = assemble_evidence(bundle, research, rows, [], [], config(), instant(at(clock="11:04:00")))
    assert updated["vetoes"][0]["thesis_id"] == fact["thesis_id"] and blocks["AAA"]
    day = "2026-09-01"
    if after_hours:
        state, closed = advance_paper(rows, updated, config(), json.loads(json.dumps(state, default=str)), instant(at(clock="18:00:00")), blocks)
        assert not closed["fills"] and state["invalidation_evidence"]["vetoes"]
        updated = deepcopy(updated)
        updated["vetoes"] = []  # The next capture can lose the source; the known invalidation survives.
        day = "2026-09-02"
    rows["intraday_points"] = [dict(quote(clock="11:04:00"), ts=at(day, "11:04:00"), market_ts=at(day, "11:04:00"), quote_id="q3")]
    state, pending = advance_paper(rows, updated, config(), json.loads(json.dumps(state, default=str)), instant(at(day, "11:04:00")), blocks)
    assert not pending["fills"] and pending["orders"][0]["reason"] == "FACT_INVALIDATION"
    rows["intraday_points"] = [dict(quote(clock="11:05:00"), ts=at(day, "11:05:00"), market_ts=at(day, "11:05:00"), quote_id="q4")]
    _, sold = advance_paper(rows, updated, config(), json.loads(json.dumps(state, default=str)), instant(at(day, "11:05:00")), blocks)
    assert sold["fills"][0]["side"] == "SELL" and len(sold["closed_cycles"]) == 1


def test_market_closed_does_not_trade_or_invent_fill():
    rows, evidence = paper_inputs()
    _, result = advance_paper(rows, evidence, config(), None, instant(at(clock="18:00:00")), {})
    assert result["status"] == "WAITING_MARKET_OPEN" and not result["fills"] and not result["orders"]


@pytest.mark.parametrize("limit", [0, 41])
def test_run_limit_is_validated_before_capture(tmp_path, limit):
    with pytest.raises(ValueError, match="max-model-calls"):
        asyncio.run(run_once(config(), tmp_path / "invalid", max_model_calls=limit))
    assert not (tmp_path / "invalid").exists()


@pytest.mark.parametrize("options", [
    {"documents_per_batch": 0}, {"documents_per_batch": 9}, {"documents_per_batch": True},
    {"model_timeout_seconds": 59}, {"model_timeout_seconds": 3601}, {"model_timeout_seconds": True},
    {"pm_reserve_calls": -1}, {"pm_reserve_calls": True}, {"max_model_calls": 2, "pm_reserve_calls": 3},
])
def test_runtime_limits_fail_before_capture(tmp_path, options):
    with pytest.raises(ValueError):
        asyncio.run(run_once(config(), tmp_path / "invalid", **options))
    assert not (tmp_path / "invalid").exists()


def test_cpu_limits_preserve_immutable_portfolio_config():
    cfg = config()
    cfg["llm"] = dict(provider="ollama", model="qwen3.5:9b", cost_policy="FREE_ONLY")
    before = deepcopy(cfg)
    runtime = review_runtime(cfg, max_model_calls=8)
    assert runtime["research_budget"]["documents_per_batch"] == 1
    assert runtime["model_timeout_seconds"] == 1800 and runtime["pm_reserve_calls"] == 1
    assert cfg == before
    remote = review_runtime(config())
    assert remote["research_budget"]["documents_per_batch"] == 8 and remote["model_timeout_seconds"] == 300


def test_paper_state_config_lock_rollback_and_commit(tmp_path):
    path = tmp_path / "paper.sqlite"
    store = PaperStore(path, config())
    store.cache("x", {"y": 1})
    store.close()
    store = PaperStore(path, config())
    assert store.cached("x") == {"y": 1}  # Public review work survives a rolled-back portfolio run.
    assert store.load() is None
    store.finish("run", instant(at()), {"example": 1}, {"mode": "PAPER_ONLY"})
    store.close()
    modified = config()
    modified["initial_cash"] = "12345"
    with pytest.raises(ValueError, match="immutable"):
        PaperStore(path, modified)


def test_unknown_calendars_exclude_entry_but_preserve_negative_facts():
    rows, ev = fixture()
    positive = dict(ev["facts"][0], thesis="Thesis")
    negative = dict(positive, id="kill", kind="GUIDANCE_CUT", thesis_id="*")
    bundle = dict(universe=ev["universe"], risk=ev["risk"], blackouts=[], source_status=[dict(ticker="AAA", status="CAPTURED")])
    research = dict(admitted=[positive, negative], selection=[dict(finding_id=positive["id"], decision="WATCHLIST", reason="Supported")], errors=[])
    evidence, blocks, cards = assemble_evidence(bundle, research, rows, [], [], config(), instant(at()))
    assert evidence["facts"] == [negative]
    assert "EMPLOYMENT_CALENDAR_UNKNOWN" in blocks["AAA"] and "EARNINGS_CALENDAR_UNKNOWN" in blocks["AAA"]
    assert cards[0]["zone_low"] is not None


def test_model_failure_is_visible_and_does_not_create_thesis(tmp_path):
    class FailedAgents:
        provider, model, traces = "test", "test", []
        async def research(self, *args):
            raise ValueError("bad response")
    store = PaperStore(tmp_path / "paper.sqlite", config())
    try:
        result = asyncio.run(research_book(dict(documents=[document()], universe=[]), FailedAgents(), store))
        assert not result["admitted"] and result["errors"][0]["stage"] == "RESEARCH_OR_CONTRARIAN"
    finally:
        store.close()


def test_real_role_pipeline_contract_and_cache_with_mock_api(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-only-not-a-key")
    monkeypatch.setenv("QUANTIA_AGENT_MODEL", "paid-production-model")
    seen = []
    def handler(request):
        body = json.loads(request.content)
        assert body["model"] == config()["llm"]["model"]
        assert body["max_tokens"] == 8192
        assert body["model"].endswith(":free") and "tools" not in body
        data = json.loads(body["messages"][1]["content"])
        if "ticker" in data:
            seen.append("RESEARCH")
            answer = proposal()
        elif "findings" in data:
            seen.append("CONTRARIAN")
            answer = review(data["findings"][0]["id"])
        else:
            seen.append("PM")
            answer = {"watchlist": [dict(finding_id=data["candidates"][0]["id"], decision="WATCHLIST", reason="Tesis respaldada por el comunicado.")]}
        return httpx.Response(200, json={"choices": [{"finish_reason": "stop", "message": {"content": json.dumps(answer)}}]})
    async def run():
        store = PaperStore(tmp_path / "paper.sqlite", config())
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                bundle = dict(documents=[document()], universe=[dict(ticker="AAA")])
                first = await research_book(bundle, DeskAgents(config()["llm"], client), store)
                assert first["selection"][0]["decision"] == "WATCHLIST" and len(first["admitted"]) == 1
                second = await research_book(bundle, DeskAgents(config()["llm"], client), store)
                assert second["cache_hits"] == ["AAA", "PM"] and second["admitted"] == first["admitted"]
                assert seen == ["RESEARCH", "CONTRARIAN", "PM"]
        finally:
            store.close()
    asyncio.run(run())


def test_provider_credit_failure_is_visible_without_repeated_requests(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-only-not-a-key")
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(402, json={"error": "not archived"})
    async def run():
        store = PaperStore(tmp_path / "paper.sqlite", config())
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                bundle = dict(documents=[document(), dict(document(), id="second", ticker="BBB")], universe=[])
                result = await research_book(bundle, DeskAgents(config()["llm"], client), store)
                assert len(requests) == 1 and len(result["errors"]) == 2
                assert all(e["error_code"] == "HTTP_402" for e in result["errors"])
                assert not result["admitted"] and result["model_traces"][-1]["status"] == "SKIPPED_PROVIDER_UNAVAILABLE"
        finally:
            store.close()
    asyncio.run(run())


def test_late_delivery_of_predecision_quote_cannot_fill():
    rows, evidence = paper_inputs()
    state, _ = advance_paper(rows, evidence, config(), None, instant(at()), {})
    delayed = deepcopy(rows)
    delayed["intraday_points"][0].update(quote_id="delayed-record", market_ts=at(clock="10:59:30"), ts=at(clock="11:02:00"))
    _, result = advance_paper(delayed, evidence, config(), state, instant(at(clock="11:02:00")), {})
    assert not result["fills"] and result["orders"][0]["side"] == "BUY"


def test_late_corporate_record_predating_fill_does_not_double_holding():
    rows, evidence = paper_inputs()
    state, _ = advance_paper(rows, evidence, config(), None, instant(at()), {})
    rows["intraday_points"][0].update(quote_id="q2", market_ts=at(clock="11:02:00"), ts=at(clock="11:02:00"))
    state, bought = advance_paper(rows, evidence, config(), state, instant(at(clock="11:02:00")), {})
    original_qty = state["positions"]["AAA"]["qty"]
    assert bought["fills"][0]["side"] == "BUY"
    rows["corporate_effects"] = [dict(id=77, ticker="AAA", effective_at=at("2026-08-28"))]
    state, result = advance_paper(rows, evidence, config(), state, instant(at(clock="11:03:00")), {})
    assert state["positions"]["AAA"]["qty"] == original_qty and not result["fills"]


def market_fixture(captured):
    rows, _ = fixture()
    data = dict(identities=[dict(ticker="AAA", asset_type="CEDEAR", currency="ARS", source_ticker="AAA")],
                quotes=[quote()], closes=[dict(ticker=r["ticker"], asset_type=r["asset_type"], currency=r["currency"],
                                               ts=r["ts"], last_price=r["close_price"]) for r in rows["daily_candles"]],
                earnings=[], corporate_effects=[], conflicts=[])
    return dict(captured_at=captured, observations=[dict(name=k, rows=v) for k, v in data.items()])


def test_market_bars_become_known_only_at_current_capture():
    rows, _, _ = normalize_market(market_fixture(at()), config()["instruments"])
    assert rows["daily_candles"][0]["available_at"] == at()
    assert rows["daily_candles"][0]["source"] == "DESK_RECORDED_LATE_SESSION_QUOTE"
    packet = market_fixture(at())
    next(o for o in packet["observations"] if o["name"] == "quotes")["rows"][0]["asset_type"] = "BOND"
    with pytest.raises(ValueError, match="identity"):
        normalize_market(packet, config()["instruments"])


def test_whole_workflow_records_archives_and_replays_without_network(tmp_path):
    from src.thesis_desk.contracts import now
    class FakeAgents:
        provider, model, traces = "fixture", "fixture", []
        async def research(self, ticker, docs):
            return []
    async def collector(cfg, directory):
        return dict(documents=[document()], universe=[], risk=[], blackouts=[],
                    source_status=[dict(ticker="AAA", status="CAPTURED")], receipts=[])
    def market(*args):
        return market_fixture(now().isoformat())
    directory, result = asyncio.run(run_once(config(), tmp_path, agents=FakeAgents(), collector=collector, market_reader=market))
    assert result["mode"] == "PAPER_ONLY" and (tmp_path / "latest.json").exists()
    assert replay(directory)["status"] == "REPLAY_MATCH"
    (directory / "result.json").write_text("{}")
    with pytest.raises(ValueError, match="integrity"):
        replay(directory)


def test_runner_owns_timeout_client_and_archives_runtime_separately(tmp_path, monkeypatch):
    from scripts import run_thesis_desk as runner
    from src.thesis_desk.contracts import now
    created = []
    class Agent:
        provider, model, traces = "fixture", "fixture", []
        def __init__(self, llm, client, budget):
            self.client = client
            created.append(self)
        async def research(self, ticker, docs):
            assert not self.client.is_closed
            assert self.client.timeout.read == 900 and self.client.timeout.connect == 10
            return []
    async def collector(cfg, directory):
        return dict(documents=[document()], universe=[], risk=[], blackouts=[],
                    source_status=[dict(ticker="AAA", status="CAPTURED")], receipts=[])
    monkeypatch.setattr(runner, "DeskAgents", Agent)
    cfg = config()
    directory, _ = asyncio.run(run_once(cfg, tmp_path, collector=collector,
        market_reader=lambda *args: market_fixture(now().isoformat()), documents_per_batch=1, model_timeout_seconds=900))
    assert created[0].client.is_closed
    assert json.loads((directory / "config.json").read_text()) == cfg
    runtime = json.loads((directory / "runtime.json").read_text())
    assert runtime["model_timeout_seconds"] == 900 and runtime["research_budget"]["documents_per_batch"] == 1
    assert "runtime.json" in json.loads((directory / "manifest.json").read_text())["artifacts"]
    assert replay(directory)["status"] == "REPLAY_MATCH"
