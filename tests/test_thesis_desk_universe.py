"""Full-universe identity, review resumption, prospective measurements and free local transport."""
import asyncio
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import json

import httpx
import pytest

from tests.test_thesis_desk_workflow import config, document, paper_inputs
from tests.test_thesis_desk_backtest import at, fixture
from src.backtesting.thesis_desk import instant, validate_evidence
from src.thesis_desk.agents import DeskAgents, citation_options, normalize_local_response, response_schema, unique_json_object
from src.thesis_desk.catalog import caja_catalog, comafi_catalog, resolve_catalog
from src.thesis_desk.contracts import digest, qualification_error, validate_llm
from src.thesis_desk.dashboard import render_dashboard
from src.thesis_desk.earnings import calendar_evidence
from src.thesis_desk.expanded_sources import CachedCollector, cnv_rows, make_document
from src.thesis_desk.measurement import measure, observe, outcome_metrics, session_target
from src.thesis_desk.paper import PaperStore, advance_paper
from src.thesis_desk.research_queue import BudgetExhausted, CallBudget, review_queue
from scripts.run_thesis_desk import ReviewRunBudget, review_runtime, seed_sources


RECEIPT = dict(source_url="https://cajadevalores.com.ar/Servicios/Cedears", sha256="test", fetched_at=at())


def test_depositary_stock_row_headers_do_not_expand_table_schema():
    html = '''<table><tr><th>CEDEAR de Acciones</th><th>Símbolo BYMA</th><th>Ticker en Mercado de Origen</th></tr>
    <tr><th>Example Inc</th><td>AAA</td><td>ABC</td></tr></table>
    <table><tr><th>CEDEAR de ETF</th><th>Símbolo BYMA</th><th>Ticker en Mercado de Origen</th></tr>
    <tr><td>A fund</td><td>ETF</td><td>FUND</td></tr></table>'''
    rows = caja_catalog(html, RECEIPT)
    assert [(r["ticker"], r["underlying"], r["underlying_kind"]) for r in rows] == [("AAA", "ABC", "COMMON_STOCK"), ("ETF", "FUND", "ETF")]


def test_comafi_explicit_underlying_and_adr_identity():
    body = json.dumps(dict(products=[dict(name="LOCAL", section="Cedear Shares", keys="ADR, ", summary="Issuer",
                                         description="<ul><li>Ticker en mercado de origen: REMOTE</li></ul>")]))
    row = comafi_catalog(body, RECEIPT)[0]
    assert row["underlying"] == "REMOTE" and row["underlying_kind"] == "DEPOSITARY_RECEIPT"
    assert row["available_at"] == at()


def test_full_catalog_preserves_unknowns_funds_and_conflicts():
    market = dict(instruments=[dict(ticker=t, asset_type="CEDEAR", currency="ARS") for t in ("A", "B", "C", "D")], registry=[])
    deposits = [dict(ticker="A", underlying="ABC", underlying_kind="COMMON_STOCK", issuer_market="NYSE"),
                dict(ticker="B", underlying="BBB", underlying_kind="ETF"),
                dict(ticker="C", underlying="CCC", underlying_kind="COMMON_STOCK"),
                dict(ticker="C", underlying="OTHER", underlying_kind="COMMON_STOCK")]
    sec = dict(fields=["ticker", "cik", "name"], data=[["ABC", 123, "Exact issuer"]])
    rows = resolve_catalog(market, deposits, [], sec, [])
    assert [r["status"] for r in rows] == ["IDENTIFIED", "EXCLUDED_ETF", "IDENTITY_CONFLICT", "IDENTITY_UNKNOWN"]
    assert rows[0]["sec_cik"] == "0000000123" and len(rows) == 4


def test_extended_universe_requires_official_program_and_excludes_funds():
    _, evidence = fixture()
    member = evidence["universe"][0]
    member.update(sleeve="EXTENDED", membership_source="COMAFI_OFFICIAL_PROGRAM", underlying_kind="DEPOSITARY_RECEIPT")
    validate_evidence(evidence)
    member["membership_source"] = "MODEL_GUESS"
    with pytest.raises(ValueError):
        validate_evidence(evidence)
    member.update(membership_source="COMAFI_OFFICIAL_PROGRAM", underlying_kind="ETF")
    with pytest.raises(ValueError):
        validate_evidence(evidence)


def test_shared_underlying_programs_are_blocked_without_guessing_correct_issuer():
    market = dict(instruments=[dict(ticker=t, asset_type="CEDEAR", currency="ARS") for t in ("GEV", "TLN")], registry=[])
    deposits = [dict(ticker=t, underlying="GEV", underlying_kind="COMMON_STOCK", issuer_market="NYSE") for t in ("GEV", "TLN")]
    rows = resolve_catalog(market, deposits, [], {}, [])
    assert all(r["status"] == "IDENTITY_CONFLICT" and r["conflicting_tickers"] == ["GEV", "TLN"] for r in rows)


def test_official_document_identity_is_scoped_to_instrument():
    receipt = dict(RECEIPT, fetched_at=at())
    body = ('<article>' + document()["text"] * 3 + '</article>').encode()
    first = make_document(dict(ticker="AAA"), body, receipt, at("2026-08-31"), "First", "SEC")
    second = make_document(dict(ticker="BBB"), body, receipt, at("2026-08-31"), "First", "SEC")
    assert first["id"] != second["id"] and first["sha256"] == second["sha256"]


def test_cnv_repeated_presentations_do_not_consume_the_document_limit():
    def row(url):
        return '<tr><td>06 nov 2026 12:00</td><td>Issuer</td><td>Official filing</td><td><a href="' + url + '">Read</a></td></tr>'
    result = cnv_rows('<table>' + row('/one') * 2 + row('/two') + '</table>')
    assert len(result) == 2 and result[0]["source_url"] != result[1]["source_url"]


def test_free_budget_persists_attempts_across_failed_runs(tmp_path):
    policy = dict(max_calls_per_run=2, max_calls_per_day=2)
    budget = CallBudget(tmp_path / "usage.sqlite", policy, "one")
    budget.reserve("RESEARCH")
    second = CallBudget(tmp_path / "usage.sqlite", policy, "two")
    second.reserve("CONTRARIAN")
    with pytest.raises(BudgetExhausted):
        budget.reserve("PM")
    assert second.summary()["attempts_today"] == 2 and second.used == 1


def test_pm_reservation_finishes_ready_issuers_without_dropping_pending_documents(tmp_path):
    docs = [document(), dict(document(), id="second", ticker="BBB")]
    finding = dict(id="finding", document_id=docs[0]["id"], ticker="AAA", kind="GUIDANCE_RAISED", published_at=docs[0]["published_at"])
    runtime = review_runtime(config(), max_model_calls=3, documents_per_batch=1)
    cfg = dict(config(), research_budget=runtime["research_budget"])
    class Agent:
        model, provider, traces, blocked_code = "fixture", "fixture", [], None
        def __init__(self, run_id):
            self.budget = ReviewRunBudget(tmp_path / "usage.sqlite", runtime["research_budget"], run_id, 1)
            self.researched = []
        async def research_many(self, batch):
            self.budget.reserve("RESEARCH")
            self.researched.extend(d["id"] for d in batch)
            return [finding] if batch[0]["ticker"] == "AAA" else []
        async def review(self, findings, documents):
            self.budget.reserve("CONTRARIAN")
            assert documents == [docs[0]]
            return [dict(finding_id="finding", verdict="SUPPORTED")]
        async def select(self, candidates):
            self.budget.reserve("PM")
            return [dict(finding_id="finding", decision="WATCHLIST", reason="Valid thesis")]
    store = PaperStore(tmp_path / "paper.sqlite", config())
    bundle = dict(documents=docs, universe=[dict(ticker="AAA"), dict(ticker="BBB")])
    first = asyncio.run(review_queue(bundle, Agent("one"), store, cfg))
    assert first["selection"][0]["decision"] == "WATCHLIST" and first["budget"]["attempts_this_run"] == 3
    assert [(r["ticker"], r["stage"]) for r in first["queue"]] == [("BBB", "RESEARCH")]
    store.close()
    store = PaperStore(tmp_path / "paper.sqlite", config())
    second_agent = Agent("two")
    cfg["research_budget"]["documents_per_batch"] = 2
    second = asyncio.run(review_queue(bundle, second_agent, store, cfg))
    assert second_agent.researched == ["second"] and not second["queue"]
    assert second["selection"] == first["selection"] and second["budget"]["attempts_this_run"] == 1
    store.close()


def test_pm_reservation_does_not_bypass_daily_budget(tmp_path):
    policy = dict(max_calls_per_run=2, max_calls_per_day=2)
    first = ReviewRunBudget(tmp_path / "usage.sqlite", policy, "one", 1)
    first.reserve("RESEARCH")
    with pytest.raises(BudgetExhausted):
        first.reserve("CONTRARIAN")
    first.reserve("PM")
    second = ReviewRunBudget(tmp_path / "usage.sqlite", policy, "two", 1)
    with pytest.raises(BudgetExhausted):
        second.reserve("PM")
    assert second.summary()["attempts_today"] == 2 and second.used == 0


def test_batched_findings_cannot_claim_another_issuers_quote():
    doc = document()
    options = citation_options(doc)
    assert options and all(value in doc["text"] and 3 <= len(value.split()) <= 25 for value in options.values())
    response = {"findings": [{"document_id": "D2", "kind": "GUIDANCE_RAISED", "quote_id": "Q1", "thesis": "Tesis válida", "invalidation": "Recorte oficial"}]}
    agent = DeskAgents(config()["llm"])
    async def ask(*args):
        return response
    agent.ask = ask
    with pytest.raises(ValueError, match="Unknown document"):
        asyncio.run(agent.research_many([doc]))


def test_queue_resumes_contrarian_without_repeating_research(tmp_path):
    doc = document()
    finding = dict(id="finding", document_id=doc["id"], ticker="AAA", kind="GUIDANCE_RAISED", published_at=doc["published_at"])
    class Agent:
        model, provider, traces, budget, blocked_code = "fixture", "fixture", [], None, None
        researches = 0
        fails = True
        async def research_many(self, docs):
            self.researches += 1
            return [finding]
        async def review(self, findings, docs):
            if self.fails:
                raise ValueError("invalid review")
            return [dict(finding_id="finding", verdict="SUPPORTED")]
        async def select(self, candidates):
            return [dict(finding_id="finding", decision="WATCHLIST", reason="Valid thesis")]
    agent = Agent()
    bundle = dict(documents=[doc], universe=[dict(ticker="AAA")])
    store = PaperStore(tmp_path / "paper.sqlite", config())
    first = asyncio.run(review_queue(bundle, agent, store, config()))
    assert first["queue"][0]["stage"] == "CONTRARIAN" and not first["selection"]
    store.close()
    store = PaperStore(tmp_path / "paper.sqlite", config())
    agent.fails = False
    second = asyncio.run(review_queue(bundle, agent, store, config()))
    assert agent.researches == 1 and not second["queue"] and second["selection"]
    store.close()


def test_new_issuer_document_refreshes_review_without_repeating_old_research(tmp_path):
    doc = document()
    later = dict(doc, id="later", published_at=at(), text="The company cuts its guidance after losing a contract.")
    finding = dict(id="finding", document_id=doc["id"], ticker="AAA", kind="GUIDANCE_RAISED", published_at=doc["published_at"])
    class Agent:
        model, provider, traces, budget, blocked_code = "fixture", "fixture", [], None, None
        researched, contexts = [], []
        async def research_many(self, docs):
            self.researched.extend(d["id"] for d in docs)
            return [finding] if any(d["id"] == doc["id"] for d in docs) else []
        async def review(self, findings, docs):
            self.contexts.append({d["id"] for d in docs})
            return [dict(finding_id="finding", verdict="FACT_VETO" if len(docs) > 1 else "SUPPORTED")]
        async def select(self, candidates):
            return [dict(finding_id="finding", decision="WATCHLIST", reason="Valid thesis")]
    agent = Agent()
    store = PaperStore(tmp_path / "paper.sqlite", config())
    bundle = dict(documents=[doc], universe=[dict(ticker="AAA")])
    first = asyncio.run(review_queue(bundle, agent, store, config()))
    assert first["selection"]
    bundle["documents"].append(later)
    second = asyncio.run(review_queue(bundle, agent, store, config()))
    assert agent.researched.count(doc["id"]) == 1 and agent.contexts[-1] == {doc["id"], "later"}
    assert not second["admitted"] and not second["selection"] and not second["queue"]
    store.close()


def test_malformed_batch_is_isolated_without_losing_other_documents(tmp_path):
    docs = [dict(document(), id="one"), dict(document(), id="two")]
    class Agent:
        model, provider, traces, budget, blocked_code = "fixture", "fixture", [], None, None
        async def research_many(self, documents):
            if len(documents) > 1 or documents[0]["id"] == "one":
                raise ValueError("Invalid response")
            return []
    store = PaperStore(tmp_path / "paper.sqlite", config())
    result = asyncio.run(review_queue(dict(documents=docs, universe=[]), Agent(), store, config()))
    assert result["documents_reviewed"] == 1
    assert [q["document_id"] for q in result["queue"]] == ["one"]
    assert len(result["errors"]) == 1
    store.close()


@pytest.mark.parametrize("done,reason,raises", [(True, "stop", False), (True, "length", True), (False, "stop", True)])
def test_local_model_has_no_key_or_paid_endpoint_and_rejects_truncation(done, reason, raises):
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=dict(done=done, done_reason=reason, message=dict(content='{"findings":{"D1":null}}')))
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            agent = DeskAgents(dict(provider="ollama", model="qwen3.5:9b", cost_policy="FREE_ONLY"), client=client)
            return await agent.research_many([document()])
    if raises:
        with pytest.raises(Exception, match="interrupted or truncated"):
            asyncio.run(run())
    else:
        assert asyncio.run(run()) == []
    assert str(calls[0].url) == "http://127.0.0.1:11434/api/chat"
    assert "authorization" not in calls[0].headers
    assert json.loads(calls[0].content)["think"] is False
    assert json.loads(calls[0].content)["format"]["properties"]["findings"]["type"] == "object"


@pytest.mark.parametrize("value", [
    {"findings": {}}, {"findings": {"D1": None, "unknown": None}}, {"findings": []},
    {"findings": {"D1": [{"kind": "GUIDANCE_RAISED"}]}},
    {"findings": {"D1": {"document_id": "other"}}},
])
def test_local_decoding_cannot_omit_add_or_repeat_document_identity(value):
    with pytest.raises(ValueError):
        normalize_local_response("RESEARCH", {"documents": [{"id": "D1"}]}, value)


def test_duplicate_json_keys_are_rejected_before_normalizing():
    with pytest.raises(ValueError, match="Duplicate JSON field"):
        json.loads('{"findings":{"D1":null,"D1":{"kind":"GUIDANCE_RAISED"}}}', object_pairs_hook=unique_json_object)


def test_local_keyed_contract_preserves_all_decisions_and_null_research():
    data = dict(documents=[dict(id="D1", citations={"Q1": "an exact source quote"}), dict(id="D2", citations={})])
    schema = response_schema("RESEARCH", data)["properties"]["findings"]
    assert schema["required"] == ["D1", "D2"] and schema["additionalProperties"] is False
    assert schema["properties"]["D2"] == {"type": "null"}
    assert normalize_local_response("RESEARCH", data, {"findings": {"D1": None, "D2": None}}) == {"findings": []}
    for role, field, source in (("PM", "watchlist", "candidates"), ("CONTRARIAN", "reviews", "findings")):
        source_data = {source: [{"id": "F1"}, {"id": "F2"}]}
        result = normalize_local_response(role, source_data, {field: {"F2": {"reason": "second"}, "F1": {"reason": "first"}}})
        assert [r["finding_id"] for r in result[field]] == ["F1", "F2"]
        with pytest.raises(ValueError, match="Invalid keyed"):
            normalize_local_response(role, source_data, {field: {"F1": None, "F2": {"reason": "second"}}})


def test_arbitrary_paid_model_is_rejected():
    with pytest.raises(ValueError):
        validate_llm(dict(provider="openrouter", model="paid-model", cost_policy="FREE_ONLY"))


@pytest.mark.parametrize("kind,quote,allowed", [
    ("GUIDANCE_REAFFIRMED", "Annual Guidance Indicators Economic Perspectives Expanded Loan Portfolio", False),
    ("GUIDANCE_REAFFIRMED", "The company reaffirms its full-year guidance today", True),
    ("DEAL_SIGNED", "have entered into a Pricing Agreement, dated October", False),
    ("DEAL_SIGNED", "entered into a definitive acquisition agreement with the company", True),
    ("GUIDANCE_REAFFIRMED", "Fitch and S&P Global Ratings revised the outlook while reaffirming credit ratings", False),
    ("GUIDANCE_RAISED", "The company raises earnings guidance following higher tax credits", True),
    ("DEAL_CLOSED", "Reason for Notification An acquisition or disposal of voting rights", False),
    ("DEAL_CLOSED", "The company completed the acquisition of the entire equity interests", True),
    ("DEAL_CLOSED", "The company discussed a potential acquisition with its advisers", False),
    ("DEAL_CLOSED", "The company completed its own shares repurchase program", False),
    ("DEAL_SIGNED", "Applied entered into an accelerated share purchase agreement to acquire shares of common stock", False),
    ("DEAL_SIGNED", "There is no binding agreement in relation to the proposed acquisition", False),
    ("DEAL_CANCELLED", "As no binding agreement in relation to the proposed offering of notes has", False),
    ("DEAL_CANCELLED", "The company terminated the definitive merger agreement yesterday", True),
    ("DEAL_CANCELLED", "The acquisition agreement was not terminated", False),
    ("LEGAL_ADVERSE", "the Company's ability to successfully execute its digital asset acquisition strategy", False),
    ("LEGAL_ADVERSE", "The regulator filed a lawsuit against the company yesterday", True),
    ("LEGAL_ADVERSE", "A lawsuit could be filed against the company in the future", False),
    ("LEGAL_ADVERSE", "The terms used are defined in the acquisition agreement", False),
    ("LEGAL_ADVERSE", "The court profiled the company in its annual publication", False),
])
def test_explicit_guidance_and_operating_thesis_required(kind, quote, allowed):
    assert (qualification_error(kind, quote) is None) is allowed


def test_contrarian_schema_binds_each_counterquote_to_its_later_issuer_document():
    fact = dict(id="f", ticker="AAA", kind="GUIDANCE_RAISED", published_at=at())
    docs = [dict(id="later", ticker="AAA", published_at=at("2026-09-02"), citations={"Q1": "the company lowered its guidance"}),
            dict(id="foreign", ticker="BBB", published_at=at("2026-09-02"), citations={"Q1": "the company cancelled the merger"}),
            dict(id="old", ticker="AAA", published_at=at("2026-08-30"), citations={"Q1": "the company cancelled the agreement"})]
    schema = response_schema("CONTRARIAN", dict(findings=[fact], documents=docs))
    choices = schema["properties"]["reviews"]["properties"]["f"]["anyOf"]
    assert len(choices) == 2  # Only the later same-issuer source can ground a veto.
    veto = choices[1]["properties"]
    assert veto["counterevidence_document_id"] == {"const": "later"}
    assert veto["counterevidence_quote"] == {"enum": ["the company lowered its guidance"]}


def test_calendar_does_not_infer_absence_or_backdate_availability():
    snapshot = dict(captured_at=at(), start="2026-09-01", end="2026-12-01", source_url="https://finance.yahoo.com/calendar/earnings",
                    rows=[dict(symbol="ABC", event_at="2026-09-02T21:00:00+00:00")])
    catalog = [dict(status="IDENTIFIED", ticker=t, underlying=u, asset_type="CEDEAR") for t,u in (("AAA","ABC"),("BBB","BCD"))]
    events, covered = calendar_evidence(snapshot, catalog, instant(at()))
    assert covered == ["AAA"] and events[0]["available_at"] == at()
    assert events[0]["start_at"] == at(clock="00:00:00")
    with pytest.raises(ValueError, match="Stale/future"):
        calendar_evidence(snapshot, catalog, instant(at()) + timedelta(hours=7))


def test_outcomes_are_unique_mature_and_net_of_two_sided_costs(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", config())
    started = instant(at())
    result = dict(catalog=[dict(ticker="AAA", status="IDENTIFIED")], paper=dict(market_state="OPEN"),
                  research=dict(findings=[dict(id="one", kind="GUIDANCE_RAISED", quote="The company raises its earnings guidance")]),
                  watchlist=[dict(ticker="AAA", fact_id="one", pm_decision="WATCHLIST")])
    observe(store, dict(intraday_points=[]), result, started)
    observe(store, dict(intraday_points=[]), result, started + timedelta(seconds=2))
    first = outcome_metrics(store, started)[0]
    assert first["cohort"] == 1 and first["pending_anchor"] == 1 and first["mean_net_hypothetical_return_pct"] is None
    anchor = started + timedelta(minutes=1)
    observe(store, dict(intraday_points=[dict(ticker="AAA", market_ts=anchor.isoformat(), last_price="100")]), result, anchor)
    assert outcome_metrics(store, anchor)[0]["immature"] == 1
    target = session_target(anchor, 5)
    assert outcome_metrics(store, target)[0]["missing"] == 1
    observe(store, dict(intraday_points=[dict(ticker="AAA", market_ts=target.isoformat(), last_price="110")]), result, target)
    actual = outcome_metrics(store, target)[0]
    expected = (Decimal("110") * Decimal("0.9985") * Decimal("0.994") / (Decimal("100") * Decimal("1.0015") * Decimal("1.006")) - 1) * 100
    assert actual["cohort"] == actual["evaluated"] == actual["mature"] == 1
    assert Decimal(actual["mean_net_hypothetical_return_pct"]) == expected
    assert outcome_metrics(store, target)[1]["mean_net_hypothetical_return_pct"] is None
    store.close()


def test_invalid_or_missing_signal_evidence_stays_visible_outside_outcome_denominator(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", config())
    for sid in ("rating", "unknown"):
        store.conn.execute("INSERT INTO desk_signals VALUES(?,?,?,NULL,NULL)", (sid, "AAA", at()))
    store.conn.execute("INSERT INTO desk_signal_evidence VALUES(?,?)", ("rating", json.dumps(dict(kind="GUIDANCE_REAFFIRMED", quote="Ratings agency reaffirms the credit outlook"))))
    result = outcome_metrics(store, instant(at()))[0]
    assert result["observed_cohort"] == result["excluded"] == 2 and result["cohort"] == 0
    assert result["exclusion_reasons"] == {"CREDIT_RATING_NOT_OPERATING_GUIDANCE": 1, "SOURCE_PROVENANCE_UNKNOWN": 1}
    assert result["mean_net_hypothetical_return_pct"] is None
    assert store.conn.execute("SELECT count(*) FROM desk_signals").fetchone()[0] == 2
    store.close()


@pytest.mark.parametrize("prefer_audited_intermediate", [False, True])
def test_cache_upgrade_revalidates_sources_keeps_timestamps_and_preserves_invalid_signal_history(tmp_path, monkeypatch, prefer_audited_intermediate):
    from scripts import migrate_thesis_desk_cache as upgrade
    from src.thesis_desk.contracts import validate_research
    from tests.test_thesis_desk_workflow import proposal, review
    run = tmp_path / "old-run"
    run.mkdir()
    cfg, doc = config(), document()
    valid = validate_research(proposal(), [doc], "AAA", instant(at()))[0]
    rating_doc = dict(doc, id="rating-doc", source_url="https://ir.amd.com/rating", text="Ratings agency reaffirms the credit outlook for the company")
    rating = dict(valid, id="rating-fact", document_id=rating_doc["id"], source_url=rating_doc["source_url"], kind="GUIDANCE_REAFFIRMED", quote=rating_doc["text"])
    code = {"src/thesis_desk/" + name: name for name in ("agents.py", "contracts.py", "research_queue.py")}
    legacy = digest(list(code.values()))
    monkeypatch.setattr(upgrade, "LEGACY_CONTRACT", legacy)
    packets = {"config.json": cfg, "sources.json": dict(documents=[doc, rating_doc]), "research.json": dict(findings=[valid, rating])}
    for name, value in packets.items():
        (run / name).write_text(json.dumps(value), encoding="utf-8")
    (run / "manifest.json").write_text(json.dumps(dict(run_id="old-run", code_sha256=code,
        artifacts={name: digest((run / name).read_bytes()) for name in packets})), encoding="utf-8")
    store = PaperStore(tmp_path / "paper.sqlite", cfg)
    original_state = dict(cash="3000000", positions={})
    store.conn.execute("UPDATE desk_state SET state=? WHERE id=1", (json.dumps(original_state),))
    store.conn.execute("INSERT INTO desk_signals VALUES(?,?,?,NULL,NULL)", (rating["id"], "AAA", at()))
    source_contract = upgrade.OPERATING_CONTRACT if prefer_audited_intermediate else legacy
    for d, f in ((doc, valid), (rating_doc, rating)):
        key = digest(["document-v2", source_contract, cfg["llm"]["provider"], cfg["llm"]["model"], d["id"]])
        store.cache(key, dict(findings=[f], reviews=review(f["id"])["reviews"], rejections=[], review_context=digest([doc["id"], rating_doc["id"]])))
    store.conn.commit()
    store.close()
    report = upgrade.migrate(tmp_path, run, tmp_path / "migration.json")
    assert report["migrated"] == report["signal_evidence_backfilled"] == len(report["rejected"]) == 1
    assert report["cache_source_contract_counts"] == {source_contract: 1}
    assert report["rejected"][0]["reason"] == "CREDIT_RATING_NOT_OPERATING_GUIDANCE"
    store = PaperStore(tmp_path / "paper.sqlite", cfg)
    new_key = digest(["document-v2", report["target_contract"], cfg["llm"]["provider"], cfg["llm"]["model"], doc["id"]])
    assert store.cached(new_key)["findings"] == [valid]
    assert store.load() == original_state
    assert outcome_metrics(store, instant(at()))[0]["exclusion_reasons"] == {"CREDIT_RATING_NOT_OPERATING_GUIDANCE": 1}
    store.close()
    (run / "sources.json").write_text("{}")
    with pytest.raises(ValueError, match="integrity"):
        upgrade.migrate(tmp_path, run, tmp_path / "invalid.json")


def test_cache_upgrade_rejects_modified_source_hash(tmp_path):
    from scripts.migrate_thesis_desk_cache import revalidate_cached
    from src.thesis_desk.contracts import validate_research
    from tests.test_thesis_desk_workflow import proposal
    doc = document()
    finding = validate_research(proposal(), [doc], "AAA", instant(at()))[0]
    finding["source_hash"] = "tampered"
    with pytest.raises(ValueError, match="differs"):
        revalidate_cached(dict(findings=[finding], reviews=None), doc, [doc], instant(at()))


def test_old_quote_cannot_anchor_new_watchlist(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", config())
    result = dict(catalog=[dict(ticker="AAA", status="IDENTIFIED")], paper=dict(market_state="OPEN"),
                  research=dict(findings=[dict(id="new", kind="GUIDANCE_RAISED", quote="The company raises its earnings guidance")]),
                  watchlist=[dict(ticker="AAA", fact_id="new", pm_decision="WATCHLIST")])
    observe(store, dict(intraday_points=[dict(ticker="AAA", market_ts=at(clock="10:59:00"), last_price="100")]), result, instant(at()))
    assert outcome_metrics(store, instant(at()))[0]["pending_anchor"] == 1
    store.close()


def test_source_seed_is_public_only_hash_verified_and_retains_timestamp(tmp_path):
    source, target = tmp_path / "one", tmp_path / "two"
    source.mkdir(); target.mkdir()
    raw = source / "doc.bin"; raw.write_bytes(b"official public source")
    a = PaperStore(source / "paper.sqlite", config())
    a.cache("source:x", dict(raw_path=str(raw), sha256=digest(raw.read_bytes()), fetched_at=at()))
    a.cache("model-secret", dict(value="not imported")); a.close()
    b = PaperStore(target / "paper.sqlite", config())
    seed_sources(b, source, target)
    assert b.cached("source:x")["fetched_at"] == at() and b.cached("model-secret") is None
    raw.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="integrity"):
        seed_sources(b, source, target)
    b.close()


def test_dashboard_escapes_source_script_injection():
    result = dict(at=at(), untrusted="</script><script>alert('x')</script>")
    html = render_dashboard(result)
    assert result["untrusted"] not in html and "\\u003c/script\\u003e" in html
    assert "cdn." not in html
