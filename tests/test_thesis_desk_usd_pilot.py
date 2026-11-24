from copy import deepcopy
from datetime import datetime, timezone

import pytest
import httpx

from src.thesis_desk.usd_pilot import (
    NY, candidate_packet, entry_session, prepare, read, run_event, save, sessions,
    validate_extension, validate_research,
)
from src.thesis_desk.contracts import digest
from src.thesis_desk.usd_pilot_market import (
    audit_decision, capture_market, event_outcome, long_return, parse_chart, summarize, validate_market,
)


def scenario():
    calendar = sessions()
    published = "2026-09-04T14:00:00Z"  # Friday during session, next open after Labor Day.
    entry = entry_session(published, calendar)
    n = next(n for n, s in enumerate(calendar) if s == entry)
    event = dict(id="one", symbol="TEST", accession="filing", published_at=published,
                 decision_cutoff=published, entry=entry, primary_nonoverlap=True,
                 exit_sessions={str(h): calendar[n+h-1] for h in (5, 10, 20)})
    bars = {s["date"]: dict(open=100., high=200., low=90., close=101.+i/10, volume=1000)
            for i, s in enumerate(calendar)}
    market = dict(asof="2026-12-01T22:00:00Z", symbols={
        "TEST": dict(bars=bars, dividends={}, splits=[]),
        "IVV": dict(bars=deepcopy(bars), dividends={}, splits=[])})
    protocol = dict(calendar=calendar, rules=dict(fee_each_side=.006, slippage_each_side=.0015))
    return event, market, protocol


def test_calendar_holiday_dst_and_early_close():
    cal = sessions()
    keyed = {s["date"]: s for s in cal}
    assert "2026-09-07" not in keyed
    assert "2026-11-26" not in keyed
    assert keyed["2026-11-27"]["close"].endswith("13:00:00-05:00")
    assert keyed["2026-10-30"]["open"].endswith("-04:00")
    assert keyed["2026-11-02"]["open"].endswith("-05:00")
    assert entry_session("2026-09-04T14:00:00Z", cal)["date"] == "2026-09-08"
    # A full sixty-minute processing allowance: 8:30 publication cannot fill at 9:30.
    assert entry_session("2026-09-04T12:30:00Z", cal)["date"] == "2026-09-08"
    assert entry_session("2026-09-04T12:29:59Z", cal)["date"] == "2026-09-04"
    with pytest.raises(ValueError, match="limited"):
        sessions("2027-01-01", "2027-02-01")


def test_horizon_counts_sessions_and_full_roundtrip_costs():
    e, m, p = scenario()
    r = event_outcome(e, 5, {"action": "BUY"}, m, p)
    assert r["entry_date"] == "2026-09-08"
    assert r["exit_date"] == "2026-09-14"
    expected = (r["exit_close"]*.9985*.994) / (100*1.0015*1.006) - 1
    assert r["agent_net"] == pytest.approx(expected)
    assert r["agent_minus_hold"] == 0
    assert r["buy_minus_benchmark"] == 0
    assert long_return(100, 100, 0)["net"] < -.014
    assert long_return(100, 110, 2, 0, 0)["net"] == pytest.approx(.12)


def test_dividends_entry_exdate_excluded_exit_exdate_included():
    e, m, p = scenario()
    entry, end = e["entry"]["date"], e["exit_sessions"]["5"]["date"]
    m["symbols"]["TEST"]["dividends"] = {entry: 100, end: 2, "2026-09-15": 50}
    r = event_outcome(e, 5, {"action": "BUY"}, m, p)
    assert r["dividends_per_share"] == 2
    assert r["hold_net"] == pytest.approx(long_return(100, r["exit_close"], 2)["net"])


@pytest.mark.parametrize("status", ["IMMATURE", "PRICE_SOURCE_FAILED", "MISSING_ENTRY_OR_EXIT",
                                    "MISSING_INTERMEDIATE_SESSION", "SPLIT_REQUIRES_VERIFICATION"])
def test_unknown_data_never_becomes_zero_or_shorter_horizon(status):
    e, m, p = scenario()
    if status == "IMMATURE":
        m["asof"] = "2026-09-14T19:59:59Z"
    elif status == "PRICE_SOURCE_FAILED":
        del m["symbols"]["TEST"]
    elif status == "MISSING_ENTRY_OR_EXIT":
        del m["symbols"]["TEST"]["bars"][e["entry"]["date"]]
    elif status == "MISSING_INTERMEDIATE_SESSION":
        del m["symbols"]["TEST"]["bars"]["2026-09-09"]
    else:
        m["symbols"]["TEST"]["splits"] = [{"date": "2026-08-01", "numerator": 2, "denominator": 1}]
    r = event_outcome(e, 5, {"action": "BUY"}, m, p)
    assert r["status"] == status
    assert r["agent_net"] is None and r["hold_net"] is None


def test_cash_is_real_abstention_but_model_error_is_not():
    e, m, p = scenario()
    cash = event_outcome(e, 5, {"action": "CASH"}, m, p)
    unknown = event_outcome(e, 5, {"action": "UNKNOWN"}, m, p)
    assert cash["agent_net"] == 0 and cash["agent_minus_hold"] == -cash["hold_net"]
    assert unknown["agent_net"] is None
    paired = summarize([cash, unknown])["primary_nonoverlap"]["5"]
    assert paired["paired"] == 1 and paired["unknown_decisions"] == 1
    assert paired["hold_mean_net"] == cash["hold_net"]  # Same denominator, not all prices.


def test_baseline_and_prior_returns_never_use_close_after_cutoff():
    e, m, p = scenario()
    before = event_outcome(e, 5, {"action": "BUY"}, m, p)
    # Acceptance is Friday morning. Friday close must not enter the baseline or prior move.
    m["symbols"]["TEST"]["bars"]["2026-09-04"]["close"] = 99999
    after = event_outcome(e, 5, {"action": "BUY"}, m, p)
    for key in ["baseline_action", "baseline_net", "prior_5", "prior_20", "announcement_to_entry"]:
        assert before[key] == after[key]


def test_missing_baseline_excludes_pair_and_nonoverlap_is_preserved():
    e, m, p = scenario()
    del m["symbols"]["TEST"]["bars"]["2026-08-28"]
    missing = event_outcome(e, 5, {"action": "BUY"}, m, p)
    assert missing["status"] == "EVALUABLE" and missing["baseline_net"] is None
    e["primary_nonoverlap"] = False
    other = event_outcome(e, 5, {"action": "CASH"}, m, p)
    summary = summarize([missing, other])
    assert summary["primary_nonoverlap"]["5"]["total"] == 1
    assert summary["primary_nonoverlap"]["5"]["paired"] == 0
    assert summary["all_events_descriptive"]["5"]["total"] == 2


def chart_fixture():
    stamp = int(datetime(2026, 9, 4, 9, 30, tzinfo=NY).timestamp())
    return {"chart": {"error": None, "result": [{
        "meta": {"symbol": "TEST", "currency": "USD", "instrumentType": "EQUITY"},
        "timestamp": [stamp],
        "indicators": {"quote": [{"open": [100], "close": [102], "low": [99], "high": [103], "volume": [100]}]},
        "events": {"dividends": {"one": {"date": stamp, "amount": .5}}}}]}}


def test_chart_rejects_wrong_identity_and_partial_session():
    body = chart_fixture()
    assert not parse_chart(body, "TEST", sessions(), "2026-09-04T19:59:00Z")["bars"]
    parsed = parse_chart(body, "TEST", sessions(), "2026-09-04T20:00:00Z")
    assert parsed["bars"]["2026-09-04"]["close"] == 102
    assert parsed["dividends"]["2026-09-04"] == .5
    body["chart"]["result"][0]["meta"]["currency"] = "ARS"
    with pytest.raises(ValueError, match="currency"):
        parse_chart(body, "TEST", sessions(), "2026-09-05T00:00:00Z")


def test_chart_rejects_duplicates_and_invalid_price_without_imputation():
    body = chart_fixture()
    quote = body["chart"]["result"][0]["indicators"]["quote"][0]
    quote["close"] = [None]
    result = parse_chart(body, "TEST", sessions(), "2026-09-05T00:00:00Z")
    assert not result["bars"]
    body = chart_fixture()
    obj = body["chart"]["result"][0]
    obj["timestamp"] *= 2
    for k in obj["indicators"]["quote"][0]:
        obj["indicators"]["quote"][0][k] *= 2
    with pytest.raises(ValueError, match="Duplicate"):
        parse_chart(body, "TEST", sessions(), "2026-09-05T00:00:00Z")


def candidate():
    return dict(id="Q1", document_id="d", source_hash="sha",
                quote="Company raises full-year guidance following increased demand for its products",
                context="Company raises full-year guidance following increased demand for its products.",
                kinds=["GUIDANCE_RAISED"])


class FixtureModel:
    def __init__(self):
        self.roles = []

    def ask(self, role, instruction, data, schema):
        self.roles.append(role)
        assert "prices" not in data and "market" not in data
        if role == "RESEARCH":
            return dict(positive=dict(candidate_id="Q1", kind="GUIDANCE_RAISED",
                                      reason="Company explicitly raises guidance"), negative=None)
        if role == "CONTRARIAN":
            return dict(positive=dict(verdict="SUPPORTED", reason="Explicit increase in company guidance"))
        return dict(action="BUY", reason="Official operating guidance raised")


def test_real_pipeline_contract_with_fixture_then_outcome():
    e, m, p = scenario()
    e.update(documents=[{"published_at": e["published_at"]}], candidates=[candidate()])
    model = FixtureModel()
    decision = run_event(e, model)
    assert model.roles == ["RESEARCH", "CONTRARIAN", "PM"]
    outcome = event_outcome(e, 5, decision, m, p)
    assert outcome["action"] == "BUY" and outcome["agent_net"] == outcome["hold_net"]
    e["documents"][0]["published_at"] = "2026-10-01T12:00:00Z"
    with pytest.raises(ValueError, match="Future"):
        run_event(e, model)


def test_source_citation_and_kind_cannot_be_invented():
    with pytest.raises(ValueError, match="source"):
        validate_research(dict(positive=dict(candidate_id="invented", kind="GUIDANCE_RAISED",
                                            reason="Unsupported invented quote"), negative=None), [candidate()])
    doc = dict(id="d", sha256="sha", source_url="https://example.test/one",
               text="Company raises full-year guidance following increased demand for its products.")
    quotes = candidate_packet([doc, doc])
    assert len({x["quote"] for x in quotes}) == len(quotes)
    assert all(x["quote"] in doc["text"] for x in quotes)


def test_quality_audit_preserves_actual_decision():
    source = candidate()
    source.update(quote="In October 2025, we signed a new definitive agreement with our partner",
                  context="In October 2025, we signed a new definitive agreement with our partner.")
    decision = dict(action="BUY", decision_cutoff="2026-10-28T20:00:00Z",
                    findings=[dict(id="positive", kind="DEAL_SIGNED", source=source)],
                    pm=dict(reason="This new $100 billion agreement improves operations"))
    original = deepcopy(decision)
    flags = audit_decision(decision)
    assert {f["code"] for f in flags} == {"HISTORICAL_TRANSACTION_AS_NEW", "PM_NUMBERS_NOT_IN_SUPPLIED_CONTEXT"}
    assert decision == original


@pytest.mark.parametrize("cohort_size", [25, 50])
def test_frozen_cohort_accession_grouping_and_overlap_do_not_depend_on_returns(tmp_path, cohort_size):
    archive, output = tmp_path / "source", tmp_path / "run"
    archive.mkdir()
    catalog = dict(catalog=[dict(status="IDENTIFIED", sec_cik=str(1000+i), underlying=f"A{i:02}",
                                 ticker=f"A{i:02}", underlying_kind="COMMON_STOCK") for i in range(50)])
    weights = [dict(Ticker=f"A{i:02}", Name=f"Company {i}", **{"Market Weight": str(50-i)}) for i in range(50)]
    save(archive / "catalog.json", catalog)
    save(archive / "ivv-snapshots.json", [dict(asof="2026-08-31", equities=weights)])
    docs = []
    for n, (accession, at) in enumerate([("first", "2026-09-01T21:00:00Z"),
                                       ("first", "2026-09-01T21:00:00Z"),
                                       ("second", "2026-09-02T21:00:00Z"),
                                       ("third", "2026-10-01T21:00:00Z"),
                                       ("future", "2026-11-01T21:00:00Z")]):
        docs.append(dict(id=str(n), ticker="A00", accession=accession, source_type="SEC",
                         source_url=f"https://www.sec.gov/Archives/edgar/data/1000/{n}",
                         sha256=str(n), text="Company raises full-year guidance after higher demand.",
                         published_at=at, historical_publication_at=at))
    save(archive / "documents.json", docs)
    p = prepare(archive, archive / "catalog.json", output, "model-digest", cohort_size=cohort_size)
    events = read(output / "events.json")
    assert [x["primary_nonoverlap"] for x in events] == [True, False, True]
    assert len(events[0]["documents"]) == 2 and len(events[1]["documents"]) == 1
    assert len(p["cohort"]) == cohort_size and all(e["accession"] != "future" for e in events)
    assert not (output / "market.json").exists()
    assert prepare(archive, archive / "catalog.json", output, "model-digest", cohort_size=cohort_size) == p
    small = prepare(archive, archive / "catalog.json", tmp_path / "small", "model-digest")
    assert p["cohort"][:25] == small["cohort"]
    validate_extension(small, p)
    assert events == read(tmp_path / "small" / "events.json")
    with pytest.raises(ValueError, match="Frozen"):
        prepare(archive, archive / "catalog.json", output, "different-model", cohort_size=cohort_size)
    with pytest.raises(ValueError, match="only 50"):
        prepare(archive, archive / "catalog.json", tmp_path / "too-many", "model-digest", cohort_size=51)
    docs[0]["source_url"] = "https://www.sec.gov/Archives/edgar/data/9999/wrong-cik"
    save(archive / "documents.json", docs)
    with pytest.raises(ValueError, match="CIK"):
        prepare(archive, archive / "catalog.json", tmp_path / "other", "model-digest")


def test_normalized_prices_must_reproduce_raw_capture(tmp_path):
    calendar = sessions()
    protocol = dict(calendar=calendar, cohort=[{"symbol": "TEST"}])
    asof = "2026-09-05T00:00:00Z"
    stock, etf = chart_fixture(), chart_fixture()
    etf["chart"]["result"][0]["meta"].update(symbol="IVV", instrumentType="ETF")
    market = dict(protocol_hash=digest(protocol), asof=asof, symbols={}, receipts={}, failures={})
    for symbol, body in [("TEST", stock), ("IVV", etf)]:
        path = tmp_path / f"{symbol}.json"
        save(path, body)
        market["symbols"][symbol] = parse_chart(body, symbol, calendar, asof)
        market["receipts"][symbol] = dict(raw_path=path.name, sha256=digest(path.read_bytes()))
    validate_market(tmp_path, protocol, market)
    market["symbols"]["TEST"]["bars"]["2026-09-04"]["close"] = 9999
    with pytest.raises(ValueError, match="Normalized"):
        validate_market(tmp_path, protocol, market)


def test_refresh_preserves_original_prices_and_cache_needs_no_network(tmp_path, monkeypatch):
    protocol = dict(calendar=sessions(), cohort=[{"symbol": "TEST"}])
    save(tmp_path / "protocol.json", protocol)
    generation, requests = [102.0], []

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            requests.append(url)
            body = chart_fixture()
            obj = body["chart"]["result"][0]
            obj["indicators"]["quote"][0]["close"] = generation[:]
            if url.endswith("/IVV"):
                obj["meta"].update(symbol="IVV", instrumentType="ETF")
            return httpx.Response(200, json=body, request=httpx.Request("GET", url))

    monkeypatch.setattr("src.thesis_desk.usd_pilot_market.httpx.Client", Client)
    first = capture_market(tmp_path)
    assert len(requests) == 2
    assert capture_market(tmp_path) == first and len(requests) == 2
    generation[0] = 102.5
    second = capture_market(tmp_path, refresh=True)
    assert len(requests) == 4 and first != second
    assert read(tmp_path / "market.json") == second
    assert read(tmp_path / "market_snapshots" / (digest(first) + ".json")) == first
    validate_market(tmp_path, protocol, first)
    validate_market(tmp_path, protocol, second)


def test_reuse_requires_unchanged_rules_identity_and_model():
    common = dict(schema="pilot", currency="USD", start="2026-09-01", end="2026-10-31",
                  model="local", model_digest="digest", decision_contract="contract",
                  rules={"cost": .006}, calendar=sessions(), source_files={"source": "sha"})
    source = dict(common, cohort=[dict(symbol="TEST", sec_cik="1")])
    target = deepcopy(source)
    target["cohort"].append(dict(symbol="NEW", sec_cik="2"))
    validate_extension(source, target)
    for key in ("rules", "model_digest", "source_files", "calendar"):
        bad = deepcopy(target)
        bad[key] = "changed"
        with pytest.raises(ValueError, match="Incompatible"):
            validate_extension(source, bad)
    target["cohort"][0]["sec_cik"] = "another-issuer"
    with pytest.raises(ValueError, match="unchanged subset"):
        validate_extension(source, target)


def test_expanded_market_reuses_common_prices_at_identical_cutoff(tmp_path, monkeypatch):
    source, target = tmp_path / "old", tmp_path / "expanded"
    source.mkdir()
    target.mkdir()
    common = dict(schema="pilot", currency="USD", start="2026-09-01", end="2026-10-31",
                  model="local", model_digest="digest", decision_contract="contract",
                  rules={"cost": .006}, calendar=sessions(), source_files={"source": "sha"})
    old_protocol = dict(common, cohort=[{"symbol": "TEST"}])
    new_protocol = dict(common, cohort=[{"symbol": "TEST"}, {"symbol": "NEW"}])
    save(source / "protocol.json", old_protocol)
    save(target / "protocol.json", new_protocol)
    market = dict(protocol_hash=digest(old_protocol), asof="2026-09-05T00:00:00Z",
                  symbols={}, receipts={}, failures={})
    for symbol in ("TEST", "IVV"):
        body = chart_fixture()
        body["chart"]["result"][0]["meta"].update(symbol=symbol, instrumentType="ETF" if symbol == "IVV" else "EQUITY")
        path = source / f"{symbol}.json"
        save(path, body)
        market["symbols"][symbol] = parse_chart(body, symbol, common["calendar"], market["asof"])
        market["receipts"][symbol] = dict(raw_path=path.name, sha256=digest(path.read_bytes()))
    save(source / "market.json", market)
    requests = []

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, params):
            requests.append((url, params))
            assert url.endswith("/NEW")
            assert params["period2"] == int(datetime(2026, 9, 5, tzinfo=timezone.utc).timestamp())
            body = chart_fixture()
            body["chart"]["result"][0]["meta"]["symbol"] = "NEW"
            return httpx.Response(200, json=body, request=httpx.Request("GET", url))

    monkeypatch.setattr("src.thesis_desk.usd_pilot_market.httpx.Client", Client)
    expanded = capture_market(target, reuse_run=source)
    assert len(requests) == 1 and expanded["asof"] == market["asof"]
    assert expanded["symbols"]["TEST"] == market["symbols"]["TEST"]
    assert expanded["symbols"]["IVV"] == market["symbols"]["IVV"]
    assert read(source / "market.json") == market
    validate_market(target, new_protocol, expanded)
    with pytest.raises(ValueError, match="refresh"):
        capture_market(target, reuse_run=source, refresh=True)
