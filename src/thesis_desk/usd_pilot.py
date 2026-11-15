"""Frozen, retrospective USD event pilot. No broker, portfolio or production writes."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
import inspect
import json
from pathlib import Path
import re
import time as clock
from zoneinfo import ZoneInfo

import httpx

from src.backtesting.thesis_desk import POSITIVE, NEGATIVE, instant
from src.thesis_desk.agents import RULES, citation_options
from src.thesis_desk.contracts import digest, normalized, qualification_error

NY = ZoneInfo("America/New_York")
MODEL = "qwen3.5:9b"
BASE = "http://127.0.0.1:11434"
CALENDAR_SOURCE = "https://www.nyse.com/trade/hours-calendars"
# Bounded pilot calendar, verified against the official 2026 NYSE calendar.
HOLIDAYS = {"2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25"}
EARLY_CLOSES = {"2026-11-27", "2026-12-24"}
OPTIONS = {"temperature": 0, "seed": 17, "num_ctx": 8192, "num_predict": 768,
           "num_thread": 4}
RESEARCH = """Identify at most one positive and one negative CURRENT operating fact in this filing.
Choose a provided candidate ID and one of its allowed kinds, or null when none qualifies.
The context must establish an actual current event for this issuer, not history, boilerplate,
financing, compensation, third-party results or an already superseded forecast.
Do not infer missing facts. A new quarterly forecast is not a raise to previous guidance.
Write a short factual reason (maximum 180 characters). Every selection needs a source quote.
No prices or subsequent documents are supplied. Output positive and negative, each null or
{candidate_id, kind, reason}. This is source classification, not a trade."""
CONTRARIAN = """Independently challenge each proposed fact using ONLY its supplied source context.
Return SUPPORTED only if the citation AND context explicitly establish this exact kind of
current operating event for this issuer. Return UNSUPPORTED for a misclassification,
historical reference, financing, employee contract, boilerplate or ambiguity.
Return one verdict and a short reason for each provided finding ID. No future knowledge."""
PM = """Decide whether the supported positive official thesis merits a hypothetical long entry
in this event-study pilot. BUY needs a specific, current operating improvement or executed
growth transaction; CASH is appropriate for weak materiality or a supported adverse fact.
Use only the provided evidence. No price targets, sizing or external knowledge.
Return action BUY or CASH and a short factual reason (maximum 180 characters).
BUY is a research label, never a broker order."""


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False,
                              default=str) + "\n", encoding="utf-8")
    tmp.replace(path)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def sessions(start="2026-07-01", end="2026-12-31"):
    if start < "2026-07-01" or end > "2026-12-31":
        raise ValueError("Pilot calendar is limited to July-December 2026")
    result = []
    day = date.fromisoformat(start)
    while day <= date.fromisoformat(end):
        key = day.isoformat()
        if day.weekday() < 5 and key not in HOLIDAYS:
            result.append(dict(date=key,
                               open=datetime.combine(day, time(9, 30), NY).isoformat(),
                               close=datetime.combine(day, time(13 if key in EARLY_CLOSES else 16), NY).isoformat()))
        day += timedelta(days=1)
    return result


def entry_session(published_at, calendar):
    ready = instant(published_at) + timedelta(minutes=60)
    return next(s for s in calendar if instant(s["open"]) > ready)


def candidate_packet(documents):
    """Same short exact citations as the existing desk; retain surrounding source text."""
    candidates, seen = [], set()
    for doc in sorted(documents, key=lambda d: d["source_url"]):
        full = normalized(doc["text"])
        for quote in citation_options(doc).values():
            kinds = sorted(k for k in POSITIVE | NEGATIVE if not qualification_error(k, quote))
            if not kinds or quote in seen:
                continue
            seen.add(quote)
            at = full.index(quote)
            candidates.append(dict(id=f"Q{len(candidates)+1}", document_id=doc["id"],
                                   source_hash=doc["sha256"], quote=quote, kinds=kinds,
                                   context=full[max(0, at-250):at+len(quote)+350]))
    return candidates


def object_schema(properties):
    return dict(type="object", properties=properties, required=list(properties),
                additionalProperties=False)


def decision_contract():
    return digest(dict(version="usd-event-pilot-v1", model=MODEL, options=OPTIONS,
                       rules=RULES, research=RESEARCH, contrarian=CONTRARIAN, pm=PM,
                       functions=[inspect.getsource(f) for f in
                                  (citation_options, qualification_error, candidate_packet,
                                   research_schema, validate_research, run_event)]))


def prepare(archive, catalog_path, output, model_digest, *, cohort_size=25):
    if type(cohort_size) is not int or not 1 <= cohort_size <= 500:
        raise ValueError("Cohort size must be an integer between 1 and 500")
    archive, output = Path(archive), Path(output)
    paths = [archive / "documents.json", archive / "ivv-snapshots.json", Path(catalog_path)]
    sources = {str(p.resolve()): digest(p.read_bytes()) for p in paths}
    docs, snapshots, catalog = (read(p) for p in paths)
    mapping = {}
    for item in catalog["catalog"]:
        if (item.get("status") == "IDENTIFIED" and item.get("sec_cik")
                and item.get("underlying_kind") == "COMMON_STOCK"):
            key = item["underlying"]
            if key in mapping and mapping[key]["sec_cik"] != item["sec_cik"]:
                raise ValueError("Conflicting underlying CIK")
            mapping[key] = item
    snapshot = next(s for s in snapshots if s["asof"] == "2026-08-31")
    holdings = sorted((h for h in snapshot["equities"] if h["Ticker"] in mapping),
                      key=lambda h: (-float(h["Market Weight"]), h["Ticker"]))[:cohort_size]
    cohort = [dict(symbol=h["Ticker"], catalog_ticker=mapping[h["Ticker"]]["ticker"],
                   sec_cik=mapping[h["Ticker"]]["sec_cik"], name=h["Name"],
                   weight_20260831=float(h["Market Weight"])) for h in holdings]
    if len(cohort) != cohort_size:
        raise ValueError(f"Expected {cohort_size} frozen underlying stocks; only {len(cohort)} are available")
    by_ticker = {c["catalog_ticker"]: c for c in cohort}
    grouped = defaultdict(list)
    for doc in docs:
        if (doc["ticker"] in by_ticker and doc["source_type"] == "SEC"
                and "2026-09-01" <= instant(doc["published_at"]).astimezone(NY).date().isoformat() <= "2026-10-31"):
            if doc.get("historical_publication_at") != doc["published_at"]:
                raise ValueError("SEC publication provenance mismatch")
            cik = str(int(by_ticker[doc["ticker"]]["sec_cik"]))
            if f"/data/{cik}/" not in doc["source_url"]:
                raise ValueError("Filing CIK disagrees with cohort")
            grouped[(doc["ticker"], doc["accession"])].append(doc)
    calendar = sessions()
    events = []
    for (ticker, accession), own in grouped.items():
        times = {d["published_at"] for d in own}
        if len(times) != 1:
            raise ValueError("One accession has conflicting acceptance timestamps")
        published = times.pop()
        entry = entry_session(published, calendar)
        events.append(dict(id=digest([ticker, accession]), symbol=by_ticker[ticker]["symbol"],
                           accession=accession, published_at=published,
                           decision_cutoff=published, entry=entry,
                           documents=[{k: d.get(k) for k in
                                       ("id", "source_url", "sha256", "published_at",
                                        "observed_at", "available_at", "acceptance_metadata_urls")}
                                      for d in own], candidates=candidate_packet(own)))
    events.sort(key=lambda e: (instant(e["entry"]["open"]), instant(e["published_at"]),
                               e["symbol"], e["accession"]))
    occupied_until = {}
    for event in events:
        i = next(n for n, s in enumerate(calendar) if s["date"] == event["entry"]["date"])
        event["exit_sessions"] = {str(h): calendar[i+h-1] for h in (5, 10, 20)}
        event["primary_nonoverlap"] = event["entry"]["date"] > occupied_until.get(event["symbol"], "")
        if event["primary_nonoverlap"]:
            occupied_until[event["symbol"]] = event["exit_sessions"]["20"]["date"]
    # No future prices are read here. Publish this protocol before market capture/evaluation.
    protocol = dict(schema="quantia-usd-pilot-v1", mode="RESEARCH_ONLY", currency="USD",
                    created_at=utcnow(), start="2026-09-01", end="2026-10-31",
                    model=MODEL, model_digest=model_digest, decision_contract=decision_contract(),
                    cohort_selection=f"TOP_{cohort_size}_AUG31_IVV_WEIGHT_WITH_CURRENT_CATALOG_SEC_MAPPING",
                    cohort=cohort, source_files=sources, events_hash=digest(events),
                    calendar=calendar, calendar_source=CALENDAR_SOURCE,
                    rules=dict(
                        event="One issuer SEC accession; primary and exhibits grouped; all events retained",
                        source_cutoff="SEC acceptance timestamp; no later documents supplied to any role",
                        extraction="Existing desk exact 20-word citation windows, first 30 per document, surrounding context; excerpt coverage is limited",
                        classification="Research then independent Contrarian then PM; supported adverse fact forces CASH",
                        execution="First regular open strictly after acceptance + 60 minutes",
                        horizons=[5, 10, 20], exit="Close of Nth session including entry session",
                        fee_each_side=0.006, slippage_each_side=0.0015,
                        costs_basis="Hypothetical conservative research costs; not actual broker fees",
                        agent="BUY earns the matched long return; CASH earns 0 without trading costs; UNKNOWN is missing",
                        hold="Buy same stock at identical entry and exit for EVERY event, even agent CASH",
                        baseline="Buy iff preceding close > SMA20 of preceding 20 completed sessions; else cash",
                        benchmark="IVV USD over identical dates and identical hypothetical costs",
                        distributions="Cash dividends after entry date through exit ex-date, no reinvestment; raw Yahoo OHLC are already split-adjusted",
                        splits="Any split between warmup start and capture end blocks affected symbol metrics pending verification; never double-adjust",
                        primary="First event per issuer, suppress subsequent overlapping 20-session windows independent of decisions/outcomes",
                        aggregation="Equal-weight event means, NOT compounded portfolio PnL; no annualization",
                        uncertainty="No independent-sample significance claims; shared market dates remain correlated",
                        maturity="Use only closed sessions at fixed market capture time; no partial horizon substitution"),
                    limitations=[
                        "Retrospective strategy design: November post is after the evaluated window",
                        "Reconstructed SEC availability, downloaded later; no contemporaneous model decisions",
                        "Current catalog used for CIK discovery; Aug31 IVV snapshot fixes cohort weights",
                        "Current-vintage Yahoo prices may contain revisions; no original-vintage guarantee",
                        "Prompt restrictions cannot prove absence of future knowledge in model weights",
                        "SEC-only excerpts do not establish full news coverage or full document review",
                        "Daily prices cannot isolate intraday announcement response",
                        "This tests thesis selection; it does not replicate the full CEDEAR RISK_V2 portfolio"])
    target = output / "protocol.json"
    if target.exists():
        existing = read(target)
        protocol["created_at"] = existing["created_at"]
        if existing != protocol or read(output / "events.json") != events:
            raise ValueError("Frozen run differs; use a new output directory")
        return existing
    if output.exists() and any(output.iterdir()):
        raise ValueError("New pilot output must be empty")
    save(target, protocol)
    save(output / "events.json", events)
    return protocol


def validate_extension(source, target):
    """Only the cohort may grow; decision rules, sources and prices' calendar stay fixed."""
    for key in ("schema", "currency", "start", "end", "model", "model_digest",
                "decision_contract", "rules", "calendar", "source_files"):
        if source[key] != target[key]:
            raise ValueError(f"Incompatible reuse protocol: {key}")
    expanded = {c["symbol"]: c for c in target["cohort"]}
    if any(expanded.get(c["symbol"]) != c for c in source["cohort"]):
        raise ValueError("Previous cohort must be an unchanged subset of the new cohort")


def reuse_decisions(source, output):
    """Reuse exact event decisions and verified responses; never reroll earlier results."""
    source, output = Path(source), Path(output)
    if source.resolve() == output.resolve():
        raise ValueError("Reuse source and destination must differ")
    old, new = read(source / "protocol.json"), read(output / "protocol.json")
    validate_extension(old, new)
    replay(source)
    events = {e["id"]: e for e in read(output / "events.json")}
    if digest(list(events.values())) != new["events_hash"]:
        raise ValueError("New event manifest changed")
    copies = []
    for path in (source / "calls").glob("*.json"):
        value = read(path)
        if (path.stem != digest([new["model_digest"], value["request"]])
                or value["response_hash"] != digest(value["response"])):
            raise ValueError("Response reuse integrity failure")
        copies.append((path, output / "calls" / path.name))
    count = 0
    for event in read(source / "events.json"):
        if events.get(event["id"]) != event:
            raise ValueError("An earlier event changed in the expanded cohort")
        path = source / "decisions" / (event["id"] + ".json")
        value = read(path)
        if value["event_hash"] != digest(event):
            raise ValueError("Earlier decision event hash differs")
        if value["action"] != "UNKNOWN":
            copies.append((path, output / "decisions" / path.name))
            count += 1
    # Verify conflicts before writing anything.
    for src, dst in copies:
        if dst.exists() and dst.read_bytes() != src.read_bytes():
            raise ValueError("Conflicting cached response or decision; refusing to overwrite")
    for src, dst in copies:
        dst.parent.mkdir(exist_ok=True)
        if not dst.exists():
            dst.write_bytes(src.read_bytes())
    receipt = dict(source_run=str(source.resolve()), source_protocol_hash=digest(old),
                   target_protocol_hash=digest(new), decisions_reused=count,
                   calls_reused=len(copies)-count)
    save(output / "reuse.json", receipt)
    return receipt


def research_schema(candidates):
    def selection(kinds):
        choices = [object_schema(dict(candidate_id={"const": c["id"]},
                                      kind={"enum": sorted(set(c["kinds"]) & kinds)},
                                      reason=dict(type="string", minLength=5, maxLength=180)))
                   for c in candidates if set(c["kinds"]) & kinds]
        return {"anyOf": [{"type": "null"}] + choices}
    return object_schema(dict(positive=selection(POSITIVE), negative=selection(NEGATIVE)))


def validate_research(value, candidates):
    if not isinstance(value, dict) or set(value) != {"positive", "negative"}:
        raise ValueError("Research must account for positive and negative")
    lookup = {c["id"]: c for c in candidates}
    result = []
    for side, kinds in (("positive", POSITIVE), ("negative", NEGATIVE)):
        item = value[side]
        if item is None:
            continue
        if not isinstance(item, dict) or set(item) != {"candidate_id", "kind", "reason"}:
            raise ValueError("Invalid finding schema")
        candidate = lookup.get(item["candidate_id"])
        if not candidate or item["kind"] not in kinds or item["kind"] not in candidate["kinds"]:
            raise ValueError("Invalid source or classification")
        if qualification_error(item["kind"], candidate["quote"]):
            raise ValueError("Citation does not support fact kind")
        if not isinstance(item["reason"], str) or not 5 <= len(item["reason"]) <= 180:
            raise ValueError("Invalid short reason")
        result.append(dict(id=side, **item, source=candidate))
    return result


class LocalPilotModel:
    """One model, sequential calls, bounded context; cache bound to exact request and digest."""
    def __init__(self, output, protocol, *, offline=False):
        self.output, self.protocol = Path(output), protocol
        self.offline = offline
        self.client = httpx.Client(timeout=httpx.Timeout(600, connect=10))

    def close(self):
        self.client.close()

    def verify(self):
        if self.offline:
            return
        tags = self.client.get(BASE + "/api/tags")
        tags.raise_for_status()
        model = next((m for m in tags.json()["models"] if m["name"] == MODEL), None)
        if not model or model["digest"] != self.protocol["model_digest"]:
            raise ValueError("Frozen open model digest differs from installed model")

    def ask(self, role, instruction, data, schema):
        payload = dict(model=MODEL, stream=False, think=False, keep_alive="30m",
                       format=schema, options=OPTIONS,
                       messages=[dict(role="system", content=RULES + "\n" + instruction),
                                 dict(role="user", content=json.dumps(data, ensure_ascii=False))])
        key = digest([self.protocol["model_digest"], payload])
        path = self.output / "calls" / (key + ".json")
        if path.exists():
            cached = read(path)
            if cached["request"] != payload or digest(cached["response"]) != cached["response_hash"]:
                raise ValueError("Model cache integrity failure")
            return cached["response"]
        if self.offline:
            raise ValueError("Offline replay needs the original cached response")
        self.verify()
        begin, started = utcnow(), clock.monotonic()
        response = self.client.post(BASE + "/api/chat", json=payload)
        response.raise_for_status()
        body = response.json()
        if not body.get("done") or body.get("done_reason") != "stop":
            raise ValueError("Model completion truncated")
        value = json.loads(body["message"]["content"])
        save(path, dict(role=role, request=payload, response=value, response_hash=digest(value),
                        model_digest=self.protocol["model_digest"], started_at=begin,
                        finished_at=utcnow(), duration_seconds=round(clock.monotonic()-started, 3),
                        prompt_eval_count=body.get("prompt_eval_count"),
                        eval_count=body.get("eval_count"), input_hash=key))
        return value


def run_event(event, model):
    # Packet construction groups ONLY this accession; no later accession can enter any role.
    if any(instant(d["published_at"]) > instant(event["decision_cutoff"]) for d in event["documents"]):
        raise ValueError("Future document in historical packet")
    if not event["candidates"]:
        return dict(action="CASH", basis="NO_QUOTE_TRIGGER", findings=[], reviews={},
                    pm=None, reason="No qualifying citation in the frozen excerpt extractor")
    data = dict(symbol=event["symbol"], cutoff=event["decision_cutoff"],
                candidates=event["candidates"])
    found = validate_research(model.ask("RESEARCH", RESEARCH, data,
                                       research_schema(event["candidates"])), event["candidates"])
    if not found:
        return dict(action="CASH", basis="AGENT_NO_THESIS", findings=[], reviews={}, pm=None,
                    reason="Research found no qualifying current official thesis")
    schema = object_schema({f["id"]: object_schema(
        dict(verdict={"enum": ["SUPPORTED", "UNSUPPORTED"]},
             reason=dict(type="string", minLength=5, maxLength=180))) for f in found})
    reviews = model.ask("CONTRARIAN", CONTRARIAN,
                        dict(symbol=event["symbol"], cutoff=event["decision_cutoff"], findings=found), schema)
    if not isinstance(reviews, dict) or set(reviews) != {f["id"] for f in found}:
        raise ValueError("Missing Contrarian review")
    for item in reviews.values():
        if (set(item) != {"verdict", "reason"} or item["verdict"] not in {"SUPPORTED", "UNSUPPORTED"}
                or not isinstance(item["reason"], str) or not 5 <= len(item["reason"]) <= 180):
            raise ValueError("Invalid Contrarian verdict")
    supported = [f for f in found if reviews[f["id"]]["verdict"] == "SUPPORTED"]
    if not any(f["kind"] in POSITIVE for f in supported) or any(f["kind"] in NEGATIVE for f in supported):
        return dict(action="CASH", basis="NO_SUPPORTED_POSITIVE_OR_ADVERSE_FACT", findings=found,
                    reviews=reviews, pm=None, reason="No supported positive thesis without adverse fact")
    pm = model.ask("PM", PM, dict(symbol=event["symbol"], cutoff=event["decision_cutoff"],
                                 findings=supported, reviews=reviews),
                   object_schema(dict(action={"enum": ["BUY", "CASH"]},
                                      reason=dict(type="string", minLength=5, maxLength=180))))
    if (not isinstance(pm, dict) or set(pm) != {"action", "reason"}
            or pm["action"] not in {"BUY", "CASH"} or not isinstance(pm["reason"], str)
            or not 5 <= len(pm["reason"]) <= 180):
        raise ValueError("Invalid PM decision")
    return dict(action=pm["action"], basis="THREE_ROLE_AGENT", findings=found,
                reviews=reviews, pm=pm, reason=pm["reason"])


def decide(output, *, max_events=None, retry_errors=False):
    output = Path(output)
    protocol, events = read(output / "protocol.json"), read(output / "events.json")
    if protocol["decision_contract"] != decision_contract() or protocol["events_hash"] != digest(events):
        raise ValueError("Frozen decision contract or source events changed")
    model = LocalPilotModel(output, protocol)
    model.verify()
    processed = 0
    try:
        for event in events:
            path = output / "decisions" / (event["id"] + ".json")
            if path.exists():
                previous = read(path)
                if previous["event_hash"] != digest(event):
                    raise ValueError("Decision source packet changed")
                if not retry_errors or previous["action"] != "UNKNOWN":
                    continue
            if max_events is not None and processed >= max_events:
                break
            started = utcnow()
            try:
                result = run_event(event, model)
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                result = dict(action="UNKNOWN", basis="MODEL_OR_VALIDATION_ERROR",
                              error_type=type(exc).__name__,
                              reason=str(exc)[:240] if not isinstance(exc, httpx.HTTPError) else "Local Ollama request failed")
            save(path, dict(event_id=event["id"], event_hash=digest(event), symbol=event["symbol"],
                            accession=event["accession"], decision_cutoff=event["decision_cutoff"],
                            classified_at=utcnow(), started_at=started,
                            model_digest=protocol["model_digest"],
                            knowledge_basis="RETROSPECTIVE_SEC_ACCEPTANCE_RECONSTRUCTION", **result))
            processed += 1
            print(json.dumps(dict(processed=processed, symbol=event["symbol"],
                                  date=event["published_at"][:10], action=result["action"],
                                  basis=result["basis"])), flush=True)
    finally:
        model.close()
    return status(output)


def status(output):
    output = Path(output)
    events = read(output / "events.json")
    decisions = [read(p) for p in (output / "decisions").glob("*.json")]
    return dict(events=len(events), completed=len(decisions),
                pending=len(events)-len(decisions), actions=dict(Counter(d["action"] for d in decisions)),
                calls=len(list((output / "calls").glob("*.json"))),
                symbols_with_events=len({e["symbol"] for e in events}))


def replay(output):
    """Re-run validation and decisions using exact archived responses, with no model/network."""
    output = Path(output)
    protocol, events = read(output / "protocol.json"), read(output / "events.json")
    if protocol["decision_contract"] != decision_contract() or protocol["events_hash"] != digest(events):
        raise ValueError("Frozen decision contract or events changed")
    for path, sha in protocol["source_files"].items():
        if digest(Path(path).read_bytes()) != sha:
            raise ValueError("Historical source archive changed")
    model = LocalPilotModel(output, protocol, offline=True)
    verified = 0
    try:
        for event in events:
            decision = read(output / "decisions" / (event["id"] + ".json"))
            if decision["action"] == "UNKNOWN":
                continue
            result = run_event(event, model)
            if any(decision.get(k) != v for k, v in result.items()):
                raise ValueError("Archived decision does not replay")
            verified += 1
    finally:
        model.close()
    return dict(status="REPLAY_MATCH", decisions=verified, network_calls=0, model_calls=0)
