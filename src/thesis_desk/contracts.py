from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import urlparse

from src.backtesting.thesis_desk import NEGATIVE, POSITIVE, decimal, instant

FREE_OPEN_MODELS = {"nvidia/nemotron-3.5-super-vl:free"}
LOCAL_OPEN_MODELS = {"qwen3.5:9b", "qwen2.5:3b"}


def validate_llm(config):
    if (set(config) != {"provider", "model", "cost_policy"}
            or not ((config.get("provider") == "openrouter" and config.get("model") in FREE_OPEN_MODELS)
                    or (config.get("provider") == "ollama" and config.get("model") in LOCAL_OPEN_MODELS))
            or config.get("cost_policy") != "FREE_ONLY"):
        raise ValueError("Desk requires an explicitly approved free open model; paid routes are forbidden")


def now():
    return datetime.now(timezone.utc)


def normalized(text):
    return " ".join(str(text).split())


def digest(value):
    if not isinstance(value, bytes):
        value = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode()
    return hashlib.sha256(value).hexdigest()


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def validate_config(config):
    if config.get("schema") != "quantia-desk-config-v1" or config.get("mode") != "PAPER_ONLY":
        raise ValueError("Only quantia-desk-config-v1 / PAPER_ONLY is supported")
    validate_llm(config.get("llm", {}))
    if not 0 < decimal(config["initial_cash"]) <= 1000000000:
        raise ValueError("Invalid fictional capital")
    if not 0 < decimal(config["vix_yellow"]) < decimal(config["vix_red"]):
        raise ValueError("Invalid fixed VIX thresholds")
    if not 1 <= config["lookback_days"] <= 14 or not 1 <= config["max_documents_per_issuer"] <= 10:
        raise ValueError("Invalid bounded source budget")
    instruments = config["instruments"]
    if not 1 <= len(instruments) <= 5000 or len({i["ticker"] for i in instruments}) != len(instruments):
        raise ValueError("Declare unique instrument/source overrides")
    if config.get("universe_mode", "CONFIGURED") not in {"CONFIGURED", "ALL_RECORDED_EQUITIES"}:
        raise ValueError("Unknown universe mode")
    budget = config.get("research_budget", {})
    if budget and not (1 <= budget["documents_per_batch"] <= 12 and 1 <= budget["max_calls_per_run"] <= budget["max_calls_per_day"] <= 1000):
        raise ValueError("Invalid bounded free-model budget")
    for instrument in instruments:
        for field in ("ticker", "underlying"):
            if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", instrument[field]):
                raise ValueError("Invalid explicit instrument mapping")
        if instrument["currency"] != "ARS" or instrument["asset_type"] != "CEDEAR" or instrument["underlying_kind"] != "COMMON_STOCK":
            raise ValueError("This paper desk supports explicitly mapped ARS common-stock CEDEARs")
        if instrument["source_kind"] not in {"rss", "html"}:
            raise ValueError("Unsupported source adapter")
        check_url(instrument["source_url"], instrument["allowed_hosts"])


def check_url(url, allowed_hosts):
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in allowed_hosts or parsed.username or parsed.password or parsed.port not in {None, 443}:
        raise ValueError("Source URL is outside its configured official hosts")


def qualification_error(kind, quote):
    """Necessary admission conditions, not an automated proof of economic relevance."""
    if kind in {"GUIDANCE_RAISED", "GUIDANCE_REAFFIRMED", "GUIDANCE_CUT"}:
        if re.search(r"\bratings?\b|\bFitch\b|\bMoody|calificaci[oó]n|\bcredit (?:outlook|grade)\b", quote, re.I):
            return "CREDIT_RATING_NOT_OPERATING_GUIDANCE"
        verbs = {"GUIDANCE_RAISED": r"rais\w*|increas\w*|upgrad\w*|elev\w*",
                 "GUIDANCE_REAFFIRMED": r"reaffirm\w*|reiterat\w*|maintain\w*|reafirm\w*|manten\w*",
                 "GUIDANCE_CUT": r"cut\w*|lower\w*|reduc\w*|recort\w*"}
        if not re.search(verbs[kind], quote, re.I) or not re.search(r"guidance|outlook|forecast|guía|proyecci", quote, re.I):
            return "GUIDANCE_CHANGE_NOT_EXPLICIT_IN_CITATION"
    deals = {"DEAL_SIGNED", "DEAL_CLOSED", "DEAL_CANCELLED"}
    if kind in deals and re.search(r"pricing agreement|credit agreement|underwrit|indenture|loan agreement|compensation|employment agreement|offering of (?:notes|bonds|debt)", quote, re.I):
        return "ROUTINE_FINANCING_OR_EMPLOYMENT_NOT_OPERATING_THESIS"
    if kind in deals:
        if re.search(r"voting rights|TR-1|major holdings|own shares|treasury shares|share buyback|share repurchase|accelerated (?:share|stock) (?:purchase|repurchase)", quote, re.I):
            return "OWNERSHIP_NOTICE_OR_BUYBACK_NOT_OPERATING_THESIS"
        if re.search(r"\bno (?:binding|definitive) agreement|\bnot (?:yet )?(?:signed|executed|completed|closed|terminated|cancelled|canceled)\b", quote, re.I):
            return "DEAL_STATUS_NEGATED_IN_CITATION"
        completion = {"DEAL_SIGNED": r"entered into|execut\w*|signed|definitive|binding|firm\w*|suscrib\w*",
                      "DEAL_CLOSED": r"complet\w*|closed|consummat\w*|finali[sz]\w*|conclu\w*|cerr\w*",
                      "DEAL_CANCELLED": r"cancel\w*|terminat\w*|abandon\w*|rescind\w*|withdraw\w*|terminad\w*|resuelt\w*"}[kind]
        if not re.search(completion, quote, re.I):
            return "DEAL_STATUS_NOT_EXPLICIT_IN_CITATION"
    if kind == "LEGAL_ADVERSE":
        legal = r"lawsuit|litigation|legal proceeding|court|regulator|\bSEC\b|\bDOJ\b|antitrust|fine[ds]?\b|penalt\w*|demanda|tribunal|multa|sanci[oó]n"
        event = r"filed|commenc\w*|served|impos\w*|charged|convict\w*|found liable|ruled against|ordered|sanction\w*|fined|present[oó]|inici[oó]|impuso|conden\w*"
        hypothetical = r"\bmay\b|\bcould\b|\bmight\b|ability to|risk factors|forward.looking|podr[ií]a"
        if not re.search(r"\b(?:" + legal + r")\b", quote, re.I) or not re.search(r"\b(?:" + event + r")\b", quote, re.I) or re.search(hypothetical, quote, re.I):
            return "LEGAL_EVENT_NOT_EXPLICIT_IN_CITATION"
    return None


def validate_research(response, documents, ticker, at):
    """An LLM cannot create sources, dates, identity, permissions or executions."""
    if not isinstance(response, dict) or set(response) != {"findings"} or not isinstance(response["findings"], list):
        raise ValueError("Research must return only findings")
    docs = {d["id"]: d for d in documents}
    findings, seen = [], set()
    if len(response["findings"]) > len(documents):
        raise ValueError("At most one material finding per document")
    for item in response["findings"]:
        if set(item) != {"document_id", "kind", "quote", "thesis", "invalidation"}:
            raise ValueError("Unexpected research fields")
        doc = docs.get(item["document_id"])
        if not doc or doc.get("ticker") != ticker or doc["id"] in seen or item["kind"] not in POSITIVE | NEGATIVE:
            raise ValueError("Unknown/duplicate source or fact kind")
        quote = normalized(item["quote"])
        if not 3 <= len(quote.split()) <= 25 or quote not in normalized(doc["text"]):
            raise ValueError("Finding citation is not an exact short source excerpt")
        if qualification_error(item["kind"], quote):
            raise ValueError(qualification_error(item["kind"], quote))
        if instant(doc["available_at"]) > at or instant(doc["published_at"]) > at:
            raise ValueError("Future source evidence")
        if not all(isinstance(item[k], str) and 5 <= len(item[k]) <= 1000 for k in ("thesis", "invalidation")):
            raise ValueError("A concise thesis and factual invalidation are required")
        seen.add(doc["id"])
        # Stable across repeat downloads/rephrasing. A new version never creates a new trading thesis.
        identity = digest([ticker, doc["source_url"], item["kind"]])
        findings.append(dict(id=identity, ticker=ticker, thesis_id=identity if item["kind"] in POSITIVE else "*",
                             kind=item["kind"], source_type=doc.get("source_type", "IR"), source_url=doc["source_url"],
                             source_hash=doc["sha256"], document_id=doc["id"], quote=quote,
                             published_at=doc["published_at"], available_at=at.isoformat(),
                             knowledge_basis="ARCHIVED_AT_TIME", thesis=item["thesis"], invalidation=item["invalidation"],
                             classification_basis="LLM_SOURCE_CITED_NOT_INDEPENDENTLY_VERIFIED"))
    return findings


def validate_review(response, findings, documents):
    if not isinstance(response, dict) or set(response) != {"reviews"} or not isinstance(response["reviews"], list):
        raise ValueError("Contrarian must return only reviews")
    expected = {f["id"] for f in findings}
    seen, reviews = set(), []
    docs = {d["id"]: d for d in documents}
    by_id = {f["id"]: f for f in findings}
    for item in response["reviews"]:
        if set(item) != {"finding_id", "verdict", "reason", "counterevidence_document_id", "counterevidence_quote"}:
            raise ValueError("Unexpected review fields")
        identity = item["finding_id"]
        if identity not in expected or identity in seen or item["verdict"] not in {"SUPPORTED", "UNSUPPORTED", "FACT_VETO"}:
            raise ValueError("Missing/duplicate review or invalid verdict")
        if not isinstance(item["reason"], str) or not 5 <= len(item["reason"]) <= 1000:
            raise ValueError("Review needs a bounded explanation")
        if item["verdict"] == "FACT_VETO":
            if by_id[identity]["kind"] not in POSITIVE:
                raise ValueError("Only a positive thesis can receive a factual veto")
            source = docs.get(item["counterevidence_document_id"])
            quote = normalized(item["counterevidence_quote"])
            if not source or not 3 <= len(quote.split()) <= 25 or quote not in normalized(source["text"]):
                raise ValueError("A factual veto needs actual counterevidence")
            if source.get("ticker") != by_id[identity]["ticker"]:
                raise ValueError("Counterevidence belongs to another issuer")
            if instant(source["published_at"]) <= instant(by_id[identity]["published_at"]):
                raise ValueError("Counterevidence must be later than the thesis")
        elif item["counterevidence_document_id"] is not None or item["counterevidence_quote"] is not None:
            raise ValueError("Counterevidence belongs only to a factual veto")
        seen.add(identity)
        reviews.append(item)
    if seen != expected:
        raise ValueError("Every finding needs a review")
    return reviews


def validate_selection(response, candidates):
    if not isinstance(response, dict) or set(response) != {"watchlist"} or not isinstance(response["watchlist"], list):
        raise ValueError("PM must return only watchlist")
    expected = {f["id"] for f in candidates}
    seen = set()
    for row in response["watchlist"]:
        if set(row) != {"finding_id", "decision", "reason"} or row["finding_id"] not in expected or row["finding_id"] in seen:
            raise ValueError("PM cannot introduce/duplicate candidates")
        if row["decision"] not in {"WATCHLIST", "PASS"} or not isinstance(row["reason"], str) or not 5 <= len(row["reason"]) <= 1000:
            raise ValueError("PM cannot size or trade")
        seen.add(row["finding_id"])
    if seen != expected:
        raise ValueError("PM must account for all reviewed candidates")
    return response["watchlist"]
