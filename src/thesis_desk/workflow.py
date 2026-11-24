from __future__ import annotations

from pathlib import Path

from src.backtesting.thesis_desk import NEGATIVE, POSITIVE, instant, validate_evidence
from src.thesis_desk import agents as agents_module
from src.thesis_desk import contracts as contracts_module
from src.thesis_desk.contracts import digest, now
from src.thesis_desk.paper import advance_paper


async def research_book(bundle, agents, store):
    findings, reviews, errors, cache_hits = [], [], [], []
    tickers = sorted({doc["ticker"] for doc in bundle["documents"]})
    contract = digest([digest(Path(m.__file__).read_bytes()) for m in (agents_module, contracts_module)])
    for ticker in tickers:
        docs = [d for d in bundle["documents"] if d["ticker"] == ticker]
        key = digest(["issuer", contract, agents.provider, agents.model, ticker, sorted(d["id"] for d in docs)])
        cached = store.cached(key)
        if cached:
            findings.extend(cached["findings"])
            reviews.extend(cached["reviews"])
            cache_hits.append(ticker)
            continue
        try:
            proposed = await agents.research(ticker, docs)
            checked = await agents.review(proposed, docs) if proposed else []
            store.cache(key, dict(findings=proposed, reviews=checked))
            findings.extend(proposed)
            reviews.extend(checked)
        except Exception as exc:
            errors.append(dict(ticker=ticker, stage="RESEARCH_OR_CONTRARIAN", error_type=type(exc).__name__,
                               error_code=agents.traces[-1].get("error_code") if agents.traces else None,
                               detail=str(exc)[:200] if isinstance(exc, ValueError) else "PROVIDER_CALL_FAILED"))
    supported = {r["finding_id"] for r in reviews if r["verdict"] == "SUPPORTED"}
    admitted = [f for f in findings if f["id"] in supported]
    members = {m["ticker"] for m in bundle["universe"]}
    candidates = [f for f in admitted if f["kind"] in POSITIVE and f["ticker"] in members]
    selection = []
    if candidates:
        key = digest(["pm", contract, agents.provider, agents.model, candidates])
        cached = store.cached(key)
        if cached:
            selection = cached
            cache_hits.append("PM")
        else:
            try:
                selection = await agents.select(candidates)
                store.cache(key, selection)
            except Exception as exc:
                errors.append(dict(stage="PM", error_type=type(exc).__name__,
                                   error_code=agents.traces[-1].get("error_code") if agents.traces else None,
                                   detail=str(exc)[:200] if isinstance(exc, ValueError) else "PROVIDER_CALL_FAILED"))
    return dict(findings=findings, reviews=reviews, admitted=admitted, selection=selection,
                errors=errors, cache_hits=cache_hits, model_traces=agents.traces)


def assemble_evidence(bundle, research, rows, earnings_blackouts, earnings_covered, config, at):
    earnings_covered = set(earnings_covered) | set(bundle.get("earnings_covered", []))
    selected = {r["finding_id"] for r in research["selection"] if r["decision"] == "WATCHLIST"}
    blocks = {i["ticker"]: [] for i in config["instruments"]}
    employment_known = any(r.get("source") == "EMPLOYMENT" and r["status"] == "CAPTURED" for r in bundle["source_status"])
    research_failed = {r["ticker"] for r in research["errors"] if r.get("ticker")}
    queued = {r["ticker"] for r in research.get("queue", [])}
    catalog = {r["ticker"]: r for r in bundle.get("catalog", [])}
    for ticker in blocks:
        if ticker in catalog and catalog[ticker]["status"] != "IDENTIFIED":
            blocks[ticker].append(catalog[ticker]["status"])
        if ticker in queued:
            blocks[ticker].append("RESEARCH_QUEUE_PENDING")
        if not employment_known:
            blocks[ticker].append("EMPLOYMENT_CALENDAR_UNKNOWN")
        if ticker not in earnings_covered:
            blocks[ticker].append("EARNINGS_CALENDAR_UNKNOWN")
        if ticker in research_failed:
            blocks[ticker].append("RESEARCH_OR_REVIEW_FAILED")
        status = next((r for r in bundle["source_status"] if r.get("ticker") == ticker), None)
        if not status or status["status"] != "CAPTURED" or status.get("truncated"):
            blocks[ticker].append("OFFICIAL_SOURCE_INCOMPLETE")
    blocks = {t: reasons for t, reasons in blocks.items() if reasons}
    facts = [f for f in research["admitted"] if f["kind"] in NEGATIVE or (f["id"] in selected and f["ticker"] not in blocks)]
    findings = {f["id"]: f for f in research.get("findings", [])}
    documents = {d["id"]: d for d in bundle.get("documents", [])}
    vetoes = []
    for review in research.get("reviews", []):
        if review["verdict"] != "FACT_VETO":
            continue
        fact = findings[review["finding_id"]]
        doc = documents[review["counterevidence_document_id"]]
        contracts_module.validate_review({"reviews": [review]}, [fact], [doc])
        vetoes.append(dict(id=digest([fact["id"], doc["id"], "FACT_VETO"]), ticker=fact["ticker"],
                           thesis_id=fact["thesis_id"], thesis_published_at=fact["published_at"],
                           published_at=doc["published_at"], available_at=at.isoformat(),
                           source_type=doc.get("source_type", "IR"), source_url=doc["source_url"],
                           source_hash=doc["sha256"], document_id=doc["id"], quote=review["counterevidence_quote"],
                           reason=review["reason"], knowledge_basis="ARCHIVED_AT_TIME"))
    evidence = dict(schema="thesis-desk-evidence-v1", facts=facts, universe=bundle["universe"], risk=bundle["risk"],
                    vetoes=vetoes, blackouts=bundle["blackouts"] + earnings_blackouts, coverage=[])
    validate_evidence(evidence)
    if any(instant(r["available_at"]) > at for key in ("facts", "vetoes", "universe", "risk", "blackouts") for r in evidence[key]):
        raise ValueError("Future evidence in current paper run")
    # Show technical readiness for every PM-selected thesis, including blocked entries.
    from src.backtesting.thesis_desk import DeskPolicy, ThesisDesk
    technical = ThesisDesk(rows, evidence, DeskPolicy(variant="RISK_V2", initial_cash=config["initial_cash"]))
    cards = []
    by_id = {f["id"]: f for f in research["admitted"]}
    for item in research["selection"]:
        fact = by_id[item["finding_id"]]
        member = technical.universe(fact["ticker"], at)
        zone = technical.zone(fact, member, at) if member else None
        reasons = list(blocks.get(fact["ticker"], []))
        if zone is None:
            reasons.append("TECHNICAL_HISTORY_UNAVAILABLE")
        cards.append(dict(ticker=fact["ticker"], fact_id=fact["id"], thesis=fact["thesis"], invalidation=fact["invalidation"],
                          source_quote=fact.get("quote"),
                          source_url=fact["source_url"], pm_decision=item["decision"], pm_reason=item["reason"],
                          zone_low=str(zone[0]) if zone else None, zone_high=str(zone[1]) if zone else None,
                          entry_blocks=reasons))
    return evidence, blocks, cards


def finish_book(bundle, research, rows, earnings_blackouts, earnings_covered, config, store, at=None):
    at = at or now()
    evidence, blocks, cards = assemble_evidence(bundle, research, rows, earnings_blackouts, earnings_covered, config, at)
    state, paper = advance_paper(rows, evidence, config, store.load(), at, blocks)
    result = dict(schema="quantia-desk-run-v1", at=at.isoformat(), mode="PAPER_ONLY", watchlist=cards,
                  research=research, source_status=bundle["source_status"], entry_blocks=blocks, paper=paper,
                  scope=[i["ticker"] for i in config["instruments"]],
                  coverage=bundle.get("coverage_basis", "CONFIGURED_ISSUERS_BOUNDED_OFFICIAL_FEEDS_NOT_ENTIRE_SP500"),
                  technical_basis="RECORDED_16_30_TO_17_ARS_QUOTES_CURRENTLY_RECONSTRUCTED", affects_execution=False)
    return state, result, evidence


def render(result):
    paper = result["paper"]
    lines = ["# Quantia — desk de tesis en simulación", "", f"Fecha: {result['at']}. Universo: {len(result['scope'])} símbolos; detalle completo en report.html y result.json.",
             f"Estado: {paper['status']}. Fills de esta corrida: {len(paper['fills'])}. Posiciones simuladas: {paper['valuation']['open_positions']}.",
             "", "## Evaluación de candidatos", ""]
    if not result["watchlist"]:
        lines.append("Sin tesis admitidas a la watchlist en esta corrida. No equivale a ausencia de oportunidades en el mercado.")
    for card in result["watchlist"]:
        lines += [f"### {card['ticker']} — {card['pm_decision']}", "", "Hipótesis de Research: " + card["thesis"],
                  f"Decisión del PM: {card['pm_reason']}",
                  f"Cita de la fuente: {card['source_quote'] or 'no disponible'}",
                  f"Invalidación: {card['invalidation']}", f"Zona técnica ARS: {card['zone_low']} — {card['zone_high']}.",
                  f"Bloqueos: {', '.join(card['entry_blocks']) or 'ninguno por evidencia/calendario; aplican precio, riesgo y cupo'}.",
                  f"[Comunicado oficial]({card['source_url']})", ""]
    lines += ["## Registro", "", f"Research: {len(result['research']['findings'])} hallazgos; Contrarian: {len(result['research']['reviews'])} revisiones.",
              f"Llamadas al modelo: {sum(t.get('attempted', t['status'] != 'SKIPPED_PROVIDER_UNAVAILABLE') for t in result['research']['model_traces'])}; reutilizaciones verificadas de caché: {len(result['research']['cache_hits'])}.",
              "Dinero, posiciones y fills son ficticios. El registro no acredita rentabilidad ni reproduce las decisiones históricas del autor.",
              "La zona usa últimas cotizaciones registradas entre 16:30 y 17:00, no el cierre oficial de BYMA.", ""]
    lines += ["Cobertura y bloqueos agregados: " + str(result.get("metrics", {}).get("coverage", {})),
              f"Errores de revisión: {len(result['research']['errors'])}; detalle por instrumento en result.json."]
    return "\n".join(lines) + "\n"
