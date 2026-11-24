"""Resumable, document-grained review queue. Every API attempt consumes a durable free-call budget."""
from __future__ import annotations

from pathlib import Path
import json
import sqlite3
import uuid

from src.backtesting.thesis_desk import POSITIVE
from src.thesis_desk import agents as agents_module, contracts as contracts_module
from src.thesis_desk.agents import passages, citation_options
from src.thesis_desk.contracts import digest, now


class BudgetExhausted(RuntimeError):
    pass


class CallBudget:
    def __init__(self, path, policy, run_id):
        self.path, self.policy, self.run_id, self.used = path, policy, run_id, 0
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,day TEXT,at TEXT,run_id TEXT,role TEXT)")

    def reserve(self, role):
        at = now()
        with sqlite3.connect(self.path, timeout=2) as conn:
            conn.execute("BEGIN IMMEDIATE")
            used_today = conn.execute("SELECT count(*) FROM attempts WHERE day=?", (at.date().isoformat(),)).fetchone()[0]
            if self.used >= self.policy["max_calls_per_run"] or used_today >= self.policy["max_calls_per_day"]:
                raise BudgetExhausted("Free API call budget exhausted; queue retained")
            conn.execute("INSERT INTO attempts VALUES(?,?,?,?,?)", (uuid.uuid4().hex, at.date().isoformat(), at.isoformat(), self.run_id, role))
        self.used += 1

    def summary(self):
        with sqlite3.connect(self.path) as conn:
            used = conn.execute("SELECT count(*) FROM attempts WHERE day=?", (now().date().isoformat(),)).fetchone()[0]
        return dict(attempts_this_run=self.used, attempts_today=used, daily_limit=self.policy["max_calls_per_day"],
                    run_limit=self.policy["max_calls_per_run"], policy="FREE_ONLY_NO_PAID_FALLBACK")


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def document_batches(documents, size):
    batch, used = [], 0
    for doc in documents:
        chars = len(json.dumps(dict(passages(doc), citations=citation_options(doc)), ensure_ascii=False))
        if batch and (len(batch) >= size or used + chars > 60000):
            yield batch
            batch, used = [], 0
        batch.append(doc)
        used += chars
    if batch:
        yield batch


async def review_queue(bundle, agents, store, config):
    contract = digest([digest(Path(m.__file__).read_bytes()) for m in (agents_module, contracts_module)] + [digest(Path(__file__).read_bytes())])
    documents = sorted(bundle["documents"], key=lambda d: (d["published_at"], d["ticker"], d["id"]))
    keys = {d["id"]: digest(["document-v2", contract, agents.provider, agents.model, d["id"]]) for d in documents}
    cached = {d["id"]: store.cached(keys[d["id"]]) for d in documents}
    issuer_docs = {t: [d for d in documents if d["ticker"] == t] for t in {d["ticker"] for d in documents}}
    contexts = {t: digest([d["id"] for d in docs]) for t, docs in issuer_docs.items()}
    for doc in documents:
        value = cached[doc["id"]]
        if value and value["findings"] and value.get("review_context") != contexts[doc["ticker"]]:
            value["reviews"] = None
    pending = [d for d in documents if not cached[d["id"]] or cached[d["id"]].get("reviews") is None]
    cache_hits = [d["id"] for d in documents if d not in pending]
    errors = []
    async def process(batch):
        new = [d for d in batch if not cached[d["id"]]]
        if new:
            findings = await agents.research_many(new)
            for doc in new:
                value = dict(findings=[f for f in findings if f["document_id"] == doc["id"]], reviews=None,
                             rejections=[r for r in getattr(agents, "rejections", []) if r["document_id"] == doc["id"]])
                if not value["findings"]:
                    value["reviews"] = []
                cached[doc["id"]] = value
                store.cache(keys[doc["id"]], value)
        findings = [f for d in batch for f in cached[d["id"]]["findings"] if cached[d["id"]]["reviews"] is None]
        if findings:
            checked = []
            for ticker in sorted({f["ticker"] for f in findings}):
                checked.extend(await agents.review([f for f in findings if f["ticker"] == ticker], issuer_docs[ticker]))
            for doc in batch:
                value = cached[doc["id"]]
                identities = {f["id"] for f in value["findings"]}
                value["reviews"] = [r for r in checked if r["finding_id"] in identities]
                value["review_context"] = contexts[doc["ticker"]]
                store.cache(keys[doc["id"]], value)

    def failure(batch, exc):
        code = "FREE_BUDGET_EXHAUSTED" if isinstance(exc, BudgetExhausted) else (
            agents.traces[-1].get("error_code", type(exc).__name__) if agents.traces else type(exc).__name__)
        detail = str(exc)[:180] if isinstance(exc, ValueError) else "MODEL_CALL_FAILED"
        errors.extend(dict(ticker=t, stage="RESEARCH_OR_CONTRARIAN", error_type=type(exc).__name__, error_code=code, detail=detail)
                      for t in sorted({d["ticker"] for d in batch}))
        print(json.dumps(dict(stage="MODEL_ERROR", documents=len(batch), error_code=code, detail=detail)), flush=True)

    # Retain oldest unseen evidence first; isolate malformed batches without dropping documents.
    for batch in document_batches(pending, config["research_budget"]["documents_per_batch"]):
        try:
            await process(batch)
        except Exception as exc:
            if isinstance(exc, BudgetExhausted) or getattr(agents, "blocked_code", None):
                failure(batch, exc)
                break
            if len(batch) > 1 and isinstance(exc, ValueError):
                for doc in batch:
                    if cached[doc["id"]] and cached[doc["id"]].get("reviews") is not None:
                        continue
                    try:
                        await process([doc])
                    except Exception as single_exc:
                        failure([doc], single_exc)
                        if isinstance(single_exc, BudgetExhausted) or getattr(agents, "blocked_code", None):
                            break
            else:
                failure(batch, exc)
        print(json.dumps(dict(stage="MODEL_REVIEW", documents_reviewed=sum(v is not None and v.get("reviews") is not None for v in cached.values()),
                              total=len(documents), attempts=agents.budget.used if getattr(agents, "budget", None) else len(agents.traces))), flush=True)
    findings, reviews, queue = [], [], []
    for doc in documents:
        value = cached[doc["id"]]
        if not value or value["reviews"] is None:
            queue.append(dict(document_id=doc["id"], ticker=doc["ticker"], published_at=doc["published_at"],
                              stage="RESEARCH" if not value else "CONTRARIAN"))
        else:
            findings.extend(value["findings"])
            reviews.extend(value["reviews"])
    supported = {r["finding_id"] for r in reviews if r["verdict"] == "SUPPORTED"}
    admitted = list({f["id"]: f for f in findings if f["id"] in supported}.values())
    members = {m["ticker"] for m in bundle["universe"]}
    awaiting = {r["ticker"] for r in queue}
    candidates = [f for f in admitted if f["kind"] in POSITIVE and f["ticker"] in members and f["ticker"] not in awaiting]
    selection = []
    for group in chunks(candidates, 20):
        key = digest(["pm-v2", contract, agents.provider, agents.model, group])
        value = store.cached(key)
        try:
            if value is None:
                value = await agents.select(group)
                store.cache(key, value)
            else:
                cache_hits.append("PM:" + key)
            selection.extend(value)
        except Exception as exc:
            errors.append(dict(stage="PM", error_type=type(exc).__name__, error_code="FREE_BUDGET_EXHAUSTED" if isinstance(exc, BudgetExhausted) else type(exc).__name__))
            queue.extend(dict(ticker=f["ticker"], document_id=f["document_id"], published_at=f["published_at"], stage="PM") for f in group)
    return dict(findings=findings, reviews=reviews, admitted=admitted, selection=selection, errors=errors, queue=queue,
                cache_hits=cache_hits, model_traces=agents.traces, documents_total=len(documents),
                rejected_findings=[r for value in cached.values() if value for r in value.get("rejections", [])],
                documents_reviewed=sum(bool(v is not None and v["reviews"] is not None) for v in cached.values()),
                budget=agents.budget.summary() if getattr(agents, "budget", None) else None)
