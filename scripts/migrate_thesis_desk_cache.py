"""Revalidate the known desk cache upgrade, preserving original evidence timestamps.

No model calls, source downloads, portfolio reset, or old-cache deletion. PM decisions
are deliberately not migrated. Unknown versions fail closed.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path

from src.backtesting.thesis_desk import instant
from src.thesis_desk import agents, contracts, research_queue
from src.thesis_desk.contracts import digest, dump, normalized, now, validate_research, validate_review
from src.thesis_desk.paper import PaperStore


LEGACY_CONTRACT = "54cebe5bdd11f4543193e600e5ef969c9d29151fc73f0926e6e323ace37b3d98"
GROUNDING_CONTRACT = "a86e0446beb09865c58a4f16e26aa8935735360a0b9a451aff20031a52ad34a1"
OPERATING_CONTRACT = "8d8dc690a5c09ab916621588228541e3681a77a2d2c42fab4597c0f37d8fe82c"
TARGET_HASHES = {
    "agents.py": "fb7a0a1a59b95d7923c742cddac2f0da6bd12a4fa56c6e20b5461571dc016e96",
    "contracts.py": "694a64a30fe7ac7f8742926aa6ab40284e0380e7fa36a350ac1f4d6e4593db6c",
}


def revalidate_cached(value, document, issuer_documents, at):
    """A narrower admission policy may retain old valid facts, never their old errors."""
    value = deepcopy(value)
    original = value["findings"]
    proposals = [{k: f[k] for k in ("document_id", "kind", "quote", "thesis", "invalidation")} for f in original]
    checked = validate_research({"findings": proposals}, [document], document["ticker"], at)
    for prior, current in zip(original, checked):
        if not instant(prior["published_at"]) <= instant(prior["available_at"]) <= at:
            raise ValueError("Invalid original knowledge timestamp")
        if {k: v for k, v in prior.items() if k != "available_at"} != {k: v for k, v in current.items() if k != "available_at"}:
            raise ValueError("Cached fact differs from its archived source")
    if value["reviews"] is not None:
        try:
            validate_review({"reviews": value["reviews"]}, original, issuer_documents)
        except ValueError:
            value["reviews"] = None
    return value


def migrate(state_dir, from_run, output):
    modules = (agents, contracts, research_queue)
    target_hashes = {Path(m.__file__).name: digest(Path(m.__file__).read_bytes()) for m in modules}
    if any(target_hashes[name] != expected for name, expected in TARGET_HASHES.items()):
        raise ValueError("This migration supports only the audited target validators")
    manifest = json.loads((from_run / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("code_changed_during_run"):
        raise ValueError("Cannot upgrade a run whose code changed during execution")
    code = {p.replace("\\", "/"): sha for p, sha in manifest["code_sha256"].items()}
    legacy = digest([code["src/thesis_desk/" + Path(m.__file__).name] for m in modules])
    if legacy not in {LEGACY_CONTRACT, GROUNDING_CONTRACT, OPERATING_CONTRACT}:
        raise ValueError("Unsupported source cache contract")
    for name, expected in manifest["artifacts"].items():
        path = (from_run / name).resolve()
        if not path.is_relative_to(from_run.resolve()) or digest(path.read_bytes()) != expected:
            raise ValueError("Source run artifact integrity failure")
    read = lambda name: json.loads((from_run / name).read_text(encoding="utf-8"))
    config, bundle, research = read("config.json"), read("sources.json"), read("research.json")
    target = digest([target_hashes[Path(m.__file__).name] for m in modules])
    at = now()
    report = dict(at=at.isoformat(), source_run=manifest["run_id"], source_contract=legacy, target_contract=target,
                  target_hashes=target_hashes, migrated=0, already_present=0, absent=0, rejected=[], signal_evidence_backfilled=0,
                  cache_source_contract_counts={}, migration_sha256=digest(Path(__file__).read_bytes()),
                  mode="LOCAL_CACHE_UPGRADE_NO_MODEL_NO_PORTFOLIO_RESET")
    docs = {d["id"]: d for d in bundle["documents"]}
    own = {t: [d for d in docs.values() if d["ticker"] == t] for t in {d["ticker"] for d in docs.values()}}
    store = PaperStore(state_dir / "paper.sqlite", config)
    try:
        for doc in docs.values():
            suffix = [config["llm"]["provider"], config["llm"]["model"], doc["id"]]
            new_key = digest(["document-v2", target] + suffix)
            if store.cached(new_key) is not None:
                report["already_present"] += 1
                continue
            # The intermediate audited contract also holds the real regression results.
            # Revalidate those against this run's exact archived source before retaining them.
            for source_contract in dict.fromkeys((OPERATING_CONTRACT, legacy)):
                value = store.cached(digest(["document-v2", source_contract] + suffix))
                if value is not None:
                    break
            if value is None:
                report["absent"] += 1
                continue
            try:
                value = revalidate_cached(value, doc, own[doc["ticker"]], at)
            except (ValueError, KeyError, TypeError) as exc:
                report["rejected"].append(dict(document_id=doc["id"], ticker=doc["ticker"], reason=str(exc)[:180]))
                continue
            store.cache(new_key, value)
            report["migrated"] += 1
            counts = report["cache_source_contract_counts"]
            counts[source_contract] = counts.get(source_contract, 0) + 1
        facts = {f["id"]: f for f in research["findings"]}
        for sid, ticker, first_seen in store.conn.execute("SELECT id,ticker,first_seen FROM desk_signals").fetchall():
            fact = facts.get(sid)
            if not fact or fact["ticker"] != ticker or instant(fact["available_at"]) > instant(first_seen):
                continue
            doc = docs.get(fact["document_id"])
            if (not doc or fact["source_hash"] != doc["sha256"] or fact["source_url"] != doc["source_url"]
                    or fact["ticker"] != doc["ticker"] or fact["published_at"] != doc["published_at"]
                    or normalized(fact["quote"]) not in normalized(doc["text"])):
                raise ValueError("Signal source provenance mismatch")
            # Preserve even an invalid old classification so metrics can explicitly exclude it.
            cursor = store.conn.execute("INSERT OR IGNORE INTO desk_signal_evidence VALUES(?,?)", (sid, json.dumps(fact)))
            report["signal_evidence_backfilled"] += cursor.rowcount
        store.conn.commit()
        dump(output, report)
        return report
    finally:
        store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--from-run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = migrate(args.state_dir, args.from_run, args.output)
    print(json.dumps({k: (len(v) if k == "rejected" else v) for k, v in result.items() if k != "target_hashes"}))


if __name__ == "__main__":
    main()
