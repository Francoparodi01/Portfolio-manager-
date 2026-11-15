"""Run the source-grounded desk with fictional capital, without touching production.

python -m scripts.run_thesis_desk --once
python -m scripts.run_thesis_desk --watch --poll-seconds 300
python -m scripts.run_thesis_desk --replay outputs/thesis-desk-free/runs/RUN_ID
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

import httpx
from dotenv import load_dotenv

from src.backtesting.thesis_desk import DeskPolicy, instant
from src.thesis_desk.agents import DeskAgents
from src.thesis_desk.contracts import digest, dump, now, validate_config
from src.thesis_desk.paper import PaperStore, advance_paper
from src.thesis_desk.sources import capture_market, collect_public, normalize_market
from src.thesis_desk.workflow import finish_book, render, research_book
from src.thesis_desk.catalog import read_market_universe
from src.thesis_desk.expanded_sources import collect_all
from src.thesis_desk.research_queue import BudgetExhausted, CallBudget, review_queue
from src.thesis_desk.measurement import observe, measure
from src.thesis_desk.dashboard import render_dashboard

ROOT = Path(__file__).resolve().parents[1]


class ReviewRunBudget(CallBudget):
    """Leave a bounded part of each pass available for the final PM stage."""

    def __init__(self, path, policy, run_id, pm_reserve_calls):
        super().__init__(path, policy, run_id)
        self.pm_reserve_calls = pm_reserve_calls

    def reserve(self, role):
        if role != "PM" and self.used >= self.policy["max_calls_per_run"] - self.pm_reserve_calls:
            raise BudgetExhausted("Research allocation exhausted; remaining calls reserved for PM")
        super().reserve(role)

    def summary(self):
        return dict(super().summary(), pm_reserve_calls=self.pm_reserve_calls)


def review_runtime(config, max_model_calls=None, documents_per_batch=None, model_timeout_seconds=None, pm_reserve_calls=None):
    """Operational limits are archived separately from the immutable portfolio policy."""
    policy = dict(config["research_budget"])
    if max_model_calls is not None:
        if type(max_model_calls) is not int or not 1 <= max_model_calls <= policy["max_calls_per_run"]:
            raise ValueError("max-model-calls must be positive and within the configured per-run limit")
        policy["max_calls_per_run"] = max_model_calls
    local = config["llm"]["provider"] == "ollama"
    batch = documents_per_batch if documents_per_batch is not None else (1 if local else policy["documents_per_batch"])
    timeout = model_timeout_seconds if model_timeout_seconds is not None else (1800 if local else 300)
    reserve = pm_reserve_calls if pm_reserve_calls is not None else min(1, policy["max_calls_per_run"] - 1)
    if type(batch) is not int or not 1 <= batch <= policy["documents_per_batch"]:
        raise ValueError("documents-per-batch must be positive and within the configured batch limit")
    if type(timeout) is not int or not 60 <= timeout <= 3600:
        raise ValueError("model-timeout-seconds must be between 60 and 3600")
    if type(reserve) is not int or not 0 <= reserve <= policy["max_calls_per_run"]:
        raise ValueError("pm-reserve-calls must be within the per-run call limit")
    policy["documents_per_batch"] = batch
    return dict(research_budget=policy, model_timeout_seconds=timeout, pm_reserve_calls=reserve)


def seed_sources(store, source, target):
    """Copy only hash-verified public receipts, preserving their original availability."""
    raw = target / "source-cache"
    raw.mkdir(parents=True, exist_ok=True)
    cache_file = source / "research-cache.sqlite"
    if not cache_file.exists():
        cache_file = source / "paper.sqlite"
    with sqlite3.connect(cache_file.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        entries = connection.execute("SELECT key,payload FROM model_cache WHERE key LIKE 'source:%'").fetchall()
    for key, serialized in entries:
        receipt = json.loads(serialized)
        origin = Path(receipt["raw_path"]).resolve()
        if not origin.is_relative_to(source.resolve()) or not origin.is_file():
            continue
        body = origin.read_bytes()
        if digest(body) != receipt["sha256"]:
            raise ValueError("Source seed integrity failure")
        destination = raw / (receipt["sha256"] + ".bin")
        if not destination.exists():
            try:
                os.link(origin, destination)
            except OSError:
                destination.write_bytes(body)
        store.cache(key, dict(receipt, raw_path=str(destination.resolve())))


async def run_once(config, state_dir, container="cocos_db", user="quantia", database="quantia", *, agents=None, collector=None, market_reader=None, seed_cache=None, max_model_calls=None, documents_per_batch=None, model_timeout_seconds=None, pm_reserve_calls=None):
    validate_config(config)
    runtime = review_runtime(config, max_model_calls, documents_per_batch, model_timeout_seconds, pm_reserve_calls)
    state_dir.mkdir(parents=True, exist_ok=True)
    run_id = now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    directory = state_dir / "runs" / run_id
    directory.mkdir(parents=True)
    code = list((ROOT / "src/thesis_desk").glob("*.py")) + [ROOT / "src/backtesting/thesis_desk.py", Path(__file__),
            ROOT / "src/core/market_calendar.py", ROOT / "config/market_holidays_ar.json", ROOT / "src/agentic/model_transport.py"]
    code_hashes = {str(p.relative_to(ROOT)): digest(p.read_bytes()) for p in code}
    store = PaperStore(state_dir / "paper.sqlite", config)
    try:
        if seed_cache and store.load() is None:
            seed_sources(store, seed_cache, state_dir)
        dump(directory / "config.json", config)
        dump(directory / "runtime.json", runtime)
        dump(directory / "policy.json", dict(policy=asdict(DeskPolicy(variant="RISK_V2", initial_cash=config["initial_cash"])),
              rules="FIXED_BEFORE_CAPTURE_NO_PARAMETER_SEARCH", execution="LOCAL_PAPER_ONLY",
              vix_yellow=config["vix_yellow"], vix_red=config["vix_red"],
              technical_basis="LAST_OBSERVED_16_30_TO_17_QUOTE_PROXY", blackout_basis="EMPLOYMENT_24H_BEFORE_1H_AFTER_EARNINGS_PREVIOUS_AND_EVENT_DAY"))
        dump(directory / "state-before.json", store.load())
        expanded = config.get("universe_mode") == "ALL_RECORDED_EQUITIES" and collector is None
        effective = config
        if expanded:
            previous = store.load() or {}
            market_universe = read_market_universe(container, user, database, directory, previous.get("positions", {}))
            bundle = await collect_all(config, directory, market_universe, store)
            effective = dict(config, instruments=bundle["catalog"])
        else:
            bundle = await (collector or collect_public)(config, directory)
        dump(directory / "sources.json", bundle)
        budget = ReviewRunBudget(state_dir / "api-usage.sqlite", runtime["research_budget"], run_id, runtime["pm_reserve_calls"]) if expanded else None
        review_config = dict(config, research_budget=runtime["research_budget"])
        async with httpx.AsyncClient(timeout=httpx.Timeout(runtime["model_timeout_seconds"], connect=10), trust_env=False) as client:
            agents = agents or DeskAgents(config["llm"], client=client, budget=budget)
            research = await review_queue(bundle, agents, store, review_config) if expanded else await research_book(bundle, agents, store)
        dump(directory / "research.json", research)
        market = (market_reader or capture_market)(effective, container, user, database, directory)
        rows, earnings, covered = normalize_market(market, effective["instruments"])
        at = now()
        state, result, evidence = finish_book(bundle, research, rows, earnings, covered, effective, store, at)
        result.update(catalog=bundle.get("catalog", []), initial_cash=config["initial_cash"],
                      model=agents.model, provider=agents.provider, run_id=run_id, source_cache_hits=bundle.get("source_cache_hits", 0))
        observe(store, rows, result, at)
        result["metrics"] = measure(store, result, state, at)
        dump(directory / "evidence.json", evidence)
        dump(directory / "paper-input.json", dict(rows=rows, evidence=evidence, config=effective, at=at.isoformat(), entry_blocks=result["entry_blocks"]))
        dump(directory / "metrics.json", result["metrics"])
        dump(directory / "result.json", result)
        (directory / "report.md").write_text(render(result), encoding="utf-8")
        (directory / "report.html").write_text(render_dashboard(result), encoding="utf-8")
        files = [p for p in directory.rglob("*") if p.is_file()]
        dump(directory / "manifest.json", dict(run_id=run_id, mode="PAPER_ONLY", config_sha256=digest(config),
              artifacts={str(p.relative_to(directory)): digest(p.read_bytes()) for p in files},
              code_sha256=code_hashes, code_changed_during_run=[name for name, sha in code_hashes.items() if digest((ROOT / name).read_bytes()) != sha],
              model=agents.model, provider=agents.provider))
        store.finish(run_id, at, state, result)
        pointer = state_dir / "latest.json.tmp"
        dump(pointer, dict(run_id=run_id, directory=str(directory.resolve()), committed=True, at=at.isoformat()))
        os.replace(pointer, state_dir / "latest.json")
        return directory, result
    except Exception as exc:
        dump(directory / "error.json", dict(status="FAILED", error_type=type(exc).__name__, at=now().isoformat(),
                                           message="Run failed; inspect source receipts and validated local inputs. No broker action exists."))
        raise
    finally:
        store.close()


def replay(directory):
    def read(name):
        return json.loads((directory / name).read_text(encoding="utf-8"))
    manifest = read("manifest.json")
    for name, expected in manifest["artifacts"].items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory.resolve()) or digest(path.read_bytes()) != expected:
            raise ValueError("Replay artifact integrity failure")
    inputs = read("paper-input.json")
    _, actual = advance_paper(inputs["rows"], inputs["evidence"], inputs["config"], read("state-before.json"),
                              instant(inputs["at"]), inputs["entry_blocks"])
    if digest(actual) != digest(read("result.json")["paper"]):
        raise ValueError("Deterministic paper replay mismatch")
    return dict(status="REPLAY_MATCH", mode="OFFLINE_NO_MODEL_OR_NETWORK", run_id=manifest["run_id"], fills=len(actual["fills"]))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/thesis_desk.json")
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--local", action="store_true", help="Use installed Qwen through loopback Ollama, with no API key or paid calls")
    parser.add_argument("--seed-cache", type=Path, help="Reuse verified public source receipts from another desk state")
    parser.add_argument("--max-model-calls", type=int, help="Bound each full-universe review pass; pending documents remain queued without changing the portfolio configuration")
    parser.add_argument("--documents-per-batch", type=int, help="Research batch limit; defaults to one document for local CPU inference")
    parser.add_argument("--model-timeout-seconds", type=int, help="Model response timeout, 60-3600 seconds; defaults to 1800 locally, 300 remotely")
    parser.add_argument("--pm-reserve-calls", type=int, help="Calls reserved for PM after Research/Contrarian; defaults to one when the pass allows at least two calls")
    parser.add_argument("--report", action="store_true", help="Render the latest committed snapshot without any API calls")
    parser.add_argument("--db-container", default="cocos_db")
    parser.add_argument("--db-user", default="quantia")
    parser.add_argument("--db-name", default="quantia")
    parser.add_argument("--replay", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--watch", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    args.state_dir = args.state_dir or ROOT / ("outputs/thesis-desk-all-local" if args.local else "outputs/thesis-desk-all")
    if args.replay:
        print(json.dumps(replay(args.replay)))
        return 0
    if args.report:
        pointer = json.loads((args.state_dir / "latest.json").read_text(encoding="utf-8"))
        directory = Path(pointer["directory"])
        result = json.loads((directory / "result.json").read_text(encoding="utf-8"))
        # Regenerated views are separate from the immutable, manifested run artifacts.
        output = directory / "view.html"
        output.write_text(render_dashboard(result), encoding="utf-8")
        print(json.dumps(dict(report=str(output.resolve()), mode="OFFLINE_NO_MODEL_OR_NETWORK")))
        return 0
    if not 60 <= args.poll_seconds <= 3600:
        parser.error("poll-seconds must be between 60 and 3600")
    load_dotenv(ROOT / ".env", override=False)
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.local:
        config["llm"] = dict(provider="ollama", model="qwen3.5:9b", cost_policy="FREE_ONLY")
        config["research_budget"].update(max_calls_per_run=300, max_calls_per_day=1000)
    validate_config(config)
    while True:
        directory, result = asyncio.run(run_once(config, args.state_dir, args.db_container, args.db_user, args.db_name, seed_cache=args.seed_cache, max_model_calls=args.max_model_calls,
                                                documents_per_batch=args.documents_per_batch, model_timeout_seconds=args.model_timeout_seconds, pm_reserve_calls=args.pm_reserve_calls))
        print(json.dumps(dict(run=str(directory.resolve()), status=result["paper"]["status"],
                              findings=len(result["research"]["findings"]), reviewed=len(result["research"]["reviews"]),
                              watchlist=sum(c["pm_decision"] == "WATCHLIST" for c in result["watchlist"]),
                              model_calls=sum(t.get("attempted", False) for t in result["research"]["model_traces"]),
                              fills=len(result["paper"]["fills"]), coverage=result["metrics"]["coverage"],
                              report=str((directory / "report.html").resolve()))), flush=True)
        if not args.watch:
            return 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
