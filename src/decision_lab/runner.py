"""Strictly ordered offline replay. Nothing from an outcome enters an adapter."""

from datetime import datetime
from .models import Dataset, CostModel, Experiment, StrategySpec, digest
from .state import EvidenceIndex, InsufficientEvidence, build_state
from .strategies import freeze_plan, implementation_manifest
from .counterfactuals import freeze_episode, evaluate_episode, comparisons
from .statistics import metrics, compare_versions


def replay(
    dataset: Dataset,
    *,
    requests: list[dict],
    owner: int,
    strategies: list[StrategySpec],
    mode: str,
    evaluated_as_of: datetime,
    experiment: Experiment,
    costs: CostModel | None = None,
    operation="walk_forward"
):
    if not strategies or len({s.strategy_version for s in strategies}) != len(
        strategies
    ):
        raise ValueError("unique explicit strategy versions required")
    if operation not in {
        "single_episode",
        "date_range",
        "walk_forward",
        "strategy_compare",
        "backfill",
    }:
        raise ValueError("unsupported replay operation")
    if operation == "single_episode" and len(requests) != 1:
        raise ValueError("single_episode requires exactly one opportunity")
    costs = costs or CostModel()
    index = EvidenceIndex(dataset)
    episodes = {}
    outcomes = {}
    rows = []
    failures = []
    seen = set()
    ordered = sorted(
        {digest(r): r for r in requests}.values(),
        key=lambda r: (r["as_of"], r.get("plan_id") or ""),
    )
    for request in ordered:
        key = digest(request)
        if key in seen:
            continue
        seen.add(key)
        as_of = datetime.fromisoformat(request["as_of"])
        if as_of > evaluated_as_of:
            raise ValueError("decision follows evaluation cutoff")
        try:
            state = build_state(
                index,
                as_of=as_of,
                owner=owner,
                plan_id=request.get("plan_id"),
                portfolio_id=request.get("portfolio_id"),
            )
        except InsufficientEvidence as exc:
            failures.append({"request": request, "reason": str(exc)})
            continue
        for strategy in strategies:
            try:
                plan = freeze_plan(state, strategy, mode)
                episode = freeze_episode(state, plan, costs, experiment)
            except InsufficientEvidence as exc:
                failures.append(
                    {
                        "request": request,
                        "strategy_version": strategy.strategy_version,
                        "reason": str(exc),
                    }
                )
                continue
            if episode.episode_id in episodes:
                continue
            # Freeze the complete decision/alternative set before the future
            # dataset becomes available to the evaluator.
            result = evaluate_episode(
                episode,
                index,
                evaluated_as_of=evaluated_as_of,
                horizons=experiment.horizons,
            )
            episodes[episode.episode_id] = episode.model_dump(mode="json")
            outcomes[episode.episode_id] = [o.model_dump(mode="json") for o in result]
            rows.extend(comparisons(episode, result))
    statistics = metrics(rows, experiment)
    comparison = []
    for strategy in strategies[1:]:
        comparison.append(
            compare_versions(
                rows,
                strategies[0].strategy_version,
                strategy.strategy_version,
                experiment,
            )
        )
    # Persist only selected evidence, never irrelevant future additions.
    selected = set()
    for e in episodes.values():
        selected.update(digest(r) for r in e["state"]["records"])
    for group in outcomes.values():
        for row in group:
            selected.update(row["evidence_hashes"])
    retained = tuple(
        e
        for e in dataset.records
        if e.known_at(evaluated_as_of)
        and (e.kind != "BAR" or e.effective_at <= evaluated_as_of)
    )
    last = max(
        (datetime.fromisoformat(r["as_of"]) for r in requests), default=evaluated_as_of
    )
    future = [s for s in index.ordered_sessions if s.open_at > last][
        : max(experiment.horizons)
    ]
    calendar_end = future[-1].close_at if future else last
    frozen_dataset = Dataset(
        records=tuple(
            sorted(retained, key=lambda e: (e.kind, e.record_id, e.available_at))
        ),
        sessions=tuple(s for s in index.ordered_sessions if s.close_at <= calendar_end),
        calendar_version=dataset.calendar_version,
    )
    config = {
        "owner": owner,
        "requests": ordered,
        "strategies": [s.model_dump(mode="json") for s in strategies],
        "mode": mode,
        "evaluated_as_of": evaluated_as_of.isoformat(),
        "experiment": experiment.model_dump(mode="json"),
        "costs": costs.model_dump(mode="json"),
        "operation": operation,
    }
    payload = {
        "schema_version": "decision-lab-run-v1",
        "config": config,
        "episodes": list(episodes.values()),
        "outcomes": [r for group in outcomes.values() for r in group],
        "comparisons": rows,
        "metrics": statistics,
        "strategy_comparisons": comparison,
        "failures": failures,
        "implementation": implementation_manifest(),
        "input_hashes": sorted(selected),
    }
    payload["replay_run_id"] = digest(payload)
    return payload, frozen_dataset


def rolling_windows(
    sessions,
    *,
    train_sessions,
    validation_sessions,
    test_sessions,
    step_sessions,
    expanding=False
):
    if min(train_sessions, validation_sessions, test_sessions, step_sessions) < 1:
        raise ValueError("positive nonoverlapping window lengths required")
    result = []
    for end_train in range(
        train_sessions,
        len(sessions) - validation_sessions - test_sessions + 1,
        step_sessions,
    ):
        train_start = 0 if expanding else end_train - train_sessions
        val_end = end_train + validation_sessions
        result.append(
            {
                "train_start": sessions[train_start].open_at.isoformat(),
                "train_end": sessions[end_train - 1].close_at.isoformat(),
                "validation_start": sessions[end_train].open_at.isoformat(),
                "validation_end": sessions[val_end - 1].close_at.isoformat(),
                "evaluation_start": sessions[val_end].open_at.isoformat(),
                "evaluation_end": sessions[
                    val_end + test_sessions - 1
                ].close_at.isoformat(),
                "mode": "EXPANDING" if expanding else "ROLLING",
                "training": "EXTERNAL_FROZEN_MODEL_REQUIRED_NO_AUTO_FIT",
            }
        )
    return result
