import copy
from datetime import timedelta
from types import SimpleNamespace

import numpy as np
import pytest

from .test_decision_lab import T, dataset_fixture, recorded_plan
from src.decision_lab.models import Experiment, CostModel, digest
from src.decision_lab.statistics import metrics, summarize, compare_versions
from src.decision_lab.runner import replay, rolling_windows
from src.analysis.date_block_statistics import interval, benjamini_hochberg


def rows_fixture(n=100):
    return [
        {
            "owner": 123,
            "episode_id": str(i),
            "opportunity_id": str(i),
            "state_id": str(i),
            "strategy_version": "A",
            "strategy_hash": "a",
            "mode": "CURRENT_POLICY_ON_HISTORICAL_DATA",
            "experiment_id": "fixture",
            "split": "HOLDOUT",
            "cost_model_hash": "cost",
            "return_basis": "TOTAL_RETURN",
            "horizon": 5,
            "as_of": (T + timedelta(days=i)).isoformat(),
            "entry_at": (T + timedelta(days=i + 1)).isoformat(),
            "exit_at": (T + timedelta(days=i + 5)).isoformat(),
            "capital_base_ars": "1000",
            "quality": "HIGH",
            "status": "MATURE",
            "primary_eligible": True,
            "plan_return": 0.03,
            "hold_return": 0.01,
            "dva": 0.02,
            "cost_drag": 0.001,
            "turnover": 0.1,
            "segments": {},
            "tickers": ["AAA"],
        }
        for i in range(n)
    ]


def experiment():
    return Experiment(experiment_id="fixture", registered_at=T, bootstrap_resamples=200)


def test_constant_dva_interval_manual_counts_and_seed():
    rows = rows_fixture()
    a = summarize(rows, experiment())
    assert a == summarize(rows, experiment())
    assert a["n"] == 100 and a["n_dates"] == 100 and a["n_effective"] == 20
    assert (
        a["mean"] == pytest.approx(0.02)
        and a["ci"]["lower"] == pytest.approx(0.02)
        and a["ci"]["upper"] == pytest.approx(0.02)
    )
    assert a["drawdown"] is None and a["sharpe"] is None


def test_invalid_pending_and_low_quality_do_not_enter_primary():
    rows = rows_fixture(40)
    rows[0].update(quality="INVALID", primary_eligible=False)
    rows[1].update(quality="LOW", primary_eligible=False)
    rows[2].update(status="PENDING", dva=None)
    assert summarize(rows, experiment())["n"] == 37
    assert summarize(rows, experiment(), primary=False)["n"] == 38
    assert summarize(rows, experiment(), primary=False)["ci"]["lower"] is None


def test_entire_date_cross_section_moves_together():
    policy = SimpleNamespace(
        bootstrap_resamples=200, bootstrap_seed=5, confidence_level=0.95
    )
    dates = np.repeat(np.arange(50), 2)
    values = np.tile([-0.1, 0.12], 50)
    ci = interval(dates, values, policy, 20)
    assert ci["lower"] == pytest.approx(0.01) and ci["upper"] == pytest.approx(0.01)
    assert interval(dates, values, policy, 40)["lower"] is None


def test_segmentation_never_pools_versions_or_promotes_exploration():
    a = rows_fixture(50)
    b = [{**r, "strategy_version": "B", "strategy_hash": "b", "dva": -0.05} for r in a]
    result = metrics(a + b, experiment())
    all_primary = [
        r for r in result if r["segment"] == "ALL" and r["population"] == "PRIMARY"
    ]
    assert len(all_primary) == 2 and {r["n"] for r in all_primary} == {50}
    assert all(r["q_value"] is None and not r["confirmatory"] for r in result)
    assert benjamini_hochberg({"a": 0.01, "b": 0.04, "missing": None}) == {
        "missing": None,
        "b": 0.06,
        "a": 0.03,
    }


def test_exact_strategy_compare_and_capital_guard():
    a = rows_fixture(50)
    b = [
        {**r, "strategy_version": "B", "strategy_hash": "b", "plan_return": 0.04}
        for r in a
    ]
    result = compare_versions(a + b, "A", "B", experiment())
    assert result["n_pairs"] == 50
    assert result["metrics"][0]["mean"] == pytest.approx(0.01)
    b[0]["capital_base_ars"] = "999"
    with pytest.raises(ValueError, match="common capital"):
        compare_versions(a + b, "A", "B", experiment())


def test_replay_idempotence_deduplicates_requests_and_future_append():
    data = dataset_fixture()
    state, plan = recorded_plan(data)
    requests = [{"as_of": T.isoformat(), "plan_id": "plan"}]
    args = dict(
        requests=requests,
        owner=123,
        strategies=[plan.strategy],
        mode="RECORDED_PLAN_EVALUATION",
        evaluated_as_of=T + timedelta(days=21),
        experiment=experiment(),
    )
    before = data.model_copy(
        update={
            "records": tuple(
                e for e in data.records if e.known_at(args["evaluated_as_of"])
            )
        }
    )
    a, da = replay(before, **args)
    b, db = replay(data, **args)
    assert a == b and da == db
    c, _ = replay(data, **{**args, "requests": requests * 2})
    assert len(c["episodes"]) == 1 and c["comparisons"] == a["comparisons"]
    assert len({r["capital_base_ars"] for r in a["outcomes"]}) == 1


def test_rolling_expanding_and_holdout_boundaries():
    sessions = list(dataset_fixture().sessions)
    windows = rolling_windows(
        sessions,
        train_sessions=10,
        validation_sessions=5,
        test_sessions=5,
        step_sessions=5,
    )
    assert len(windows) == 5
    assert all(
        w["train_end"]
        < w["validation_start"]
        < w["validation_end"]
        < w["evaluation_start"]
        for w in windows
    )
    expanding = rolling_windows(
        sessions,
        train_sessions=10,
        validation_sessions=5,
        test_sessions=5,
        step_sessions=5,
        expanding=True,
    )
    assert len({w["train_start"] for w in expanding}) == 1
