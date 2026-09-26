from datetime import timedelta
import json

import numpy as np
import pytest
from pydantic import ValidationError

from examples.analytics_v2.sample import sample
from src.analysis.analytics_v2.bootstrap import benjamini_hochberg, date_draws, interval, spearman
from src.analysis.analytics_v2.episodes import build_episodes, unique_rows
from src.analysis.analytics_v2.gates import build_gates, hypothesis
from src.analysis.analytics_v2.matching import match_actions
from src.analysis.analytics_v2.metrics import distribution, equity_metrics
from src.analysis.analytics_v2.models import AnalyticsPolicy, Dataset, GatePolicy, canonical
from src.analysis.analytics_v2.outcomes import build_outcomes, cost_return
from src.analysis.analytics_v2.pnl import economic_pnl, execution_facts
from src.analysis.analytics_v2.reporting import build, persist, report, validate


@pytest.fixture(scope="module")
def inputs():
    return sample()


@pytest.fixture(scope="module")
def bundle(inputs):
    data, policy = inputs
    return build(data, policy, code_commit="test-commit", code_hash="test-code")


def test_dedup_is_idempotent_preserves_repeated_evidence_and_flips(inputs):
    data, policy = inputs
    rec = data.recommendations[0]
    repeated = rec.model_copy(update={"recommendation_id": "repeat", "decision_as_of": rec.decision_as_of + timedelta(hours=1)})
    flip = rec.model_copy(update={"recommendation_id": "sell", "direction": "SELL", "decision_as_of": rec.decision_as_of + timedelta(hours=2)})
    episodes, links = build_episodes([rec, repeated, rec, flip], policy.dedup_policy_version)
    assert len(episodes) == 2 and len(links) == 3
    assert episodes[0]["recommendation_count"] == 2
    assert links[-1]["link_reason"] == "DIRECTION_CHANGE"
    assert build_episodes([flip, rec, repeated], policy.dedup_policy_version) == (episodes, links)
    with pytest.raises(ValueError, match="conflicting"):
        build_episodes([rec, rec.model_copy(update={"score": 99})], "v2")


@pytest.mark.parametrize("change,reason", [({"explicit_close_or_reopen": True}, "EXPLICIT_CLOSE_REOPEN"),
                                         ({"direction": "NEUTRAL"}, "DIRECTION_CHANGE"),
                                         ({"direction": "ABSTAIN"}, "DIRECTION_CHANGE")])
def test_episode_boundaries(inputs, change, reason):
    rec = inputs[0].recommendations[0]
    second = rec.model_copy(update={"recommendation_id": "next", "decision_as_of": rec.decision_as_of + timedelta(hours=1), **change})
    eps, links = build_episodes([rec, second], "v2")
    assert len(eps) == 2 and links[-1]["link_reason"] == reason
    expired = rec.model_copy(update={"recommendation_id": "expired", "decision_as_of": rec.expiry_at})
    assert build_episodes([rec, expired], "v2")[1][-1]["link_reason"] == "EXPIRED"


def test_frozen_naive_nonfinite_and_ambiguous_inputs_rejected(inputs):
    data, policy = inputs
    with pytest.raises(ValidationError):
        policy.version = "changed"
    for field, value in (("decision_as_of", "2026-01-01T12:00:00"), ("score", float("nan")), ("ambiguous", True)):
        payload = data.recommendations[0].model_dump()
        payload[field] = value
        with pytest.raises(ValidationError):
            type(data.recommendations[0]).model_validate(payload)
    with pytest.raises(ValidationError):
        AnalyticsPolicy(experiment_id="x", evaluated_as_of=policy.evaluated_as_of, primary_hypotheses=("x",))


def test_costs_monotone_neutral_abstain():
    values = [cost_return(.08, "BUY", b) for b in (0, 75, 150, 250, 400)]
    assert values == sorted(values, reverse=True)
    assert cost_return(.08, "NEUTRAL", 400) == 0
    assert cost_return(.08, "ABSTAIN", 400) is None
    assert cost_return(None, "BUY", 0) is None
    with pytest.raises(ValueError):
        cost_return(.1, "BUY", -1)


def test_manual_metrics_and_missing_denominators():
    m = distribution([.1, .2, -.1, 0])
    assert m["ev_net"] == pytest.approx(.05)
    assert m["win_rate"] == .5
    assert m["avg_winner"] == pytest.approx(.15)
    assert m["avg_loser"] == -.1
    assert m["payoff_ratio"] == pytest.approx(1.5)
    assert m["profit_factor_unit_notional"] == pytest.approx(3)
    assert m["top1_positive"] == pytest.approx(2/3)
    assert m["top3_positive"] == 1
    assert m["ev_without_top1"] == pytest.approx(0)
    assert distribution([])["ev_net"] is None
    assert distribution([0, 0])["top1_positive"] is None
    with pytest.raises(ValueError):
        distribution([float("inf")])


def test_equity_metrics_do_not_chain_trade_rows():
    r = [.1, -.1, .1]
    m = equity_metrics(r, periods_per_year=1, hac_lags=0)
    assert m["max_drawdown"] == pytest.approx(-.1)
    assert m["sharpe_hac"] == pytest.approx(np.mean(r)/np.std(r))
    assert m["sortino"] == pytest.approx(np.mean(r)/np.sqrt(.01/3))
    assert "reason_code" in equity_metrics([None, 0])
    assert equity_metrics([.1, .2])["sortino"] is None


def test_ic_ties_perfect_reverse_and_constant():
    assert spearman([1, 2, 2, 4], [1, 2, 2, 4]) == pytest.approx(1)
    assert spearman([1, 2, 3], [3, 2, 1]) == pytest.approx(-1)
    assert spearman([1, 1, 1], [1, 2, 3]) is None
    assert spearman([1, np.nan], [1, 2]) is None


def test_bootstrap_replay_cross_section_and_insufficient_dates(inputs):
    policy = inputs[1]
    dates = np.repeat(np.arange(30), 2)
    values = np.tile([-.1, .1], 30)
    ci = interval(dates, values, policy, 5)
    assert ci == interval(dates, values, policy, 5)
    assert ci["lower"] == ci["upper"] == 0
    assert ci["p_value"] == 1
    assert interval(dates, values, policy, 40)["lower"] is None
    assert interval([], [], policy, 5)["estimate"] is None
    assert list(date_draws(7, 3, 1, 1))[0].shape == (7,)
    constant = interval(np.arange(30), np.ones(30) * .02, policy, 5)
    assert constant["lower"] == pytest.approx(.02)


def test_bh_includes_missing_declared_hypotheses():
    q = benjamini_hochberg({"a": .01, "b": .03, "c": None})
    assert q == {"c": None, "b": .045, "a": .03}
    with pytest.raises(ValueError):
        benjamini_hochberg({"x": 2})


def test_economic_reconciliation_slippage_and_flows(inputs):
    data, _ = inputs
    economic, _ = economic_pnl(data)
    assert economic[0]["economic_pnl_net"] == 18.5
    assert economic[0]["reconciliation_residual"] == 0
    assert economic[0]["execution_shortfall"] == -1.5
    changed = data.model_copy(update={"executions": tuple(f.model_copy(update={"reference_price": 50}) for f in data.executions)})
    altered, _ = economic_pnl(changed)
    assert altered[0]["economic_pnl_net"] == 18.5
    assert altered[0]["execution_shortfall"] != economic[0]["execution_shortfall"]
    p = data.economic_periods[0].model_copy(update={"net_external_flows": 10, "ending_nav": 1028.5})
    points = (*data.nav_points[:-1], data.nav_points[-1].model_copy(update={"nav": 1028.5}))
    flow, _ = economic_pnl(data.model_copy(update={"economic_periods": (p,), "nav_points": points}))
    assert flow[0]["economic_pnl_net"] == 18.5 and flow[0]["return_on_beginning_nav"] is None


@pytest.mark.parametrize("changes,reason", [({"fees": None}, "MISSING_FLOWS"), ({"coverage_confirmed": False}, "COVERAGE"),
                                         ({"ending_nav": 2000}, "NAV_BOUNDARY"), ({"realized_pnl": 100}, "RECONCILIATION"),
                                         ({"fees": 2}, "FILL_FEES")])
def test_economic_missing_fails_closed(inputs, changes, reason):
    data = inputs[0]
    p = data.economic_periods[0].model_copy(update=changes)
    rows, _ = economic_pnl(data.model_copy(update={"economic_periods": (p,)}))
    assert rows[0]["economic_pnl_net"] is None and reason in rows[0]["reason_code"]
    assert economic_pnl(Dataset(label="empty"))[0][0]["status"] == "INCOMPLETE"


def test_maturity_repeats_swaps_and_matching(bundle):
    tables = bundle["tables"]
    assert len(tables["episodes"]) < len(tables["recommendations"])
    assert any(r["outcome_status"] == "PENDING" and r["horizon_days"] == 40 for r in tables["outcomes"])
    assert any(r["outcome_status"] == "AMBIGUOUS" for r in tables["outcomes"])
    for m in tables["bot_vs_human"]:
        if m["comparable"]:
            if m["human_action"] == "FOLLOW":
                assert m["paired_delta_bot_minus_human"] == 0
            elif m["human_action"] == "IGNORE":
                assert m["human_cf_return"] == 0 and m["delta_scope"] == "local_decision_delta"
    assert all(r["status"] == "DISABLED_SHADOW" for r in tables["gates"] if r["source_module"] == "SWAP")
    assert all(r["capital_authority"] is False for r in tables["gates"])
    assert all(r["n_episodes"] == 1 for r in tables["swaps"])
    swap = [r for r in tables["cost_sensitivity"] if r["source_module"] == "SWAP" and r["horizon_days"] == 5]
    assert sorted(swap, key=lambda r: r["cost_bps"])[0]["ev_net"] > sorted(swap, key=lambda r: r["cost_bps"])[-1]["ev_net"]


def test_pending_and_ambiguity_isolation(inputs, bundle):
    data, policy = inputs
    changed = data.model_copy(update={"recommendations": tuple(r for r in data.recommendations if r.recommendation_id not in {"PENDING40", "AMBIGUOUS"})})
    other = build(changed, policy, code_commit="test-commit", code_hash="test-code")
    for metric in bundle["tables"]["cohort_metrics"]:
        if metric["source_module"] == "CORE" and metric["horizon_days"] == 40:
            matched = next(r for r in other["tables"]["cohort_metrics"] if r["source_module"] == "CORE" and r["horizon_days"] == 40)
            assert matched["ev_net"] == metric["ev_net"]


def test_no_lookahead_input_order_and_future_revisions(inputs, bundle):
    data, policy = inputs
    visible = data.as_of(policy.evaluated_as_of)
    original = visible.bars[0]
    future = original.model_copy(update={"revision": 99, "close": 9999, "available_at": policy.evaluated_as_of + timedelta(days=1)})
    changed = visible.model_copy(update={"bars": (*reversed(visible.bars), future), "recommendations": tuple(reversed(visible.recommendations))})
    assert build(changed, policy, code_commit="test-commit", code_hash="test-code") == bundle


def test_missing_bars_calendar_and_basis(inputs):
    data, policy = inputs
    data = data.as_of(policy.evaluated_as_of)
    eps, _ = build_episodes(data.recommendations[:1], "v2")
    no_bars = build_outcomes(data.model_copy(update={"bars": ()}), eps, policy)
    assert all(r["outcome_status"] == "UNAVAILABLE" for r in no_bars)
    assert all(r["reason_code"] == "MISSING_BAR" for r in no_bars)
    no_calendar = build_outcomes(data.model_copy(update={"sessions": ()}), eps, policy)
    assert all(r["reason_code"] == "CALENDAR_INCOMPLETE" for r in no_calendar)


def test_persist_report_validate_and_tamper(tmp_path, bundle):
    folder = persist(bundle, tmp_path)
    assert persist(bundle, tmp_path) == folder
    assert validate(folder) == bundle
    path = report(folder)
    assert "SYNTHETIC" in path.read_text(encoding="utf-8")
    assert len(list((folder / "charts").glob("*.png"))) == 7
    assert validate(folder) == bundle
    assert report(folder) == path
    (folder / "tables" / "gates.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        validate(folder)
    with pytest.raises(ValueError, match="hash"):
        persist(bundle, tmp_path)


def test_preregistration_time_enforced(inputs):
    data, policy = inputs
    changed = policy.model_copy(update={"primary_hypotheses": ("x",), "preregistered_at": policy.evaluated_as_of})
    with pytest.raises(ValueError, match="preregistration"):
        build(data, changed, code_commit="test", code_hash="test")


def test_gate_boundaries_and_confirmation(inputs):
    policy = inputs[1]
    row = {"account_id": "A", "source_module": "CORE", "cohort": "BOT", "horizon_days": 5,
           "cost_scenario": "RESEARCH_BASE", "n_raw": 100, "n_episodes": 100, "n_effective": 60,
           "maturity_coverage": .9, "top1_positive": .4, "top3_positive": .75, "ev_net": .0025,
           "ic": .03, "n_unavailable": 0, "n_ambiguous": 0, "ev_lower": .001, "ic_lower": .001,
           "ev_p_value": .001, "ic_p_value": .001, "inference_stable": True,
           "positive_periods": 3, "total_periods": 3}
    policy = policy.model_copy(update={"primary_hypotheses": (hypothesis(row, "ev"), hypothesis(row, "ic"))})
    stress = {**row, "cost_scenario": "STRESS", "ev_net": .001}
    assert build_gates([row, stress], policy)[0][0]["status"] == "CONFIRMED"
    for changes, status in (({"n_effective": 19}, "IMMATURE"), ({"ev_net": 0}, "FAIL"),
                            ({"n_unavailable": 1}, "OBSERVE"), ({"n_episodes": 99}, "PROVISIONAL_GUARDED"),
                            ({"top1_positive": .40001}, "FAIL"), ({"ic_lower": 0}, "PROVISIONAL_GUARDED")):
        assert build_gates([{**row, **changes}, stress], policy)[0][0]["status"] == status


def test_matching_execution_decomposition_keeps_selection_identical(inputs, bundle):
    data, policy = inputs
    match = next(r for r in bundle["tables"]["bot_vs_human"] if r["economic_trade_id"] and r["horizon_days"] == 5 and r["cost_scenario"] == "RESEARCH_BASE")
    assert match["selection_delta_human_minus_bot"] == 0
    assert match["execution_delta"] < 0
    assert match["actual_execution_return"] == pytest.approx(18.5/100.5)
    assert match["bot_cf_return"] + match["timing_delta"] + match["execution_delta"] + match["cost_delta"] == pytest.approx(match["reference_normalized_actual_return"])
    fill = data.executions[0].model_copy(update={"fill_price": 101.5})
    changed = data.model_copy(update={"executions": (fill, data.executions[1])})
    matches = match_actions(changed, bundle["tables"]["outcomes"], policy)
    after = next(r for r in matches if r["match_id"] == match["match_id"])
    assert after["selection_delta_human_minus_bot"] == 0
    assert after["execution_delta"] < match["execution_delta"]
    assert after["timing_delta"] == match["timing_delta"]


@pytest.mark.parametrize("change,reason", [({"action": "MODIFIED"}, "MODIFIED_OR_UNKNOWN"),
                                         ({"instrument_id": "WRONG"}, "INFORMATION_SET"),
                                         ({"ambiguous": True}, "AMBIGUOUS_ACTION")])
def test_matching_exclusions(inputs, bundle, change, reason):
    data, policy = inputs
    action = data.human_actions[0].model_copy(update=change)
    matches = match_actions(data.model_copy(update={"human_actions": (action,)}), bundle["tables"]["outcomes"], policy)
    target = next(r for r in matches if r["human_action"] == action.action and r["decision_as_of"] == action.decision_as_of.isoformat())
    assert not target["comparable"] and reason in target["reason_code"]


def test_currency_and_price_series_missing_fail_closed(inputs):
    data, policy = inputs
    data = data.as_of(policy.evaluated_as_of)
    rec = data.recommendations[0]
    rec = rec.model_copy(update={"price_series_id": None})
    altered = data.model_copy(update={"recommendations": (rec,)})
    episodes, _ = build_episodes(altered.recommendations, "v2")
    assert all(r["reason_code"] == "PRICE_SERIES_UNSPECIFIED" for r in build_outcomes(altered, episodes, policy))
    fill = data.executions[0].model_copy(update={"currency": "USD", "fees": None})
    economics, _ = economic_pnl(data.model_copy(update={"executions": (fill, data.executions[1])}))
    assert economics[0]["status"] == "INCOMPLETE"
    assert "CURRENCY_MISMATCH" in economics[0]["reason_code"]


def test_cli_build_report_validate_and_code_identity(tmp_path, inputs, monkeypatch, capsys):
    from src.analysis.analytics_v2 import cli
    data, policy = inputs
    data = data.model_copy(update={"recommendations": data.recommendations[:1]})
    data_file, policy_file = tmp_path / "input.json", tmp_path / "policy.json"
    data_file.write_text(data.model_dump_json(), encoding="utf-8")
    policy_file.write_text(policy.model_dump_json(), encoding="utf-8")
    commit, code_hash = cli.code_identity()
    assert len(commit) == 40 and len(code_hash) == 64
    cli.main(["build", "--input", str(data_file), "--policy", str(policy_file), "--output", str(tmp_path / "runs")])
    folder = next((tmp_path / "runs").iterdir())
    cli.main(["report", str(folder)])
    cli.main(["validate", str(folder)])
    assert '"status": "VALID"' in capsys.readouterr().out
    monkeypatch.setattr(cli, "code_identity", lambda: (commit, "changed"))
    with pytest.raises(ValueError, match="code hash"):
        cli.main(["validate", str(folder)])


def test_manifest_metadata_inventory_and_conflict(tmp_path, bundle):
    from src.analysis.analytics_v2.reporting import write_exact
    folder = persist(bundle, tmp_path)
    with pytest.raises(ValueError, match="immutable"):
        write_exact(folder / "derived.json", b"different")
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    original = manifest_path.read_text()
    manifest["code_hash"] = "wrong"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="metadata"):
        validate(folder)
    manifest_path.write_text(original)
    (folder / "extra.txt").write_text("untracked")
    with pytest.raises(ValueError, match="inventory"):
        validate(folder)


def test_empty_bundle_and_missing_economic_charts(tmp_path, inputs):
    empty = build(Dataset(label="EMPTY"), inputs[1], code_commit="test", code_hash="test")
    folder = persist(empty, tmp_path)
    report(folder)
    assert validate(folder) == empty
    assert empty["tables"]["gates"] == []


def test_strict_policy_validation(inputs):
    from src.analysis.analytics_v2.models import CostPolicy
    policy = inputs[1].model_dump()
    for update in ({"horizons": (0,)}, {"horizons": (5, 5)}, {"primary_hypotheses": ("x", "x")}):
        with pytest.raises(ValidationError):
            AnalyticsPolicy.model_validate({**policy, **update})
    with pytest.raises(ValidationError):
        CostPolicy(scenarios=())


def test_reference_missing_and_unbalanced_execution_are_explicit(inputs, bundle):
    data, policy = inputs
    bad_fill = data.executions[0].model_copy(update={"reference_price": None})
    altered = data.model_copy(update={"executions": (bad_fill, data.executions[1])})
    matching = match_actions(altered, bundle["tables"]["outcomes"], policy)
    assert any(r["execution_reason"] == "FILL_COSTS_OR_CONTEMPORANEOUS_REFERENCE_UNAVAILABLE" for r in matching)
    facts = execution_facts(altered, [])
    assert facts[0]["execution_shortfall"] is None
    assert facts[0]["link_reason"] == "EPISODE_LINK_UNAVAILABLE"


def test_offline_engine_has_no_network_dependency(monkeypatch, inputs):
    import socket
    def forbidden(*args, **kwargs):
        raise AssertionError("analytics attempted network access")
    monkeypatch.setattr(socket, "create_connection", forbidden)
    result = build(Dataset(label="offline"), inputs[1], code_commit="test", code_hash="test")
    assert result["summary"]["operational_authority"] == "AUDIT_ONLY"
