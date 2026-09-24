import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.decision_lab.models import (
    Evidence,
    Dataset,
    Session,
    Experiment,
    canonical,
    digest,
)
from src.decision_lab.state import EvidenceIndex, InsufficientEvidence, build_state
from src.decision_lab.strategies import current_spec, freeze_plan


T = datetime(2026, 3, 3, 20, tzinfo=timezone.utc)


def ev(
    kind,
    payload,
    *,
    at=T,
    available=None,
    quality="POINT_IN_TIME_SAFE",
    owner=None,
    rid=None,
    revision=None,
):
    return Evidence(
        kind=kind,
        record_id=rid or digest([kind, payload, at.isoformat()]),
        effective_at=at,
        available_at=available or at,
        revision_at=revision,
        source="fixture",
        quality=quality,
        owner=owner,
        payload_json=canonical(payload),
    )


def test_evidence_deep_freeze_and_hash():
    e = ev("MACRO", {"vix": 17})
    original = e.content_hash
    e.payload["vix"] = 99
    assert e.payload["vix"] == 17 and e.content_hash == original
    with pytest.raises(ValueError):
        e.source = "changed"
    with pytest.raises(ValueError):
        ev("MACRO", {"vix": float("nan")})


def test_timezone_and_revision_availability():
    with pytest.raises(ValueError):
        ev("MACRO", {}, at=T.replace(tzinfo=None))
    e = ev("MACRO", {}, revision=T + timedelta(days=1))
    assert not e.known_at(T)


def test_duplicate_evidence_versions_fail_instead_of_last_write_wins():
    e = ev("MACRO", {})
    with pytest.raises(ValueError, match="duplicate evidence"):
        Dataset(records=(e, e), sessions=(), calendar_version="fixture")


def test_preregistration_cannot_follow_holdout_or_overlap_training():
    with pytest.raises(ValueError, match="preregistered"):
        Experiment(
            experiment_id="e",
            registered_at=T,
            confirmatory=True,
            family=("DVA20",),
            evaluation_start=T,
        )
    with pytest.raises(ValueError, match="strictly ordered"):
        Experiment(experiment_id="e", registered_at=T, train_end=T, evaluation_start=T)


def dataset_fixture(*, quantity=10, cash=1000, sell=4):
    portfolio = ev(
        "PORTFOLIO",
        {
            "cash_ars": cash,
            "positions": [
                {"ticker": "AAA", "quantity": quantity, "mark_ars": 100, "lot_size": 1}
            ],
        },
        owner=123,
        rid="portfolio",
    )
    plan = ev(
        "PLAN",
        {
            "complete": True,
            "cash_before": cash,
            "feasible": True,
            "portfolio_snapshot_id": "portfolio",
            "orders": [
                {
                    "ticker": "AAA",
                    "side": "SELL",
                    "quantity": sell,
                    "reference_price": 100,
                    "target_amount_ars": sell * 100,
                    "action": "SELL_PARTIAL",
                    "executable": True,
                    "current_weight": 0.5,
                    "target_weight": 0.3,
                }
            ],
        },
        owner=123,
        rid="plan",
    )
    sessions = []
    bars = []
    for i in range(1, 45):
        opened = T + timedelta(days=i, hours=-7)
        session = Session(
            session_id=f"s{i}", open_at=opened, close_at=opened + timedelta(hours=6)
        )
        sessions.append(session)
        bars.append(
            ev(
                "BAR",
                {
                    "ticker": "AAA",
                    "session_id": session.session_id,
                    "open": 100,
                    "high": 125,
                    "low": 90,
                    "close": 100 + i,
                    "volume": 1000,
                    "price_mode": "RAW_AS_TRADED",
                },
                at=session.close_at,
            )
        )
    return Dataset(
        records=(portfolio, plan, *bars),
        sessions=tuple(sessions),
        calendar_version="test-calendar",
    )


def recorded_plan(dataset):
    state = build_state(EvidenceIndex(dataset), as_of=T, owner=123, plan_id="plan")
    spec = current_spec(
        "recorded_plan_v1",
        registered_at=T,
        code_version="fixture",
        config_hash="fixture",
    )
    return state, freeze_plan(state, spec, "RECORDED_PLAN_EVALUATION")


def test_future_snapshots_universe_news_and_revision_do_not_change_state():
    data = dataset_fixture()
    state, _ = recorded_plan(data)
    future = (
        ev(
            "PORTFOLIO",
            {"cash_ars": 9999, "positions": []},
            at=T + timedelta(days=1),
            owner=123,
        ),
        ev(
            "UNIVERSE",
            {
                "instruments": [
                    {"ticker": "FUTUREWINNER", "enabled": True, "operable": True}
                ]
            },
            available=T + timedelta(seconds=1),
        ),
        ev(
            "NEWS",
            {
                "published_at": (T + timedelta(seconds=1)).isoformat(),
                "headline": "future outcome",
            },
        ),
        ev(
            "MACRO",
            {"vix": 80},
            at=T - timedelta(days=1),
            revision=T + timedelta(days=1),
        ),
    )
    changed, _ = recorded_plan(
        data.model_copy(update={"records": data.records + future})
    )
    assert changed == state


def test_missing_portfolio_and_wrong_owner_fail_closed():
    with pytest.raises(InsufficientEvidence, match="PORTFOLIO"):
        build_state(EvidenceIndex(dataset_fixture()), as_of=T, owner=999)


def test_future_labels_rejected_even_nested_in_features():
    with pytest.raises(ValueError, match="future outcome"):
        ev("FEATURES", {"nested": {"outcome_20d": 0.5}}, owner=123)


def test_recorded_plan_preserves_quantity_and_is_not_historical_strategy_replay():
    state, plan = recorded_plan(dataset_fixture())
    assert plan.orders[0].quantity == 4 and plan.orders[0].target_weight == 0.3
    assert plan.affected_tickers == ("AAA",)
    with pytest.raises(InsufficientEvidence, match="NOT_POLICY_REEXECUTION"):
        freeze_plan(state, plan.strategy, "HISTORICAL_POLICY_REPLAY")
    with pytest.raises(InsufficientEvidence, match="RUNTIME_DIFFERS"):
        freeze_plan(
            state,
            plan.strategy.model_copy(update={"implementation_hash": "wrong"}),
            "RECORDED_PLAN_EVALUATION",
        )


def test_modern_policy_cannot_be_backdated_as_historical():
    state, _ = recorded_plan(dataset_fixture())
    spec = current_spec(
        "hold_v1",
        registered_at=T + timedelta(days=10),
        code_version="fixture",
        config_hash="fixture",
    )
    with pytest.raises(InsufficientEvidence, match="NOT_AVAILABLE_AT_T"):
        freeze_plan(state, spec, "HISTORICAL_POLICY_REPLAY")
    assert freeze_plan(state, spec, "CURRENT_POLICY_ON_HISTORICAL_DATA").orders == ()
    with pytest.raises(InsufficientEvidence, match="TRAINING"):
        freeze_plan(
            state,
            spec.model_copy(update={"training_end": T}),
            "CURRENT_POLICY_ON_HISTORICAL_DATA",
        )


from src.decision_lab.models import CostModel
from src.decision_lab.counterfactuals import (
    freeze_episode,
    evaluate_episode,
    comparisons,
)


def run_fixture(data=None, costs=None, cutoff=None):
    data = data or dataset_fixture()
    state, plan = recorded_plan(data)
    episode = freeze_episode(
        state,
        plan,
        costs or CostModel(),
        Experiment(experiment_id="fixture", registered_at=T),
    )
    outcomes = evaluate_episode(
        episode, EvidenceIndex(data), evaluated_as_of=cutoff or T + timedelta(days=45)
    )
    return episode, outcomes


def test_manual_common_capital_hold_plan_partial_and_dva():
    episode, outcomes = run_fixture()
    rows = {o.alternative: o for o in outcomes if o.horizon == 5}
    assert {o.capital_base_ars for o in rows.values()} == {Decimal(2000)}
    assert len({(o.entry_at, o.exit_at) for o in rows.values()}) == 1
    assert rows["HOLD"].net_return == pytest.approx(0.025)
    assert json.loads(rows["HOLD"].ending_positions_json) == {"AAA": "10"}
    assert rows["HOLD"].executed_orders_json == "[]"
    assert rows["PLAN"].gross_return == pytest.approx(0.015)
    assert rows["PLAN"].net_return == pytest.approx(0.0135)
    assert rows["PARTIAL_50"].net_return == pytest.approx(0.01925)
    assert rows["CASH"].net_return == pytest.approx(-0.00375)
    assert comparisons(episode, outcomes)[0]["dva"] == pytest.approx(-0.0115)
    assert not comparisons(episode, outcomes)[0]["primary_eligible"]


def test_cost_monotonicity_and_no_double_count():
    _, a = run_fixture(costs=CostModel(fee_bps=0))
    _, b = run_fixture(costs=CostModel(fee_bps=75, slippage_bps=100))
    a = next(o for o in a if o.alternative == "PLAN" and o.horizon == 5)
    b = next(o for o in b if o.alternative == "PLAN" and o.horizon == 5)
    assert b.costs_ars == Decimal("6.9700")  # 4 slippage + 2.97 fees
    assert b.gross_return == a.gross_return
    assert b.net_return < b.gross_return
    assert b.cost_drag == pytest.approx(float(b.costs_ars / 2000))


def test_missing_and_pending_are_never_zero_returns():
    data = dataset_fixture()
    _, rows = run_fixture(data, cutoff=T + timedelta(days=6))
    assert all(o.net_return is None for o in rows if o.horizon == 40)
    assert (
        next(o for o in rows if o.horizon == 40 and o.alternative == "PLAN").status
        == "PENDING"
    )
    data = data.model_copy(
        update={
            "records": tuple(
                e
                for e in data.records
                if not (e.kind == "BAR" and e.payload["session_id"] == "s5")
            )
        }
    )
    _, rows = run_fixture(data)
    row = next(o for o in rows if o.horizon == 5 and o.alternative == "PLAN")
    assert row.status == "UNAVAILABLE" and row.net_return is None
    assert "MISSING_CLOSE" in row.reason


def test_future_bars_do_not_change_frozen_outputs_or_hashes():
    data = dataset_fixture()
    cutoff = T + timedelta(days=21)
    before = data.model_copy(
        update={"records": tuple(e for e in data.records if e.available_at <= cutoff)}
    )
    ep1, o1 = run_fixture(before, cutoff=cutoff)
    ep2, o2 = run_fixture(data, cutoff=cutoff)
    assert ep1 == ep2 and o1 == o2 and digest(o1) == digest(o2)


def test_split_raw_prices_quantity_and_no_double_adjustment():
    data = dataset_fixture()
    records = []
    for e in data.records:
        if e.kind == "BAR":
            p = e.payload
            p.update(open=50, close=50, high=50, low=50)
            e = e.model_copy(update={"payload_json": canonical(p)})
        records.append(e)
    records.append(
        ev(
            "CORPORATE_ACTION",
            {"ticker": "AAA", "type": "SPLIT", "quantity_factor": 2},
            at=T + timedelta(hours=1),
        )
    )
    _, rows = run_fixture(
        data.model_copy(update={"records": tuple(records)}), costs=CostModel(fee_bps=0)
    )
    row = next(o for o in rows if o.horizon == 5 and o.alternative == "HOLD")
    assert row.net_return == 0 and json.loads(row.ending_positions_json) == {
        "AAA": "20"
    }
    row = next(o for o in rows if o.horizon == 5 and o.alternative == "PLAN")
    assert json.loads(row.executed_orders_json)[0]["quantity"] == "8"
    assert row.net_return == 0


def test_total_return_requires_action_coverage_and_lot_rounding_is_visible():
    _, rows = run_fixture(costs=CostModel(return_basis="TOTAL_RETURN"))
    assert next(o for o in rows if o.alternative == "HOLD").status == "UNAVAILABLE"
    _, rows = run_fixture(dataset_fixture(sell=3))
    half = next(o for o in rows if o.alternative == "PARTIAL_50")
    assert json.loads(half.executed_orders_json)[0]["quantity"] == "1"
    assert "PARTIAL_OR_PLAN_QUANTITY_ROUNDED_TO_LOT" in half.warnings


def test_best_candidate_uses_frozen_ranking_not_future_winner():
    data = dataset_fixture()
    universe = ev(
        "UNIVERSE",
        {
            "universe_quality": "EXACT",
            "instruments": [
                {
                    "ticker": "BBB",
                    "enabled": True,
                    "operable": True,
                    "candidate_score": 0.8,
                    "reference_price_ars": 10,
                    "lot_size": 1,
                },
                {
                    "ticker": "CCC",
                    "enabled": True,
                    "operable": True,
                    "candidate_score": 0.1,
                    "reference_price_ars": 10,
                    "lot_size": 1,
                },
            ],
        },
    )
    episode, _ = run_fixture(
        data.model_copy(update={"records": data.records + (universe,)})
    )
    best = next(
        a
        for a in json.loads(episode.alternatives_json)
        if a["name"] == "BEST_AVAILABLE_CANDIDATE"
    )
    assert best["orders"][-1]["ticker"] == "BBB"


def test_real_core_kernels_offline_deterministic(monkeypatch):
    import socket

    monkeypatch.setattr(
        socket.socket,
        "connect",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("network forbidden")),
    )
    data = dataset_fixture()
    config = {
        "scope": "PORTFOLIO_ONLY_NO_RADAR",
        "lookback_sessions": 70,
        "sentiment_enabled": False,
        "portfolio_history": [
            {
                "scraped_at": (T - timedelta(days=1)).isoformat(),
                "total_value_ars": 2500,
            },
            {"scraped_at": T.isoformat(), "total_value_ars": 2500},
        ],
        "events_complete": True,
    }
    records = [e for e in data.records if e.kind != "PLAN"]
    portfolio = records[0].payload
    portfolio["positions"].append(
        {"ticker": "BBB", "quantity": 5, "mark_ars": 100, "lot_size": 1}
    )
    records[0] = records[0].model_copy(update={"payload_json": canonical(portfolio)})
    records.extend(
        [
            ev("CONFIG", config, owner=123),
            ev(
                "UNIVERSE",
                {
                    "universe_quality": "EXACT",
                    "instruments": [
                        {"ticker": ticker, "enabled": True, "operable": True}
                        for ticker in ("AAA", "BBB")
                    ],
                },
            ),
            ev(
                "MACRO",
                {
                    "vix": 17,
                    "sp500_trend": 0.02,
                    "ccl": 1400,
                    "riesgo_pais": 500,
                    "dxy_trend": 0,
                    "wti": 70,
                    "sp500": 6000,
                    "sp500_chg": 0.1,
                    "dow": 40000,
                    "dow_chg": 0.1,
                    "vix_chg": 0,
                    "dxy": 100,
                    "dxy_chg": 0,
                    "tnx": 4,
                    "tnx_chg": 0,
                },
            ),
        ]
    )
    sessions = list(data.sessions)
    for i in range(70):
        close = T - timedelta(days=70 - i)
        session = Session(
            session_id=f"past{i}", open_at=close - timedelta(hours=6), close_at=close
        )
        sessions.append(session)
        price = 80 + i * 0.3 + (i % 3)
        records.append(
            ev(
                "BAR",
                {
                    "ticker": "AAA",
                    "session_id": session.session_id,
                    "open": price,
                    "high": price + 1,
                    "low": price - 1,
                    "close": price,
                    "volume": 10000,
                    "price_mode": "RAW_AS_TRADED",
                },
                at=close,
            )
        )
    for bar in list(records):
        if bar.kind == "BAR" and bar.effective_at < T:
            payload = bar.payload
            payload["ticker"] = "BBB"
            payload["close"] += 0.5
            records.append(ev("BAR", payload, at=bar.effective_at))
    state = build_state(
        EvidenceIndex(
            Dataset(
                records=tuple(records),
                sessions=tuple(sessions),
                calendar_version="fixture",
            )
        ),
        as_of=T,
        owner=123,
    )
    spec = current_spec(
        "quantia_core_v1",
        registered_at=T,
        code_version="fixture",
        config_hash=digest(config),
    )
    p1 = freeze_plan(state, spec, "CURRENT_POLICY_ON_HISTORICAL_DATA")
    p2 = freeze_plan(state, spec, "CURRENT_POLICY_ON_HISTORICAL_DATA")
    assert p1 == p2
    assert "FROZEN_CORE_KERNELS" in p1.mechanism_json


def test_blocked_missing_quantities_survive_as_null_without_fake_execution():
    data = dataset_fixture()
    records = list(data.records)
    source = records[1].payload
    source["orders"].append(
        {
            "ticker": "BBB",
            "side": "BUY",
            "quantity": None,
            "reference_price": None,
            "target_amount_ars": 0,
            "executable": False,
            "blocked": True,
            "action": "HOLD",
            "restriction": "MISSING_QUOTE",
        }
    )
    records[1] = records[1].model_copy(update={"payload_json": canonical(source)})
    episode, rows = run_fixture(data.model_copy(update={"records": tuple(records)}))
    assert episode.plan.orders[1].quantity is None
    assert next(o for o in rows if o.alternative == "PLAN").status == "MATURE"


def test_adjusted_outcome_prices_reject_incompatible_share_basis():
    data = dataset_fixture()
    records = []
    for record in data.records:
        if record.kind == "BAR":
            p = record.payload
            p["price_mode"] = "POINT_IN_TIME_ADJUSTED"
            p["adjustment_as_of"] = record.available_at.isoformat()
            record = record.model_copy(update={"payload_json": canonical(p)})
        records.append(record)
    _, rows = run_fixture(data.model_copy(update={"records": tuple(records)}))
    assert next(o for o in rows if o.alternative == "HOLD").status == "UNAVAILABLE"
