from copy import deepcopy
import json

import pytest

from src.technical_desk import compact_prompts as compact
from src.technical_desk.dry_dispatch import abstention_fixture
from src.technical_desk.desk_transport import build_request, serialized_request
from tests.test_technical_desk_prompts import market_fixture, responses


def full_fixture():
    view, context = market_fixture()
    for tf in ("D", "W"):
        view["ohlcv"][tf] = [dict(session_date="2026-11-01", open="100.2", high="101.6", low="99.2", close="100.4",
                                 relative_volume="1.123456789", relative_volume_status="AVAILABLE")]
        view["features"][tf] = dict(latest=dict(
            range=dict(high="101.6", low="99.2"), open_extrema=[], structure={},
            indicators={"rsi": dict(value="55.123456789", status="AVAILABLE", reason=None),
                        "sma_20": dict(value="100.123456789", status="AVAILABLE", reason=None),
                        "relative_strength": dict(value=None, status="UNAVAILABLE", reason="NO_ARS_BENCHMARK")}),
            missing={"relative_strength": "NO_ARS_BENCHMARK"})
    return view, context


def manifest():
    return dict(model_id="synthetic/test", endpoint="local/fp8", provider_name="Fixture",
                allowed_resolved_models=["synthetic/test"])


@pytest.mark.parametrize("role", ["elliott", "wyckoff", "structure", "momentum", "moderator"])
def test_role_projection_no_schema_duplication_and_original_precision_unchanged(role):
    view, context = full_fixture()
    original = deepcopy(view)
    kwargs = dict(specialist_responses=responses(view), replica_id=1) if role == "moderator" else {}
    rendered = compact.render_compact_prompt(role, view, context=context, **kwargs)
    body = json.loads(rendered["messages"][1]["content"])
    assert view == original
    assert "output_schema" not in body and "$defs" not in rendered["messages"][1]["content"]
    request = build_request(rendered, manifest=manifest(), role=role, max_output_tokens=4096)
    assert request["response_format"]["json_schema"]["strict"] is True
    assert serialized_request(request)
    if role == "momentum":
        assert body["features"]["D"]["rsi"]["value"] == "55.1235"
        assert body["features"]["D"]["sma_20"]["value"] == "100"
        assert "prices" not in body
    if role == "wyckoff":
        table = body["prices"]["D"]
        assert all(isinstance(row, list) for row in table["rows"])
        assert table["rows"][0][table["columns"].index("high")] == "102"
        assert table["rows"][0][table["columns"].index("relative_volume")] == "1.1235"
        assert "indicators" not in body["features"]["D"]
    if role == "moderator":
        assert "prices" not in body and "features" not in body
        assert body["pivots"] == {"D": [], "W": []}
        assert body["validator_results"]


def test_elliott_moderator_anchors_exist_and_only_cited_pivots_are_included():
    view, context = full_fixture()
    raw = abstention_fixture("elliott", view)
    ids = [p["id"] for p in view["pivots"]["D"] if p["id"].startswith("P5:")]
    raw["results"][0].update(state="VALID", abstention_reason=None, scenarios=[dict(method="ELLIOTT", timeframe="D",
        family="IMPULSE", causal_anchors=[dict(role="origin", anchor_id=ids[0])],
        structure_anchors=[dict(role="end", anchor_id=ids[1])], direction="BULLISH", role="PRIMARY",
        structure_state="CANDIDATE", conditions=[
            dict(kind="CONFIRMATION", field="CLOSE", operator="GE", level="101", persistence_bars=2),
            dict(kind="INVALIDATION", field="LOW", operator="LE", level="90", persistence_bars=1)],
        targets=[], horizon_sessions=20, uncertainty="HIGH", text="Possible impulse")])
    envelopes = responses(view)
    envelopes[0]["response_json"] = json.dumps(raw)
    body = compact.compact_view("moderator", view, context=context, specialist_responses=envelopes, replica_id=1)
    table = body["pivots"]["D"]
    assert {row[table["columns"].index("id")] for row in table["rows"]} == set(ids)
    assert body["pivots"]["W"] == []
    assert "NOT_EVALUABLE_NO_EXACT_CANDIDATE_MATCH" in compact.wire_json(body)
    raw["results"][0]["scenarios"][0]["causal_anchors"][0]["anchor_id"] = "P5:D:" + "f" * 64
    envelopes[0]["response_json"] = json.dumps(raw)
    with pytest.raises(ValueError):
        compact.compact_view("moderator", view, context=context, specialist_responses=envelopes, replica_id=1)


@pytest.mark.parametrize("word", ["owner_chat_id", "positions", "montos", "posición"])
def test_private_data_cannot_escape_compact_or_final_transport_guard(word):
    view, context = full_fixture()
    view["features"]["D"]["latest"]["range"][word] = "PRIVATE_CANARY"
    with pytest.raises(ValueError, match="PRIVATE_DATA"):
        compact.render_compact_prompt("structure", view, context=context)


def test_projection_rejects_missing_feature_anchor_and_cross_replica():
    view, context = full_fixture()
    view["features"]["D"]["latest"]["structure"] = dict(anchor_ids=["P2:D:" + "f" * 64])
    with pytest.raises(ValueError, match="FEATURE_ANCHOR"):
        compact.compact_view("structure", view, context=context)
    with pytest.raises(ValueError, match="SAME_REPLICA"):
        compact.compact_view("moderator", view, context=context, specialist_responses=responses(view, 2), replica_id=1)


def test_all_rule_results_survive_but_internal_details_remain_in_full_precision_evidence():
    view, context = full_fixture()
    ids = [p["id"] for p in view["pivots"]["D"] if p["id"].startswith("P2:")]
    rule = dict(rule_id="WAVE2", status="REJECTED", reason="RETRACE_AT_LEAST_100_PERCENT", details={"actual": "1.00000000000001"})
    view["features"]["elliott_scales"] = {"D": {"P2": {"candidates": [dict(pivot_ids=ids, status="REJECTED", rules=[rule])]}}}
    original = deepcopy(view)
    projected = compact.compact_view("elliott", view, context=context)
    text = compact.wire_json(projected)
    assert all(word in text for word in ("WAVE2", "REJECTED", "RETRACE_AT_LEAST_100_PERCENT"))
    assert "details" not in text and "1.00000000000001" not in text
    assert view == original


def test_moderator_cap_blocks_without_truncation(monkeypatch):
    view, context = full_fixture()
    policy = compact.projection_policy()
    policy["roles"]["moderator"]["full_input_cap_utf8_bytes"] = 32
    monkeypatch.setattr(compact, "projection_policy", lambda: policy)
    with pytest.raises(ValueError, match="NOT_STARTED_MODERATOR_VIEW_CAP"):
        compact.compact_view("moderator", view, context=context, specialist_responses=responses(view), replica_id=1)
