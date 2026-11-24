"""Offline compact request and budget comparison using captured endpoint prices."""
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal

from .compact_prompts import render_compact_prompt, projection_policy, wire_json
from .desk_transport import build_request, serialized_request
from .dry_dispatch import abstention_fixture
from .endpoint_pricing import load_endpoint_pricing, calculate_smoke_budget
from .evidence_capture import content_hash
from .pilot_protocol_v3 import PacketSpec
from . import pilot_protocol_v31 as pilot
from .role_contracts import AnchorContext, PacketContext, packet_spec_for_v31


def context_from_market_view(view):
    anchors = tuple(AnchorContext(p["id"], tf, datetime.fromisoformat(p["pivot_time"]),
                                 datetime.fromisoformat(p["confirmed_at"]))
                    for tf in ("D", "W") for p in view["pivots"][tf])
    packet = PacketSpec(view["packet_id"], view["packet_digest"], view["instrument_id"],
        view["instrument_id"].split(":")[-1], Decimal(view["features"]["price_tick"]),
        tuple(a.anchor_id for a in anchors), ("D", "W"), "READY",
        atr_by_timeframe=tuple((tf, Decimal(view["features"]["atr_by_timeframe"][tf])) for tf in ("D", "W")))
    return PacketContext(packet, datetime.fromisoformat(view["cuts"]["cutoff"]), anchors)


def byte_ceiling(request, *, role):
    policy = projection_policy()
    actual = len(serialized_request(request))
    ceiling = actual + policy["input_bound"]["overhead_tokens"]
    if role == "moderator":
        literals = [message["content"] for message in request["messages"] if message["role"] == "user"]
        if len(literals) != 1:
            raise ValueError("EXACT_ONE_MODERATOR_USER_MESSAGE_REQUIRED")
        encoded = len(wire_json(literals[0]).encode("utf-8"))
        cap = policy["roles"][role]["full_input_cap_utf8_bytes"]
        if encoded > cap:
            raise ValueError("NOT_STARTED_MODERATOR_VIEW_CAP_EXCEEDED")
        ceiling += cap - encoded
    return ceiling


def prepare_compact_requests(views, contexts):
    if set(views) != set(contexts) or len(views) != 5 or "AMD" not in views:
        raise ValueError("EXACT_FIVE_PACKETS_INCLUDING_AMD_REQUIRED")
    rendered = []
    for ticker in sorted(views):
        view, context = views[ticker], contexts[ticker]
        for replica in (1, 2):
            for role in pilot.AGENTS:
                responses = [dict(agent=agent, replica_id=replica, fixture_only=True,
                    response_json=wire_json(abstention_fixture(agent, view)))
                    for agent in pilot.AGENTS if agent != "moderator"] if role == "moderator" else None
                prompt = render_compact_prompt(role, view, context=context,
                    specialist_responses=responses, replica_id=replica)
                rendered.append(dict(ticker=ticker, replica=replica, agent=role, rendered=prompt,
                                     preview_fixture_only=role == "moderator"))
    return rendered


def evaluate_selection(rendered, contexts, selection, *, as_of):
    price = load_endpoint_pricing(selection["capture_manifest"], model_id=selection["model_id"],
                                  endpoint=selection["endpoint"], as_of=as_of)
    result = dict(model_id=selection["model_id"], endpoint=selection["endpoint"], status="NOT_STARTED",
        dispatch_authorized=False, pricing=price, requests=[], pilot=None, smoke=None,
        reasons=["STAGE1B1_OFFLINE_ONLY_SMOKE_NOT_AUTHORIZED"], model_calls=0)
    if price["status"] != "READY":
        result["reasons"].extend(price["reasons"])
        return result
    manifest = price["manifest"]
    caps = projection_policy()["output_caps_tokens"]
    for entry in rendered:
        role = entry["agent"]
        body = build_request(entry["rendered"], manifest=manifest, role=role, max_output_tokens=caps[role],
                             requested_model=selection["model_id"], requested_endpoint=selection["endpoint"])
        bound = byte_ceiling(body, role=role)
        within_context = bound + caps[role] <= price["context_length"]
        within_input = price["max_prompt_tokens"] is None or bound <= price["max_prompt_tokens"]
        within_output = price["max_completion_tokens"] is None or caps[role] <= price["max_completion_tokens"]
        identity = dict(ticker=entry["ticker"], replica=entry["replica"], agent=role,
                        manifest_hash=content_hash(manifest), request_hash=content_hash(body),
                        projection_hash=entry["rendered"]["projection_policy_hash"])
        result["requests"].append(dict(**identity, request_id="req-"+content_hash(identity),
            request=body, input_byte_ceiling=bound, output_token_cap=caps[role],
            context_check="PASS" if within_context and within_input and within_output else "FAIL",
            prompt_bytes=len(wire_json(body["messages"]).encode("utf-8")),
            body_bytes=len(serialized_request(body)), preview_fixture_only=entry["preview_fixture_only"]))
    model_digest = content_hash(manifest)
    envelopes = tuple(pilot.CallEnvelope(role,
        max(item["input_byte_ceiling"] for item in result["requests"] if item["agent"] == role),
        caps[role], price["pricing"], model_digest) for role in pilot.AGENTS)
    budget = pilot.plan_pilot_budget(tuple(packet_spec_for_v31(contexts[t]) for t in sorted(contexts)), envelopes, as_of=as_of)
    result["pilot"] = asdict(budget)
    smoke_envelopes = tuple(pilot.CallEnvelope(role,
        next(item["input_byte_ceiling"] for item in result["requests"] if item["ticker"] == "AMD" and
             item["replica"] == 1 and item["agent"] == role), caps[role], price["pricing"], model_digest)
        for role in pilot.AGENTS)
    result["smoke"] = calculate_smoke_budget(smoke_envelopes, as_of=as_of)
    result["context_pass"] = all(item["context_check"] == "PASS" for item in result["requests"])
    result["feasibility"] = "READY" if budget.status == "READY" and result["smoke"]["status"] == "READY" and result["context_pass"] else "NOT_STARTED"
    if not result["context_pass"]:
        result["reasons"].append("CONTEXT_OR_ENDPOINT_TOKEN_LIMIT_EXCEEDED")
    result["byte_ceiling_label"] = projection_policy()["input_bound"]["label"]
    return result
