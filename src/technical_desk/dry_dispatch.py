"""Offline request planning only. No model client, dispatch or network imports."""
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal
import json

from . import pilot_protocol_v31 as pilot
from .evidence_capture import canonical, content_hash
from .prompts import render_prompt, MAX_RESPONSE_UTF8_BYTES as RESPONSE_BYTE_CAP
from .role_contracts import packet_spec_for_v31

VERSION = "technical-desk-dry-dispatch-v1"
MAX_OUTPUT_TOKENS = dict(elliott=4096,wyckoff=4096,structure=3072,momentum=3072,moderator=4096)
MAX_RESPONSE_UTF8_BYTES = {role:RESPONSE_BYTE_CAP for role in pilot.AGENTS}
TOKEN_BOUND = dict(version="utf8-byte-upper-planning-bound-v1", tokens_per_utf8_byte=1,
                   request_overhead_tokens=1024, model_tokenizer_verified=False,
                   scope="PLANNING_ASSUMPTION_MUST_VERIFY_TOKENIZER_AND_PROVIDER_BEFORE_EXECUTION",
                   output_cap_includes_reasoning=True)
FIXTURE_PRICING = dict(kind="NON_REAL_PRICING_FIXTURE", model_id="FIXTURE_NOT_A_REAL_MODEL",
                       provider="FIXTURE_NOT_A_REAL_PROVIDER", input_usd_per_million="0.10",
                       output_usd_per_million="0.40", extra_usd_per_attempt="0", verified=False)


def abstention_fixture(role, view):
    return dict(contract_version="technical-desk-role-output-v1",packet_id=view["packet_id"],
        packet_digest=view["packet_digest"],agent=role,mode="SHADOW_ONLY",affects_analysis=False,affects_execution=False,
        horizon_unit="BYMA_TRADING_SESSIONS",volume_basis="RELATIVE_UNVERIFIED" if role=="wyckoff" else "NOT_USED",
        evaluation_cohort=pilot.evaluation_cohort(role),results=[dict(timeframe=tf,state="ABSTAIN",scenarios=[],
        abstention_reason="DRY_RUN_FIXTURE_NO_MODEL_RESPONSE",uncertainty="UNKNOWN") for tf in ("D","W")])


def input_token_bound(request, *, pending_specialists=False):
    # Count the entire serialized request, including the duplicate transport schema.
    upper = len(canonical(request)) + TOKEN_BOUND["request_overhead_tokens"]
    if pending_specialists:
        # JSON escaping can cost six ASCII bytes per response byte (e.g. control characters).
        # Existing fixture bytes remain counted, adding slack rather than subtracting placeholders.
        upper += 6 * sum(MAX_RESPONSE_UTF8_BYTES[role] for role in pilot.AGENTS if role != "moderator")
    return upper


def build_dry_run(packet_views, contexts, *, as_of, pricing_fixture=None):
    fixture = dict(FIXTURE_PRICING if pricing_fixture is None else pricing_fixture)
    if (set(fixture) != set(FIXTURE_PRICING) or fixture["kind"] != "NON_REAL_PRICING_FIXTURE" or
            fixture["verified"] is not False):
        raise ValueError("ONLY_EXPLICIT_NON_REAL_FIXTURE_ALLOWED")
    if len(packet_views) != 5 or set(packet_views) != set(contexts):
        raise ValueError("EXACT_FIVE_PACKETS_AND_CONTEXTS_REQUIRED")
    requests = []
    for ticker in sorted(packet_views):
        view,context = packet_views[ticker],contexts[ticker]
        for replica in (1,2):
            dependencies = []
            for role in pilot.AGENTS:
                responses = None
                if role == "moderator":
                    responses = [dict(agent=agent,replica_id=replica,fixture_only=True,
                        response_json=json.dumps(abstention_fixture(agent,view))) for agent in pilot.AGENTS if agent != "moderator"]
                rendered = render_prompt(role,view,specialist_responses=responses,replica_id=replica,packet_context=context)
                request = dict(schema=VERSION,mode="DRY_RUN",dispatch_authorized=False,
                    agent=role,replica_id=replica,packet_id=view["packet_id"],packet_digest=view["packet_digest"],
                    requested_model=None,requested_provider=None,parameters=dict(temperature=0,max_output_tokens=MAX_OUTPUT_TOKENS[role],
                        reasoning_tokens_included_in_cap=True),response_format=view["output_schema"],
                    max_response_utf8_bytes=MAX_RESPONSE_UTF8_BYTES[role],attempt_index=0,retry_parent_request_id=None,
                    depends_on_request_ids=list(dependencies) if role == "moderator" else [],
                    pending_specialist_responses=role == "moderator",preview_fixture_only=role == "moderator",**rendered)
                request["request_id"] = "req-"+content_hash(request)
                item = dict(request=request,request_hash=content_hash(request),
                            max_billable_input_tokens=input_token_bound(request,pending_specialists=role=="moderator"))
                requests.append(item)
                if role != "moderator":
                    dependencies.append(request["request_id"])
    packets = tuple(packet_spec_for_v31(contexts[ticker]) for ticker in sorted(contexts))
    envelopes = tuple(pilot.CallEnvelope(role,max(r["max_billable_input_tokens"] for r in requests if r["request"]["agent"]==role),
        MAX_OUTPUT_TOKENS[role],pilot.PricingEvidence(fixture["model_id"],fixture["kind"],as_of,as_of+timedelta(days=1),
        Decimal(fixture["input_usd_per_million"]),Decimal(fixture["output_usd_per_million"]),
        Decimal(fixture["extra_usd_per_attempt"]),True),content_hash(dict(fixture_model=fixture["model_id"]))) for role in pilot.AGENTS)
    fixture_budget = pilot.plan_pilot_budget(packets,envelopes,as_of=as_of)
    if fixture_budget.required_reservation_usd is None:
        raise ValueError("INVALID_FIXTURE_BUDGET:"+",".join(fixture_budget.reasons))
    return dict(schema=VERSION,status="NOT_STARTED",reasons=["MODEL_NOT_VERIFIED","PROVIDER_NOT_FIXED","PRICING_NOT_VERIFIED","DRY_RUN_ONLY"],
        dispatch_authorized=False,model_calls=0,request_count=len(requests),requests=requests,token_bound=TOKEN_BOUND,
        fixture_pricing=fixture,fixture_budget=dict(status=fixture_budget.status,
            required_reservation_usd=fixture_budget.required_reservation_usd,budget_usd=fixture_budget.budget_usd,
            reasons=fixture_budget.reasons,real_price=False,dispatch_authorized=False,
            formula="5*2*(1+1 retry)*sum_role(max_input_tokens*input_rate/1e6+max_output_tokens*output_rate/1e6+extra)",
            envelopes=[dict(agent=e.agent,input_token_bound=e.max_billable_input_tokens,output_token_cap=e.max_billable_output_tokens)
                       for e in envelopes]),actual_calculated_cost_usd=None,billed_cost_usd=None,billing_status="BILLED_COST_UNKNOWN")
