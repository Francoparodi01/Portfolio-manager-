from copy import deepcopy
from decimal import Decimal
import socket

import pytest

from src.technical_desk.dry_dispatch import build_dry_run, input_token_bound, FIXTURE_PRICING, MAX_RESPONSE_UTF8_BYTES
from src.technical_desk.evidence_capture import content_hash, canonical
from tests.test_technical_desk_prompts import market_fixture, NOW


def inputs():
    pairs={ticker:market_fixture(ticker) for ticker in ("AMD","GDX","GOOGL","MU","NVDA")}
    return {t:p[0] for t,p in pairs.items()},{t:p[1] for t,p in pairs.items()}


def test_exact_fifty_requests_without_sockets_and_budget_formula(monkeypatch):
    def forbidden(*args,**kwargs):
        raise AssertionError("NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket.socket,"connect",forbidden)
    monkeypatch.setattr(socket,"create_connection",forbidden)
    monkeypatch.setattr(socket,"getaddrinfo",forbidden)
    views,contexts=inputs()
    result=build_dry_run(views,contexts,as_of=NOW)
    assert result["status"]=="NOT_STARTED" and not result["dispatch_authorized"] and result["model_calls"]==0
    assert len({r["request"]["request_id"] for r in result["requests"]})==result["request_count"]==50
    assert all(r["request_hash"]==content_hash(r["request"]) for r in result["requests"])
    envelopes=result["fixture_budget"]["envelopes"]
    expected=sum((Decimal(e["input_token_bound"])*Decimal("0.10")+Decimal(e["output_token_cap"])*Decimal("0.40"))/1000000 for e in envelopes)*20
    assert result["fixture_budget"]["required_reservation_usd"]==expected
    assert result["actual_calculated_cost_usd"] is None and result["billing_status"]=="BILLED_COST_UNKNOWN"
    indexed={r["request"]["request_id"]:r["request"] for r in result["requests"]}
    for item in result["requests"]:
        request=item["request"]
        assert request["requested_model"] is None and request["requested_provider"] is None
        if request["agent"]=="moderator":
            dependencies=[indexed[key] for key in request["depends_on_request_ids"]]
            assert len(dependencies)==4 and all(d["replica_id"]==request["replica_id"] and d["packet_id"]==request["packet_id"] for d in dependencies)
            assert item["max_billable_input_tokens"] >= len(canonical(request))+6*4*16384
        else:
            assert request["depends_on_request_ids"]==[]


def test_requests_are_deterministic_and_full_hash_changes_with_parameters():
    views,contexts=inputs()
    first=build_dry_run(views,contexts,as_of=NOW)
    second=build_dry_run(views,contexts,as_of=NOW)
    assert first==second
    mutated=deepcopy(first["requests"][0]["request"])
    mutated["parameters"]["temperature"]=1
    assert content_hash(mutated)!=first["requests"][0]["request_hash"]


def test_fixture_cannot_be_relabelled_as_real_or_remove_budget_gate():
    views,contexts=inputs()
    fixture=dict(FIXTURE_PRICING,verified=True)
    with pytest.raises(ValueError,match="NON_REAL_FIXTURE"):
        build_dry_run(views,contexts,as_of=NOW,pricing_fixture=fixture)
    fixture=dict(FIXTURE_PRICING,input_usd_per_million="1000")
    result=build_dry_run(views,contexts,as_of=NOW,pricing_fixture=fixture)
    assert result["fixture_budget"]["status"]==result["status"]=="NOT_STARTED"
