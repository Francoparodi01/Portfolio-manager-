from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import hashlib
import json

import pytest

from src.technical_desk import compact_dry_run as dry
from src.technical_desk.compact_prompts import wire_json
from src.technical_desk.pilot_protocol_v31 import PricingEvidence
from tests.test_technical_desk_compact_prompts import full_fixture
from tests.test_technical_desk_prompts import NOW


def inputs():
    views = {}
    for ticker in ("AMD", "GDX", "GOOGL", "MU", "NVDA"):
        view, _ = full_fixture()
        view.update(instrument_id=f"BYMA:CEDEAR:{ticker}:ARS", packet_id=f"test-{ticker}",
                    packet_digest=hashlib.sha256(ticker.encode()).hexdigest())
        views[ticker] = view
    contexts = {t: dry.context_from_market_view(v) for t, v in views.items()}
    return views, contexts


def fake_price(context_length=262144):
    return dict(status="READY", context_length=context_length, max_prompt_tokens=None, max_completion_tokens=16384,
        reasons=[], manifest=dict(model_id="synthetic/test", endpoint="local/fp8", provider_name="Fixture",
                                 allowed_resolved_models=["synthetic/test"]),
        pricing=PricingEvidence("synthetic/test", "SYNTHETIC_TEST_ONLY", NOW-timedelta(hours=1), NOW+timedelta(hours=1),
                                Decimal("0.075"), Decimal("0.4"), Decimal(0), True))


def test_compact_budget_fifty_requests_smoke_and_context_are_consistent(monkeypatch):
    import socket
    def no_network(*args, **kwargs):
        raise AssertionError("NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    monkeypatch.setattr(dry, "load_endpoint_pricing", lambda *args, **kwargs: fake_price())
    views, contexts = inputs()
    original = deepcopy(views)
    prepared = dry.prepare_compact_requests(views, contexts)
    result = dry.evaluate_selection(prepared, contexts, dict(capture_manifest="fixture", model_id="synthetic/test", endpoint="local/fp8"), as_of=NOW)
    assert len(result["requests"]) == len({r["request_id"] for r in result["requests"]}) == 50
    assert result["context_pass"] and result["feasibility"] == "READY"
    assert result["status"] == "NOT_STARTED" and not result["dispatch_authorized"]
    assert views == original
    rates = fake_price()["pricing"]
    expected = 20 * sum((Decimal(max(r["input_byte_ceiling"] for r in result["requests"] if r["agent"] == role))*rates.input_usd_per_million+
                         Decimal(next(r["output_token_cap"] for r in result["requests"] if r["agent"] == role))*rates.output_usd_per_million)/1000000
                         for role in ("elliott", "wyckoff", "structure", "momentum", "moderator"))
    assert result["pilot"]["required_reservation_usd"] == expected
    assert result["smoke"]["base_calls"] == result["smoke"]["maximum_attempts"] == 5
    assert result["smoke"]["retry_limit"] == 0
    moderator = next(r["request"] for r in result["requests"] if r["agent"] == "moderator")
    prior_bound = dry.byte_ceiling(moderator, role="moderator")
    modified = deepcopy(moderator)
    modified["messages"][1]["content"] = wire_json(dict(text='"\\' * 1000))
    assert dry.byte_ceiling(modified, role="moderator") == prior_bound
    modified["messages"][1]["content"] = wire_json(dict(text='"\\' * 65536))
    with pytest.raises(ValueError, match="MODERATOR_VIEW_CAP"):
        dry.byte_ceiling(modified, role="moderator")


def test_expired_price_never_constructs_requests(monkeypatch):
    monkeypatch.setattr(dry, "load_endpoint_pricing", lambda *args, **kwargs:
                        dict(status="NOT_STARTED", reasons=["CAPTURE_NOT_CURRENT_AT_CUTOFF"]))
    result = dry.evaluate_selection([], {}, dict(capture_manifest="fixture", model_id="synthetic/test", endpoint="local/fp8"), as_of=NOW)
    assert result["status"] == "NOT_STARTED" and result["requests"] == []
    assert result["pilot"] is None and "CAPTURE_NOT_CURRENT_AT_CUTOFF" in result["reasons"]


def test_context_limit_failure_blocks_feasibility_even_if_budget_fits(monkeypatch):
    monkeypatch.setattr(dry, "load_endpoint_pricing", lambda *args, **kwargs: fake_price(8192))
    views, contexts = inputs()
    result = dry.evaluate_selection(dry.prepare_compact_requests(views, contexts), contexts,
        dict(capture_manifest="fixture", model_id="synthetic/test", endpoint="local/fp8"), as_of=NOW)
    assert not result["context_pass"] and result["feasibility"] == "NOT_STARTED"
    assert "CONTEXT_OR_ENDPOINT_TOKEN_LIMIT_EXCEEDED" in result["reasons"]
