from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import socket

import pytest

from src.technical_desk.endpoint_pricing import calculate_smoke_budget, load_endpoint_pricing
from src.technical_desk.pilot_protocol_v31 import AGENTS, CallEnvelope


CAPTURED = datetime(2026, 11, 23, 2, 59, 27, tzinfo=timezone.utc)
MODEL = "nvidia/nemotron-3-nano-30b-a3b"
ENDPOINT = "nebius/fp8"


def fixture_capture(tmp_path, *, change_data=None, change_manifest=None):
    data = {"data": {"id": MODEL, "endpoints": [{
        "model_id": MODEL, "tag": ENDPOINT, "provider_name": "Nebius",
        "quantization": "fp8", "context_length": 262144,
        "max_prompt_tokens": None, "max_completion_tokens": 235929,
        "supported_parameters": ["response_format", "structured_outputs", "max_tokens"],
        "supports_implicit_caching": True,
        "pricing": {"prompt": "0.00000006", "completion": "0.00000024",
                    "input_cache_read": "0.000000012", "discount": 0},
    }]}}
    if change_data:
        change_data(data)
    raw = json.dumps(data).encode()
    raw_path = tmp_path / "20261123T025927Z-fixture-endpoints.raw.json"
    raw_path.write_bytes(raw)
    manifest = {
        "url": f"https://openrouter.ai/api/v1/models/{MODEL}/endpoints",
        "requested_at_utc": CAPTURED.isoformat(), "authorization_header_sent": False,
        "http_status": 200, "file": raw_path.name, "sha256": sha256(raw).hexdigest(),
        "bytes": len(raw), "status": "VERIFIED_PUBLIC_RESPONSE",
    }
    if change_manifest:
        change_manifest(manifest)
    path = tmp_path / raw_path.name.replace(".raw.json", ".manifest.json")
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path, raw_path


def load(path, **kwargs):
    return load_endpoint_pricing(path, model_id=MODEL, endpoint=ENDPOINT, as_of=kwargs.get("as_of", CAPTURED))


def endpoint(data):
    return data["data"]["endpoints"][0]


def test_exact_prices_and_24h_coverage_without_network(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    path, raw_path = fixture_capture(tmp_path)
    result = load(path)
    assert result["status"] == "READY" and result["dispatch_authorized"] is False
    price = result["pricing"]
    assert price.input_usd_per_million == Decimal("0.06")
    assert price.output_usd_per_million == Decimal("0.24")
    assert price.extra_usd_per_attempt == 0 and price.covers_all_billable_usage
    assert price.verified_at == CAPTURED and price.valid_until == CAPTURED + timedelta(hours=24)
    assert raw_path.as_posix() in price.source and sha256(raw_path.read_bytes()).hexdigest() in price.source
    assert "endpoint=nebius/fp8" in price.source
    assert result["cache_read"] == {"status": "PUBLISHED", "usd_per_million": Decimal("0.012")}
    assert result["cache_write"] == {"status": "UNKNOWN", "usd_per_million": None}
    assert result["reasoning_billing_basis"] == "OUTPUT_TOKENS_WITHIN_OUTPUT_CAP"
    assert result["cache_reservation_basis"] == "FULL_INPUT_PRICE_NO_DISCOUNT"
    assert result["manifest"]["allowed_resolved_models"] == [MODEL]
    assert result["context_length"] == 262144 and result["provider_name"] == "Nebius"


def test_cache_not_published_remains_unknown_and_full_input_is_reserved(tmp_path):
    path, _ = fixture_capture(tmp_path, change_data=lambda d: endpoint(d)["pricing"].pop("input_cache_read"))
    result = load(path)
    assert result["status"] == "READY"
    assert result["cache_read"]["status"] == result["cache_write"]["status"] == "UNKNOWN"
    assert result["cache_read"]["usd_per_million"] is None
    assert result["pricing"].input_usd_per_million == Decimal("0.06")


@pytest.mark.parametrize("delta,ready", [(timedelta(hours=24), True),
                                        (timedelta(hours=24, microseconds=1), False),
                                        (timedelta(microseconds=-1), False)])
def test_capture_freshness_is_closed_interval_and_future_is_not_usable(tmp_path, delta, ready):
    path, _ = fixture_capture(tmp_path)
    result = load(path, as_of=CAPTURED + delta)
    assert result["status"] == ("READY" if ready else "NOT_STARTED")
    assert (result["pricing"] is not None) == ready


@pytest.mark.parametrize("field,value,reason", [
    ("sha256", "0" * 64, "CAPTURE_SHA256_MISMATCH"),
    ("bytes", 1, "CAPTURE_BYTES_MISMATCH"),
    ("http_status", 503, "CAPTURE_HTTP_NOT_200"),
    ("status", "UNVERIFIED", "CAPTURE_NOT_VERIFIED_PUBLIC_RESPONSE"),
    ("authorization_header_sent", True, "PUBLIC_CAPTURE_AUTHORIZATION_MISMATCH"),
    ("url", "https://example.invalid", "CAPTURE_MODEL_URL_MISMATCH"),
    ("file", "../elsewhere.raw.json", "CAPTURE_FILE_MISMATCH"),
    ("requested_at_utc", "2026-11-23T02:59:27", "CAPTURE_UTC_REQUIRED"),
    ("requested_at_utc", "2026-11-23T02:59:27-03:00", "CAPTURE_UTC_REQUIRED"),
])
def test_manifest_integrity_and_public_source_fail_closed(tmp_path, field, value, reason):
    path, _ = fixture_capture(tmp_path, change_manifest=lambda m: m.update({field: value}))
    result = load(path)
    assert result["status"] == "NOT_STARTED" and result["pricing"] is None
    assert result["reasons"] == [reason]


@pytest.mark.parametrize("change,reason", [
    (lambda d: d["data"].update(id="other/model"), "CAPTURE_MODEL_ID_MISMATCH"),
    (lambda d: endpoint(d).update(model_id="other/model"), "ENDPOINT_MODEL_ID_MISMATCH"),
    (lambda d: endpoint(d).update(tag="nebius/fp8/flex"), "EXACT_UNIQUE_ENDPOINT_REQUIRED"),
    (lambda d: d["data"]["endpoints"].append(deepcopy(endpoint(d))), "EXACT_UNIQUE_ENDPOINT_REQUIRED"),
    (lambda d: endpoint(d).update(context_length=None), "ENDPOINT_CONTEXT_REQUIRED"),
    (lambda d: endpoint(d).update(provider_name=""), "ENDPOINT_PROVIDER_REQUIRED"),
    (lambda d: endpoint(d).update(supported_parameters=["response_format"]), "STRICT_SCHEMA_PARAMETERS_NOT_SUPPORTED"),
    (lambda d: endpoint(d)["pricing"].pop("completion"), "INPUT_OUTPUT_PRICING_REQUIRED"),
    (lambda d: endpoint(d)["pricing"].update(prompt="NaN"), "INVALID_PRICE_PROMPT"),
    (lambda d: endpoint(d)["pricing"].update(prompt=-1), "INVALID_PRICE_PROMPT"),
    (lambda d: endpoint(d)["pricing"].update(extra_charge="0.01"), "UNBOUNDED_EXTRA_PRICE_CATEGORY"),
    (lambda d: endpoint(d)["pricing"].update(input_cache_read="0.0001"), "CACHE_PRICE_EXCEEDS_FULL_INPUT_CEILING"),
    (lambda d: endpoint(d).update(max_completion_tokens=True), "INVALID_ENDPOINT_TOKEN_LIMIT"),
])
def test_exact_endpoint_and_billable_coverage_fail_closed(tmp_path, change, reason):
    path, _ = fixture_capture(tmp_path, change_data=change)
    result = load(path)
    assert result["status"] == "NOT_STARTED" and result["pricing"] is None
    assert result["reasons"] == [reason]


def test_explicit_request_fee_is_included_and_discount_not_applied(tmp_path):
    path, _ = fixture_capture(tmp_path, change_data=lambda d: endpoint(d)["pricing"].update(request="0.001", discount="0.5"))
    result = load(path)
    assert result["status"] == "READY"
    assert result["pricing"].extra_usd_per_attempt == Decimal("0.001")
    assert result["pricing"].input_usd_per_million == Decimal("0.06")


def test_missing_capture_and_naive_cutoff_not_started(tmp_path):
    assert load(tmp_path / "missing.manifest.json")["status"] == "NOT_STARTED"
    path, _ = fixture_capture(tmp_path)
    result = load(path, as_of=CAPTURED.replace(tzinfo=None))
    assert result["reasons"] == ["AWARE_PRICING_CUTOFF_REQUIRED"]


def test_smoke_uses_exactly_five_roles_one_replica_no_retries(tmp_path):
    path, _ = fixture_capture(tmp_path)
    price = load(path)["pricing"]
    envelopes = tuple(CallEnvelope(role, 1000, 100, price, "a" * 64) for role in AGENTS)
    result = calculate_smoke_budget(envelopes, as_of=CAPTURED)
    assert result["status"] == "READY" and not result["dispatch_authorized"]
    assert result["base_calls"] == result["maximum_attempts"] == 5 and result["retry_limit"] == 0
    assert result["required_reservation_usd"] == Decimal("0.000420")
    assert set(result["by_role_usd"]) == set(AGENTS)
    expired = calculate_smoke_budget(envelopes, as_of=CAPTURED + timedelta(days=2))
    assert expired["status"] == "NOT_STARTED" and expired["required_reservation_usd"] is None
    assert calculate_smoke_budget(envelopes[:4], as_of=CAPTURED)["status"] == "NOT_STARTED"
    high = tuple(CallEnvelope(role, 10_000_000, 100, price, "a" * 64) for role in AGENTS)
    assert calculate_smoke_budget(high, as_of=CAPTURED)["reasons"] == ["SMOKE_RESERVATION_EXCEEDS_BUDGET"]
