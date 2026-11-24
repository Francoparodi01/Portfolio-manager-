"""Offline endpoint prices from hash-verified public captures; no model client.

READY means that the capture covers a text-only reservation at the supplied
cutoff, never permission to dispatch. Unknown cache prices remain unknown; the
reservation assumes every input token is billed at the full input price.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from pathlib import Path
import re

from .pilot_protocol_v31 import AGENTS, CallEnvelope, PricingEvidence


PRICING_LOADER_VERSION = "technical-desk-endpoint-pricing-v1"
CAPTURE_VALIDITY = timedelta(hours=24)
MILLION = Decimal(1_000_000)


class PricingUnavailable(ValueError):
    pass


def _require(condition, reason):
    if not condition:
        raise PricingUnavailable(reason)


def _decimal(value, field):
    _require(not isinstance(value, bool) and isinstance(value, (str, int, float, Decimal)),
             "INVALID_PRICE_" + field.upper())
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise PricingUnavailable("INVALID_PRICE_" + field.upper()) from exc
    _require(result.is_finite() and result >= 0, "INVALID_PRICE_" + field.upper())
    return result


def _aware(value):
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _utc_capture(value):
    _require(isinstance(value, str), "CAPTURE_UTC_REQUIRED")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PricingUnavailable("CAPTURE_UTC_REQUIRED") from exc
    _require(_aware(result) and result.utcoffset() == timedelta(0), "CAPTURE_UTC_REQUIRED")
    return result.astimezone(timezone.utc)


def _strict_json(raw):
    def no_duplicates(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result, "DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=no_duplicates)


def _cache_price(prices, key):
    if key not in prices:
        return {"status": "UNKNOWN", "usd_per_million": None}
    return {"status": "PUBLISHED", "usd_per_million": _decimal(prices[key], key) * MILLION}


def load_endpoint_pricing(capture_manifest_path, *, model_id, endpoint, as_of):
    """Return PricingEvidence only for the exact model/endpoint and fresh capture.

    The source contract is the stage-1 public endpoint capture and its sibling
    manifest. Explicit cache writes, tools, media and web search are outside the
    covered request surface. A published cache price above the input price is
    rejected because the full-input reservation would no longer be a ceiling.
    Unknown or nonzero extra billing categories fail closed.
    """
    result = {
        "status": "NOT_STARTED", "pricing": None, "context_length": None,
        "provider_name": None, "max_prompt_tokens": None, "max_completion_tokens": None,
        "cache_read": {"status": "UNKNOWN", "usd_per_million": None},
        "cache_write": {"status": "UNKNOWN", "usd_per_million": None},
        "cache_reservation_basis": "FULL_INPUT_PRICE_NO_DISCOUNT",
        "reasoning_billing_basis": "OUTPUT_TOKENS_WITHIN_OUTPUT_CAP",
        "coverage": "TEXT_ONLY_NO_TOOLS_NO_MEDIA_NO_EXPLICIT_CACHE_WRITE",
        "reasons": [], "manifest": None, "dispatch_authorized": False,
        "loader_version": PRICING_LOADER_VERSION,
    }
    try:
        _require(_aware(as_of), "AWARE_PRICING_CUTOFF_REQUIRED")
        _require(isinstance(model_id, str) and re.fullmatch(r"[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.:-]+", model_id), "EXACT_MODEL_ID_REQUIRED")
        _require(isinstance(endpoint, str) and re.fullmatch(r"[a-zA-Z0-9_.-]+(?:/[a-zA-Z0-9_.-]+)+", endpoint), "EXACT_ENDPOINT_REQUIRED")
        manifest_path = Path(capture_manifest_path).resolve()
        manifest = _strict_json(manifest_path.read_bytes())
        _require(isinstance(manifest, dict), "INVALID_CAPTURE_MANIFEST")
        _require(type(manifest.get("http_status")) is int and manifest["http_status"] == 200, "CAPTURE_HTTP_NOT_200")
        _require(manifest.get("status") == "VERIFIED_PUBLIC_RESPONSE", "CAPTURE_NOT_VERIFIED_PUBLIC_RESPONSE")
        _require(manifest.get("authorization_header_sent") is False, "PUBLIC_CAPTURE_AUTHORIZATION_MISMATCH")
        source_url = "https://openrouter.ai/api/v1/models/" + model_id + "/endpoints"
        _require(manifest.get("url") == source_url, "CAPTURE_MODEL_URL_MISMATCH")
        captured_at = _utc_capture(manifest.get("requested_at_utc"))
        valid_until = captured_at + CAPTURE_VALIDITY
        result.update(verified_at=captured_at, valid_until=valid_until)
        _require(captured_at <= as_of <= valid_until, "CAPTURE_NOT_CURRENT_AT_CUTOFF")
        raw_name = manifest.get("file")
        _require(isinstance(raw_name, str) and Path(raw_name).name == raw_name
                 and manifest_path.name == raw_name.replace(".raw.json", ".manifest.json")
                 and raw_name.endswith("-endpoints.raw.json"), "CAPTURE_FILE_MISMATCH")
        raw_path = manifest_path.with_name(raw_name)
        raw = raw_path.read_bytes()
        raw_hash = sha256(raw).hexdigest()
        _require(isinstance(manifest.get("sha256"), str) and manifest["sha256"] == raw_hash, "CAPTURE_SHA256_MISMATCH")
        _require(type(manifest.get("bytes")) is int and manifest["bytes"] == len(raw), "CAPTURE_BYTES_MISMATCH")
        data = _strict_json(raw).get("data")
        _require(isinstance(data, dict) and data.get("id") == model_id, "CAPTURE_MODEL_ID_MISMATCH")
        endpoints = data.get("endpoints")
        _require(isinstance(endpoints, list), "ENDPOINT_LIST_REQUIRED")
        matches = [item for item in endpoints if isinstance(item, dict) and item.get("tag") == endpoint]
        _require(len(matches) == 1, "EXACT_UNIQUE_ENDPOINT_REQUIRED")
        selected = matches[0]
        _require(selected.get("model_id") == model_id, "ENDPOINT_MODEL_ID_MISMATCH")
        provider_name = selected.get("provider_name")
        _require(isinstance(provider_name, str) and provider_name.strip(), "ENDPOINT_PROVIDER_REQUIRED")
        context_length = selected.get("context_length")
        _require(type(context_length) is int and context_length > 0, "ENDPOINT_CONTEXT_REQUIRED")
        supported = selected.get("supported_parameters")
        _require(isinstance(supported, list) and all(isinstance(p, str) for p in supported)
                 and {"response_format", "structured_outputs", "max_tokens"}.issubset(supported), "STRICT_SCHEMA_PARAMETERS_NOT_SUPPORTED")
        prices = selected.get("pricing")
        _require(isinstance(prices, dict) and {"prompt", "completion"}.issubset(prices), "INPUT_OUTPUT_PRICING_REQUIRED")
        input_price = _decimal(prices["prompt"], "prompt") * MILLION
        output_price = _decimal(prices["completion"], "completion") * MILLION
        cache_read = _cache_price(prices, "input_cache_read")
        cache_write = _cache_price(prices, "input_cache_write")
        for cache in (cache_read, cache_write):
            _require(cache["usd_per_million"] is None or cache["usd_per_million"] <= input_price,
                     "CACHE_PRICE_EXCEEDS_FULL_INPUT_CEILING")
        extra = _decimal(prices.get("request", "0"), "request")
        covered_fields = {"prompt", "completion", "input_cache_read", "input_cache_write", "request", "discount"}
        for field, value in prices.items():
            if field not in covered_fields:
                _require(_decimal(value, field) == 0, "UNBOUNDED_EXTRA_PRICE_CATEGORY")
        # Discounts are deliberately ignored; no reduced price is presumed.
        if "discount" in prices:
            _decimal(prices["discount"], "discount")
        for field in ("max_prompt_tokens", "max_completion_tokens"):
            limit = selected.get(field)
            _require(limit is None or (type(limit) is int and limit > 0), "INVALID_ENDPOINT_TOKEN_LIMIT")
        source = f"{raw_path.as_posix()}#sha256={raw_hash};endpoint={endpoint};url={source_url}"
        evidence = PricingEvidence(
            model_id=model_id, source=source, verified_at=captured_at, valid_until=valid_until,
            input_usd_per_million=input_price, output_usd_per_million=output_price,
            extra_usd_per_attempt=extra, covers_all_billable_usage=True,
        )
        result.update(
            status="READY", pricing=evidence, context_length=context_length,
            provider_name=provider_name, max_prompt_tokens=selected.get("max_prompt_tokens"),
            max_completion_tokens=selected.get("max_completion_tokens"),
            cache_read=cache_read, cache_write=cache_write,
            supports_implicit_caching=selected.get("supports_implicit_caching"),
            supported_parameters=tuple(supported), capture_sha256=raw_hash,
            capture_manifest_path=str(manifest_path), capture_path=str(raw_path),
            manifest={"model_id": model_id, "endpoint": endpoint,
                      "provider_name": provider_name, "allowed_resolved_models": [model_id],
                      "context_length": context_length, "quantization": selected.get("quantization"),
                      "capture_sha256": raw_hash, "verified_at": captured_at.isoformat(),
                      "valid_until": valid_until.isoformat()},
        )
    except (PricingUnavailable, OSError, ValueError, TypeError, AttributeError) as exc:
        result["reasons"] = [str(exc) if isinstance(exc, PricingUnavailable) else "UNREADABLE_OR_INVALID_PRICING_CAPTURE"]
    return result


def calculate_smoke_budget(envelopes, *, as_of, budget_usd=Decimal("0.50")):
    """Reserve exactly AMD x one replica x five roles, no automatic retry.

    This is deliberately separate from the immutable 5-package pilot formula.
    Its inputs must be AMD-specific envelopes, not maxima across other tickers.
    """
    result = {"status": "NOT_STARTED", "required_reservation_usd": None,
              "budget_usd": budget_usd, "base_calls": 5, "maximum_attempts": 5,
              "retry_limit": 0, "by_role_usd": {}, "dispatch_authorized": False,
              "reasons": []}
    try:
        _require(_aware(as_of), "AWARE_PRICING_CUTOFF_REQUIRED")
        _require(isinstance(budget_usd, Decimal) and budget_usd.is_finite()
                 and 0 < budget_usd <= Decimal("0.50"), "SMOKE_CAP_USD_050_REQUIRED")
        _require(isinstance(envelopes, tuple) and len(envelopes) == len(AGENTS)
                 and all(isinstance(e, CallEnvelope) for e in envelopes)
                 and {e.agent for e in envelopes} == set(AGENTS), "FIVE_ROLE_ENVELOPES_REQUIRED")
        costs = {}
        for envelope in envelopes:
            _require(type(envelope.max_billable_input_tokens) is int and envelope.max_billable_input_tokens > 0
                     and type(envelope.max_billable_output_tokens) is int and envelope.max_billable_output_tokens > 0,
                     "POSITIVE_TOKEN_BOUNDS_REQUIRED")
            _require(isinstance(envelope.model_digest, str) and re.fullmatch(r"[a-f0-9]{64}", envelope.model_digest), "MODEL_DIGEST_REQUIRED")
            price = envelope.pricing
            _require(isinstance(price, PricingEvidence) and price.currency == "USD"
                     and price.covers_all_billable_usage is True and price.model_id and price.source,
                     "COMPLETE_USD_PRICING_REQUIRED")
            _require(_aware(price.verified_at) and _aware(price.valid_until)
                     and price.verified_at <= as_of <= price.valid_until, "PRICING_NOT_CURRENT_AT_CUTOFF")
            costs[envelope.agent] = (
                envelope.max_billable_input_tokens * _decimal(price.input_usd_per_million, "input")
                + envelope.max_billable_output_tokens * _decimal(price.output_usd_per_million, "output")
            ) / MILLION + _decimal(price.extra_usd_per_attempt, "request")
        total = sum(costs.values(), Decimal(0))
        result.update(required_reservation_usd=total, by_role_usd=costs)
        if total > budget_usd:
            result["reasons"] = ["SMOKE_RESERVATION_EXCEEDS_BUDGET"]
        else:
            result["status"] = "READY"
    except (PricingUnavailable, ValueError, TypeError, AttributeError) as exc:
        result["reasons"] = [str(exc) if isinstance(exc, PricingUnavailable) else "INVALID_SMOKE_BUDGET_INPUT"]
    return result
