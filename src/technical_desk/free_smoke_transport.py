"""One explicitly authorized free Desk attempt; no retry, defaults, or fallback."""
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import http.client
import json
import re
from time import perf_counter
from urllib.parse import urlencode, urlsplit

from .desk_transport import (
    DeskTransportError, MAX_HTTP_RESPONSE_BYTES, build_request,
    response_record, serialized_request,
)

FREE_SMOKE_MODEL = "nvidia/nemotron-3.5-super-vl:free"
ZERO_PRICE = {"prompt": 0, "completion": 0, "request": 0}
TIMEOUT_SECONDS = 180


def _zero(value):
    try:
        return not isinstance(value, bool) and Decimal(str(value)).is_finite() and Decimal(str(value)) == 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _free_manifest(manifest):
    if not isinstance(manifest, dict):
        raise DeskTransportError("FREE_SMOKE_MANIFEST_REQUIRED")
    if (manifest.get("model_id") != FREE_SMOKE_MODEL or
            manifest.get("allowed_resolved_models") != [FREE_SMOKE_MODEL] or
            manifest.get("endpoint") != "nvidia" or manifest.get("provider_name") != "Nvidia"):
        raise DeskTransportError("FREE_SMOKE_IDENTITY_MISMATCH")
    pricing = manifest.get("pricing")
    if not isinstance(pricing, dict) or not all(_zero(pricing.get(key)) for key in ("prompt", "completion")):
        raise DeskTransportError("FREE_SMOKE_ZERO_PRICING_REQUIRED")
    reasoning = manifest.get("reasoning")
    if reasoning is not None and not (
            isinstance(reasoning, dict) and (
                reasoning == {"effort": "low"} or
                (set(reasoning) == {"enabled"} and reasoning["enabled"] is False))):
        raise DeskTransportError("FREE_SMOKE_REASONING_NOT_PREREGISTERED")
    return manifest


def build_free_request(rendered, *, manifest, role, max_output_tokens):
    """Reuse immutable schema and add server-side zero-price routing ceilings."""
    manifest = _free_manifest(manifest)
    request = build_request(rendered, manifest=manifest, role=role,
                            max_output_tokens=max_output_tokens)
    request["provider"]["max_price"] = deepcopy(ZERO_PRICE)
    if manifest.get("reasoning") is not None:
        request["reasoning"] = deepcopy(manifest["reasoning"])
    serialized_request(request)
    return request


def _destination(base_url, fixture_mode):
    parsed = urlsplit(base_url)
    clean = (parsed.username is None and parsed.password is None and not parsed.query and
             not parsed.fragment and parsed.path in ("", "/api/v1"))
    if fixture_mode is True:
        if not clean or parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
            raise DeskTransportError("EXPLICIT_LOOPBACK_FIXTURE_REQUIRED")
        return http.client.HTTPConnection, "127.0.0.1", parsed.port
    if fixture_mode is not False or not clean or parsed.scheme != "https" or parsed.netloc != "openrouter.ai":
        raise DeskTransportError("EXACT_OPENROUTER_HTTPS_ORIGIN_REQUIRED")
    return http.client.HTTPSConnection, "openrouter.ai", 443


def _selected_headers(headers, secret):
    result = {}
    for key, value in headers:
        key = key.lower()
        if key == "retry-after" or key.startswith(("x-ratelimit-", "ratelimit-")):
            result[key] = "REDACTED" if secret in value else value
    return result


def _exchange(destination, method, path, *, api_key, body=None):
    factory, host, port = destination
    connection = factory(host, port, timeout=TIMEOUT_SECONDS)
    started = perf_counter()
    try:
        headers = {"Authorization": "Bearer " + api_key, "Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
        result = dict(http_status=response.status, raw_response=raw,
                      selected_headers=_selected_headers(response.getheaders(), api_key), error_code=None)
        if api_key.encode("utf-8") in raw:
            result.update(raw_response=None, error_code="SECRET_ECHO_NOT_RETAINED")
        elif len(raw) > MAX_HTTP_RESPONSE_BYTES:
            result.update(error_code="HTTP_RESPONSE_BYTE_CAP_EXCEEDED")
        return result
    except (OSError, http.client.HTTPException, ValueError):
        return dict(http_status=None, raw_response=None, selected_headers={},
                    error_code="HTTP_TRANSPORT_FAILED")
    finally:
        connection.close()


def _json(raw):
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else None
    except (ValueError, TypeError, UnicodeError):
        return None


def _identity_present(response):
    if response.get("provider") is not None:
        return response.get("model") is not None
    metadata = response.get("openrouter_metadata")
    if not isinstance(metadata, dict):
        return False
    endpoints = metadata.get("endpoints")
    if not isinstance(endpoints, dict) or not isinstance(endpoints.get("available"), list):
        return False
    return response.get("model") is not None and any(
        isinstance(item, dict) and item.get("selected") is True and item.get("provider") is not None
        for item in endpoints["available"])


def send_free_attempt(request, *, manifest, api_key, base_url="https://openrouter.ai",
                      fixture_mode=False, forbidden_values=()):
    """Send once after caller durably records intent and verifies fresh catalog pricing.

    Returned raw bytes contain no credential. HTTP redirects are never followed.
    A failure to resolve identity blocks later calls, without repeating this POST.
    """
    manifest = _free_manifest(manifest)
    destination = _destination(base_url, fixture_mode)
    if not isinstance(api_key, str) or not api_key.strip() or any(c in api_key for c in "\r\n"):
        raise DeskTransportError("OPENROUTER_API_KEY_REQUIRED")
    expected = build_free_request({"messages": request.get("messages")}, manifest=manifest,
                                  role="moderator", max_output_tokens=request.get("max_tokens"))
    if request != expected:
        raise DeskTransportError("FREE_SMOKE_REQUEST_CONTRACT_CHANGED")
    # Exact final body, including a credential canary, immediately before HTTP.
    body = serialized_request(request, forbidden_values=tuple(forbidden_values) + (api_key,))
    started = perf_counter()
    result = dict(status="ABORTED", abort=True, retry_eligible=False, http_status=None,
                  raw_response=None, raw_generation=None, selected_headers={}, generation_headers={},
                  response_json=None, json_valid=False, finish_reason=None, identity_usage=None,
                  request_sha256=hashlib.sha256(body).hexdigest(), response_sha256=None,
                  generation_sha256=None, error_code=None)

    def done():
        result["latency_ms"] = round((perf_counter() - started) * 1000, 3)
        for raw_key, hash_key in (("raw_response", "response_sha256"), ("raw_generation", "generation_sha256")):
            raw = result[raw_key]
            result[hash_key] = hashlib.sha256(raw).hexdigest() if raw is not None else None
        return result

    exchange = _exchange(destination, "POST", "/api/v1/chat/completions", api_key=api_key, body=body)
    result.update(exchange)
    if result["error_code"]:
        if result["error_code"] == "HTTP_TRANSPORT_FAILED":
            result.update(status="TRANSPORT_ERROR", abort=False, retry_eligible=True)
        return done()
    response = _json(result["raw_response"])
    result.update(response_json=response, json_valid=response is not None)
    if result["http_status"] != 200:
        retry = result["http_status"] == 429 or result["http_status"] >= 500
        result.update(status="HTTP_ERROR", abort=not retry, retry_eligible=retry,
                      error_code="HTTP_STATUS_ERROR")
        return done()
    if response is None:
        result.update(status="INVALID_JSON", abort=False, retry_eligible=True,
                      error_code="RESPONSE_JSON_INVALID")
        return done()
    if response.get("error"):
        result.update(error_code="PROVIDER_ERROR_RESPONSE")
        return done()
    choices = response.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        result["finish_reason"] = choices[0].get("finish_reason")
    # Check any published identity before issuing the metadata GET.
    if response.get("model") is not None and response["model"] != FREE_SMOKE_MODEL:
        result.update(error_code="RESOLVED_FREE_MODEL_MISMATCH")
        return done()
    if response.get("provider") is not None and str(response["provider"]).casefold() != "nvidia":
        result.update(error_code="RESOLVED_FREE_PROVIDER_MISMATCH")
        return done()
    generation = None
    if not _identity_present(response):
        identity = response.get("id")
        if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", identity):
            result.update(error_code="GENERATION_ID_REQUIRED_FOR_RESOLUTION")
            return done()
        metadata = _exchange(destination, "GET", "/api/v1/generation?" + urlencode({"id": identity}),
                             api_key=api_key)
        result.update(raw_generation=metadata["raw_response"], generation_headers=metadata["selected_headers"])
        data = _json(metadata["raw_response"])
        if metadata["error_code"] or metadata["http_status"] != 200 or not isinstance(data, dict) or not isinstance(data.get("data"), dict):
            result.update(error_code="GENERATION_IDENTITY_UNRESOLVED")
            return done()
        generation = data["data"]
    try:
        record = response_record(response, manifest=manifest, generation=generation)
    except (ValueError, TypeError, AttributeError, KeyError):
        result.update(error_code="RESOLVED_IDENTITY_OR_USAGE_INVALID")
        return done()
    result["identity_usage"] = record
    for cost in (record["usage_cost"], record["generation_total_cost"]):
        if cost is not None and not _zero(cost):
            result.update(error_code="NONZERO_OR_INVALID_COST_REPORTED")
            return done()
    # No documented field proves the exact endpoint tag; keep it UNKNOWN.
    result.update(status="RECEIVED", abort=False)
    return done()
