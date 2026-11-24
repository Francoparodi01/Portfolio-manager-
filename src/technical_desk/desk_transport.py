"""Desk-only OpenRouter contract; this phase can send solely to local HTTP fixtures.

Docs checked 2026-11-23 (no inference request):
https://openrouter.ai/docs/guides/features/structured-outputs
https://openrouter.ai/docs/guides/routing/provider-selection
https://openrouter.ai/docs/api/api-reference/chat/create-a-chat-completion
https://openrouter.ai/docs/api/api-reference/generations/get-request-&-usage-metadata-for-a-generation
https://openrouter.ai/docs/cookbook/administration/usage-accounting
"""
from copy import deepcopy
import hashlib
import http.client
import json
from urllib.parse import urlencode, urlsplit

from .pilot_protocol_v31 import AGENTS
from .prompts import assert_private_free
from .role_contracts import role_output_schema

TRANSPORT_VERSION = "technical-desk-transport-v1"
MAX_HTTP_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_PRIVACY_SCAN_DEPTH = 64  # Fixed before compact-prompt validation; never truncate.


class DeskTransportError(ValueError):
    """Sanitized failure code: never include prompts or remote error contents."""


def strict_role_output_schema():
    """Emit every v1 default explicitly; retain its type and all constraints.

    Targets remain arrays, with [] representing absence. The immutable v1 parser
    still accepts omitted defaults. This wire schema requires their explicit
    representation; it does not add nullable targets or alter stored contracts.
    """
    schema = deepcopy(role_output_schema())

    def visit(node):
        if isinstance(node, dict):
            node.pop("default", None)
            if node.get("type") == "object":
                node["additionalProperties"] = False
                node["required"] = list(node.get("properties", {}))
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(schema)
    return schema


def _manifest(manifest):
    required = ("model_id", "endpoint", "provider_name", "allowed_resolved_models")
    if not isinstance(manifest, dict) or any(key not in manifest for key in required):
        raise DeskTransportError("EXPLICIT_MANIFEST_REQUIRED")
    for key in required[:3]:
        if not isinstance(manifest[key], str) or not manifest[key].strip():
            raise DeskTransportError("EXPLICIT_MANIFEST_REQUIRED")
    allowed = manifest["allowed_resolved_models"]
    if (not isinstance(allowed, list) or not allowed or
            any(not isinstance(item, str) or not item.strip() for item in allowed)):
        raise DeskTransportError("RESOLVED_MODEL_ALLOWLIST_REQUIRED")
    return manifest


def build_request(rendered, *, manifest, role, max_output_tokens,
                  requested_model=None, requested_endpoint=None):
    manifest = _manifest(manifest)
    if role not in AGENTS:
        raise DeskTransportError("UNKNOWN_ROLE")
    if "role" in rendered and rendered["role"] != role:
        raise DeskTransportError("RENDERED_ROLE_MISMATCH")
    if requested_model is not None and requested_model != manifest["model_id"]:
        raise DeskTransportError("REQUEST_MODEL_MANIFEST_MISMATCH")
    if requested_endpoint is not None and requested_endpoint != manifest["endpoint"]:
        raise DeskTransportError("REQUEST_ENDPOINT_MANIFEST_MISMATCH")
    if type(max_output_tokens) is not int or max_output_tokens <= 0:
        raise DeskTransportError("POSITIVE_OUTPUT_CAP_REQUIRED")
    messages = deepcopy(rendered.get("messages"))
    if (not isinstance(messages, list) or not messages or
            any(not isinstance(item, dict) or set(item) != {"role", "content"} or
                item["role"] not in {"system", "user"} or
                not isinstance(item["content"], str) for item in messages)):
        raise DeskTransportError("TEXT_ONLY_MARKET_MESSAGES_REQUIRED")
    request = dict(model=manifest["model_id"], messages=messages, temperature=0,
                   max_tokens=max_output_tokens, stream=False,
                   provider=dict(only=[manifest["endpoint"]], allow_fallbacks=False,
                                 require_parameters=True),
                   response_format=dict(type="json_schema", json_schema=dict(
                       name="technical_desk_role_output_v1", strict=True,
                       schema=strict_role_output_schema())))
    serialized_request(request)
    return request


def serialized_request(request, *, forbidden_values=()):
    """Scan exact final UTF-8 body and decoded/nested JSON text immediately pre-send."""
    body = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")
    assert_private_free(body.decode("utf-8"), forbidden_values=forbidden_values)

    def scan(value, depth=0):
        if depth > MAX_PRIVACY_SCAN_DEPTH:
            raise DeskTransportError("PROMPT_ENCODING_DEPTH_EXCEEDED")
        assert_private_free(value, forbidden_values=forbidden_values)
        if isinstance(value, dict):
            for item in value.values():
                scan(item, depth + 1)
        elif isinstance(value, list):
            for item in value:
                scan(item, depth + 1)
        elif isinstance(value, str):
            try:
                decoded = json.loads(value)
            except (ValueError, TypeError):
                return
            if decoded != value:
                scan(decoded, depth + 1)

    scan(json.loads(body))
    return body


def _validate_request(request, manifest):
    if request.get("model") != manifest["model_id"]:
        raise DeskTransportError("REQUEST_MODEL_MANIFEST_MISMATCH")
    if request.get("provider") != dict(only=[manifest["endpoint"]], allow_fallbacks=False,
                                       require_parameters=True):
        raise DeskTransportError("REQUEST_ENDPOINT_MANIFEST_MISMATCH")
    expected = build_request({"messages": request.get("messages")}, manifest=manifest,
                             role="moderator", max_output_tokens=request.get("max_tokens"))
    if request != expected:
        raise DeskTransportError("REQUEST_CONTRACT_CHANGED")


def _json_http(host, port, method, path, body=None):
    connection = http.client.HTTPConnection(host, port, timeout=5)
    try:
        connection.request(method, path, body=body,
                           headers={"Content-Type": "application/json"} if body else {})
        response = connection.getresponse()
        raw = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
        if response.status != 200:
            raise DeskTransportError("LOCAL_HTTP_REQUEST_FAILED")
        if len(raw) > MAX_HTTP_RESPONSE_BYTES:
            raise DeskTransportError("HTTP_RESPONSE_BYTE_CAP_EXCEEDED")
        decoded = json.loads(raw)
        if not isinstance(decoded, dict) or decoded.get("error"):
            raise DeskTransportError("INVALID_HTTP_RESPONSE")
        return decoded, raw
    except (OSError, ValueError, http.client.HTTPException) as exc:
        if isinstance(exc, DeskTransportError):
            raise
        raise DeskTransportError("LOCAL_HTTP_REQUEST_FAILED") from None
    finally:
        connection.close()


def _provider_from_response(response):
    # provider is sometimes returned; the published mandatory response schema
    # guarantees id/model, not provider. Generation metadata is the fallback.
    provider = response.get("provider")
    if provider is not None:
        return provider
    metadata = response.get("openrouter_metadata") or {}
    endpoints = metadata.get("endpoints") or {}
    selected = [item.get("provider") for item in endpoints.get("available", [])
                if item.get("selected") is True and item.get("provider") is not None]
    if len(set(selected)) > 1:
        raise DeskTransportError("AMBIGUOUS_RESPONDING_PROVIDER")
    return selected[0] if selected else None


def response_record(response, *, manifest, generation=None):
    """Capture native usage without turning missing observations into zero."""
    manifest = _manifest(manifest)
    generation = generation or {}
    generation_id = response.get("id")
    if generation and generation.get("id") != generation_id:
        raise DeskTransportError("GENERATION_ID_MISMATCH")
    model = response.get("model")
    provider = _provider_from_response(response)
    for value in (model, generation.get("model")):
        if value is not None and value not in manifest["allowed_resolved_models"]:
            raise DeskTransportError("RESOLVED_MODEL_MANIFEST_MISMATCH")
    if model and generation.get("model") and model != generation["model"]:
        raise DeskTransportError("CONFLICTING_RESOLVED_MODELS")
    for value in (provider, generation.get("provider_name")):
        if value is not None and (not isinstance(value, str) or
                value.casefold() != manifest["provider_name"].casefold()):
            raise DeskTransportError("RESPONDING_PROVIDER_MANIFEST_MISMATCH")
    model = model if model is not None else generation.get("model")
    provider = provider if provider is not None else generation.get("provider_name")
    if model is None or provider is None:
        raise DeskTransportError("UNRESOLVED_MODEL_OR_PROVIDER")
    usage = response.get("usage") or {}
    prompt_details = usage.get("prompt_tokens_details") or {}
    completion_details = usage.get("completion_tokens_details") or {}

    def native(value, key):
        return value if value is not None else generation.get(key)

    tokens = dict(input=native(usage.get("prompt_tokens"), "native_tokens_prompt"),
                  output=native(usage.get("completion_tokens"), "native_tokens_completion"),
                  reasoning=native(completion_details.get("reasoning_tokens"), "native_tokens_reasoning"),
                  cache_read=native(prompt_details.get("cached_tokens"), "native_tokens_cached"),
                  cache_write=prompt_details.get("cache_write_tokens"))
    if any(value is not None and (type(value) is not int or value < 0) for value in tokens.values()):
        raise DeskTransportError("INVALID_TOKEN_USAGE")
    return dict(transport_version=TRANSPORT_VERSION, requested_model=manifest["model_id"],
                requested_endpoint=manifest["endpoint"], resolved_model=model,
                responding_provider=provider, resolved_endpoint=None,
                endpoint_resolution_status="NOT_REPORTED_BY_DOCUMENTED_RESPONSE",
                generation_id=generation_id, tokens=tokens,
                usage_cost=usage.get("cost"), cost_details=deepcopy(usage.get("cost_details")),
                generation_total_cost=generation.get("total_cost"),
                generation_upstream_inference_cost=generation.get("upstream_inference_cost"),
                usage_source="CHAT_AND_GENERATION" if generation else "CHAT",
                response_json=deepcopy(response))


def send_local_fixture(request, *, manifest, base_url, fixture_mode=False, forbidden_values=()):
    """No credentials/environment or production URL: only an explicit fake server.

    The local server exercises the same body and metadata parsing, including the
    generation-ID query. A later authorized phase must implement live activation.
    """
    if fixture_mode is not True:
        return dict(status="NOT_STARTED", reason="LIVE_TRANSPORT_DISABLED_STAGE1B1",
                    actual_model_calls=0)
    manifest = _manifest(manifest)
    parsed = urlsplit(base_url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or
            parsed.username is not None or parsed.password is not None or parsed.query or
            parsed.fragment or parsed.path not in ("", "/api/v1") or not parsed.port):
        raise DeskTransportError("EXPLICIT_LOOPBACK_FIXTURE_REQUIRED")
    _validate_request(request, manifest)
    # Last body operation before the HTTP POST: exact serialized privacy guard.
    body = serialized_request(request, forbidden_values=forbidden_values)
    response, raw = _json_http("127.0.0.1", parsed.port, "POST", "/api/v1/chat/completions", body)
    generation = None
    generation_raw = None
    if response.get("model") is None or _provider_from_response(response) is None:
        generation_id = response.get("id")
        if not isinstance(generation_id, str) or not generation_id.strip():
            raise DeskTransportError("GENERATION_ID_REQUIRED_FOR_RESOLUTION")
        metadata, generation_raw = _json_http("127.0.0.1", parsed.port, "GET",
            "/api/v1/generation?" + urlencode({"id": generation_id}))
        generation = metadata.get("data")
        if not isinstance(generation, dict):
            raise DeskTransportError("INVALID_GENERATION_METADATA")
    result = response_record(response, manifest=manifest, generation=generation)
    result.update(status="RECEIVED_LOCAL_FIXTURE", actual_model_calls=0,
                  request_sha256=hashlib.sha256(body).hexdigest(),
                  response_sha256=hashlib.sha256(raw).hexdigest(),
                  generation_response_sha256=(hashlib.sha256(generation_raw).hexdigest()
                                               if generation_raw is not None else None))
    return result
