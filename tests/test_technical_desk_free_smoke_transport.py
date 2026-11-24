"""Free smoke transport tests use an explicit loopback server only."""
from copy import deepcopy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from threading import Thread

import pytest

from src.technical_desk.desk_transport import DeskTransportError, serialized_request
from src.technical_desk.free_smoke_transport import (
    FREE_SMOKE_MODEL, build_free_request, send_free_attempt,
)

KEY = "fixture-credential-only-never-real"


@pytest.fixture(autouse=True)
def loopback_only(monkeypatch):
    connect, resolve = socket.socket.connect, socket.getaddrinfo

    def checked_connect(sock, address):
        assert address[0] == "127.0.0.1", "EXTERNAL_SOCKET_FORBIDDEN"
        return connect(sock, address)

    def checked_resolve(host, *args, **kwargs):
        assert host == "127.0.0.1", "EXTERNAL_DNS_FORBIDDEN"
        return resolve(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", checked_connect)
    monkeypatch.setattr(socket, "getaddrinfo", checked_resolve)


@pytest.fixture
def manifest():
    return dict(model_id=FREE_SMOKE_MODEL, endpoint="nvidia", provider_name="Nvidia",
                allowed_resolved_models=[FREE_SMOKE_MODEL], pricing={"prompt": "0", "completion": "0"},
                reasoning=None)


def request(manifest):
    return build_free_request({"messages": [dict(role="system", content="SHADOW_ONLY"),
        dict(role="user", content='{"instrument_id":"BYMA:CEDEAR:AMD:ARS"}')]},
        manifest=manifest, role="elliott", max_output_tokens=4096)


@pytest.fixture
def server():
    state = dict(requests=[], status=200, get_status=200, headers={}, generation={},
        response=dict(id="gen-fixture-001", model=FREE_SMOKE_MODEL, provider="Nvidia",
            choices=[dict(finish_reason="stop", message={"content": "{}"})],
            usage=dict(prompt_tokens=10, completion_tokens=20, cost=0,
                prompt_tokens_details={"cached_tokens": 3}, completion_tokens_details={"reasoning_tokens": 5})))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _reply(self, status, raw):
            self.send_response(status)
            for key, value in state["headers"].items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            state["requests"].append(dict(method="POST", path=self.path,
                body=self.rfile.read(int(self.headers["Content-Length"])),
                auth_matches=self.headers.get("Authorization") == "Bearer " + KEY))
            raw = state["response"] if isinstance(state["response"], bytes) else json.dumps(state["response"]).encode()
            self._reply(state["status"], raw)

        def do_GET(self):
            state["requests"].append(dict(method="GET", path=self.path,
                auth_matches=self.headers.get("Authorization") == "Bearer " + KEY))
            self._reply(state["get_status"], json.dumps({"data": state["generation"]}).encode())

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    state["url"] = f"http://127.0.0.1:{httpd.server_port}/api/v1"
    try:
        yield state
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=3)


def send(manifest, server, body=None):
    return send_free_attempt(body or request(manifest), manifest=manifest, api_key=KEY,
                             fixture_mode=True, base_url=server["url"])


def test_free_routing_strict_schema_and_exact_wire_body(server, manifest):
    body = request(manifest)
    result = send(manifest, server, body)
    assert result["status"] == "RECEIVED" and result["abort"] is False
    actual = json.loads(server["requests"][0]["body"])
    assert actual == body
    assert actual["provider"] == dict(only=["nvidia"], allow_fallbacks=False, require_parameters=True,
                                      max_price=dict(prompt=0, completion=0, request=0))
    assert actual["response_format"]["json_schema"]["strict"] is True
    assert actual["temperature"] == 0 and "reasoning" not in actual
    assert server["requests"][0]["auth_matches"]
    assert result["request_sha256"] == hashlib.sha256(server["requests"][0]["body"]).hexdigest()
    assert result["response_sha256"] == hashlib.sha256(result["raw_response"]).hexdigest()
    assert result["finish_reason"] == "stop" and result["latency_ms"] >= 0
    usage = result["identity_usage"]
    assert usage["tokens"] == dict(input=10, output=20, reasoning=5, cache_read=3, cache_write=None)
    assert usage["usage_cost"] == 0 and usage["resolved_endpoint"] is None
    assert KEY not in repr(result)


def test_reasoning_low_requires_frozen_manifest(server, manifest):
    manifest["reasoning"] = {"effort": "low"}
    body = request(manifest)
    assert body["reasoning"] == {"effort": "low"}
    assert send(manifest, server, body)["status"] == "RECEIVED"
    manifest["reasoning"] = None
    with pytest.raises(DeskTransportError, match="CONTRACT_CHANGED"):
        send(manifest, server, body)
    assert len(server["requests"]) == 1


def test_reasoning_disabled_is_exact_on_serialized_and_http_body(server, manifest):
    baseline = request(manifest)
    manifest["reasoning"] = {"enabled": False}
    body = request(manifest)
    assert body == {**baseline, "reasoning": {"enabled": False}}
    serialized = serialized_request(body)
    assert json.loads(serialized)["reasoning"] == {"enabled": False}
    assert b'"reasoning":{"enabled":false}' in serialized
    result = send(manifest, server, body)
    assert result["status"] == "RECEIVED"
    assert server["requests"][0]["body"] == serialized
    actual = json.loads(server["requests"][0]["body"])
    assert actual["reasoning"]["enabled"] is False
    assert "exclude" not in actual["reasoning"] and "effort" not in actual["reasoning"]
    for field in ("response_format", "max_tokens", "temperature", "provider"):
        assert actual[field] == baseline[field]
    manifest["reasoning"] = None
    with pytest.raises(DeskTransportError, match="CONTRACT_CHANGED"):
        send(manifest, server, body)
    assert len(server["requests"]) == 1


@pytest.mark.parametrize("reasoning", [
    {"enabled": 0}, {"enabled": 0.0}, {"enabled": "false"},
    {"enabled": None}, {"enabled": True}, {"enabled": False, "exclude": True},
    {"enabled": False, "effort": "low"}, {"exclude": True}, {}, False, 0, [],
    {"effort": "low", "exclude": True},
])
def test_invalid_reasoning_blocks_before_socket(server, manifest, reasoning):
    body = request(manifest)
    manifest["reasoning"] = reasoning
    with pytest.raises(DeskTransportError, match="REASONING_NOT_PREREGISTERED"):
        request(manifest)
    with pytest.raises(DeskTransportError, match="REASONING_NOT_PREREGISTERED"):
        send(manifest, server, body)
    assert server["requests"] == []


@pytest.mark.parametrize("field,value", [("model_id", "nvidia/nemotron-paid"),
    ("endpoint", "another"), ("provider_name", "Another"),
    ("allowed_resolved_models", [FREE_SMOKE_MODEL, "nvidia/paid"]),
    ("pricing", {"prompt": "0.1", "completion": "0"}),
    ("pricing", {"prompt": "0"}), ("pricing", {"prompt": False, "completion": 0})])
def test_nonfree_or_unverified_manifest_blocks_before_socket(server, manifest, field, value):
    body = request(manifest)
    manifest[field] = value
    with pytest.raises(DeskTransportError):
        send(manifest, server, body)
    assert server["requests"] == []


@pytest.mark.parametrize("mutation", ["fallback", "max_price", "model", "extra"])
def test_mutated_body_blocks_before_socket(server, manifest, mutation):
    body = request(manifest)
    if mutation == "fallback":
        body["provider"]["allow_fallbacks"] = True
    elif mutation == "max_price":
        body["provider"]["max_price"]["prompt"] = 1
    elif mutation == "model":
        body["model"] = "nvidia/nemotron-paid"
    else:
        body["unknown"] = True
    with pytest.raises(DeskTransportError, match="CONTRACT_CHANGED"):
        send(manifest, server, body)
    assert server["requests"] == []


@pytest.mark.parametrize("contamination", ['{"owner":"secret"}', '{"posiciones":1}',
    '{"amount":100}', KEY])
def test_final_guard_blocks_private_data_before_http(server, manifest, contamination):
    body = request(manifest)
    body["messages"][1]["content"] = contamination
    with pytest.raises(ValueError):
        send(manifest, server, body)
    assert server["requests"] == []


def test_429_preserves_retry_headers_and_never_retries(server, manifest):
    server.update(status=429, response={"error": {"message": "rate limited"}},
                  headers={"Retry-After": "120", "X-RateLimit-Remaining": "0"})
    result = send(manifest, server)
    assert result["status"] == "HTTP_ERROR" and result["retry_eligible"] and not result["abort"]
    assert result["selected_headers"] == {"retry-after": "120", "x-ratelimit-remaining": "0"}
    assert result["raw_response"] and len(server["requests"]) == 1


@pytest.mark.parametrize("field,value", [("model", "nvidia/nemotron-3.5-super-vl"), ("provider", "Other")])
def test_resolved_nonfree_identity_aborts_and_preserves_raw(server, manifest, field, value):
    server["response"][field] = value
    result = send(manifest, server)
    assert result["abort"] and not result["retry_eligible"] and result["raw_response"]
    assert len(server["requests"]) == 1


@pytest.mark.parametrize("cost", [0.001, "NaN", -1, False])
def test_nonzero_or_invalid_usage_cost_aborts(server, manifest, cost):
    server["response"]["usage"]["cost"] = cost
    result = send(manifest, server)
    assert result["abort"] and result["error_code"] == "NONZERO_OR_INVALID_COST_REPORTED"
    assert result["identity_usage"]["usage_cost"] == cost


def test_missing_usage_is_unknown(server, manifest):
    del server["response"]["usage"]
    result = send(manifest, server)
    assert result["status"] == "RECEIVED"
    assert all(value is None for value in result["identity_usage"]["tokens"].values())
    assert result["identity_usage"]["usage_cost"] is None


def test_generation_get_resolves_missing_identity(server, manifest):
    del server["response"]["provider"]
    server["generation"] = dict(id="gen-fixture-001", model=FREE_SMOKE_MODEL, provider_name="Nvidia",
                                total_cost=0)
    result = send(manifest, server)
    assert result["status"] == "RECEIVED" and result["raw_generation"]
    assert [r["method"] for r in server["requests"]] == ["POST", "GET"]
    assert server["requests"][1]["auth_matches"]
    assert result["identity_usage"]["usage_source"] == "CHAT_AND_GENERATION"


def test_generation_mismatch_aborts_without_post_retry(server, manifest):
    del server["response"]["provider"]
    server["generation"] = dict(id="gen-fixture-001", model="nvidia/paid", provider_name="Nvidia")
    result = send(manifest, server)
    assert result["abort"] and not result["retry_eligible"]
    assert len(server["requests"]) == 2


def test_missing_identity_and_generation_id_abort(server, manifest):
    server["response"].pop("provider")
    server["response"].pop("id")
    result = send(manifest, server)
    assert result["abort"] and result["error_code"] == "GENERATION_ID_REQUIRED_FOR_RESOLUTION"
    assert len(server["requests"]) == 1


def test_invalid_json_is_retry_eligible_but_not_retried(server, manifest):
    server["response"] = b"not json"
    result = send(manifest, server)
    assert result["status"] == "INVALID_JSON" and result["retry_eligible"]
    assert result["raw_response"] == b"not json" and len(server["requests"]) == 1


def test_redirect_is_not_followed(server, manifest):
    server.update(status=302, headers={"Location": "https://example.invalid/secret"})
    result = send(manifest, server)
    assert result["abort"] and not result["retry_eligible"] and len(server["requests"]) == 1


def test_secret_echo_is_never_returned(server, manifest):
    server["response"] = {"error": KEY}
    result = send(manifest, server)
    assert result["abort"] and result["raw_response"] is None
    assert KEY not in repr(result)


@pytest.mark.parametrize("url,fixture", [("http://openrouter.ai", False),
    ("https://openrouter.ai.evil.invalid", False), ("https://openrouter.ai:443", False),
    ("https://user:pass@openrouter.ai", False), ("http://localhost:1234", True),
    ("https://openrouter.ai", True)])
def test_only_exact_live_origin_or_explicit_loopback(manifest, url, fixture):
    with pytest.raises(DeskTransportError):
        send_free_attempt(request(manifest), manifest=manifest, api_key=KEY,
                          base_url=url, fixture_mode=fixture)


def test_transport_exception_is_sanitized_without_retry(server, manifest, monkeypatch):
    import src.technical_desk.free_smoke_transport as transport
    calls = []

    def fail(self, *args, **kwargs):
        calls.append(True)
        raise OSError(KEY)

    monkeypatch.setattr(transport.http.client.HTTPConnection, "request", fail)
    result = send(manifest, server)
    assert result["status"] == "TRANSPORT_ERROR" and result["retry_eligible"]
    assert len(calls) == 1 and KEY not in repr(result)
