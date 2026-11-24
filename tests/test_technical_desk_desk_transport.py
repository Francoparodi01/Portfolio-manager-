"""Only a fake loopback HTTP server; outbound sockets fail in every test."""
from copy import deepcopy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from threading import Thread
from urllib.parse import parse_qs, urlsplit

import pytest

from src.technical_desk.desk_transport import (
    DeskTransportError, build_request, response_record, send_local_fixture,
    serialized_request, strict_role_output_schema,
)
from src.technical_desk.role_contracts import role_output_schema


@pytest.fixture(autouse=True)
def only_loopback_sockets(monkeypatch):
    connect = socket.socket.connect
    resolve = socket.getaddrinfo

    def guarded_connect(sock, address):
        assert address[0] == "127.0.0.1", "EXTERNAL_SOCKET_FORBIDDEN"
        return connect(sock, address)

    def guarded_resolve(host, *args, **kwargs):
        assert host == "127.0.0.1", "EXTERNAL_DNS_FORBIDDEN"
        return resolve(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket, "getaddrinfo", guarded_resolve)


@pytest.fixture
def manifest():
    return dict(model_id="nvidia/nemotron-3-super-120b-a12b", endpoint="deepinfra/bf16",
                provider_name="DeepInfra",
                allowed_resolved_models=["nvidia/nemotron-3-super-120b-a12b"])


def make_request(manifest):
    return build_request({"messages": [dict(role="system", content="SHADOW_ONLY"),
        dict(role="user", content='{"instrument_id":"BYMA:CEDEAR:AMD:ARS"}')],
        "packet_id": "local-metadata-never-sent"}, manifest=manifest,
        role="elliott", max_output_tokens=4096)


@pytest.fixture
def fake_server():
    state = dict(requests=[], response={}, generation={}, status=200, get_status=200)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            state["requests"].append(dict(method="POST", path=self.path, body=raw,
                                          headers=dict(self.headers)))
            self.send_response(state["status"])
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(state["response"]).encode())

        def do_GET(self):
            state["requests"].append(dict(method="GET", path=self.path))
            self.send_response(state["get_status"])
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"data": state["generation"]}).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state["url"] = f"http://127.0.0.1:{server.server_port}/api/v1"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_strict_wire_schema_preserves_contract_and_explicit_default_semantics(manifest):
    original = role_output_schema()
    original_hash = hashlib.sha256(json.dumps(original, sort_keys=True).encode()).hexdigest()
    strict = strict_role_output_schema()

    def inspect(node):
        if isinstance(node, dict):
            assert "default" not in node
            if node.get("type") == "object":
                assert node["additionalProperties"] is False
                assert set(node["required"]) == set(node["properties"])
            for value in node.values():
                inspect(value)
        elif isinstance(node, list):
            for value in node:
                inspect(value)

    inspect(strict)
    assert strict["$defs"]["ScenarioOutput"]["properties"]["targets"]["type"] == "array"
    assert strict["properties"]["affects_execution"]["const"] is False
    assert strict["properties"]["affects_analysis"]["const"] is False
    assert hashlib.sha256(json.dumps(role_output_schema(), sort_keys=True).encode()).hexdigest() == original_hash
    request = make_request(manifest)
    assert request["response_format"]["json_schema"]["strict"] is True
    assert request["response_format"]["json_schema"]["schema"] == strict
    assert "local-metadata-never-sent" not in serialized_request(request).decode()
    assert all("$defs" not in message["content"] for message in request["messages"])


@pytest.mark.parametrize("field,value", [("requested_model", "wrong/model"),
                                          ("requested_endpoint", "nebius/fp8")])
def test_manifest_overrides_abort(field, value, manifest):
    with pytest.raises(DeskTransportError, match="MANIFEST_MISMATCH"):
        build_request({"messages": []}, manifest=manifest, role="elliott",
                      max_output_tokens=4096, **{field: value})


def test_manifest_is_mandatory_no_environment_default(monkeypatch):
    monkeypatch.setenv("QUANTIA_AGENT_MODEL", "default-must-not-be-read")
    with pytest.raises(DeskTransportError, match="EXPLICIT_MANIFEST_REQUIRED"):
        build_request({"messages": []}, manifest={}, role="elliott", max_output_tokens=4096)


def test_rendered_role_must_match_explicit_requested_role(manifest):
    rendered = dict(messages=[dict(role="user", content="SHADOW_ONLY")], role="elliott")
    with pytest.raises(DeskTransportError, match="RENDERED_ROLE_MISMATCH"):
        build_request(rendered, manifest=manifest, role="moderator", max_output_tokens=4096)
    assert build_request(rendered, manifest=manifest, role="elliott", max_output_tokens=4096)


def test_compact_nested_tables_keep_privacy_guard_and_finite_depth(manifest):
    def nested(value, levels):
        for _ in range(levels):
            value = dict(rows=[value])
        return value

    request = make_request(manifest)
    request["messages"][1]["content"] = json.dumps(nested(["P2:D:pivot", "10.00"], 20))
    assert serialized_request(request)
    request["messages"][1]["content"] = json.dumps(nested({"owner": "private"}, 20))
    with pytest.raises(ValueError, match="PRIVATE_DATA_IN_RENDERED_PROMPT"):
        serialized_request(request)
    request["messages"][1]["content"] = json.dumps(nested("market", 40))
    with pytest.raises(DeskTransportError, match="PROMPT_ENCODING_DEPTH_EXCEEDED"):
        serialized_request(request)


def test_production_gate_returns_not_started_without_network(manifest):
    result = send_local_fixture(make_request(manifest), manifest=manifest,
                                base_url="https://openrouter.ai/api/v1")
    assert result == dict(status="NOT_STARTED", reason="LIVE_TRANSPORT_DISABLED_STAGE1B1",
                          actual_model_calls=0)


@pytest.mark.parametrize("url", ["https://openrouter.ai/api/v1", "http://localhost:8080",
                                 "http://127.0.0.2:8080", "http://127.0.0.1:8080/other",
                                 "http://127.0.0.1:8080?x=1"])
def test_fixture_requires_explicit_literal_loopback(url, manifest):
    with pytest.raises(DeskTransportError, match="EXPLICIT_LOOPBACK_FIXTURE_REQUIRED"):
        send_local_fixture(make_request(manifest), manifest=manifest, base_url=url, fixture_mode=True)


def test_local_http_exact_request_pin_full_usage_and_hashes(fake_server, manifest):
    fake_server["response"] = dict(id="gen-fixture", model=manifest["model_id"],
        provider="DeepInfra", choices=[], usage=dict(prompt_tokens=100, completion_tokens=20,
        prompt_tokens_details=dict(cached_tokens=30, cache_write_tokens=0),
        completion_tokens_details=dict(reasoning_tokens=7), cost=0.001,
        cost_details=dict(upstream_inference_cost=None, upstream_inference_prompt_cost=0.0001)))
    request = make_request(manifest)
    result = send_local_fixture(request, manifest=manifest, base_url=fake_server["url"], fixture_mode=True)
    assert result["status"] == "RECEIVED_LOCAL_FIXTURE"
    assert result["actual_model_calls"] == 0
    assert len(fake_server["requests"]) == 1
    sent = fake_server["requests"][0]
    assert sent["path"] == "/api/v1/chat/completions"
    assert "Authorization" not in sent["headers"]
    assert json.loads(sent["body"])["provider"] == dict(only=["deepinfra/bf16"],
        allow_fallbacks=False, require_parameters=True)
    assert result["request_sha256"] == hashlib.sha256(sent["body"]).hexdigest()
    assert result["tokens"] == dict(input=100, output=20, reasoning=7, cache_read=30, cache_write=0)
    assert result["usage_cost"] == 0.001
    assert result["cost_details"]["upstream_inference_cost"] is None
    assert result["resolved_endpoint"] is None


def test_generation_id_resolves_missing_identity_with_native_tokens(fake_server, manifest):
    fake_server["response"] = dict(id="gen-fixture & 1", choices=[], usage={})
    fake_server["generation"] = dict(id="gen-fixture & 1", model=manifest["model_id"],
        provider_name="DeepInfra", native_tokens_prompt=321, native_tokens_completion=45,
        native_tokens_reasoning=0, native_tokens_cached=12, total_cost=0.0004)
    result = send_local_fixture(make_request(manifest), manifest=manifest,
                                base_url=fake_server["url"], fixture_mode=True)
    assert len(fake_server["requests"]) == 2
    query = urlsplit(fake_server["requests"][1]["path"])
    assert query.path == "/api/v1/generation"
    assert parse_qs(query.query) == {"id": ["gen-fixture & 1"]}
    assert result["resolved_model"] == manifest["model_id"]
    assert result["responding_provider"] == "DeepInfra"
    assert result["tokens"] == dict(input=321, output=45, reasoning=0, cache_read=12, cache_write=None)
    assert result["usage_cost"] is None
    assert result["cost_details"] is None
    assert result["generation_total_cost"] == 0.0004
    assert result["generation_response_sha256"] is not None


def test_missing_usage_is_unknown_not_zero(manifest):
    result = response_record(dict(id="gen-fixture", model=manifest["model_id"], provider="DeepInfra"),
                             manifest=manifest)
    assert all(value is None for value in result["tokens"].values())
    assert result["usage_cost"] is result["cost_details"] is result["generation_total_cost"] is None


@pytest.mark.parametrize("field,value,error", [("model", "wrong/model", "RESOLVED_MODEL"),
    ("provider", "Nebius", "RESPONDING_PROVIDER")])
def test_resolved_identity_mismatch_aborts(fake_server, manifest, field, value, error):
    fake_server["response"] = dict(id="gen-fixture", model=manifest["model_id"], provider="DeepInfra")
    fake_server["response"][field] = value
    with pytest.raises(DeskTransportError, match=error):
        send_local_fixture(make_request(manifest), manifest=manifest,
                           base_url=fake_server["url"], fixture_mode=True)


def test_generation_conflict_aborts(fake_server, manifest):
    fake_server["response"] = dict(id="gen-fixture", model=manifest["model_id"])
    fake_server["generation"] = dict(id="different-id", model=manifest["model_id"], provider_name="DeepInfra")
    with pytest.raises(DeskTransportError, match="GENERATION_ID_MISMATCH"):
        send_local_fixture(make_request(manifest), manifest=manifest,
                           base_url=fake_server["url"], fixture_mode=True)


@pytest.mark.parametrize("contamination", ['{"owner":"private"}', '{"posiciones":1}',
    '{"montos":1}', r'{"\u006fwner":"private"}', r'{"posici\u00f3n":1}'])
def test_final_serialized_privacy_guard_prevents_http(fake_server, manifest, contamination):
    request = make_request(manifest)
    request["messages"][1]["content"] = contamination
    with pytest.raises(ValueError, match="PRIVATE_DATA_IN_RENDERED_PROMPT"):
        send_local_fixture(request, manifest=manifest, base_url=fake_server["url"], fixture_mode=True)
    assert fake_server["requests"] == []


def test_final_guard_canary_after_render_and_mutated_route_blocked(fake_server, manifest):
    request = make_request(manifest)
    request["messages"][1]["content"] = "sensitive-synthetic-canary"
    with pytest.raises(ValueError, match="PRIVATE_DATA"):
        send_local_fixture(request, manifest=manifest, base_url=fake_server["url"],
                           fixture_mode=True, forbidden_values=["sensitive-synthetic-canary"])
    request = make_request(manifest)
    request["provider"]["allow_fallbacks"] = True
    with pytest.raises(DeskTransportError, match="ENDPOINT_MANIFEST_MISMATCH"):
        send_local_fixture(request, manifest=manifest, base_url=fake_server["url"], fixture_mode=True)
    assert fake_server["requests"] == []


def test_error_body_is_not_exposed_and_redirect_is_not_followed(fake_server, manifest):
    fake_server["status"] = 302
    fake_server["response"] = dict(error="sensitive-synthetic-error")
    with pytest.raises(DeskTransportError) as caught:
        send_local_fixture(make_request(manifest), manifest=manifest,
                           base_url=fake_server["url"], fixture_mode=True)
    assert str(caught.value) == "LOCAL_HTTP_REQUEST_FAILED"
    assert len(fake_server["requests"]) == 1


def test_documented_selected_provider_metadata(manifest):
    result = response_record(dict(id="gen-fixture", model=manifest["model_id"],
        openrouter_metadata=dict(endpoints=dict(available=[dict(selected=True, provider="DeepInfra")]))),
        manifest=manifest)
    assert result["responding_provider"] == "DeepInfra"


def test_unresolved_provider_aborts_without_inventing_route_identity(fake_server, manifest):
    fake_server["response"] = dict(id="gen-fixture", model=manifest["model_id"])
    fake_server["generation"] = dict(id="gen-fixture", model=manifest["model_id"])
    with pytest.raises(DeskTransportError, match="UNRESOLVED_MODEL_OR_PROVIDER"):
        send_local_fixture(make_request(manifest), manifest=manifest,
                           base_url=fake_server["url"], fixture_mode=True)
