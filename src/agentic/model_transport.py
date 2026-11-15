"""Provider transport for the analytical agent; no financial capabilities."""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from .contracts import AgentModelError

logger = logging.getLogger(__name__)
OPENROUTER_URL = "https://openrouter.ai/api/v1"
NEMOTRON_MODEL = "nvidia/nemotron-3-ultra-550b-a55b"


def agent_provider() -> str:
    provider = os.getenv("QUANTIA_AGENT_PROVIDER", "ollama").strip().lower()
    if provider not in {"ollama", "openrouter"}:
        raise ValueError("QUANTIA_AGENT_PROVIDER must be ollama or openrouter")
    return provider


def default_agent_model() -> str:
    return os.getenv("QUANTIA_AGENT_MODEL") or (
        NEMOTRON_MODEL if agent_provider() == "openrouter" else "qwen3.5:9b"
    )


def agent_base_url(provider: str, override: str | None = None) -> str:
    if provider == "openrouter":
        # A local Ollama URL must never receive an OpenRouter credential.
        if override and override.rstrip("/") != OPENROUTER_URL:
            raise ValueError("OpenRouter requires its official HTTPS API endpoint")
        return OPENROUTER_URL
    return (override or os.getenv("QUANTIA_AGENT_OLLAMA_URL")
            or os.getenv("OLLAMA_URL") or "http://host.docker.internal:11434").rstrip("/")


async def chat_content(
    client: httpx.AsyncClient, *, provider: str, base_url: str, payload: dict[str, Any],
    max_output_tokens: int | None = None,
) -> str:
    """Translate the existing JSON controller contract, preserving local Ollama.

    Responses/errors never expose credentials, prompts, or private reasoning.
    Provider failures remain failures; there is no silent cross-model fallback.
    """
    if provider == "ollama":
        response = await client.post(f"{base_url}/api/chat", json=payload)
        response.raise_for_status()
        return str((response.json().get("message") or {}).get("content") or "")
    if provider != "openrouter":
        raise AgentModelError("Unsupported agent provider")
    agent_base_url(provider, base_url)
    key = (os.getenv("OPENROUTER_API_KEY") or os.getenv("openrouter") or "").strip()
    if not key:
        raise AgentModelError("OpenRouter credential missing (OPENROUTER_API_KEY or openrouter)")
    options = payload.get("options") or {}
    effort = os.getenv("QUANTIA_OPENROUTER_REASONING_EFFORT", "low").strip().lower()
    if effort not in {"none", "minimal", "low", "medium", "high"}:
        raise AgentModelError("Invalid OpenRouter reasoning effort")
    max_tokens = max(512, min(16384, int(max_output_tokens if max_output_tokens is not None
                                      else os.getenv("QUANTIA_OPENROUTER_MAX_TOKENS", "4096"))))
    request = {
        "model": payload["model"], "messages": payload["messages"], "stream": False,
        "temperature": options.get("temperature", 0.0), "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
        "provider": {"require_parameters": True},
        "reasoning": {"effort": effort, "exclude": True},
    }
    try:
        response = await client.post(
            f"{base_url}/chat/completions", json=request,
            headers={"Authorization": f"Bearer {key}", "X-Title": "Quantia Agent"},
        )
    except httpx.HTTPError:
        raise AgentModelError("OpenRouter network request failed") from None
    if response.status_code >= 400:
        raise AgentModelError(f"OpenRouter HTTP {response.status_code}")
    try:
        body = response.json()
        choice = body["choices"][0]
        content = choice["message"].get("content")
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        raise AgentModelError("OpenRouter returned an invalid response") from None
    if choice.get("finish_reason") != "stop":
        raise AgentModelError("OpenRouter completion was interrupted or truncated")
    if not isinstance(content, str) or not content.strip():
        raise AgentModelError("OpenRouter returned empty content")
    # Token counts only; never log content, reasoning, keys, or provider errors.
    usage = body.get("usage") or {}
    logger.info("[CHAT][PROVIDER] provider=openrouter model=%s prompt_tokens=%s completion_tokens=%s",
                payload["model"], usage.get("prompt_tokens"), usage.get("completion_tokens"))
    return content
