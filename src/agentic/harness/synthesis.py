from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any

import httpx

from .schemas import EvidenceObject, TaskSpec

logger = logging.getLogger(__name__)


class GroundedSynthesizer:
    """Natural-language renderer constrained to compact verified evidence.

    Deterministic renderers remain the fallback if the model times out, returns
    invalid text or fails numeric verification. The normal conversational path
    is model-written rather than phrase-template-written.
    """

    def __init__(self, *, model: str, base_url: str | None = None, timeout_seconds: float = 45) -> None:
        self.model = model
        self.base_url = (
            base_url
            or os.getenv("QUANTIA_AGENT_OLLAMA_URL")
            or os.getenv("OLLAMA_URL")
            or "http://host.docker.internal:11434"
        ).rstrip("/")
        self.timeout_seconds = float(os.getenv("QUANTIA_HARNESS_SYNTHESIS_TIMEOUT_SECONDS", str(timeout_seconds)))
        self.max_chars = int(os.getenv("QUANTIA_HARNESS_SYNTHESIS_EVIDENCE_CHARS", "3500"))
        self.context_tokens = max(
            1536,
            min(16384, int(os.getenv("QUANTIA_SYNTHESIS_CONTEXT_TOKENS", "4096"))),
        )
        self.num_predict = max(
            96,
            min(800, int(os.getenv("QUANTIA_SYNTHESIS_NUM_PREDICT", "220"))),
        )
        self.keep_alive = os.getenv("QUANTIA_OLLAMA_KEEP_ALIVE", "30m")

    @staticmethod
    def _extract_answer(content: str) -> str:
        clean = str(content or "").strip()
        if not clean:
            return ""
        if clean.startswith("```"):
            clean = re.sub(r"^```(?:json)?\s*", "", clean, flags=re.IGNORECASE)
            clean = re.sub(r"\s*```$", "", clean).strip()

        candidates = [clean]
        start = clean.find("{")
        end = clean.rfind("}")
        if start >= 0 and end > start:
            candidates.append(clean[start : end + 1])

        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(parsed, dict):
                answer = str(parsed.get("answer") or "").strip()
                if answer:
                    return answer

        if len(clean) >= 20 and not clean.startswith("{"):
            return clean
        return ""

    @staticmethod
    def _compact_dict(item: EvidenceObject, payload: dict[str, Any]) -> dict[str, Any]:
        schema = str(payload.get("schema_version") or "")
        if schema == "bot-follow-pnl-v1":
            keys = (
                "schema_version", "source", "mode", "as_of", "lookback_days", "plans_total",
                "plans_closed_5d", "plans_closed_10d", "plans_closed_20d",
                "bot_pnl_5d_ars", "bot_pnl_10d_ars", "bot_pnl_20d_ars", "scope", "limitations",
            )
            return {key: payload.get(key) for key in keys if key in payload}
        if schema == "bot-follow-pnl-normalized-v1":
            keys = (
                "schema_version", "source", "mode", "as_of", "lookback_days",
                "candidate_plans_total", "raw_plans_total", "excluded_missing_notional",
                "episodes_total", "duplicates_removed", "episodes_closed_5d", "episodes_closed_10d",
                "episodes_closed_20d", "pnl_5d_ars", "pnl_10d_ars", "pnl_20d_ars", "scope",
                "episode_definition", "limitations",
            )
            return {key: payload.get(key) for key in keys if key in payload}
        if schema == "run-evidence-provenance-v1":
            return {
                "schema_version": schema,
                "status": payload.get("status"),
                "referenced_run_id": payload.get("referenced_run_id"),
                "referenced_goal": payload.get("referenced_goal"),
                "sources": payload.get("sources") or [],
            }
        if schema in {"persisted-decision-evidence-v1", "agent-decision-evidence-v1"}:
            signals = payload.get("signals") if isinstance(payload.get("signals"), list) else []
            return {
                "schema_version": schema,
                "analysis_run_id": payload.get("analysis_run_id"),
                "evaluated_at": payload.get("evaluated_at"),
                "snapshot_as_of": payload.get("snapshot_as_of"),
                "latest_portfolio_snapshot_as_of": payload.get("latest_portfolio_snapshot_as_of"),
                "signals": [
                    {
                        key: signal.get(key)
                        for key in ("ticker", "decision", "final_score", "status", "reason", "as_of")
                        if key in signal
                    }
                    for signal in signals[:12]
                    if isinstance(signal, dict)
                ],
                "warnings": payload.get("warnings") or [],
            }
        if item.tool_name == "get_portfolio_snapshot":
            positions = payload.get("positions") if isinstance(payload.get("positions"), list) else []
            return {
                "scraped_at": payload.get("scraped_at"),
                "total_value_ars": payload.get("total_value_ars"),
                "cash_ars": payload.get("cash_ars"),
                "positions": [
                    {
                        key: position.get(key)
                        for key in ("ticker", "quantity", "price", "market_value_ars", "weight", "pnl_pct")
                        if key in position
                    }
                    for position in positions[:15]
                    if isinstance(position, dict)
                ],
            }
        if isinstance(payload.get("report"), str):
            return {
                key: payload.get(key)
                for key in ("source", "mode", "as_of", "lookback_days", "verdict", "cost_bps", "min_sample")
                if key in payload
            } | {"report": str(payload.get("report"))[:2200]}

        # Generic bounded projection: keep scalar metadata and compact simple
        # collections, but never send an unbounded raw payload to the model.
        compact: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                compact[key] = value
            elif isinstance(value, list) and len(value) <= 12:
                compact[key] = value
            elif isinstance(value, dict) and len(value) <= 12:
                compact[key] = value
        return compact

    @classmethod
    def _compact_payload(cls, item: EvidenceObject) -> dict[str, Any] | str:
        if isinstance(item.payload, dict):
            return cls._compact_dict(item, item.payload)
        return str(item.payload)[:2200]

    async def synthesize(self, *, task: TaskSpec, evidence: list[EvidenceObject], fallback: str) -> str:
        if os.getenv("QUANTIA_HARNESS_SYNTHESIS_ENABLED", "true").lower() not in {"1", "true", "yes", "on"}:
            return fallback

        # Provenance must reproduce the exact audited source list. Keep that one
        # intent deterministic; all normal analytical answers are model-written.
        bypass_intents = {
            value.strip()
            for value in os.getenv(
                "QUANTIA_HARNESS_SYNTHESIS_BYPASS_INTENTS",
                "evidence_provenance",
            ).split(",")
            if value.strip()
        }
        if task.intent in bypass_intents:
            logger.info("[CHAT][SYNTHESIS] bypass intent=%s", task.intent)
            return fallback

        bundle: list[dict[str, Any]] = []
        used = 0
        for item in evidence:
            compact = self._compact_payload(item)
            serialized = json.dumps(compact, ensure_ascii=False, default=str)
            remaining = max(0, self.max_chars - used)
            if remaining <= 0:
                break
            if len(serialized) > remaining:
                serialized = serialized[:remaining]
                compact_for_model: dict[str, Any] | str = serialized + " [truncado]"
            else:
                compact_for_model = compact
            used += len(serialized)
            bundle.append({
                "source": item.source,
                "mode": item.mode.value,
                "quality": item.quality.value,
                "timestamp": item.timestamp.isoformat(),
                "warnings": item.warnings,
                "data": compact_for_model,
            })

        system = (
            "Sos la voz conversacional final de Quantia. Contestá en español natural, directo y breve. "
            "No uses una plantilla fija: adaptá la redacción a la pregunta y al contexto semántico del task. "
            "Los hechos financieros salen EXCLUSIVAMENTE de evidence; no calcules números nuevos ni completes datos faltantes. "
            "Podés reformatear un número observado, pero no cambiar su valor ni sumar horizontes alternativos. "
            "Diferenciá siempre plan hipotético, episodio normalizado, fill real, PnL bruto y PnL neto. "
            "Si evidence informa planes excluidos por falta de notional, mencioná esa cobertura sólo cuando sea material. "
            "Un score no es retorno. Una señal no es una operación ejecutada. SHADOW/RESEARCH no es producción. "
            "Mencioná la limitación material más importante sin recitar advertencias innecesarias. "
            "No muestres JSON, nombres internos de tools ni trazas salvo que el usuario pregunte explícitamente por fuentes. "
            "No ejecutes ni prometas operaciones. Máximo 6 líneas normalmente. "
            "Devolvé sólo JSON válido con la forma {\"answer\":\"texto final\"}."
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": task.model_dump(mode="json"),
                            "evidence": bundle,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            "stream": False,
            "format": "json",
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": 0.15,
                "num_predict": self.num_predict,
                "num_ctx": self.context_tokens,
            },
        }
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await asyncio.wait_for(
                    client.post(f"{self.base_url}/api/chat", json=payload),
                    timeout=self.timeout_seconds,
                )
            response.raise_for_status()
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "[CHAT][SYNTHESIS] model=%s elapsed_ms=%s evidence_chars=%s",
                self.model,
                elapsed_ms,
                used,
            )
            data = response.json()
            content = str((data.get("message") or {}).get("content") or "")
            answer = self._extract_answer(content)
            if answer:
                return answer[:6000]
            logger.warning("[CHAT][SYNTHESIS] fallback model=%s error=EmptyOrInvalidAnswer", self.model)
            return fallback
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.warning(
                "[CHAT][SYNTHESIS] fallback model=%s elapsed_ms=%s error=%s detail=%s",
                self.model,
                elapsed_ms,
                type(exc).__name__,
                str(exc)[:300],
            )
            return fallback
