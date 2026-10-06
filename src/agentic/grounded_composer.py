from __future__ import annotations

import asyncio
import json
import math
import os
import re
from dataclasses import dataclass
from typing import Any

import httpx

from .contracts import AgentDecision, AgentModelError, validate_answer
from .evidence_gate import EvidenceGateResult, successful_payloads


_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])[-+]?\$?(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)%?(?![A-Za-z])")


@dataclass(frozen=True)
class VerifierVerdict:
    action: str
    issues: tuple[str, ...] = ()


class GroundedAgentComposer:
    """Synthesis and verification over already gathered evidence.

    The composer has no tool-selection capability. The verifier can only approve,
    request one grounded re-synthesis, or force the deterministic safe renderer.
    """

    def __init__(
        self,
        *,
        synthesis_model: str,
        verifier_model: str,
        base_url: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self.synthesis_model = synthesis_model
        self.verifier_model = verifier_model
        self.base_url = (
            base_url
            or os.getenv("QUANTIA_AGENT_OLLAMA_URL")
            or os.getenv("OLLAMA_URL")
            or "http://host.docker.internal:11434"
        ).rstrip("/")
        self.timeout_seconds = float(
            timeout_seconds
            if timeout_seconds is not None
            else os.getenv("QUANTIA_AGENT_COMPOSER_TIMEOUT_SECONDS", "90")
        )
        self.keep_alive = os.getenv("QUANTIA_OLLAMA_KEEP_ALIVE", "30m")
        self.context_tokens = max(2048, min(32768, int(os.getenv("QUANTIA_AGENT_SYNTHESIS_CONTEXT_TOKENS", "8192"))))
        self.num_predict = max(128, min(1600, int(os.getenv("QUANTIA_AGENT_SYNTHESIS_NUM_PREDICT", "650"))))

    @staticmethod
    def _compact_history(history: list[dict[str, Any]], gate: EvidenceGateResult) -> dict[str, Any]:
        payloads = successful_payloads(history)
        compact: dict[str, Any] = {
            "completion_gate": gate.to_dict(),
            "normalized": gate.normalized,
            "sources": {},
        }
        for name, payload in payloads.items():
            if name == "get_portfolio_snapshot":
                positions = payload.get("positions") if isinstance(payload.get("positions"), list) else []
                compact["sources"][name] = {
                    "scraped_at": payload.get("scraped_at"),
                    "total_value_ars": payload.get("total_value_ars"),
                    "cash_ars": payload.get("cash_ars"),
                    "positions": [
                        {
                            key: row.get(key)
                            for key in ("ticker", "quantity", "price", "market_value_ars", "weight", "current_weight")
                            if key in row
                        }
                        for row in positions[:30]
                        if isinstance(row, dict)
                    ],
                }
            elif name == "get_decision_evidence":
                plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else {}
                decisions = plan.get("decisions") if isinstance(plan.get("decisions"), list) else []
                compact["sources"][name] = {
                    "schema_version": payload.get("schema_version"),
                    "analysis_run_id": payload.get("analysis_run_id"),
                    "evaluated_at": payload.get("evaluated_at"),
                    "snapshot_as_of": payload.get("snapshot_as_of"),
                    "snapshot_stale_reason": payload.get("snapshot_stale_reason"),
                    "scope": payload.get("scope"),
                    "cash_ars": payload.get("cash_ars"),
                    "decisions": [
                        {
                            key: row.get(key)
                            for key in (
                                "ticker", "action", "current_weight", "target_weight",
                                "theoretical_target_weight", "executable_target_weight",
                                "signal_class", "portfolio_intent", "reason_primary",
                                "reason_secondary", "block_code",
                            )
                            if key in row
                        }
                        for row in decisions[:30]
                        if isinstance(row, dict)
                    ],
                    "signals": [
                        {
                            "ticker": row.get("ticker"),
                            "decision": row.get("decision"),
                            "final_score": row.get("final_score"),
                            "technical_regime": row.get("technical_regime"),
                            "trend_score": row.get("trend_score"),
                            "risk": next(
                                (
                                    layer.get("raw_score", layer.get("weighted"))
                                    for layer in (row.get("layers") or [])
                                    if isinstance(layer, dict) and str(layer.get("name") or "").lower() == "risk"
                                ),
                                None,
                            ),
                        }
                        for row in (payload.get("signals") or [])[:30]
                        if isinstance(row, dict)
                    ],
                }
            else:
                compact["sources"][name] = payload
        return compact

    async def _call(self, *, model: str, messages: list[dict[str, str]], num_predict: int) -> str:
        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "format": "json",
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": 0.0,
                "num_predict": num_predict,
                "num_ctx": self.context_tokens,
            },
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await asyncio.wait_for(
                client.post(f"{self.base_url}/api/chat", json=payload),
                timeout=self.timeout_seconds,
            )
        response.raise_for_status()
        data = response.json()
        return str((data.get("message") or {}).get("content") or "")

    @staticmethod
    def _json_object(content: str) -> dict[str, Any]:
        clean = str(content or "").strip()
        if clean.startswith("```"):
            clean = re.sub(r"^```(?:json)?\s*", "", clean, flags=re.IGNORECASE)
            clean = re.sub(r"\s*```$", "", clean).strip()
        candidates = [clean]
        start, end = clean.find("{"), clean.rfind("}")
        if start >= 0 and end > start:
            candidates.append(clean[start : end + 1])
        for candidate in candidates:
            try:
                value = json.loads(candidate)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                return value
        raise AgentModelError("composer model did not return a valid JSON object")

    async def synthesize(
        self,
        *,
        goal: str,
        intent: str,
        history: list[dict[str, Any]],
        gate: EvidenceGateResult,
        repair_issues: tuple[str, ...] = (),
    ) -> str:
        bundle = self._compact_history(history, gate)
        system = (
            "Sos el modelo de síntesis final de Quantia. No sos controller y no podés pedir tools. "
            "Usá exclusivamente la evidencia JSON provista. No inventes campos ni números. "
            "Diferenciá current_weight, theoretical_target_weight y executable_target_weight. "
            "WATCH/BLOCKED no son órdenes; un plan no es un fill. Una posición presente en snapshot "
            "pero ausente de signals/decisions puede describirse como frozen/no evaluable/residual, "
            "pero nunca asumir target 0. Si falta frozen_weight explícito, decilo. "
            "Identificá datos stale si snapshot_stale_reason lo indica. "
            "Respondé en español, directo y verificable. Devolvé sólo JSON {\"answer\":\"...\"}."
        )
        user_payload = {
            "goal": goal,
            "intent": intent,
            "evidence": bundle,
            "repair_issues": list(repair_issues),
        }
        raw = await self._call(
            model=self.synthesis_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, default=str)},
            ],
            num_predict=self.num_predict,
        )
        answer = str(self._json_object(raw).get("answer") or "").strip()
        return validate_answer(answer)

    @staticmethod
    def _parse_number(token: str) -> tuple[float, bool] | None:
        raw = str(token or "").strip().replace("$", "")
        percent = raw.endswith("%")
        raw = raw.rstrip("%")
        if not raw:
            return None
        if "." in raw and "," in raw:
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif raw.count(".") > 1:
            raw = raw.replace(".", "")
        elif raw.count(",") > 1:
            raw = raw.replace(",", "")
        else:
            raw = raw.replace(",", ".")
        try:
            value = float(raw)
        except ValueError:
            return None
        if not math.isfinite(value):
            return None
        return value, percent

    @staticmethod
    def _numeric_matches(value: float, source: float) -> bool:
        return abs(value - source) <= max(1e-8, 1e-5 * max(abs(value), abs(source), 1.0))

    def deterministic_issues(self, *, answer: str, gate: EvidenceGateResult) -> tuple[str, ...]:
        issues: list[str] = []
        normalized = gate.normalized or {}
        evidence_text = json.dumps(normalized, ensure_ascii=False, default=str)
        source_values: list[float] = []
        for token in _NUMBER_RE.findall(evidence_text):
            parsed = self._parse_number(token)
            if parsed:
                source_values.append(parsed[0])
        unmatched: list[str] = []
        for token in _NUMBER_RE.findall(answer):
            parsed = self._parse_number(token)
            if not parsed:
                continue
            value, percent = parsed
            candidates = {value, value / 100.0 if percent else value * 100.0}
            if value in {0.0, 1.0, 2.0, 3.0}:
                continue
            if not any(
                self._numeric_matches(candidate, source)
                for candidate in candidates
                for source in source_values
            ):
                unmatched.append(token)
        if unmatched:
            issues.append("unsupported_numeric_claims:" + ",".join(list(dict.fromkeys(unmatched))[:8]))

        lower = answer.lower()
        if any(term in lower for term in ("fill", "se ejecutó", "se ejecuto", "fue ejecutada", "fue ejecutado")):
            issues.append("plan_presented_as_fill")

        executable_clauses = re.findall(r"target ejecutable[^.\n]{0,100}", lower)
        for row in normalized.get("decisions", []):
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "")
            action = str(row.get("action") or "").upper()
            theoretical = row.get("theoretical_target_weight")
            executable = row.get("executable_target_weight")
            if ticker and action in {"WATCH", "BLOCKED"}:
                ticker_re = re.escape(ticker.lower())
                same_sentence = rf"\b{ticker_re}\b[^.\n]{{0,100}}\b(comprar|vender|ejecutar)\b"
                next_sentence = (
                    rf"\b{ticker_re}\b[^.\n]{{0,100}}\b(?:watch|blocked)\b[.\n]+"
                    rf"\s*[^.\n]{{0,100}}\b(comprar|vender|ejecutar)\b[^.\n]{{0,60}}\b{ticker_re}\b"
                )
                if re.search(same_sentence, lower) or re.search(next_sentence, lower):
                    issues.append(f"non_order_action_presented_as_order:{ticker}:{action}")
            if isinstance(theoretical, (int, float)) and isinstance(executable, (int, float)) and theoretical != executable:
                for clause in executable_clauses:
                    for token in _NUMBER_RE.findall(clause):
                        parsed = self._parse_number(token)
                        if not parsed:
                            continue
                        value, percent = parsed
                        normalized_value = value / 100.0 if percent else value
                        if self._numeric_matches(normalized_value, float(theoretical)) and not self._numeric_matches(
                            normalized_value, float(executable)
                        ):
                            issues.append(f"theoretical_target_confused_with_executable:{ticker}")
                            break

        for ticker in normalized.get("non_evaluable_positions", []):
            if re.search(rf"\b{re.escape(str(ticker).lower())}\b[^.\n]{{0,80}}target[^.\n]{{0,20}}\b0(?:[.,]0+)?%?", lower):
                issues.append(f"non_evaluable_assigned_zero_target:{ticker}")

        if normalized.get("snapshot_stale_reason") and not any(term in lower for term in ("stale", "desactual", "antigu", "viejo")):
            issues.append("stale_evidence_not_identified")
        return tuple(dict.fromkeys(issues))

    async def verify(
        self,
        *,
        goal: str,
        answer: str,
        history: list[dict[str, Any]],
        gate: EvidenceGateResult,
    ) -> VerifierVerdict:
        deterministic = self.deterministic_issues(answer=answer, gate=gate)
        if deterministic:
            return VerifierVerdict("RESYNTHESIZE", deterministic)

        bundle = self._compact_history(history, gate)
        system = (
            "Sos el verifier de Quantia. No podés cambiar decisiones financieras ni redactar una respuesta nueva. "
            "Verificá que cada número exista en evidencia, que target teórico no se confunda con ejecutable, "
            "que WATCH/BLOCKED no se describa como orden, que plan no sea fill, que frozen/no evaluable no tenga "
            "target 0 inventado, que stale esté marcado y que no haya campos inventados. "
            "Respondé sólo JSON con action=APPROVE|RESYNTHESIZE|SAFE_RENDER e issues=[...]."
        )
        try:
            raw = await self._call(
                model=self.verifier_model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps({"goal": goal, "answer": answer, "evidence": bundle}, ensure_ascii=False, default=str)},
                ],
                num_predict=300,
            )
            value = self._json_object(raw)
            action = str(value.get("action") or "").strip().upper()
            issues = tuple(str(item)[:300] for item in (value.get("issues") or []) if str(item).strip())
            if action not in {"APPROVE", "RESYNTHESIZE", "SAFE_RENDER"}:
                return VerifierVerdict("SAFE_RENDER", ("invalid_verifier_action",))
            return VerifierVerdict(action, issues)
        except Exception as exc:
            return VerifierVerdict("SAFE_RENDER", (f"verifier_unavailable:{type(exc).__name__}",))

    async def compose(
        self,
        *,
        goal: str,
        intent: str,
        history: list[dict[str, Any]],
        gate: EvidenceGateResult,
    ) -> tuple[AgentDecision | None, VerifierVerdict]:
        try:
            answer = await self.synthesize(goal=goal, intent=intent, history=history, gate=gate)
        except Exception as exc:
            return None, VerifierVerdict("SAFE_RENDER", (f"synthesis_unavailable:{type(exc).__name__}",))

        verdict = await self.verify(goal=goal, answer=answer, history=history, gate=gate)
        if verdict.action == "RESYNTHESIZE":
            try:
                answer = await self.synthesize(
                    goal=goal,
                    intent=intent,
                    history=history,
                    gate=gate,
                    repair_issues=verdict.issues,
                )
            except Exception as exc:
                return None, VerifierVerdict("SAFE_RENDER", (f"resynthesis_unavailable:{type(exc).__name__}",))
            second = await self.verify(goal=goal, answer=answer, history=history, gate=gate)
            if second.action != "APPROVE":
                return None, second
            verdict = second
        if verdict.action != "APPROVE":
            return None, verdict
        return (
            AgentDecision(
                kind="final",
                answer=answer,
                rationale="Síntesis grounded sobre evidencia completa del run actual; verifier aprobó.",
                answer_origin="grounded_synthesis_v2",
                objective_status="EXPLAINED",
            ),
            verdict,
        )


__all__ = ["GroundedAgentComposer", "VerifierVerdict"]
