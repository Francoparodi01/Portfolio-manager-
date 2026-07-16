"""Evidence-bounded LLM narratives for Quantia.

This module is intentionally offline from trading runtime. It converts closed
evidence packets into structured prompts for a local Qwen/Ollama model, then
normalizes and validates the JSON response. It does not read/write the DB, does
not call the planner, and does not publish to Telegram.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import httpx


PROMPT_VERSION = "qwen_narratives_v1"
SCHEMA_VERSION = 1
DEFAULT_MODEL = os.getenv(
    "QUANTIA_LLM_MODEL",
    os.getenv("SENTIMENT_OLLAMA_MODEL", "qwen2.5:3b"),
)
DEFAULT_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("QUANTIA_LLM_TIMEOUT_SECONDS", "45"))

VALID_EFFECTIVE_ACTIONS = {
    "buy",
    "sell",
    "reduce",
    "hold",
    "rebalance",
    "no_action",
}

MARKET_NARRATIVE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "headline",
        "executive_summary",
        "sections",
        "caveats",
        "insufficiency_flag",
    ],
    "properties": {
        "headline": {"type": "string"},
        "executive_summary": {"type": "string"},
        "sections": {
            "type": "array",
            "minItems": 1,
            "maxItems": 6,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "paragraph", "supporting_fact_ids"],
                "properties": {
                    "title": {"type": "string"},
                    "paragraph": {"type": "string"},
                    "supporting_fact_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "caveats": {"type": "array", "items": {"type": "string"}},
        "insufficiency_flag": {"type": "boolean"},
    },
}

DECISION_EXPLANATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "effective_action",
        "short_explanation",
        "primary_reason_codes",
        "supporting_fact_ids",
        "constraints_applied",
        "rejected_interpretations",
        "insufficiency_flag",
    ],
    "properties": {
        "effective_action": {
            "type": "string",
            "enum": sorted(VALID_EFFECTIVE_ACTIONS),
        },
        "short_explanation": {"type": "string"},
        "primary_reason_codes": {"type": "array", "items": {"type": "string"}},
        "supporting_fact_ids": {"type": "array", "items": {"type": "string"}},
        "constraints_applied": {"type": "array", "items": {"type": "string"}},
        "rejected_interpretations": {"type": "array", "items": {"type": "string"}},
        "insufficiency_flag": {"type": "boolean"},
    },
}


@dataclass(frozen=True)
class NarrativeSection:
    title: str
    paragraph: str
    supporting_fact_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "paragraph": self.paragraph,
            "supporting_fact_ids": list(self.supporting_fact_ids),
        }


@dataclass(frozen=True)
class MarketNarrative:
    headline: str
    executive_summary: str
    sections: tuple[NarrativeSection, ...]
    caveats: tuple[str, ...]
    insufficiency_flag: bool
    model: str
    prompt_version: str
    schema_version: int
    input_fingerprint: str
    raw_response: dict[str, Any]
    generated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "headline": self.headline,
            "executive_summary": self.executive_summary,
            "sections": [section.to_dict() for section in self.sections],
            "caveats": list(self.caveats),
            "insufficiency_flag": self.insufficiency_flag,
            "model": self.model,
            "prompt_version": self.prompt_version,
            "schema_version": self.schema_version,
            "input_fingerprint": self.input_fingerprint,
            "generated_at": self.generated_at.isoformat(),
            "raw_response": self.raw_response,
        }


@dataclass(frozen=True)
class DecisionExplanation:
    effective_action: str
    short_explanation: str
    primary_reason_codes: tuple[str, ...]
    supporting_fact_ids: tuple[str, ...]
    constraints_applied: tuple[str, ...]
    rejected_interpretations: tuple[str, ...]
    insufficiency_flag: bool
    model: str
    prompt_version: str
    schema_version: int
    input_fingerprint: str
    raw_response: dict[str, Any]
    generated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "effective_action": self.effective_action,
            "short_explanation": self.short_explanation,
            "primary_reason_codes": list(self.primary_reason_codes),
            "supporting_fact_ids": list(self.supporting_fact_ids),
            "constraints_applied": list(self.constraints_applied),
            "rejected_interpretations": list(self.rejected_interpretations),
            "insufficiency_flag": self.insufficiency_flag,
            "model": self.model,
            "prompt_version": self.prompt_version,
            "schema_version": self.schema_version,
            "input_fingerprint": self.input_fingerprint,
            "generated_at": self.generated_at.isoformat(),
            "raw_response": self.raw_response,
        }


def packet_fingerprint(packet: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        _json_safe(dict(packet)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def known_fact_ids(packet: Mapping[str, Any]) -> set[str]:
    ids: set[str] = set()
    for item in packet.get("evidence_items") or []:
        if isinstance(item, Mapping):
            fact_id = _clean_id(item.get("fact_id"))
            if fact_id:
                ids.add(fact_id)
    return ids


def build_market_report_prompt(packet: Mapping[str, Any]) -> str:
    _validate_packet(packet, expected_type="market_report")
    payload = json.dumps(_json_safe(dict(packet)), ensure_ascii=False, sort_keys=True, indent=2)
    allowed_fact_ids = _allowed_fact_ids_block(packet)
    return (
        "Sos la capa narrativa local de Quantia, ejecutada con Qwen/Ollama.\n"
        "Tu tarea es redactar una lectura diaria a partir de un MarketReportPacket cerrado.\n"
        "No inventes numeros, tickers, precios, motivos ni eventos. No recomiendes compras, "
        "ventas, sizing ni targets nuevos. Distingui entre datos de mercado, cartera, "
        "planner, riesgo, sentiment y gaps de cobertura.\n"
        "Usa solo los tickers tal como aparecen en el packet. No expandas tickers a nombres "
        "de empresas, marcas, sectores o aliases si ese nombre no viene explicitamente en el "
        "input. Regla dura: no escribas patrones como TICKER (nombre); menciona solo el "
        "ticker. No digas politica de cobertura: si corresponde, deci cobertura de precios.\n"
        "No interpretes market_value_ars como ganancia o perdida. Ganancia/perdida diaria "
        "solo puede salir de facts day_pnl_ars o day_change_pct. Si un day_change_pct es "
        "negativo, no lo describas como positivo.\n"
        "Si existen evidence_items con kind='statement', basate primero en statement.text. "
        "No recalcules ni combines valores crudos si el statement ya resume la lectura.\n"
        "Si un numeric fact trae display_value, copia ese display_value literalmente para "
        "montos y porcentajes; no reformatees decimales ni conviertas a millones.\n"
        "Cada seccion debe incluir supporting_fact_ids existentes en el packet. Copia los "
        "fact_ids literalmente desde la lista permitida; no los traduzcas, no cambies puntos "
        "por guiones bajos y no inventes alias. Si falta cobertura material o datos criticos, "
        "marca insufficiency_flag=true y explicalo como caveat. Responde en espanol "
        "rioplatense, claro y operativo.\n"
        "Responde SOLO con JSON valido usando exactamente el schema solicitado.\n\n"
        f"FACT_IDS PERMITIDOS:\n{allowed_fact_ids}\n\n"
        f"MARKET_REPORT_PACKET:\n{payload}"
    )


def build_decision_explanation_prompt(packet: Mapping[str, Any]) -> str:
    _validate_packet(packet, expected_type="decision_evidence")
    payload = json.dumps(_json_safe(dict(packet)), ensure_ascii=False, sort_keys=True, indent=2)
    allowed_fact_ids = _allowed_fact_ids_block(packet)
    return (
        "Sos el explicador de decisiones de Quantia, ejecutado con Qwen/Ollama.\n"
        "Explicas una decision ya tomada por el sistema; no la evaluas, no la cambias y "
        "no sugeris una accion nueva. No confundas ejecucion, rebalanceo, scoring, tesis "
        "ni restricciones. Si fue rebalanceo, no lo conviertas en tesis bearish. Si fue "
        "bloqueo por restriccion, no lo conviertas en falta de conviccion.\n"
        "Usa solo el ticker tal como aparece en el packet. No expandas tickers a nombres de "
        "empresas, marcas, sectores o aliases si ese nombre no viene explicitamente en el input. "
        "Regla dura: no escribas patrones como TICKER (nombre); menciona solo el ticker.\n"
        "Toda afirmacion sensible debe apoyarse en supporting_fact_ids existentes. Copia los "
        "fact_ids literalmente desde la lista permitida; no los traduzcas, no cambies puntos "
        "por guiones bajos y no inventes alias. Si falta evidencia suficiente, marca "
        "insufficiency_flag=true. Responde en espanol, breve y operativo. Responde SOLO con "
        "JSON valido usando exactamente el schema solicitado.\n\n"
        f"FACT_IDS PERMITIDOS:\n{allowed_fact_ids}\n\n"
        f"DECISION_EVIDENCE_PACKET:\n{payload}"
    )


async def generate_market_narrative_with_ollama(
    packet: Mapping[str, Any],
    *,
    model: str = DEFAULT_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    client: httpx.AsyncClient | None = None,
    max_validation_attempts: int = 3,
) -> MarketNarrative:
    return await _generate_with_ollama(
        packet,
        task="market_report",
        model=model,
        ollama_url=ollama_url,
        timeout_seconds=timeout_seconds,
        client=client,
        max_validation_attempts=max_validation_attempts,
    )


async def explain_decision_with_ollama(
    packet: Mapping[str, Any],
    *,
    model: str = DEFAULT_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    client: httpx.AsyncClient | None = None,
    max_validation_attempts: int = 3,
) -> DecisionExplanation:
    return await _generate_with_ollama(
        packet,
        task="decision_evidence",
        model=model,
        ollama_url=ollama_url,
        timeout_seconds=timeout_seconds,
        client=client,
        max_validation_attempts=max_validation_attempts,
    )


def normalize_market_narrative(
    packet: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    model: str,
) -> MarketNarrative:
    _validate_packet(packet, expected_type="market_report")
    fact_ids = known_fact_ids(packet)
    headline = _clean_text(payload.get("headline"), limit=160)
    executive_summary = _clean_text(payload.get("executive_summary"), limit=900)
    if not headline or not executive_summary:
        raise ValueError("market narrative requires headline and executive_summary")

    sections_raw = payload.get("sections")
    if not isinstance(sections_raw, list) or not sections_raw:
        raise ValueError("market narrative requires at least one section")
    sections: list[NarrativeSection] = []
    for raw in sections_raw[:6]:
        if not isinstance(raw, Mapping):
            continue
        section = _normalize_section(raw, fact_ids=fact_ids)
        sections.append(section)
    if not sections:
        raise ValueError("market narrative contains no valid sections")

    insufficiency_flag = bool(payload.get("insufficiency_flag"))
    if _requires_insufficiency_flag(packet) and not insufficiency_flag:
        raise ValueError("insufficiency_flag must be true when coverage/completeness is low")
    _validate_no_unsupported_ticker_expansions(
        packet,
        [
            headline,
            executive_summary,
            *(section.title for section in sections),
            *(section.paragraph for section in sections),
            *_text_list(payload.get("caveats"), max_items=6, item_limit=500),
        ],
    )

    return MarketNarrative(
        headline=headline,
        executive_summary=executive_summary,
        sections=tuple(sections),
        caveats=tuple(_text_list(payload.get("caveats"), max_items=6, item_limit=500)),
        insufficiency_flag=insufficiency_flag,
        model=str(model),
        prompt_version=PROMPT_VERSION,
        schema_version=SCHEMA_VERSION,
        input_fingerprint=packet_fingerprint(packet),
        raw_response=_json_safe(dict(payload)),
        generated_at=datetime.now(timezone.utc),
    )


def normalize_decision_explanation(
    packet: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    model: str,
) -> DecisionExplanation:
    _validate_packet(packet, expected_type="decision_evidence")
    fact_ids = known_fact_ids(packet)
    expected_action = _packet_effective_action(packet)
    effective_action = _clean_id(payload.get("effective_action")).lower()
    if effective_action not in VALID_EFFECTIVE_ACTIONS:
        raise ValueError(f"invalid effective_action: {effective_action!r}")
    if expected_action and effective_action != expected_action:
        raise ValueError(
            f"effective_action mismatch: packet={expected_action!r}, output={effective_action!r}"
        )

    short_explanation = _clean_text(payload.get("short_explanation"), limit=900)
    if not short_explanation:
        raise ValueError("short_explanation is required")
    reason_codes = tuple(
        _clean_code(code) for code in _text_list(payload.get("primary_reason_codes"), max_items=8)
    )
    reason_codes = tuple(code for code in reason_codes if code)
    if not reason_codes:
        raise ValueError("primary_reason_codes is required")

    supporting = _canonical_supporting_fact_ids(
        _text_list(payload.get("supporting_fact_ids"), max_items=20),
        fact_ids=fact_ids,
    )
    _validate_supporting_fact_ids(supporting, fact_ids=fact_ids, location="decision")

    insufficiency_flag = bool(payload.get("insufficiency_flag"))
    if _requires_insufficiency_flag(packet) and not insufficiency_flag:
        raise ValueError("insufficiency_flag must be true when coverage/completeness is low")
    _validate_decision_semantics(packet, reason_codes)
    _validate_no_unsupported_ticker_expansions(
        packet,
        [
            short_explanation,
            *_text_list(payload.get("rejected_interpretations"), max_items=8, item_limit=220),
        ],
    )

    return DecisionExplanation(
        effective_action=effective_action,
        short_explanation=short_explanation,
        primary_reason_codes=reason_codes,
        supporting_fact_ids=supporting,
        constraints_applied=tuple(
            _clean_code(item) for item in _text_list(payload.get("constraints_applied"), max_items=10)
        ),
        rejected_interpretations=tuple(
            _clean_text(item, limit=220)
            for item in _text_list(payload.get("rejected_interpretations"), max_items=8)
        ),
        insufficiency_flag=insufficiency_flag,
        model=str(model),
        prompt_version=PROMPT_VERSION,
        schema_version=SCHEMA_VERSION,
        input_fingerprint=packet_fingerprint(packet),
        raw_response=_json_safe(dict(payload)),
        generated_at=datetime.now(timezone.utc),
    )


async def _generate_with_ollama(
    packet: Mapping[str, Any],
    *,
    task: str,
    model: str,
    ollama_url: str,
    timeout_seconds: float,
    client: httpx.AsyncClient | None,
    max_validation_attempts: int,
) -> MarketNarrative | DecisionExplanation:
    if task == "market_report":
        prompt = build_market_report_prompt(packet)
        schema = MARKET_NARRATIVE_SCHEMA
        normalizer = normalize_market_narrative
        system = (
            "Sos el redactor financiero local de Quantia. Respondes solo JSON valido "
            "y no das recomendaciones nuevas."
        )
    elif task == "decision_evidence":
        prompt = build_decision_explanation_prompt(packet)
        schema = DECISION_EXPLANATION_SCHEMA
        normalizer = normalize_decision_explanation
        system = (
            "Sos el auditor narrativo local de Quantia. Respondes solo JSON valido, "
            "explicas decisiones existentes y no cambias la accion."
        )
    else:
        raise ValueError(f"unsupported task: {task!r}")

    owns_client = client is None
    http_client = client or httpx.AsyncClient(timeout=timeout_seconds)
    messages = [{"role": "system", "content": system}, {"role": "user", "content": prompt}]
    attempts = max(1, min(3, int(max_validation_attempts)))
    try:
        for attempt in range(attempts):
            response = await http_client.post(
                f"{ollama_url.rstrip('/')}/api/chat",
                json={
                    "model": model,
                    "stream": False,
                    "format": schema,
                    "options": {"temperature": 0.0, "num_predict": 1100},
                    "messages": messages,
                },
            )
            response.raise_for_status()
            envelope = response.json()
            content = envelope.get("message", {}).get("content", "")
            try:
                payload = _extract_json_object(content)
                return normalizer(packet, payload, model=model)
            except ValueError as exc:
                if attempt + 1 >= attempts:
                    raise
                allowed_fact_ids = ", ".join(sorted(known_fact_ids(packet)))
                messages.extend(
                    [
                        {"role": "assistant", "content": str(content)},
                        {
                            "role": "user",
                            "content": (
                                "La respuesta anterior incumple el contrato: "
                                f"{exc}. Corregila completa. Usa solo fact_ids existentes, "
                                "copiados literalmente de esta lista: "
                                f"{allowed_fact_ids}. Respeta effective_action si aplica, "
                                "marca insufficiency_flag cuando la cobertura sea baja y "
                                "no uses parentesis para expandir tickers; si el error "
                                "menciona unsupported ticker expansion, elimina el texto "
                                "entre parentesis y deja solo el ticker. "
                                "devolve solo JSON valido."
                            ),
                        },
                    ]
                )
    finally:
        if owns_client:
            await http_client.aclose()
    raise RuntimeError("Ollama did not produce a valid narrative")


def _validate_packet(packet: Mapping[str, Any], *, expected_type: str) -> None:
    if not isinstance(packet, Mapping):
        raise ValueError("packet must be a mapping")
    packet_type = _clean_id(packet.get("packet_type")).lower()
    if packet_type != expected_type:
        raise ValueError(f"packet_type must be {expected_type!r}")
    fact_ids = known_fact_ids(packet)
    if not fact_ids:
        raise ValueError("packet must include evidence_items with fact_id")
    if len(fact_ids) != len([item for item in packet.get("evidence_items") or [] if isinstance(item, Mapping)]):
        raise ValueError("packet contains duplicate or empty fact_id values")


def _allowed_fact_ids_block(packet: Mapping[str, Any]) -> str:
    return "\n".join(f"- {fact_id}" for fact_id in sorted(known_fact_ids(packet)))


def _normalize_section(raw: Mapping[str, Any], *, fact_ids: set[str]) -> NarrativeSection:
    title = _clean_text(raw.get("title"), limit=90)
    paragraph = _clean_text(raw.get("paragraph"), limit=900)
    supporting = tuple(
        _clean_id(item) for item in _text_list(raw.get("supporting_fact_ids"), max_items=12)
    )
    supporting = _canonical_supporting_fact_ids(supporting, fact_ids=fact_ids)
    if not title or not paragraph:
        raise ValueError("section requires title and paragraph")
    if not supporting:
        raise ValueError(f"section {title!r} requires supporting_fact_ids")
    _validate_supporting_fact_ids(supporting, fact_ids=fact_ids, location=f"section {title!r}")
    return NarrativeSection(title=title, paragraph=paragraph, supporting_fact_ids=supporting)


def _canonical_supporting_fact_ids(values: Sequence[str], *, fact_ids: set[str]) -> tuple[str, ...]:
    lower_map = {fact_id.lower(): fact_id for fact_id in fact_ids}
    alias_map = {fact_id.replace(".", "_").lower(): fact_id for fact_id in fact_ids}
    for fact_id in fact_ids:
        match = re.fullmatch(r"statement\.position\.([A-Za-z0-9._-]+)\.move", fact_id)
        if match:
            ticker = match.group(1).lower()
            alias_map.setdefault(f"position_{ticker}", fact_id)
            alias_map.setdefault(f"{ticker}_move", fact_id)
    if "statement.portfolio.overview" in fact_ids:
        alias_map.setdefault("portfolio", "statement.portfolio.overview")
        alias_map.setdefault("portfolio_overview", "statement.portfolio.overview")
    if "statement.coverage" in fact_ids:
        alias_map.setdefault("coverage", "statement.coverage")
    out: list[str] = []
    for value in values:
        fact_id = _clean_id(value)
        if not fact_id:
            continue
        key = fact_id.lower()
        out.append(
            fact_id
            if fact_id in fact_ids
            else lower_map.get(key, alias_map.get(key, fact_id))
        )
    return tuple(out)


def _validate_supporting_fact_ids(
    values: Sequence[str],
    *,
    fact_ids: set[str],
    location: str,
) -> None:
    missing = sorted({item for item in values if item not in fact_ids})
    if missing:
        raise ValueError(f"{location} references unknown fact_ids: {missing}")


def _requires_insufficiency_flag(packet: Mapping[str, Any]) -> bool:
    coverage = _ratio_value((packet.get("coverage") or {}).get("priced_weight_pct"))
    completeness = _ratio_value(packet.get("data_completeness"))
    low_coverage = coverage is not None and coverage < 0.80
    low_completeness = completeness is not None and completeness < 0.80
    return low_coverage or low_completeness


def _packet_effective_action(packet: Mapping[str, Any]) -> str:
    raw = packet.get("effective_action")
    if raw is None and isinstance(packet.get("decision"), Mapping):
        raw = packet["decision"].get("effective_action")
    value = _clean_id(raw).lower()
    aliases = {
        "buy_rebalance": "rebalance",
        "sell_rebalance": "rebalance",
        "sell_partial": "reduce",
        "sell_full": "sell",
        "blocked": "no_action",
        "skipped": "no_action",
    }
    value = aliases.get(value, value)
    return value if value in VALID_EFFECTIVE_ACTIONS else ""


def _validate_decision_semantics(packet: Mapping[str, Any], reason_codes: Sequence[str]) -> None:
    scope = _clean_code(packet.get("decision_scope") or packet.get("thesis_scope"))
    emitted = set(reason_codes)
    forbidden: set[tuple[str, str]] = {
        ("rebalance", "bearish_thesis"),
        ("rebalance", "momentum_sell"),
        ("constraint_block", "momentum_sell"),
    }
    conflicts = sorted(code for expected_scope, code in forbidden if scope == expected_scope and code in emitted)
    if conflicts:
        raise ValueError(f"invalid semantic mix for scope={scope!r}: {conflicts}")


def _validate_no_unsupported_ticker_expansions(
    packet: Mapping[str, Any],
    texts: Sequence[str],
) -> None:
    tickers = _packet_tickers(packet)
    if not tickers:
        return
    packet_text = json.dumps(_json_safe(dict(packet)), ensure_ascii=False).lower()
    for text in texts:
        for match in re.finditer(r"\b([A-Z][A-Z0-9._-]{1,12})\s*\(([^)]{2,90})\)", str(text or "")):
            ticker = match.group(1).upper()
            expansion = match.group(2).strip()
            if re.match(r"^[+\-$0-9.,%\s]+$", expansion):
                continue
            if ticker in tickers and expansion.lower() not in packet_text:
                raise ValueError(
                    f"unsupported ticker expansion for {ticker}: {expansion!r}"
                )


def _packet_tickers(packet: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    raw_ticker = str(packet.get("ticker") or "").upper().strip()
    if raw_ticker:
        out.add(raw_ticker)
    for item in packet.get("evidence_items") or []:
        if not isinstance(item, Mapping):
            continue
        ticker = str(item.get("ticker") or "").upper().strip()
        if ticker:
            out.add(ticker)
        fact_id = str(item.get("fact_id") or "")
        match = re.match(r"position\.([A-Za-z0-9._-]+)\.", fact_id)
        if match:
            out.add(match.group(1).upper())
    return out


def _extract_json_object(text: str) -> dict[str, Any]:
    content = str(text or "").strip()
    if not content:
        raise ValueError("empty LLM response")
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", content):
        try:
            parsed, _ = decoder.raw_decode(content[match.start():])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("no JSON object in LLM response")


def _text_list(value: Any, *, max_items: int, item_limit: int = 120) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("expected a list of strings")
    out: list[str] = []
    for item in value[:max_items]:
        text = _clean_text(item, limit=item_limit)
        if text:
            out.append(text)
    return out


def _clean_text(value: Any, *, limit: int) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:limit]


def _clean_id(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"[^A-Za-z0-9_.:-]", "", text)[:120]


def _clean_code(value: Any) -> str:
    return _clean_id(value).lower()


def _ratio_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    if parsed > 1.0 and parsed <= 100.0:
        parsed = parsed / 100.0
    return max(0.0, min(1.0, parsed))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (str, int, bool)):
        return value
    return str(value)
