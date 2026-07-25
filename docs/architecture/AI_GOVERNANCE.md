# Gobernanza de IA en Quantia

## Principio

Quantia debe usar IA como capa de lectura, critica, extraccion y explicacion. La IA no debe ser autoridad final sobre pesos, restricciones, ejecucion ni capital. El motor cuantitativo, el optimizer, el planner y las politicas de riesgo siguen siendo deterministas y auditables.

## Estado actual

| Componente | Uso actual de IA | Estado de gobierno |
|---|---|---|
| `src/analysis/nlp_scorer.py` | Scoring de noticias/sentiment con heuristica/Ollama | Parcial: hay persistencia, pero falta versionado formal de prompt/schema |
| `src/analysis/synthesis.py` | Enriquecimiento local de sintesis y explicacion | Parcial: el LLM ayuda a explicar, pero falta evidence contract estricto |
| `src/analysis/shadow_causal.py` | Analisis causal shadow evidence-bound | Mas avanzado: tiene `PROMPT_VERSION`, schema y temperatura controlada |
| `sentiment_raw/scored/aggregated` | Cadena de evidencia textual | Buena base, falta normalizar evidence IDs hacia decisiones live |
| `decision_log.layers` | Payload de capas y razones | Util, pero no garantiza trazabilidad por afirmacion |

## Frontera permitida

Permitido:

- Extraer entidades desde noticias o reportes.
- Clasificar tono/sentiment con evidencia.
- Resumir factores que ya estan en el packet.
- Criticar una decision con base en datos congelados.
- Explicar outcomes despues de ocurridos.
- Generar preguntas de auditoria o alertas.
- Proponer hipotesis para Strategy Lab.

Prohibido:

- Definir target weights.
- Reemplazar optimizer.
- Saltar guards de riesgo.
- Ejecutar ordenes.
- Fabricar facts sin fuente.
- Usar datos posteriores al `as_of`.
- Cambiar thresholds live por texto generado.
- Mezclar explicacion con decision operativa sin version y aprobacion.

## Pipeline recomendado

```mermaid
flowchart LR
    evidence["Structured Evidence<br/>snapshots, prices, news, fills"] --> extractor["LLM Extractor"]
    extractor --> validator1["Schema Validator"]
    validator1 --> quant["Quant Engine<br/>deterministic"]
    quant --> critic["LLM Critic<br/>shadow only"]
    quant --> explainer["LLM Explainer"]
    critic --> validator2["Evidence Validator"]
    explainer --> validator2
    validator2 --> audit["Audit Store"]
    audit --> monitor["Monitor / Telegram / Reports"]
```

## Evidence packet

Toda llamada LLM deberia recibir un paquete con IDs, no texto suelto.

```json
{
  "packet_id": "evidence_packet:2026-07-22:run-id",
  "as_of": "2026-07-22T13:00:00Z",
  "market_snapshot_id": "market:...",
  "portfolio_snapshot_id": "portfolio:...",
  "feature_snapshot_id": "features:...",
  "decision_log_ids": [123, 124],
  "sentiment_raw_ids": [77, 78],
  "sentiment_scored_ids": [91],
  "broker_fill_ids": [],
  "allowed_claims": ["price", "trend", "risk", "sentiment", "cost", "outcome"],
  "forbidden": ["future_data", "trade_execution", "target_weight_override"]
}
```

## Salida estructurada

Ejemplo de contrato:

```json
{
  "schema_version": "ai_explanation_v1",
  "prompt_version": "decision_explainer_v1",
  "model": "local-model-name",
  "temperature": 0.0,
  "decision_id": "decision:...",
  "claims": [
    {
      "claim": "El bloqueo se explica por riesgo elevado y delta chico contra costos.",
      "claim_type": "risk",
      "evidence_ids": ["decision_log:123", "planner_guard:buy_guard_v1"],
      "confidence": 0.74
    }
  ],
  "unknowns": ["No hay order_id persistido para comparar plan contra fill."],
  "verdict": "audit_only"
}
```

## Validacion

Validaciones minimas antes de persistir:

- JSON parseable.
- `schema_version` esperado.
- `prompt_version` presente.
- `model` presente.
- Todas las afirmaciones tienen al menos un `evidence_id`.
- Ningun `evidence_id` apunta a datos posteriores al `as_of`.
- El output no contiene campos prohibidos como `target_weight_override` o `execute_order`.
- Si falta evidencia, el modelo debe responder `unknown`, no inferir.

Pydantic puede ser una buena opcion para estos contratos si el proyecto acepta la dependencia. Si no, se puede empezar con dataclasses, `jsonschema` o validadores locales pequenos.

## Versionado

Cada output de IA debe guardar:

- `prompt_version`
- `schema_version`
- `model`
- `temperature`
- `max_tokens` o equivalente
- `retry_count`
- `input_hash`
- `output_hash`
- `created_at`
- `as_of`
- `evidence_packet_id`

Shadow causal ya va en esta direccion. La prioridad es llevar esa disciplina a `nlp_scorer.py` y `synthesis.py`.

## Fallbacks

La falla de IA no debe bloquear el ciclo productivo salvo que el componente sea estrictamente requerido para una alerta informativa.

Politica recomendada:

- Sentiment: si LLM falla, usar heuristica y marcar `scorer='heuristic_fallback'`.
- Synthesis explanation: si LLM falla, usar explicacion deterministica de capas.
- Critic: si LLM falla, omitir critic y guardar `status='skipped'`.
- Causal post-mortem: si LLM falla, reintentar fuera del ciclo live.

## Cache

Cachear por `input_hash + prompt_version + model + schema_version`.

Beneficios:

- Reproducibilidad.
- Menos latencia.
- Menor costo.
- Comparacion limpia ante cambios de prompt/modelo.

No cachear si el input incluye timestamps relativos sin normalizar o datos no ordenados deterministicamente.

## Riesgos y controles

| Riesgo | Control |
|---|---|
| Hallucination | Evidence IDs obligatorios y claims validados |
| Prompt drift | `prompt_version` persistido |
| Modelo cambia sin aviso | `model` y hash de output |
| Latencia live | Fallback deterministico |
| Mezcla decision/explicacion | Frontera de campos prohibidos |
| Look-ahead | `as_of` y validacion temporal |
| Explicaciones demasiado persuasivas | Separar verdict audit-only de decision |
| Costos crecientes | Cache y limites de uso |

## Tablas sugeridas

```sql
CREATE TABLE ai_evidence_packets (
    packet_id TEXT PRIMARY KEY,
    as_of TIMESTAMPTZ NOT NULL,
    run_id TEXT,
    payload JSONB NOT NULL,
    input_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ai_explanations (
    id BIGSERIAL PRIMARY KEY,
    packet_id TEXT NOT NULL REFERENCES ai_evidence_packets(packet_id),
    decision_id TEXT,
    prompt_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    model TEXT NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    output JSONB NOT NULL,
    output_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Orden recomendado

1. Versionar prompt/schema/model en sentiment y synthesis.
2. Crear `EvidencePacket` dataclass.
3. Validar outputs de `shadow_causal.py` con helper compartido.
4. Persistir explicaciones live solo como `audit_only`.
5. Recien despues exponerlas en monitor/Telegram.

## Decision arquitectonica

La IA debe quedar subordinada a evidencia y contratos. Quantia mejora si el LLM ayuda a leer mejor el mercado y explicar mejor los resultados; empeora si se convierte en una segunda autoridad opaca de decision.
