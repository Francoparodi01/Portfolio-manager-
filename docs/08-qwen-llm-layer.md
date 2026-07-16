# Qwen LLM Layer

## Alcance

Esta capa integra Qwen local via Ollama como generador narrativo de Quantia. No
decide operaciones, no toca thresholds, no consulta el planner y no publica por
si sola. El input es siempre un paquete cerrado de evidencia y el output es JSON
validable antes de renderizar.

## Casos v0

1. Informe diario de mercado: `MarketReportPacket -> MarketNarrative`.
2. Explicador de decisiones: `DecisionEvidencePacket -> DecisionExplanation`.

El informe prioriza lectura agregada, cobertura y caveats. El explicador prioriza
alineacion exacta con la decision real: accion efectiva, reason codes,
restricciones y fact ids que soportan cada afirmacion.

## Reglas

- Modelo base: `QUANTIA_LLM_MODEL`, default `qwen2.5:3b`.
- Runtime: Ollama en `OLLAMA_URL`.
- Temperatura: `0.0`.
- Salida: JSON schema via `format`.
- Sin DB live en el lab.
- Sin Telegram en el lab.
- Sin fallback pago.
- Si falta cobertura material, `insufficiency_flag=true`.
- Ningun `supporting_fact_id` puede referenciar un hecho ausente.
- Rebalanceo no se puede explicar como tesis bearish.
- Bloqueo por restriccion no se puede explicar como venta por momentum.

## Flujo seguro

```text
read-only marts or fixtures
  -> deterministic packet builder
  -> input hash
  -> Qwen/Ollama JSON
  -> structural validation
  -> factual validation
  -> semantic validation
  -> deterministic renderer
  -> optional publish gate
```

## Lab CLI

Solo imprime prompt o JSON validado; no persiste nada:

```powershell
python scripts\run_qwen_narrative_lab.py tests\fixtures\llm_market_report_packet.json --print-prompt
python scripts\run_qwen_narrative_lab.py tests\fixtures\llm_market_report_packet.json --model qwen2.5:3b
python scripts\run_qwen_narrative_lab.py tests\fixtures\llm_decision_evidence_packet.json --model qwen2.5:3b
```

Preview read-only contra la DB actual:

```powershell
# No llama a Ollama: packet JSON para auditoria.
python scripts\run_qwen_daily_preview.py --mode packet

# No llama a Ollama: fallback deterministico y fiel a facts.
python scripts\run_qwen_daily_preview.py --mode template

# Llama a Qwen/Ollama: usar como shadow hasta pasar evals.
python scripts\run_qwen_daily_preview.py --mode text --ollama-url http://localhost:11434
```

## Telegram

El bot expone la misma salida segura como boton `IA Preview` y comando:

```text
/ia_preview
/qwen_preview
```

Ambos ejecutan `scripts/run_qwen_daily_preview.py --mode template`, con
`--owner-chat-id` en modo multiusuario. No llaman a Ollama, no publican
operaciones, no escriben en DB y no reinician servicios.

En el estado actual, `template` es la salida segura para uso manual porque solo
renderiza statements deterministas del packet. `text` valida estructura y
fact_ids, pero Qwen2.5:3b puede mezclar interpretaciones semanticas; por eso no
debe publicarse automaticamente todavia.

## Proximo paso

Crear el builder read-only de `DecisionEvidencePacket` desde una fila de
`decision_log`, y sumar una bateria de evals con casos reales. La integracion
al scheduler debe esperar a que `text` tenga consistencia semantica aceptable;
mientras tanto, `template` puede usarse como preview manual fiel a datos.
