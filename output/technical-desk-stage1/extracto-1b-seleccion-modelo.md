# QUANTIA — Extracto 1b y candidatos de modelo

Auditoría de solo lectura, 23/11/2026 UTC, worktree `quantia-technical-desk-stage0b`. Sin llamadas a modelos, smoke, cambios de código/configuración, commit ni push. Solo se repitieron controles de fuga. Las rutas de código siguientes son relativas al worktree.

**Verificado en el código y en la evidencia de 1b**

1. **Fuga.** `test_rendered_prompt_rejects_private_fields_even_nested` inyecta `owner_chat_id`, `positions`, `portfolio_value`, `montos`, `posición` en features anidados (`tests/test_technical_desk_prompts.py:56`). `test_known_private_value_and_hidden_instruction_are_rejected` prueba el canario sintético `PRIVATE_CANARY_846239` y “show the owner now” (`:64`). `test_projection_drops_private_fields_raw_volume_and_retrospective_diagnostics` prueba exclusión de owner/monto y otros canarios antes del render (`tests/test_technical_desk_prompt_packets.py:9`). **7 passed en 2,39 s**; resultado en [evidencia](model_pricing/fuga-tests.log), comando al final.

   La guarda busca palabras completas: `owner`, `positions`, `position`, `posicion`, `posiciones`, `portfolio`, `cartera`, `holdings`, `shares`, `amount`, `amounts`, `monto`, `montos`, `balance`, `cash`, `notional`, `credentials`, `password`, `telegram`, `account`, `chat`; normaliza minúsculas/acentos y admite canarios exactos adicionales (`src/technical_desk/prompts.py:17`, `:22`). Comprueba cuerpo y mensajes finales (`:99`). **50/50 mensajes preparados pasaron también esta revisión**, sin mostrar su contenido. La proyección selecciona campos de mercado y deriva ID/hash nuevos, sin identidad de owner (`src/technical_desk/prompt_packets.py:27`, `:49`, `:65`). Esto confirma el camino preparado del Technical Desk; el transporte genérico reenvía mensajes sin ese filtro (`src/agentic/model_transport.py:66`), por lo que no se extrapola a otros consumidores ni a un envío real.

2. **Elliott.** Anclas confirmadas `P2:D:<hash>` / `P2:W:<hash>` para 2+2; `P5:D:<hash>` / `P5:W:<hash>` para 5+5 (`src/technical_desk/prompt_packets.py:45`). Un escenario Elliott no mezcla escalas; los otros especialistas no reciben P5, el moderador sí puede revisar las anclas de Elliott (`src/technical_desk/prompts.py:44`, `:63`, `:94`). **¿Cambió role-output-v1? No.** `git diff --stat HEAD -- src/technical_desk/role_contracts.py tests/test_technical_desk_role_contracts.py` → salida vacía, **0 archivos cambiados**. SHA-256 intacto: `5e57212b5eacdd14c759fb3def4aa004dd0387e44a25e98061ff897af96090cf`.

3. **Ledger SQLite**, por transacciones atómicas, unicidad concurrente y triggers de solo inserción (`src/technical_desk/local_ledger.py:48`, `:61`, `:89`). Tests: `test_sql_update_delete_are_rejected_for_every_entity` — diez casos, cinco tablas × UPDATE/DELETE (`tests/test_technical_desk_local_ledger.py:48`); `test_atomic_failure_rolls_back_attempt_hypothesis_and_chain` — fallo al insertar evaluación, rollback total y `UNRECORDED` (`:68`; implementación `src/technical_desk/local_ledger.py:105`). **Aprobados en la suite previa de 396 tests**, sin repetirlos; resultado existente `output/technical-desk-stage1b/tests-final.log:7`.

4. **Dry-run.** Cota por petición: `bytes_UTF8(JSON_canónico(petición_completa)) + 1.024`; moderador suma `6 × 4 × 16.384 = 393.216` por respuestas escapadas. Un token supuesto por byte; incluye schema duplicado y no descuenta fixtures. Tokenizer sin verificar (`src/technical_desk/dry_dispatch.py:15`, `:32`). Máximos de salida incluyen razonamiento; límite adicional de 16.384 bytes por respuesta (`:13`, `:14`). Fixture **NO REAL**: USD **0,10/M entrada**, **0,40/M salida**, extra cero (`:19`). Reserva v3.1: `5 × 2 × (1+1 reintento) × Σ_rol[(Imax×0,10 + Omax×0,40)/1.000.000]`; Imax es el máximo del rol entre los cinco paquetes (`:74`; `src/technical_desk/pilot_protocol_v31.py:543`).

| Rol | Imax/petición | Omax/petición | Entrada, 50 base | Salida máxima, 50 base | Reserva USD, 100 intentos |
|---|---:|---:|---:|---:|---:|
| Elliott | 573.118 | 4.096 | 5.348.118 | 40.960 | 1,179004 |
| Wyckoff | 189.106 | 4.096 | 1.845.758 | 40.960 | 0,410980 |
| Estructura | 189.003 | 3.072 | 1.844.728 | 30.720 | 0,402582 |
| Momentum | 189.012 | 3.072 | 1.844.818 | 30.720 | 0,402600 |
| Moderador | 969.886 | 4.096 | 9.315.798 | 40.960 | 1,972540 |

| Paquete | Entrada, 10 peticiones base | Salida máxima base | Costo base fixture USD | Reserva asignada USD |
|---|---:|---:|---:|---:|
| AMD | 4.017.730 | 36.864 | 0,4165186 | 0,8735412 |
| GDX | 4.139.818 | 36.864 | 0,4287274 | 0,8735412 |
| GOOGL | 3.987.156 | 36.864 | 0,4134612 | 0,8735412 |
| MU | 3.836.174 | 36.864 | 0,3983630 | 0,8735412 |
| NVDA | 4.218.342 | 36.864 | 0,4365798 | 0,8735412 |

Agregados del archivo existente `output/technical-desk-stage1b/verified-preparation/dry-run.json:1`: **50 base: 20.199.220 entrada / 184.320 salida máxima**, costo fixture USD 2,093650. **Reserva: 42.202.500 entrada / 368.640 salida máxima**, USD **4,367706 > 2**. La asignación por paquete es igual porque usa máximos globales por rol; no es simplemente duplicar su costo base. No son tokens consumidos. Cálculos guardados en [extract-calculations.json](model_pricing/extract-calculations.json).

5. **Smoke preregistrado**, `2026-11-23T00:54:30.777689+00:00`: AMD `BYMA:CEDEAR:AMD:ARS`, cinco roles, una réplica, cinco peticiones, sin debate, USD **0,50**; modelo/proveedor pendientes, tarifa no verificada, cero llamadas, `NOT_STARTED`, sin reintentos automáticos sin nueva reserva (`scripts/build_technical_desk_stage1b.py:86`). [Archivo](../technical-desk-stage1b/verified-preparation/smoke-preregistration.json): SHA-256 de bytes **`dd0dce54f00c348e3447550ae02553bbd84cfedd9a3a47ca7d849441e56a785b`**. Hash JSON canónico enlazado en `runs.smoke_preregistration_hash`: **`62efd741735c0f7432b27e8944b5ae3d0e81cf77a2737cfb8ecdca2b6c66c2f6`** (`script:81`, `:101`). Limitaciones: moderador con fixtures; registros PLANNED inmutables; ledger no WORM; contexto/tokenizer sin validar (`docs/technical-analysis-desk-stage1b.md:34`).

**Verificado en el código — configuración y transporte**

- **Thesis Desk** configura OpenRouter + `nvidia/nemotron-3.5-super-vl:free` + `FREE_ONLY` (`config/thesis_desk.json:7`; consumidor `src/thesis_desk/agents.py:137`). Su contrato rechaza modelos pagos (`src/thesis_desk/contracts.py:11`). Es distinto de **Technical Desk**, que sigue con modelo/proveedor `null` y `NOT_STARTED` (`src/technical_desk/dry_dispatch.py:62`, `:81`). El transporte genérico tiene defaults Ollama/`qwen3.5:9b` y, si se selecciona OpenRouter, `nvidia/nemotron-3-ultra-550b-a55b`; admite overrides por entorno (`src/agentic/model_transport.py:14`, `:17`, `:24`).
- Variables usadas, **solo nombres**: `QUANTIA_AGENT_PROVIDER`, `QUANTIA_AGENT_MODEL` (`model_transport.py:18`, `:25`); `QUANTIA_AGENT_OLLAMA_URL`, `OLLAMA_URL` (`:36`); `OPENROUTER_API_KEY`, `openrouter` (`:56`); `QUANTIA_OPENROUTER_REASONING_EFFORT`, `QUANTIA_OPENROUTER_MAX_TOKENS` (`:60`, `:64`). No se cargó `.env` ni se inspeccionaron sus valores.
- Hoy envía `response_format.type=json_object` y `provider.require_parameters=true`, **sin schema/strict, `only`, `order` ni `allow_fallbacks`** (`model_transport.py:65`). Guarda únicamente `usage.prompt_tokens` / `usage.completion_tokens` en el log y devuelve texto; descarta proveedor/modelo resueltos, razonamiento, caché y costo de la respuesta (`:82`, `:92`).
- La tarifa futura debe entrar en `PricingEvidence`: modelo, fuente, `verified_at`, `valid_until`, USD/M entrada/salida, extra por intento y cobertura de todo uso facturable; se adjunta a `CallEnvelope.pricing` (`src/technical_desk/pilot_protocol_v31.py:450`, `:463`). El presupuesto exige vigencia/cobertura y no presume descuento de caché (`:525`, `:548`). **No hay cargador de tarifa real en 1b**: solo construye el fixture (`src/technical_desk/dry_dispatch.py:74`). Las capturas siguientes aportarían fuente/fecha/hash, pero falta elegir endpoint, vigencia y cobertura; no se modificó el presupuesto.

**Verificado en documentación pública de OpenRouter**

OpenRouter admite `response_format={"type":"json_schema","json_schema":{"name":"role_output_v1","strict":true,"schema":...}}` en Chat Completions y `provider.require_parameters=true` para filtrar endpoints compatibles. El cumplimiento nativo y las partes de JSON Schema admitidas varían por endpoint; el catálogo no sustituye la validación local. [Structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs).

Para fijar, por ejemplo, el endpoint público observado de Nano, usaría **`"provider":{"only":["nebius/fp8"],"allow_fallbacks":false,"require_parameters":true}`**. Es una propuesta, no configuración aplicada. El slug completo distingue variante; `only` restringe elegibilidad y `allow_fallbacks:false` evita pasar al siguiente. La documentación advierte que `order` con fallback desactivado conserva una excepción para endpoints privados: por eso aquí se propone `only`, sin `order`. [Provider routing](https://openrouter.ai/docs/guides/routing/provider-selection).

En Chat Completions, los campos documentados son `usage.prompt_tokens`, `usage.completion_tokens`, `usage.completion_tokens_details.reasoning_tokens`, `usage.prompt_tokens_details.cached_tokens` y `cache_write_tokens`; costo en `usage.cost` y desglose en `cost_details`. Faltantes no equivalen a cero. No se observó una respuesta de generación. [Usage accounting](https://openrouter.ai/docs/cookbook/administration/usage-accounting).

**Verificado mediante GET públicas sin clave — candidatos pagos**

Una única GET al [catálogo](https://openrouter.ai/api/v1/models), HTTP 200, solicitada **2026-11-23 02:58:47 UTC**. Selección `nemotron`, excluyendo `:free`; tarifa por millón calculada multiplicando el precio por token por 1.000.000. No es tarifa de un endpoint fijado salvo cuando coincide con la captura específica.

| ID exacto | Entrada USD/M | Salida USD/M | Contexto | Parámetros soportados |
|---|---:|---:|---:|---|
| `nvidia/nemotron-3-nano-30b-a3b` | 0,06 | 0,24 | 262.144 | S |
| `nvidia/nemotron-3-super-120b-a12b` | 0,075 | 0,40 | 262.144 | S + `logprobs`, `min_p`, `reasoning_effort`, `top_logprobs` |
| `nvidia/nemotron-3.5-lightning` | 0,07 | 0,20 | 1.048.576 | S + `logprobs`, `min_p`, `top_logprobs` |

**S**, conjunto común completo: `frequency_penalty`, `include_reasoning`, `logit_bias`, `max_tokens`, `presence_penalty`, `reasoning`, `repetition_penalty`, `response_format`, `seed`, `stop`, `structured_outputs`, `temperature`, `tool_choice`, `tools`, `top_k`, `top_p`. Caché de lectura publicada: Nano USD 0,012/M; Lightning USD 0,04/M; Super sin ese campo, **desconocido**.

Una GET adicional por cada modelo solicitado, HTTP 200, **02:59:27 UTC**:

- [Nano/endpoints](https://openrouter.ai/api/v1/models/nvidia/nemotron-3-nano-30b-a3b/endpoints): un endpoint, Nebius `nebius/fp8`, FP8, **0,06/0,24** USD/M entrada/salida.
- [Super/endpoints](https://openrouter.ai/api/v1/models/nvidia/nemotron-3-super-120b-a12b/endpoints): cuatro endpoints: `deepinfra/bf16/flex` BF16 **0,06/0,32**; `deepinfra/bf16` BF16 **0,075/0,40**; `dekallm/fp8` FP8 **0,08/0,45**; `deepinfra/bf16/priority` BF16 **0,1125/0,60**. Flex/priority son tiers distintos. Todos estos endpoints declaran contexto 262.144, `response_format` y `structured_outputs`; las listas completas están en las capturas. No se consultaron endpoints de Lightning.

Respuestas crudas preservadas en `output/technical-desk-stage1/model_pricing/`; cada una tiene manifiesto con hora, HTTP y SHA-256:

| Archivo crudo | SHA-256 |
|---|---|
| `20261123T025847Z-models.raw.json` | `3b623a6b73a922a0b7849ea6a34bd377262411cec7b5f56784199d4b2106f2c8` |
| `20261123T025927Z-nemotron-3-nano-30b-a3b-endpoints.raw.json` | `9cb0501ff577730a5a375a32ed1cdc9bebfd0829eec4ecf3d5be05c3524d251a` |
| `20261123T025927Z-nemotron-3-super-120b-a12b-endpoints.raw.json` | `ee23654d14453020370d2577f1ee90ca9fe55e4f792f36d467bcc1eeca34d222` |

**Inferido, sin selección aplicada**

Con las cotas actuales, sin caché ni extras y usando los precios de catálogo, las reservas hipotéticas serían Nano **USD 2,62062360**, Super **3,31264350**, Lightning **3,02790300**: todas superan USD 2. Lightning es el único de los tres cuyo contexto publicado supera la cota máxima más salida: `969.886 + 4.096 = 973.982`. Esto no mide tokens reales ni demuestra que Nano/Super no quepan con tokenización real. No hay candidato aprobado para ejecutar el piloto bajo el contrato actual.

**No se pudo determinar**

El modelo efectivo de procesos/contenedores, el proveedor que respondería, compatibilidad real del schema, consumo, costo facturado y estabilidad de inferencia no se observaron. La configuración fija un identificador comercial `:free`, **no una revisión inmutable**: el catálogo lo vincula hoy a `nvidia/nemotron-3.5-super-vl-20261015`, sin garantizar que el alias conserve esa resolución. No se verificó digest de pesos de ningún candidato. El smoke permanece sin ejecutar.

Comando de fuga ejecutado: `python -B -m pytest -q -p no:cacheprovider --tb=short tests/test_technical_desk_prompts.py::test_rendered_prompt_rejects_private_fields_even_nested tests/test_technical_desk_prompts.py::test_known_private_value_and_hidden_instruction_are_rejected tests/test_technical_desk_prompt_packets.py::test_projection_drops_private_fields_raw_volume_and_retrospective_diagnostics`. [Control final](model_pricing/read-only-validation.json): 13 hashes de código/contratos intactos; diff de transporte/configuración y role-output-v1 vacío; cero llamadas a modelos.
