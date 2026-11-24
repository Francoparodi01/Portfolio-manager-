**QUANTIA — Etapa 1b.1 — 2026-11-23**

Las dos selecciones cumplen presupuesto y contexto en el cálculo offline. Ejecución: `NOT_STARTED`, `SHADOW_ONLY`; cero llamadas a modelos, sin smoke, escrituras de base, commit ni push. Evidencia: `output/technical-desk-stage1b1/verified-v2.1/summary.json` y `validation.json` en su directorio padre.

**Proyección y tamaño.** Tablas `columns/rows`, precios al tick con `ROUND_HALF_EVEN`, indicadores a seis decimales y RSI/volumen relativo a cuatro, exclusivamente en la vista. Elliott recibe ambas escalas, Fibonacci y resultados de reglas; Wyckoff volumen relativo D, estructura rangos/rupturas y Momentum sus indicadores. El moderador recibe cuatro hipótesis validadas de la misma réplica, evaluaciones y solo los pivotes citados; no recibe velas ni features crudos (`src/technical_desk/compact_prompts.py:32`, `:111`, `:148`). Las anclas conservan `P2|P5:D|W:sha256` y deben existir tanto en la proyección como en el contexto completo (`:99`).

Preregistro final: `technical-desk-projection-v2.1`, **2026-11-23T06:55:22.530888Z**, SHA-256 `ca6a6bbf03ba72ac43e929ee81a1a44cebf50f34b72d99a56b7aeb974cb92f81`; cálculo posterior a las 06:55:48Z. Especificación: `docs/technical-desk-stage1b1-projection-v2.1.json:2`; registro: `output/technical-desk-stage1b1/projection-preregistration-v2.1.json`. La primera proyección v2 excedió el contexto de Elliott: quedó conservada en `verified/`. Se preregistró v2.1 antes de recalcular, eliminando de la vista los detalles internos de las reglas y conservando identificador, estado y motivo de todas ellas; evidencia y validador mantienen precisión completa (`compact_prompts.py:93`).

Bytes UTF-8 máximos de `messages` entre las diez peticiones de cada rol, antes → después:

| Rol | Antes | Después |
|---|---:|---:|
| Elliott | 566.671 | 204.102 |
| Wyckoff | 182.659 | 50.857 |
| Estructura | 182.552 | 25.080 |
| Momentum | 182.563 | 5.893 |
| Moderador | 569.938 | 2.969 |

El moderador usa abstenciones ficticias en esta medición; **no representa el tamaño de respuestas reales**. Su contenido JSON, contando el escape dentro del cuerpo HTTP, tiene un tope preregistrado de 65.536 bytes; excederlo bloquea sin truncar. La reserva usa ese máximo (`compact_prompts.py:210`, `src/technical_desk/compact_dry_run.py:27`).

**Transporte y tarifa.** El Desk genera `response_format.json_schema.strict=true` desde una copia de `role-output-v1`, exige campos explícitos y elimina defaults solo del schema de transporte; los objetivos vacíos se expresan con `[]`. El schema no se duplica en mensajes. Modelo y endpoint salen del manifiesto; `provider={only:[endpoint],allow_fallbacks:false,require_parameters:true}` y cualquier discrepancia aborta (`src/technical_desk/desk_transport.py:29`, `:68`). El guard examina el cuerpo final y JSON anidado justo antes del envío local (`:98`, `:241`).

Se registran modelo/proveedor resueltos, tokens de entrada/salida/razonamiento/lectura y escritura de caché, `usage.cost` y `cost_details`; ausencias quedan en `None`. Si falta identidad, se consulta el id de generación; identidad incompatible o irresoluble aborta (`desk_transport.py:176`, `:244`). Referencias verificadas: [structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs), [usage accounting](https://openrouter.ai/docs/cookbook/administration/usage-accounting) y [generation metadata](https://openrouter.ai/docs/api/api-reference/generations/get-request-&-usage-metadata-for-a-generation).

El cargador verifica SHA-256, fuente pública, modelo/endpoint, parámetros y vigencia **captura + 24 h**. Sin evidencia vigente devuelve `NOT_STARTED`; razonamiento se presupuesta como salida. Reserva caché a precio completo de entrada, sin descuentos; tarifas no publicadas permanecen `UNKNOWN`. Cobertura limitada a texto, sin herramientas, medios ni escrituras explícitas de caché (`src/technical_desk/endpoint_pricing.py:73`, `:137`, `:154`). Las capturas usadas vencen el **24/11/2026 a las 02:59:27 UTC** (fracción exacta por modelo en el manifiesto).

**Reservas calculadas, no costos observados.** Entrada: **1 token por byte UTF-8 del cuerpo HTTP + 1.024**, como techo conservador, no medición del tokenizer; moderador reserva el espacio hasta su tope. Salida: Elliott/Wyckoff/Moderador 4.096; Estructura/Momentum 3.072. Piloto v3.1 intacto: 50 llamadas base y reserva de un reintento por llamada, usando el máximo por rol entre paquetes. Smoke: AMD, cinco roles, una réplica, cero reintentos; sin debate (`compact_dry_run.py:27`, `:89`; `endpoint_pricing.py:178`).

| Modelo / endpoint | USD entrada/salida por millón | Piloto / tope 2 | Smoke / tope 0,50 | Máxima entrada + salida / contexto |
|---|---:|---:|---:|---:|
| `nvidia/nemotron-3-super-120b-a12b` / `deepinfra/bf16` | 0,075 / 0,40 | 0,7187985 | 0,034396050 | 209.815 + 4.096 ≤ 262.144 |
| `nvidia/nemotron-3-nano-30b-a3b` / `nebius/fp8` | 0,06 / 0,24 | 0,5455056 | 0,02604018 | 209.808 + 4.096 ≤ 262.144 |

Las 50 peticiones de cada selección cumplen contexto y límites separados del endpoint (`compact_dry_run.py:77`). Nebius publica caché de lectura a USD 0,012/M; no se aplicó ese descuento.

**Validación.** Comandos desde el worktree de 0b/0c:

- `$deskTests = @(rg --files tests -g 'test_technical_desk*.py'); python -B -m pytest -q -p no:cacheprovider --tb=short @deskTests tests/test_contextual_g3.py` → **469 passed, 101,89 s**. Incluye anclas/fuga, precisión original, 50 peticiones, precios caducados, contexto y HTTP falso con sockets restringidos a loopback. Salida: `output/technical-desk-stage1b1/tests-final.log`.
- `python -B scripts/build_technical_desk_stage1b1.py --selection output/technical-desk-stage1b1/model-selection.json --preregistration output/technical-desk-stage1b1/projection-preregistration-v2.1.json --output output/technical-desk-stage1b1/verified-v2.1` → **exit 0**, dos selecciones factibles; guard de red/SQLite/subprocesos activo (`scripts/build_technical_desk_stage1b1.py:15`).
- `python -m py_compile src/technical_desk/compact_prompts.py src/technical_desk/compact_dry_run.py src/technical_desk/desk_transport.py src/technical_desk/endpoint_pricing.py scripts/build_technical_desk_stage1b1.py tests/test_technical_desk_compact_prompts.py tests/test_technical_desk_compact_dry_run.py tests/test_technical_desk_desk_transport.py tests/test_technical_desk_endpoint_pricing.py` → **9 archivos PASS**; `py-compile.log`.
- `git diff --check` y `git diff --no-index --check -- NUL <archivo nuevo>` → **PASS**; `diff-check.json`. Auditoría de artefactos: **100 hashes/guards, cinco fuentes y nueve archivos protegidos PASS**, incluyendo v2/v3/v3.1, role-output-v1 y transporte genérico; `validation.json` y `preserved-sha256.json`.

**Limitaciones.** No se verificaron inferencia real, calidad, tokens reales ni facturación. El transporte habilitado solo acepta el servidor HTTP local de pruebas; activar tráfico real queda pendiente. OpenRouter documenta modelo y proveedor, pero no certifica el tag de endpoint/cuantización en su respuesta: `resolved_endpoint=None` (`desk_transport.py:214`, `:224`). Las hipótesis sin coincidencia exacta con un candidato Elliott conservan `NOT_EVALUABLE`; no se inventa validación (`compact_prompts.py:139`). Ninguna de estas comprobaciones autoriza iniciar el smoke.
