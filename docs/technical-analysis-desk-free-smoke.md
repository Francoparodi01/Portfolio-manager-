**QUANTIA — Smoke gratuito / COMPLETED_WITH_FAILURES**

AMD, una réplica, sin debate: **cuatro especialistas, dos intentos cada uno; ocho respuestas inválidas y ninguna hipótesis validada**. Todos agotaron la salida en razonamiento y devolvieron `content=null`. El moderador quedó `NOT_STARTED` por falta de las cuatro hipótesis válidas. Los ocho intentos reportaron `usage.cost=0`. Se usó el alias autorizado `openrouter` como `OPENROUTER_API_KEY` exclusivamente en memoria, sin modificar el `.env`.

Los criterios se registraron antes de leer candidatos, a las **2026-11-23T15:19:38.524749+00:00**. SHA-256: `c9782b60985f8fd1ff880e7af754e510052a89c85ef8e6668042ca2b8b13002b`. Contexto mínimo: **267.389** = ceil(1,25 × (209.815 + 4.096)), tomando el mayor rol del dry-run 1b.1 (Elliott/NVDA); no se sustituyó por la cota menor de AMD. Desempate: mayor contexto, luego id alfabético. Fuente: captura pública del catálogo del **23/11/2026, 02:58:47 UTC**; 476 modelos, diez con prefijo nvidia/nemotron.

| Id (prefijo `nvidia/`) | Contexto | Schema requerido | Entrada/salida cero | Resultado |
|---|---:|---|---|---|
| `nemotron-3-nano-30b-a3b` | 262.144 | Sí | No | id sin :free; contexto insuficiente; precio no cero |
| `nemotron-3-nano-omni-30b-a3b-reasoning:free` | 256.000 | No | Sí | faltan response_format/structured_outputs; contexto insuficiente |
| `nemotron-3-super-120b-a12b` | 262.144 | Sí | No | id sin :free; contexto insuficiente; precio no cero |
| `nemotron-3-ultra-550b-a55b` | 262.144 | Sí | No | id sin :free; contexto insuficiente; precio no cero |
| `nemotron-3-ultra-550b-a55b:free` | 1.000.000 | No | Sí | faltan response_format/structured_outputs |
| `nemotron-3.5-content-safety` | 131.072 | No | No | id sin :free; faltan response_format/structured_outputs; contexto insuficiente; precio no cero |
| `nemotron-3.5-lightning` | 1.048.576 | Sí | No | id sin :free; precio no cero |
| `nemotron-3.5-lightning:free` | 1.000.000 | No | Sí | faltan response_format/structured_outputs |
| `nemotron-3.5-super-vl` | 1.048.576 | Sí | No | id sin :free; precio no cero |
| `nemotron-3.5-super-vl:free` | 1.000.000 | Sí | Sí | PASA |

El único candidato apto tuvo una GET pública sin clave a `/api/v1/models/nvidia/nemotron-3.5-super-vl:free/endpoints`, **200**, 2026-11-23T15:21:36.230145+00:00. Un endpoint: `nvidia`, proveedor `Nvidia`, contexto 1.000.000, entrada máxima 868.928, salida máxima 131.072, precio entrada/salida 0. Captura SHA-256: `1b9428ddf642cde4103669d1ac87729b980d61d1d4df361e5ab89c69a0ead030`.

Manifiesto congelado: SHA-256 `2de850080e367191b4de6793d4ad9861ff385ab8e66828bf75a5e3f02d73e092`. Temperatura 0; salida Elliott/Wyckoff/Moderador 4.096 y Estructura/Momentum 3.072; sin debate ni fallbacks. Se exige schema estricto y `provider.max_price={prompt:0,completion:0,request:0}`. No se envía esfuerzo bajo porque el catálogo no expone `supported_efforts`; se conserva `LOW_EFFORT_NOT_EXPOSED_BY_CATALOG`. Referencias: [precio máximo y pin de proveedor](https://openrouter.ai/docs/guides/routing/provider-selection), [opciones de razonamiento](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens).

**Fase B**

Tokens por intento: entrada y salida fueron iguales en ambos intentos de cada rol; **toda la salida se registró como razonamiento**. La cota de entrada es el techo conservador de un token por byte. Caché y latencias se muestran como intento 1 → 2.

| Rol | Resultado | finish_reason | Entrada real / cota | Salida real / máximo | Caché leída | Latencia (s) |
|---|---|---|---:|---:|---:|---:|
| Elliott | Inválida × 2 | length × 2 | 89.768 / 190.131 | 4.096 / 4.096 | 0 → 86.720 | 36,934 → 40,103 |
| Wyckoff | Inválida × 2 | length × 2 | 33.951 / 56.202 | 4.096 / 4.096 | 0 → 30.352 | 36,247 → 28,776 |
| Estructura | Inválida × 2 | length × 2 | 17.479 / 30.422 | 3.072 / 3.072 | 0 → 17.344 | 30,942 → 25,857 |
| Momentum | Inválida × 2 | length × 2 | 3.074 / 11.620 | 3.072 / 3.072 | 0 → 0 | 31,575 → 25,220 |
| Moderador | NOT_STARTED | Sin observación | Sin observación | Sin observación | Sin observación | Sin observación |

Antes del primer POST, la GET pública del catálogo confirmó nuevamente modelo `:free`, precio de entrada/salida 0, parámetros y contexto: **2026-11-23T18:52:29.889422+00:00**, HTTP 200, SHA-256 `65da3db6057d467a24cecd0f5348e0934bebae40092d2c697e9eb2e42fe82ada`. El envío exige evidencia de menos de 60 segundos. En las ocho respuestas: HTTP 200, modelo exacto `nvidia/nemotron-3.5-super-vl:free`, proveedor `Nvidia`. El endpoint solicitado fue `nvidia`; **endpoint resuelto desconocido**, porque la respuesta no lo expone. Se mantuvieron el pin, `max_price=0` y la prohibición de fallback.

Se realizó únicamente el reintento autorizado por JSON inválido, con cuerpo idéntico y cinco segundos de espera por rol. No hubo 429 ni cambios de prompt/parámetros. Totales: **288.544 tokens de entrada, 28.672 de salida/razonamiento y 134.416 de caché leída**; caché escrita 0 según la API. `usage.cost` y los campos de costo del proveedor reportaron 0 en todos los intentos; no es una conciliación de factura. Costo hipotético a tarifa paga: **USD 0,05675744**, usando USD 0,15/M de entrada y 0,47/M de salida del mismo modelo sin `:free`, catálogo del 23/11/2026 02:58:47 UTC. Es una estimación superior sin descuento de caché, no gasto facturado.

**Validador:** `parse_role_output`, anclas y conteos de Elliott quedaron **NOT_EVALUABLE**: faltó el contenido JSON sobre el cual ejecutarlos. Cero respuestas válidas, cero abstenciones explícitas; no puede inferirse que no existan impulsos candidatos. El moderador no recibió respuestas incompletas ni tuvo una llamada fabricada para completar cinco roles.

**Respuestas completas:** [ZIP con los ocho cuerpos HTTP originales](../output/technical-desk-stage1b1/free-smoke/responses-complete.zip), [índice con SHA-256 por respuesta](../output/technical-desk-stage1b1/free-smoke/responses-index.json) y [resumen de ejecución](../output/technical-desk-stage1b1/free-smoke/execution-2/summary.json). No existen cinco respuestas de roles distintos: existen cuatro roles con su primer intento y reintento, preservados íntegramente. El archivo incluye esta limitación.

**Controles ejecutados**

- `python -B -m pytest -q -p no:cacheprovider --tb=short tests/test_technical_desk_free_smoke_transport.py` → **38 passed, 17,19 s**. HTTP falso; guard de gasto/fuga, identidad, respuesta/caché desconocida, 429 y redirects.
- `python -B -m pytest -q -p no:cacheprovider --tb=short tests/test_technical_desk_free_smoke.py` → **14 passed, 9,25 s**. Cinco roles, intención persistida antes de enviar, un reintento idéntico, espera por cabeceras, moderador dependiente de cuatro resultados válidos y conservación reversible de bytes.
- `python -B -m pytest -q -p no:cacheprovider --tb=short tests/test_technical_desk_free_smoke.py -k relative_runpy_filename` → **1 passed, 14 deselected, 1,01 s**. Regresión de la ruta relativa del launcher: **53 tests distintos aprobados** en total, sin repetir llamadas reales.
- `python -B output/technical-desk-stage1b1/free-smoke/select_phase_a.py` → **11 comprobaciones deterministas PASS**, un candidato apto. `py_compile` de los cuatro archivos nuevos y `git diff --check` → PASS; nueve SHA-256 protegidos intactos, incluyendo v2/v3/v3.1, role-output-v1 y transporte genérico.

Auditoría independiente de solo lectura: **14 controles correctos**. Ledger local: 1 run, 8 intentos, 16 evaluaciones, 0 hipótesis; cadena de 25 eventos verificada, head `b87015a9f42975e5a8dd816596aa94f14086991580d6c539d2e1870b658c85ec`. Ocho hashes de respuestas y peticiones coinciden; cuatro reintentos idénticos; nueve archivos protegidos intactos. Escaneo literal de la clave en los artefactos: ver `security-audit.json`; no se guarda ni imprime el secreto.

Una preparación anterior (`execution/`) falló por resolver la ruta del launcher antes de registrar el run: **cero POST y cero eventos**. Se corrigió y probó antes del smoke conservado en `execution-2/`; no hubo ajustes entre sus ocho llamadas.

Evidencia fuera de Git: `output/technical-desk-stage1b1/free-smoke/`, incluidos criterios, selección, capturas, manifiesto, peticiones, ledger, respuestas y auditoría final. Código: `src/technical_desk/free_smoke_transport.py:43` y `:124`; ejecutor: `scripts/run_technical_desk_free_smoke.py:205`. Limitación principal: esta combinación de modelo y máximos de salida no produjo JSON útil; el soporte anunciado de schema no garantiza una respuesta completa. Sin commit, push ni acceso a base productiva. **No se inició el piloto.**
