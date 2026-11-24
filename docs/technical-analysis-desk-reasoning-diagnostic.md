**Resultado: JSON válido; Elliott termina con abstención explícita en D y W.**

El ajuste desactiva razonamiento en este transporte experimental y explicita reglas existentes en el prompt de Elliott. La última respuesta pasó `parse_role_output` y la validación del paquete: `finish_reason=stop`, 307 tokens de salida, 0 de razonamiento y `usage.cost=0`. Sigue siendo `SHADOW_ONLY`; no se ejecutaron moderador ni piloto. Fecha de las pruebas: 23/11/2026, 21:16–21:23 de Argentina (24/11 UTC).

**Diagnóstico de los ocho originales**

Los ocho HTTP 200 tenían JSON externo válido, pero `message.content=null`, `refusal=null` y `finish_reason=native_finish_reason=length`. En todos, `completion_tokens=reasoning_tokens=max_tokens`: el razonamiento agotó la salida antes del JSON del rol. No hubo pérdida de contenido por el parser ni evidencia de una negativa del modelo. [Auditoría original](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-v1/original-failures-audit.json).

| Rol; dos intentos | Entrada por intento | Salida = razonamiento por intento | Costo informado, ambos |
|---|---:|---:|---:|
| Elliott | 89.768 | 4.096 | USD 0 |
| Wyckoff | 33.951 | 4.096 | USD 0 |
| Estructura | 17.479 | 3.072 | USD 0 |
| Momentum | 3.074 | 3.072 | USD 0 |

El manifiesto original tenía `reasoning=null`; el constructor omitía ese parámetro. `max_tokens` era el límite total, sin reserva separada para contenido final. El contexto anunciado era 1.000.000 y el máximo de salida del endpoint 131.072: los topes de 3.072/4.096 procedían del cliente. El schema estricto sí se enviaba. Código: [constructor de petición](../src/technical_desk/desk_transport.py#L87), [control de razonamiento](../src/technical_desk/free_smoke_transport.py#L38), [lectura y validación del contenido](../scripts/run_technical_desk_free_smoke.py#L257).

OpenRouter documenta que razonamiento y contenido comparten `max_tokens`; ocultar razonamiento con `exclude` no ahorra ese presupuesto. El [modelo gratuito](https://openrouter.ai/nvidia/nemotron-3.5-super-vl:free) lo activa por defecto y admite desactivarlo. La ausencia de `supported_efforts` no significaba que fuera obligatorio: el catálogo decía `mandatory=false`. [Documentación de razonamiento](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens). NVIDIA documenta `none/low/high`, con `high` predeterminado; la traducción de OpenRouter no se dio por supuesta: se comprobó con las llamadas. [API NVIDIA](https://docs.api.nvidia.com/nim/reference/nvidia-nemotron-3_5-super-vl-120b-a12b-infer).

**Cambios mínimos y controles**

- [free_smoke_transport.py:38](../src/technical_desk/free_smoke_transport.py#L38): acepta exactamente `reasoning={"enabled":false}`; rechaza `0`, cadenas y campos adicionales. Conserva modelo/proveedor fijados, schema, temperatura, topes, guard de fuga y precio máximo cero. No usa `exclude` para simular ahorro.
- [compact_prompts.py:229](../src/technical_desk/compact_prompts.py#L229): Elliott pasa a prompt **v2.3**. Explicita cohorte/volumen, copia exacta de identidad, un solo PRIMARY, abstención cuando no hay escenarios y ancla causal `origin`. Son reglas ya existentes del contrato; no se normaliza ni repara la respuesta después de recibirla.
- [diagnose_technical_desk_reasoning.py:81](../scripts/diagnose_technical_desk_reasoning.py#L81): experimentos acotados, preregistro con hash, intención persistida antes del envío, catálogo fresco antes de cada POST y ledger local. Dos controles de formato, luego Elliott; dos seguimientos separados de un solo Elliott. Sin reintentos automáticos ni moderador. Los controles sintéticos no se almacenan como hipótesis de mercado.

El paquete AMD mantuvo digest `ccf95456530b388513c75719e34aa3becd4ada5c37b4efc2aaae21f0fcdb1a3d`. En los tres ensayos Elliott, datos, schema, proveedor, temperatura y máximo 4.096 fueron idénticos; solo evolucionaron las instrucciones del sistema. Los dos controles mínimos diferían exclusivamente en `reasoning.enabled=false`.

**Cinco llamadas nuevas: resultados completos, incluidos los rechazos**

| Prueba | Entrada / cota | Salida / máximo | Razonamiento | finish_reason | Resultado del contrato | Latencia |
|---|---:|---:|---:|---|---|---:|
| JSON mínimo, configuración anterior | 294 / 6.666 | 512 / 512 | 361 | length | JSON truncado; quedan 151 tokens visibles | 4,663 s |
| Mismo mínimo, razonamiento apagado | 294 / 6.696 | 282 / 512 | 0 | stop | Copia sintética exacta y válida | 2,606 s |
| Elliott, prompt v2.1 | 89.768 / 190.161 | 364 / 4.096 | 0 | stop | JSON completo, rechazo por cohorte incorrecta | 8,636 s |
| Elliott, prompt v2.2 | 89.789 / 190.236 | 3.005 / 4.096 | 0 | stop | JSON completo, múltiples PRIMARY y VALID vacío | 27,326 s |
| Elliott, prompt v2.3 | 89.912 / 190.816 | 307 / 4.096 | 0 | stop | **ABSTAIN válido en D y W** | 8,113 s |

Las cotas son el techo conservador de un token por byte más overhead. Las cinco respuestas reportaron modelo exacto `nvidia/nemotron-3.5-super-vl:free`, proveedor `Nvidia`, HTTP 200 y costo 0. El endpoint solicitado fue `nvidia`; el resuelto continúa desconocido porque no aparece en la respuesta. No hubo 429 ni fallback. La identidad corta del paquete era correcta: `market-ccf95456530b388513c75719`; el primer Elliott había inventado una identidad con el hash completo, además de usar la cohorte de Wyckoff.

Totales nuevos: **270.057 tokens de entrada, 4.470 de salida, incluidos 361 de razonamiento**, caché leída/escrita 0 reportada. Costo API: **USD 0**. Hipotético a tarifa paga congelada: **USD 0,04260945**, sin descuento de caché; fuente: catálogo del 23/11/2026 02:58:47 UTC, USD 0,15/M entrada y 0,47/M salida. No es facturación. Los ocho originales habían consumido 288.544 de entrada y 28.672 de salida/razonamiento, con costo API 0 e hipotético USD 0,05675744.

**Límite del resultado Elliott**

La respuesta final dice `NO_IMPULSE_CANDIDATE`, incertidumbre `HIGH`, en D y W, sin escenarios ni anclas citadas. Pasa el contrato como abstención; no valida un conteo ni demuestra ausencia de patrones. El paquete contiene 2 ventanas diarias compatibles en P2 y 1 en P5; semanal tiene 0 compatibles (3 ventanas P2 y 1 P5 rechazadas). La explicación del modelo no distingue esos candidatos históricos de un escenario actual. La selección de conteos y la calidad analítica siguen sin demostrarse; apagar razonamiento resolvió el formato observado, no prueba equivalencia analítica ni estabilidad entre réplicas.

**Verificación y evidencia**

- `python -m pytest tests/test_technical_desk_free_smoke_transport.py -q` → **52 passed, 24,58 s**; HTTP local, serialización y rechazo de parámetros inválidos.
- `python -B -m pytest -q -p no:cacheprovider --tb=short tests/test_technical_desk_reasoning_diagnostic.py tests/test_technical_desk_free_smoke.py` → **25 passed, 13,17 s**, antes de los seguimientos.
- `python -B -m pytest -q -p no:cacheprovider --tb=short tests/test_technical_desk_reasoning_diagnostic.py tests/test_technical_desk_compact_prompts.py tests/test_technical_desk_prompts.py tests/test_technical_desk_compact_dry_run.py` → **43 passed, 4,25 s**, sobre el ajuste final; incluye guards de red y fuga. Los comandos comparten tests; no se suman como ejecuciones independientes.
- `python -B -m py_compile scripts/diagnose_technical_desk_reasoning.py src/technical_desk/compact_prompts.py tests/test_technical_desk_reasoning_diagnostic.py` y compilación del transporte/tests → PASS. `git diff --check` y revisión de whitespace de archivos nuevos → PASS.
- SHA-256: **9/9 archivos protegidos intactos**, incluidos contratos v2/v3/v3.1, role-output-v1 y transporte genérico. Auditoría SQLite con `mode=ro`: tres cadenas válidas, 19 eventos, cinco intentos y una salida Elliott válida. Los 13 cuerpos originales coinciden con sus hashes; escaneo literal de la clave: 0 coincidencias en 47 archivos revisados.

Preregistros conservados: [v1, tres llamadas](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-v1/plan.sha256.json), [v2, un Elliott](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-v2/plan.sha256.json), [v3, un Elliott](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-v3/plan.sha256.json). [Auditoría y consumo por intento](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-review/audit.json), [13 respuestas completas con índice SHA-256](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-review/original-and-diagnostic-responses.zip), [respuesta final Elliott](../output/technical-desk-stage1b1/free-smoke/reasoning-diagnostic-v3/execution/elliott_disabled.response.raw.json).

Sin commit, push, escrituras productivas ni cambios de credenciales. El alias autorizado se usó únicamente en memoria. No se inició el piloto.
