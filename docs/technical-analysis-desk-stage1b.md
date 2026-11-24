# Technical Analysis Desk — etapa 1b

Preparación offline cerrada, `SHADOW_ONLY`. **50 peticiones construidas; ejecución `NOT_STARTED`.** Sin llamadas a modelos, escrituras en la base productiva, commit ni push. HEAD conservado: `5e57d01578acc51a29499a0812e16756f486b402`.

**Implementación y referencias (`ruta:línea`).**

- `src/technical_desk/prompts.py:14`: cinco plantillas `technical-desk-prompt-{rol}-v1`, SHA-256 de plantilla, instrucciones y mensajes; archivos en `src/technical_desk/prompt_templates/`. Proyección permitida en `prompt_packets.py:24`; exclusión de datos privados y rechazo de contaminación en `prompts.py:22`. Volumen D relativo, W absoluto ausente; diagnósticos retrospectivos excluidos.
- `src/technical_desk/prompts.py:36`: anclas `P2:D:<hash>`, `P2:W:<hash>`, `P5:D:<hash>`, `P5:W:<hash>`, siempre confirmadas. Un escenario Elliott usa una sola escala; 5+5 se reserva a Elliott y su evaluación por el moderador. `NO_IMPULSE_CANDIDATE` admite abstención. **No cambió `role-output-v1`**. Aislamiento y cuatro respuestas del mismo paquete/réplica: `prompts.py:54`.
- `src/technical_desk/dry_dispatch.py:42`: 5 paquetes × 2 réplicas × 5 roles. Los 10 moderadores tienen cuatro dependencias cada uno; sus previews contienen abstenciones sintéticas rotuladas `fixture_only`, nunca respuestas observadas. Un envío futuro requiere renderizar respuestas reales, nueva petición completa y nuevo hash.
- `src/technical_desk/local_ledger.py:48`: SQLite local, elegido por transacciones atómicas, unicidad concurrente y triggers contra UPDATE/DELETE. `runs`, `attempts`, `hypotheses`, `evaluations` más cadena SHA-256; hash de petición completa y respuesta textual exacta. Inserción conjunta de intento e hijos, idempotencia por `request_id`, conflicto si cambia contenido y `UNRECORDED` ante fallo de persistencia (`:82`). Modelos/proveedor, parámetros, prompt, tokens y costos quedan registrados; los desconocidos son `null`, con `BILLED_COST_UNKNOWN`. Verificación de cadena: `:139`.
- `scripts/build_technical_desk_stage1b.py:14`: guardas offline y SQLite restringido a la carpeta de salida. Preregistro smoke en `:86`: AMD, cinco roles, una réplica, sin debate, USD 0,50, modelo/proveedor pendientes, sin ejecución ni reintentos automáticos autorizados.

**Cota y reserva fijadas** (`dry_dispatch.py:13` y `:32`). Entrada: un token por byte UTF-8 de la petición serializada completa + 1.024; moderador agrega `6 × 4 × 16.384` bytes para las cuatro respuestas escapadas. Es una cota de planificación que requiere verificar tokenizer/proveedor antes de enviar. Cada respuesta tiene un límite adicional de 16.384 bytes; la salida incluye razonamiento dentro del máximo.

| Rol | Entrada máxima estimada | Salida máxima |
|---|---:|---:|
| Elliott | 573.118 | 4.096 |
| Wyckoff | 189.106 | 4.096 |
| Estructura | 189.003 | 3.072 |
| Momentum | 189.012 | 3.072 |
| Moderador | 969.886 | 4.096 |

Fixture **NO REAL**: USD 0,10/M entrada, USD 0,40/M salida, extra cero. Fórmula v3.1 intacta: `5 × 2 × (1 + 1 reintento) × Σ(costos máximos por rol)`, reserva para 100 intentos. Resultado **USD 4,367706 > USD 2**: también falla la factibilidad con este fixture. No representa tarifa, consumo ni costo observado; no se ajustaron límites para hacerlo entrar. Sin modelo/tarifa verificados continúa `NOT_STARTED` incluso si otro fixture cupiera.

**Evidencia fuera de Git:** `output/technical-desk-stage1b/verified-preparation/` contiene manifiesto de prompts/fuentes/inputs, 50 peticiones completas, preregistro, resumen y `ledger.sqlite3`: **1 run, 50 intentos PLANNED, 0 hipótesis, 0 evaluaciones**. Tokens, respuestas y costos reales desconocidos. `output/technical-desk-stage1b/artifact-validation.json` verifica peticiones, aislamiento, reserva, cadena y hashes; `preserved-sha256.json` conserva los originales. **8/8 SHA-256 intactos:** implementaciones y tests de v2, v3, v3.1 y role-output-v1.

**Validación ejecutada (PowerShell):**

- `$deskTests = @(rg --files tests -g 'test_technical_desk*.py'); python -m pytest -q --tb=short @deskTests tests/test_contextual_g3.py` → **396 passed en 95,56 s**, log `output/technical-desk-stage1b/tests-final.log`. Incluye sockets bloqueados, datos privados, escalas, aislamiento, presupuesto, idempotencia concurrente, UPDATE/DELETE, rollback atómico y manipulación de cadena. Una corrida previa detectó un fixture de conflicto con metadatos incoherentes; se corrigió el fixture y se repitió la suite.
- `python scripts/build_technical_desk_stage1b.py --output output/technical-desk-stage1b/verified-preparation` → exit 0, 50 peticiones, cadena verificada, `NOT_STARTED`; log `verified-build.log` en la carpeta de etapa.
- `python -m py_compile src/technical_desk/prompts.py src/technical_desk/prompt_packets.py src/technical_desk/dry_dispatch.py src/technical_desk/local_ledger.py scripts/build_technical_desk_stage1b.py` → exit 0.
- `git diff --check` → exit 0; se revisaron además los archivos nuevos sin seguimiento mediante `git diff --no-index --check NUL <archivo>`.

**Límites:** no se validaron modelo, proveedor, tokenizer, ventana de contexto ni tarifas reales; ninguna corrida económica está habilitada. El ledger de preparación conserva registros PLANNED inmutables: un envío futuro exige una petición con configuración real y nueva identidad; no puede actualizar esos registros. Los triggers/cadena no equivalen a almacenamiento WORM frente a un administrador del archivo. Wyckoff conserva `RELATIVE_UNVERIFIED` y evaluación separada. El smoke permanece únicamente preregistrado.
