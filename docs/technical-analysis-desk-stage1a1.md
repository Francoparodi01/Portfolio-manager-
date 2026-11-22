**QUANTIA — Etapa 1a.1 cerrada, SHADOW_ONLY (2026-11-22).** Cinco paquetes congelados, última rueda 2026-11-13. Sin llamadas a modelos ni escrituras de base; 1b sin iniciar. La escala Elliott 5+5 fue aprobada explícitamente.

**Comparador nuevo:** `technical-desk-semantic-v3.1` / `technical-desk-pilot-v3.1`.

| Objetivos de los PRIMARY | Resultado |
|---|---|
| Ambos ausentes | `target_state=NONE`; compara confirmación/invalidación en ticks y ATR. Comparable, sin reintento. |
| Solo uno presente | `TARGET_PRESENCE_CHANGE`, estructural binario; sin dispersión de objetivos ni reintento. |
| Ambos presentes, igual cantidad | Emparejamiento ordenado y dispersión continua como v3. |
| Ambos presentes, distinta cantidad | Conserva ambos conjuntos, sin emparejarlos ni calcular distancias de objetivos. |

`adapt_scenario_to_v31` acepta objetivos ausentes; desaparece ese bloqueo del flujo nuevo. El adaptador legado conserva su restricción para v3. Alternativos completos en ledger, excluidos del ruido. Continúan 5 × 2 × 5, USD 2, sin debate y `NOT_STARTED` sin modelo/tarifa. **Cuatro SHA-256 intactos:** módulos v2/v3 y sus tests.

**Causalidad:** el mismo `assert_prefix_causal` pasó 145 recortes reales D/W. Un feature exclusivo del test lee `close[k+1]`; la ejecución sin capturar la excepción falla en k=0: `PREFIX_CAUSALITY_VIOLATION ... deliberately_broken_next_close`, exit 1. [Salida completa del fallo esperado](../output/technical-desk-stage1a1/negative-causality.log). El feature roto no entra en los paquetes.

**Elliott 2+2:** todas las ventanas de seis pivotes de la cadena alternada confirmada. Estados C/R/I/NE = RULES_COMPATIBLE/REJECTED/INCOMPLETE/NOT_EVALUABLE. “Sin solapamiento” selecciona cronológicamente intervalos cuyo inicio sea posterior al final anterior, sin compartir barra ni pivote; se calcula por separado para todas y para compatibles.

| Paquete | C/R/I/NE | Sin solapamiento: todas / C | Real C % | Control C/ventanas (C %) |
|---|---|---:|---:|---:|
| AMD D | 2/53/0/0 | 10 / 1 | 3,6364 | 2093/46780 (4,4741) |
| AMD W | 0/3/0/0 | 1 / 0 | 0 | 168/5829 (2,8821) |
| GDX D | 4/54/0/0 | 10 / 2 | 6,8966 | 1997/46959 (4,2526) |
| GDX W | 0/6/0/0 | 1 / 0 | 0 | 189/5876 (3,2165) |
| GOOGL D | 2/50/0/0 | 9 / 1 | 3,8462 | 2059/46047 (4,4715) |
| GOOGL W | 0/6/0/0 | 1 / 0 | 0 | 173/5829 (2,9679) |
| MU D | 2/47/0/0 | 9 / 2 | 4,0816 | 2035/46842 (4,3444) |
| MU W | 0/4/0/0 | 1 / 0 | 0 | 171/5732 (2,9833) |
| NVDA D | 0/57/0/0 | 10 / 0 | 0 | 2012/46556 (4,3217) |
| NVDA W | 0/7/0/0 | 2 / 0 | 0 | 154/5825 (2,6438) |

Control: **1.000 series por ticker/marco, 10.000 total**, semilla maestra **2026111601** y semilla por réplica derivada con SHA-256. Misma longitud y timestamps; sigma muestral de logretornos del propio paquete. Retornos gaussianos centrados y reescalados a esa sigma antes del tick; OHLC con mechas independientes `abs(N(0,1)) × sigma/4`, precios redondeados al tick. Mismo detector y validador, con equivalencia contra 1a verificada. La tasa del control divide totales de ventanas, no promedia porcentajes de series.

[Preregistro](../output/technical-desk-stage1a1/elliott/preregistration.json) escrito **17:54:09 UTC**, antes del cálculo; terminó 17:55:08 UTC. Hash del registro: `4c324691c37758a5996abae7377e978b7825d0e78b3f5c0f6abf318b04dac76c`. Procedimiento: `c25b7eaf7819b7672e4fe31fde1bd10478b9635d38ed1691d89752d222923376`.

Limitaciones: centrar fuerza el cierre final al inicial antes del tick (puente logarítmico, control condicionado); no representa caminatas sin esa condición. Calibración en la misma muestra, sin regímenes, saltos ni dinámica intradiaria real; W se simula independientemente de D. Desvío máximo de sigma por redondeo: **0,371% relativo**. Las ventanas solapadas no son observaciones independientes; las disjuntas tampoco garantizan independencia. La frecuencia no mide rentabilidad, predicción ni significancia; los diez contrastes son descriptivos.

**Escala secundaria Elliott 5+5**, `technical-desk-elliott-scale-5x5-v1`, registrada junto a 2+2 antes de calcular. Hash: `a2cf065010c88764d74be3e73ea1898e145bd53078ff6367a2be5627b930006a`. Mismo tratamiento de empates y validador; sin cambiar las features base ni elegir una escala ganadora. Solo datos reales; no añade otras 10.000 simulaciones.

| Ticker | Pivotes confirmados D/W | Ventanas de seis D/W |
|---|---:|---:|
| AMD | 36 / 6 | 24 / 1 |
| GDX | 35 / 4 | 24 / 0 |
| GOOGL | 31 / 9 | 24 / 2 |
| MU | 31 / 5 | 22 / 0 |
| NVDA | 38 / 6 | 29 / 0 |

**Owner coincide:** `true`.

**Volumen:** 30 fechas → nueve clusters, enlace transitivo cuando dos flags consecutivos distan ≤3 ruedas del calendario BYMA archivado; política y hashes registrados antes del cálculo. Movimiento firmado de la primera rueda: `(close[inicio] − close[rueda anterior]) / ATR14 Wilder[rueda anterior]`. Sigue fuera de las features causales.

| Ticker | Inicio → fin | Flags | Movimiento / ATR previo |
|---|---|---:|---:|
| AMD | 2025-11-21 → 2025-11-27 | 4 | −0,1741 |
| AMD | 2026-03-23 → 2026-03-27 | 4 | +0,1682 |
| AMD | 2026-10-27 → 2026-10-29 | 3 | +0,7842 |
| GDX | 2025-10-30 → 2025-11-03 | 2 | NOT_EVALUABLE |
| GDX | 2026-01-20 → 2026-01-21 | 2 | +0,5644 |
| GDX | 2026-07-28 → 2026-07-29 | 2 | −0,5022 |
| GOOGL | 2026-07-21 → 2026-07-23 | 3 | −0,4165 |
| MU | 2026-04-22 → 2026-05-04 | 8 | +1,7302 |
| MU | 2026-06-18 → 2026-06-23 | 2 | +1,1096 |

GDX inicial carecía de 14 barras para ATR previo; NVDA no tiene flags. Son diagnósticos para revisión, no ajustes acreditados. [Política y registro de volumen](../output/technical-desk-stage1a1/volume-preregistration.json).

**Validación (PowerShell, worktree 0b/0c):**

```powershell
$deskTests = @(rg --files tests -g 'test_technical_desk*.py')
python -m pytest -q @deskTests tests/test_contextual_g3.py
# 354 passed in 43.75s
python -m pytest -q --tb=short output/technical-desk-stage1a1/negative_causality_probe.py
# 1 failed in 0.47s; exit 1 esperado, por fuga futura
python -m py_compile src/technical_desk/causality.py src/technical_desk/pilot_protocol_v31.py src/technical_desk/role_contracts.py src/technical_desk/elliott_control.py src/technical_desk/volume_clusters.py scripts/run_technical_desk_stage1a1_elliott.py scripts/build_technical_desk_stage1a1_volume.py
# exit 0
git diff --check
# exit 0
python output/technical-desk-stage1a1/validate_phase.py
# PASS: 10000 series; 10 semillas reproducidas; 145 recortes; 4 SHA preservados
```

Fuera de `src/technical_desk` y tests, esta fase agregó cinco líneas a `.gitignore`, el CLI Elliott (134 líneas), el CLI volumen (145 líneas) y este informe. El `git diff --stat` contra HEAD también contiene cambios previos del desk: `.gitignore` +18; diseño +11/−8; etapa 0 +2. Los nuevos archivos no figuran en ese comando hasta agregarlos al índice; el detalle mediante `--no-index --stat` está guardado.

Evidencia completa local, excluida de Git: [validación](../output/technical-desk-stage1a1/validation.json), [Elliott](../output/technical-desk-stage1a1/elliott/manifest.json), [clusters](../output/technical-desk-stage1a1/volume-clusters.json), [diff stat fuera de módulo/tests](../output/technical-desk-stage1a1/outside-diff-stat.txt). El informe y código se publican en la rama; la evidencia privada queda local. La baseline 93edb79 precede al commit del desk y contiene trabajo previo.
