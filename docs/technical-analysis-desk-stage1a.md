**QUANTIA — Etapa 1a cerrada: features y validación determinista, SHADOW_ONLY.**

Se procesaron los cinco paquetes congelados de 0c (última rueda: 2026-11-13; corte de captura: 2026-11-15). Se agregaron features D/W, validador Elliott, contratos estrictos para los cinco roles y escaneo retrospectivo de volumen. No se ejecutaron modelos, prompts, dispatcher ni debate. Sin acceso a la base, cambios productivos, commit o push; etapa 1b sin iniciar.

| Ticker | Velas D/W | Pivotes confirmados D/W | Ventanas de 5 pivotes alternados D/W | Estado |
|---|---:|---:|---:|---|
| AMD | 267 / 57 | 73 / 12 | 56 / 4 | READY SHADOW_ONLY |
| GDX | 267 / 57 | 76 / 13 | 59 / 7 | READY SHADOW_ONLY |
| GOOGL | 264 / 56 | 70 / 14 | 53 / 7 | READY SHADOW_ONLY |
| MU | 267 / 57 | 65 / 11 | 50 / 5 | READY SHADOW_ONLY |
| NVDA | 267 / 57 | 70 / 17 | 58 / 8 | READY SHADOW_ONLY |

Los pivotes confirmados incluyen todos los detectados; las ventanas se cuentan sobre la cadena alternada activa, reemplazando un extremo del mismo tipo solamente cuando otro más extremo queda confirmado. Cinco pivotes delimitan cuatro ondas: el impulso completo requiere seis pivotes. READY conserva el alcance relativo de 0c; la unidad de volumen continúa PARTIAL / RELATIVE_UNVERIFIED.

En **cada ticker**, falta fuerza relativa D/W (`NO_COMPATIBLE_ARS_BENCHMARK`) y SMA200 W (`NEEDS_200_COMPLETE_BARS`: solo 56–57 semanas). En la última barra no faltan los otros indicadores pedidos. Las primeras barras conservan sus ausencias por calentamiento, sin ventanas parciales.

**Parámetros fijados antes del cálculo.** [Política versionada](technical-desk-stage1a-policy.json), registrada el 2026-11-16 a las 11:52:20 UTC; SHA-256 `b0b57ce27eb736fcb3eb267f3c1b5a0702f1f464e1bcdd55deb6b70e6c79a37e`.

- Pivotes: 2 barras izquierdas + 2 derechas, por separado en D/W; empate conserva el primero; extremo simultáneo HIGH/LOW se excluye por ambigüedad. `pivot_time` y `confirmed_at` distintos; extremos sin confirmación quedan CANDIDATE. Comparación de timestamps como instantes con zona horaria.
- Swings y HH/HL/LH/LL sobre pivotes confirmados; igualdad dentro de un tick. Fibonacci: retrocesos 0.236/0.382/0.5/0.618/0.786; extensiones 1/1.272/1.618/2/2.618. Rango de 20 barras previas, ruptura por cierre a un tick y persistencia de dos cierres contra el rango congelado al primer cruce.
- RSI Wilder14; MACD 12/26/9 con EMA iniciada por SMA completa; SMA20/50/200; ATR Wilder14 idéntico al valor de v3 en los diez marcos. Aritmética Decimal de 34 dígitos. Ticks ARS archivados: AMD25, GDX10, GOOGL5, MU25, NVDA10.
- Elliott: orden/alternancia, onda 2 inferior al 100%, onda 3 no más corta, onda 4 sin solapamiento y extremos de todas las barras de cada tramo. Tolerancia de un tick para extremos/longitud; cero para retroceso completo y solapamiento. Resultado por regla y agregado `RULES_COMPATIBLE`, `REJECTED`, `INCOMPLETE` o `NOT_EVALUABLE`. Diagonales: `UNSUPPORTED_PATTERN`; ABC: solo cronología/geometría básica. Compatibilidad no prueba que el conteo sea correcto.

**Contratos de roles.** `technical-desk-role-output-v1`: Elliott, Wyckoff, estructura, momentum y moderador; D/W, PRIMARY/ALTERNATIVE, una confirmación y una invalidación, objetivos opcionales, horizonte entero en ruedas BYMA, abstención con motivo e incertidumbre cualitativa. JSON estricto: extras, claves repetidas, probabilidades numéricas y coerciones rechazados. Autoridad fijada por código: `mode=SHADOW_ONLY`, `affects_analysis=false`, `affects_execution=false`. Wyckoff mantiene su cohorte separada y volumen relativo no verificado; no se suma volumen absoluto W.

El consumidor debe usar `parse_role_output`: el schema solo no valida contexto ni reglas cruzadas. v2/v3 permanecen intactos. Como v3 exige objetivos, el adaptador devuelve `NOT_COMPATIBLE / V3_REQUIRES_TARGETS_ABSENT_IN_ROLE_OUTPUT` cuando el nuevo contrato los omite. No fabrica niveles. Las 25 abstenciones usadas para verificar integración son fixtures, con cero respuestas imputadas al piloto; siguen vigentes 5 paquetes × 2 réplicas × 5 roles, USD 2 y `NOT_STARTED` sin modelo/tarifa.

**Volumen: 30 fechas candidatas a revisión, no ajustes identificados.** Mediana de las 10 ruedas anteriores frente a las 10 iniciadas en la fecha, umbral ≥3 o ≤1/3; 1.237 ventanas evaluadas y 95 bordes sin ventana completa. Es retrospectivo, queda fuera de las features causales y registra cuándo termina la ventana posterior.

- AMD (11): 2025-11-{21,25,26,27}; 2026-03-{23,25,26,27}; 2026-10-{27,28,29}.
- GDX (6): 2025-10-30; 2025-11-03; 2026-01-{20,21}; 2026-07-{28,29}.
- GOOGL (3): 2026-07-{21,22,23}.
- MU (10): 2026-04-{22,23,24,27,28,29,30}; 2026-05-04; 2026-06-{18,23}.
- NVDA: ninguna fecha marcada por este umbral.

**Owner y preservación.** El capturador de auditoría usa exclusivamente `ADMIN_CHAT_IDS`; con varios administradores exige `--owner-chat-id` perteneciente a esa lista. Validación anterior a conectar a la base y sin fallback a `TELEGRAM_CHAT_ID`. Se probó con entorno/conexión simulados: esta etapa no cargó `.env`, no validó credenciales reales ni recapturó o cambió el owner de la evidencia congelada.

**Validación ejecutada desde el worktree 0b/0c.**

```powershell
python -m pytest -q tests/test_technical_desk_evidence.py tests/test_technical_desk_pilot_protocol.py tests/test_technical_desk_pilot_protocol_v3.py tests/test_technical_desk_stage0c.py tests/test_technical_desk_features.py tests/test_technical_desk_elliott.py tests/test_technical_desk_role_contracts.py tests/test_technical_desk_owner_scope.py tests/test_technical_desk_volume_scan.py tests/test_contextual_g3.py
# 299 passed in 2.65s
python -m py_compile src/technical_desk/features.py src/technical_desk/elliott_rules.py src/technical_desk/role_contracts.py src/technical_desk/volume_scan.py src/technical_desk/owner_scope.py scripts/build_technical_desk_stage1a.py scripts/audit_technical_desk_stage0b.py
# exit 0
git diff --check
# exit 0; solo avisos CRLF de archivos previos
python scripts/build_technical_desk_stage1a.py --packets output/technical-desk-stage0c/packets --policy docs/technical-desk-stage1a-policy.json --registration output/technical-desk-stage1a/preregistration.json --output output/technical-desk-stage1a/verified-final
# exit 0; cinco paquetes
python output/technical-desk-stage1a/validate_artifacts.py
# PASS: 145 recortes reales, 130 recortes sintéticos; 5 paquetes y schema idénticos byte a byte
```

Incluye los cuatro casos sintéticos Elliott pedidos, extremos intermedios, empates, ruptura persistente, JSON/autoridad y regresiones de zona horaria/anclas. Cuatro SHA-256 preservados (módulos v2/v3 y sus tests), manifiestos iguales salvo hora de generación; cuatro guardas bloquean sockets, resolución DNS, SQLite y subprocess en el proceso offline. El recorrido ejecutado escribe solo archivos de salida; ninguna escritura de base o `market_candles`.

Evidencia fuera de Git: [validación y hashes](../output/technical-desk-stage1a/validation.json), [manifiesto y fechas](../output/technical-desk-stage1a/verified-final/manifest.json), [schema](../output/technical-desk-stage1a/verified-final/role-output.schema.json) y [tests](../output/technical-desk-stage1a/tests.log). La causalidad probada corresponde al contenido de esta captura; no reconstruye vintages históricos ni demuestra rentabilidad. Quedan explícitas las ausencias de benchmark/SMA200 W y la unidad de volumen no verificada.
