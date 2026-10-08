# T1 — Telegram Contextual Shadow

## Resultado

**T1: PASS.** El comando `/analisis_contextual` quedó implementado y desplegado
en `cocos_telegram_bot` con `SHADOW_ONLY`, `affects_analysis=false` y
`affects_execution=false`. El despliegue usa el commit
`13160516920a0a5e4d8b336bd1bfef15c087e3a1` de
`feature/contextual-e1-contracts`. `main` permaneció en
`f3c6bbeacda4f96226f98c40fb50861068b7abbe`, sin merge ni push.

La validación ejecutó el mismo runner que invoca Telegram, con portfolio y
mercado reales, pero no envió un mensaje. No hubo llamadas a una API de broker,
órdenes, intents ejecutables, escrituras en `decision_log`, cambios de posición
o cambios de cash. `/analisis` conserva su handler y su flujo anteriores.

## Arquitectura

El comando agrega una vista después de evaluar el pipeline productivo:

```text
/analisis_contextual
  ├─ sincronización operativa existente
  ├─ pipeline productivo en memoria
  │    └─ scoring → signals → optimizer → planner
  ├─ productive_view congelada
  ├─ captura G3 de mercado real
  │    └─ observaciones append-only + cutoff PIT
  ├─ feature_snapshot_v3 + contexto E2 por activo
  ├─ comparación productive_view antes/después
  ├─ plan envelope SHADOW_ONLY, no ejecutable
  └─ render Telegram: plan productivo + contexto shadow
```

La implementación reutiliza `run_analysis.main` con Telegram y persistencia
productiva deshabilitados. El contexto se adjunta después de congelar scores,
signals, decisiones, órdenes teóricas, cantidades y cash. No entra como input
del scoring, optimizer o planner.

El nuevo runner está en
[`run_contextual_shadow.py`](../scripts/run_contextual_shadow.py), los contratos
y el renderer en
[`contextual_telegram.py`](../src/analysis/contextual_telegram.py), y el handler
en [`telegram_bot.py`](../scripts/telegram_bot.py).

## Safety contract

La protección opera en tres niveles:

1. El runner exige `no_persist=true`, `run_intent=exploratory`, owner positivo y
   run ID explícito para la evaluación productiva en memoria.
2. Antes y después de construir E2 compara scores, signals, decisions, order
   intents, quantities y cash. Una diferencia produce `FAIL_CLOSED`.
3. La base sólo acepta para este flujo un envelope con
   `authority_mode=SHADOW_ONLY`, `gate=SHADOW_ONLY`, `feasible=false`, flags de
   autoridad en `false`, nocionales en cero y cash sin cambio. Un trigger impide
   insertar o actualizar cualquier `order_intent` asociado.

La migración
[`20261009_telegram_contextual_shadow.sql`](../migrations/20261009_telegram_contextual_shadow.sql)
es aditiva y protectiva. Se aplicó dos veces al PostgreSQL operativo para
comprobar idempotencia. La prueba transaccional confirmó que el trigger rechaza
un intent del plan shadow y que el contrato productivo previo sigue aceptando
sus inserts. No hubo backfill inventado.

## Captura real de validación

| Campo | Valor |
|---|---|
| capture_id | `169c66eb-ba5f-45b0-9f4d-1b5cf8052b0a` |
| run_id | `409da5c5-49af-45e0-a8a9-0d500277b78c` |
| plan_id | `b4539d2a-f66a-4833-8b6c-d0757750b1e3` |
| portfolio_snapshot_id | `043b3c1a-ac6f-4a6d-ad59-6c228e4a0dc6` |
| owner | `1259412316` |
| cutoff | `2026-10-08T02:39:08.702030Z` |
| code_version | `13160516920a0a5e4d8b336bd1bfef15c087e3a1` |
| activos contextualizados | 11 |
| observaciones capturadas | 2.916 |
| observaciones efectivas | 2.904 |
| velas abiertas excluidas | 12 |
| contextual snapshots | 11 |
| confianza agregada | `HIGH` (`0.981818...`) |

Las 2.904 observaciones efectivas están vinculadas exactamente mediante
`contextual_snapshot_candles` a sus filas de
`market_candle_observations`. Cada fila conserva observation ID, identidad de
barra, digest, provider/source, mercado, moneda, intervalo, `bar_start`,
`bar_end`, `available_at`, `scraped_at` e `is_closed`. El hash ordenado de la
lista de IDs es
`742194df7b8d77477b21fc1436dd0f48b0b38967cb1ab449371a674404feacc4`
y el inputs hash es
`a1ddfdf9d0cfeb38e6f672cafabf3426f35a2d32adf8f658fe63bab85c577a15`.

La captura de mercado y el análisis registraron ambos
`STARTED → COMPLETE`. Intentos anteriores que fallaron por historia insuficiente
quedaron en `STARTED → FAILED`; no se promovieron ni borraron.

Evidencia:
[`contextual-t1-real-capture.json`](evidence/contextual-t1-real-capture.json) y
[`contextual-t1-real-audit.json`](evidence/contextual-t1-real-audit.json).

## Ejemplos reales

Para IREN, el plan productivo de la corrida fue `SELL_PARTIAL`, mientras la
señal base siguió siendo `HOLD`, con score `-0.0751` y conviction `0.6667`. La
vista shadow mostró tendencia diaria `DOWNTREND`, semanal `MIXED`, RVOL
`1.4454`, volumen `NORMAL`, breakout `IN_RANGE`, RS20 `-0.1304`, RS60 `+0.0162`
y RS120 `-0.2185`. El invalidador de fuerza relativa fue `WARN`; el resto no
cambió el plan.

Para NVDA, el plan productivo fue `WATCH` y la señal `HOLD`, con score `+0.0530`
y conviction `0.3333`. El contexto mostró tendencia diaria y semanal
`UPTREND`, RVOL `2.8557`, expansión de volumen, breakout `IN_RANGE` y fuerza
relativa positiva en 20, 60 y 120 sesiones. Todos los invalidadores fueron
`PASS`. El resultado terminó con `Impacto contextual: NONE · SHADOW ONLY`.

SNDK demostró el tratamiento de evidencia incompleta: con 99 barras cerradas,
la estructura semanal y RS120 quedaron `UNKNOWN` por historia insuficiente. No
se rellenaron. Sus métricas diarias, RVOL y RS20/RS60 sí se calcularon con la
evidencia disponible.

## Formato Telegram

El reporte contiene el encabezado experimental, portfolio, cash, cutoff, run,
conteo de evidencia y confianza. Por activo separa explícitamente el plan
productivo del bloque `Contexto [SHADOW]`, muestra estructura, RVOL, volumen,
breakout, fuerza relativa e invalidadores, y cierra con impacto contextual
nulo.

La salida real midió 9.096 caracteres y 9.533 bytes UTF-8. El splitter produjo
3 mensajes; el mayor tuvo 3.878 caracteres. El documento completo y los tres
fragmentos pasaron el validador HTML de Telegram. La primera envoltura de
auditoría había interpretado por error el retorno `(bool, errors)` como `None`;
se corrigió la auditoría, sin cambio de producto.

## Auditoría PIT y no regresión

La relectura comprobó:

- cero candles, `bar_end`, `available_at` o `scraped_at` posteriores al cutoff;
- cero barras abiertas vinculadas como input;
- 2.916 identidades de barra distintas y cero identidades ambiguas;
- 2.904 IDs efectivos iguales a los IDs del payload y a los IDs vinculados;
- todos los digests de observación válidos;
- inputs hash y 11 snapshot hashes reproducidos;
- plan shadow con nocional cero, cash sin cambio y cero intents;
- scores, signals, decisions, order intents, quantities y cash iguales antes y
  después de adjuntar contexto.

Evidencia:
[`contextual-t1-non-regression.json`](evidence/contextual-t1-non-regression.json).

## Despliegue

Sólo se reconstruyó y recreó `cocos_telegram_bot`. Scheduler, monitor y frontend
no se redeplegaron. El contenedor corre la imagen
`sha256:63d87dd52f5fdb38e534bf76d2a0a2affb37fb4293ed794d1315617609887a96`,
expone el code version correcto, tiene cero reinicios y registró el menú nativo
con `/analisis_contextual`. `.env` conservó el mismo SHA-256 antes y después.

Evidencia:
[`contextual-t1-deployment.json`](evidence/contextual-t1-deployment.json).

## Tests

La suite focalizada T1/E1/E2 terminó con **81 passed, 1 skipped**. La integración
contra TimescaleDB descartable validó migración, idempotencia, envelope shadow,
rechazo de intents y compatibilidad productiva; el bloque combinado terminó con
**33 passed**. Después del ajuste prospectivo de historia mínima, las pruebas
del runner terminaron con **21 passed**. La repetición final del módulo T1 y su
integración PostgreSQL terminó con **9 passed**.

La suite completa terminó con **814 passed, 27 skipped, 7 failed**. Las siete
fallas son baseline conocidas: menú nativo con `agente`/`analytics`, dependencia
ausente `pypfopt`, mock de radar exploratorio, header de radar compacto,
longitud de help, menú principal y línea shadow del radar compacto. No apareció
una falla nueva de T1. El detalle está en
[`contextual-t1-tests.json`](evidence/contextual-t1-tests.json).

## UNKNOWN y límites

Se preservan explícitamente como UNKNOWN:

- benchmark sectorial no configurado;
- unidad contractual de volumen;
- calendario BYMA versionado;
- ratio depositario;
- dimensiones de ventanas largas cuando la historia real no alcanza.

El comando está desplegado para prueba manual. La validación no envió mensajes
Telegram y no ejecutó el comando en nombre del usuario. E1/E2/G3 conservan
autoridad nula. Este trabajo no inicia E3 ni autoriza cambios de scoring,
thresholds, optimizer, planner o ejecución.
