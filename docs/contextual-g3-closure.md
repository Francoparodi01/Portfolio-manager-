# Contextual Analysis — G3.1 Prospective Real Capture

## Resultado

**G3: PASS.** Una captura real nueva se creó íntegramente bajo los contratos
G3, terminó `COMPLETE`, produjo un plan formal sin ejecución y pudo reproducirse
exactamente con AS-OF replay. Las observaciones efectivas, sus digests, los
input hashes, el payload contextual y el snapshot hash coinciden. Dos replays
independientes fueron idénticos.

E1/E2 continúan en `SHADOW_ONLY`, con `affects_analysis=false` y
`affects_execution=false`. No cambiaron scoring, señales, thresholds,
optimizer ni planner. No hubo órdenes, llamadas a una API de broker ni
mensajes Telegram. No se comenzó E3.

## 1. Schema y migración

La verificación previa encontró el schema G3 ya instalado en el PostgreSQL
operativo:

- `observation_digest` presente;
- `market_evidence_capture_events` y su vista de estado presentes;
- constraints de identidad G3 presentes;
- triggers de validación e inmutabilidad habilitados;
- cero snapshots fuera de `SHADOW_ONLY` o con autoridad de análisis/ejecución.

Antes de la captura había 968 observaciones legacy con digest `NULL` y ninguna
observación G3. No se inventó ni se hizo backfill de esos digests. La migración
no tuvo que volver a aplicarse al entorno operativo. Su idempotencia se verificó
aplicándola dos veces en TimescaleDB descartable antes de la corrida.

Después de los intentos G3 existen 1.040 observaciones con digest y las 968
legacy siguen en `NULL`. La migración utilizada es
[`20261008_contextual_g3.sql`](../migrations/20261008_contextual_g3.sql).

## 2. Intento fallido preservado

El primer intento fuera de rueda conservó correctamente su estado:

| Componente | ID | Lifecycle |
|---|---|---|
| captura de mercado | `37cc77f1-58bd-4b10-b2c9-365da87970c8` | `STARTED → COMPLETE` |
| análisis | `d2726900-39d5-481e-bcfc-41aaf5ba708e` | `STARTED → FAILED` |

`run_analysis` aplicó su política normal fuera de rueda y convirtió el análisis
en exploratory/no-persist. No se creó plan ni snapshot y el intento no fue
promovido. Tampoco hubo órdenes o Telegram.

Para la captura de cierre se agregó una excepción interna de auditoría. Sus
defaults conservan la política previa y sólo acepta persistencia fuera de rueda
con `no_telegram`, `no_llm`, `no_sentiment`, `skip_radar`, owner explícito,
run ID explícito e intención `formal_plan`. No cambia el cálculo del plan.

## 3. Captura real completa

| Campo | Valor |
|---|---|
| capture_id | `29ac8eaa-7c45-4ad4-8324-59919df7ca43` |
| run_id | `8d1cb9c5-f304-43b2-9955-281ffc723ac4` |
| plan_id | `6e191e89-59a6-4ccb-9670-49f75af07757` |
| portfolio_snapshot_id | `cca82db8-5aa9-41a8-a6f3-2292de527960` |
| owner | `1259412316` |
| code_version | `8d89d2c13cb37b892ae8e083f5de89c9c79d224e` |
| cutoff | `2026-10-07T22:00:49.072501Z` |
| contextual snapshot | `context:0f40d3753252df389593060b` |
| activo contextual | AMD |
| benchmark general | SPY |
| señal / score / conviction | BUY / `0.0542` / `0.3333` |

La captura usó mercado TradingView/BYMA real y el portfolio real persistido.
El plan contiene intents auditables, pero el runner no tiene ruta de ejecución
de broker y se invocó con Telegram deshabilitado.

## 4. Evidencia de mercado

| Rol | Capturadas | Efectivas | Excluidas |
|---|---:|---:|---:|
| AMD / ASSET | 260 | 259 | 1 |
| SPY / GENERAL_BENCHMARK | 260 | 259 | 1 |
| Total | 520 | 518 | 2 |

Todas las observaciones tienen `observation_id`, identidad de barra, identidad
de observación, digest, provider/source, mercado, moneda, intervalo y contratos
temporales. Los 518 inputs efectivos están ligados por ordinal al snapshot.

La última barra de AMD y la última de SPY quedaron persistidas como
`is_closed=false`, sin `bar_end`, y con `effective_snapshot_input=false`. No
aparecen en `contextual_snapshot_candles` ni influyen en el frame contextual.

La lista completa está en
[`contextual-g3-real-capture.json`](evidence/contextual-g3-real-capture.json).

## 5. Snapshot E2

El snapshot conservó:

- `contextual-market-snapshot-v1` / `contextual-market-v1`;
- confianza contextual `1.0`, estado `HIGH`;
- los cinco invalidadores descriptivos, todos `PASS`;
- observation inputs hash
  `e09f90a0dacd271ae29a770a0b81d87959ef46510725c50e8ca4fbd289b09383`;
- asset input digest
  `c7a2bceea7ce86d70930d67938bfb267105364738620a8562af56770f48686b3`;
- SPY input digest
  `c99df20dff3a3b91b2e8d0f392d363a8a9cc963739177d272060c8c91be75e2e`;
- contextual snapshot hash
  `3934504ec58038748d8a63eba23d72ec8cb493bf13651ac300d5541e3a9bc91c`.

Permanecen legítimamente UNKNOWN el benchmark sectorial, ratio depositario,
unidad contractual de volumen y calendario BYMA versionado.

## 6. AS-OF replay

La auditoría se hizo en una transacción `READ ONLY, REPEATABLE READ` con el
cutoff exacto de la corrida. El replay obtuvo:

- las mismas 518 observation IDs;
- los mismos 518 digests;
- los mismos input digests;
- el mismo payload contextual;
- el mismo snapshot ID y snapshot hash;
- cero observaciones o barras posteriores al cutoff;
- cero barras abiertas entre los inputs;
- cero observaciones de capturas incompletas promovidas.

El evento `COMPLETE` de la captura de mercado ocurrió antes del cutoff. Por eso
la selección no depende retrospectivamente de un terminal que aún no existía.

Evidencia:
[`contextual-g3-real-replay.json`](evidence/contextual-g3-real-replay.json).

## 7. Determinismo

Se ejecutaron dos lecturas AS-OF y dos reconstrucciones independientes. Las
listas de observaciones, payloads, snapshot IDs y hashes fueron idénticos. El
snapshot real original también coincide con ambos replays.

Evidencia:
[`contextual-g3-real-determinism.json`](evidence/contextual-g3-real-determinism.json).

## 8. Lifecycle

La captura de mercado registró:

```text
STARTED  2026-10-07T22:00:41.365468Z
COMPLETE 2026-10-07T22:00:44.119426Z
```

El análisis registró:

```text
STARTED  2026-10-07T22:00:44.121926Z
COMPLETE 2026-10-07T22:00:50.128920Z
```

Cada lifecycle tiene un solo terminal, owner estable y timestamps coherentes.
La captura de mercado estaba completa antes del cutoff del plan; el análisis
se completó después de persistir y verificar snapshot y plan.

## 9. Inmutabilidad

Sobre una observación de la captura real se intentaron, cada uno dentro de una
transacción revertida:

- `UPDATE` de observación;
- `DELETE` de observación;
- `DELETE` de vínculo snapshot-observación;
- `UPDATE` de evento de lifecycle;
- `TRUNCATE` de observaciones.

Los cinco intentos fueron bloqueados. El conteo permaneció en 520 filas y todos
los digests fueron recalculados y validados.

## 10. No regresión

El payload productivo grabado antes de adjuntar E2 y su variante shadow tienen
el mismo hash:
`676285368efbcd92f2ee363de1277405e22ef320f27f2ba82361077491b6662e`.

Scores, signals, decisions, order intents, quantities y cash son iguales. El
snapshot permanece `SHADOW_ONLY`, `affects_analysis=false` y
`affects_execution=false`. Órdenes ejecutadas: cero.

Evidencia:
[`contextual-g3-real-non-regression.json`](evidence/contextual-g3-real-non-regression.json).

## 11. Tests

La suite focalizada terminó con **78 passed** e incluyó G1, E2, G2, G3,
integraciones contra TimescaleDB descartable, exclusión de barras abiertas y
la política segura de persistencia audit-only fuera de rueda.

La suite completa terminó con **806 passed, 26 skipped, 7 failed**. Las siete
fallas son las mismas baseline conocidas: menú/comandos Telegram, dependencia
ausente `pypfopt`, mock de radar exploratorio, header del radar compacto,
longitud de help, menú principal y línea shadow del radar compacto. No apareció
una falla nueva relacionada con G3.1.

## 12. Legacy gap y cierre

El run G2 `74db5161-562c-428a-b1cd-4620b9ee106b` mantiene su gap histórico:
dos observaciones abiertas estuvieron asociadas sólo mediante `run_id` y sus
digests/lifecycle no existían al capturarse. No se reparó ni se completó ese
pasado.

La captura prospectiva prueba los contratos que faltaban y reclasifica **G3 a
PASS**. Este cierre no autoriza E3 ni una promoción productiva del contexto.
