# Contextual Analysis — G2 Real PIT Capture

## Resultado

**G2: PASS.** Una corrida formal nueva persistió plan, decisiones, velas
observadas, `feature_snapshot_v3` y snapshot contextual E2 con el mismo
`owner/run_id/plan_id`. La relectura encontró cero violaciones PIT para IREN y
SPY. El snapshot continuó en `SHADOW_ONLY`; el hash productivo fue idéntico con
y sin adjuntar E2. No se invocó una API de órdenes ni se envió Telegram.

Este resultado cierra G2. No implementa E3 ni autoriza cambios de scoring,
thresholds, optimizer, planner o ejecución.

## 1. Delta de schema observado antes de migrar

La auditoría read-only se hizo sobre PostgreSQL operativo el
2026-10-07 17:58:50 UTC. `market_candles` tenía:

- identidad: `ticker`, `long_ticker`, `asset_type`, `currency`, `venue`,
  `interval`;
- OHLCV: `open_price`, `high_price`, `low_price`, `close_price`, `volume`;
- procedencia mínima: `source`, `scraped_at`;
- clave mutable: `UNIQUE (ts, long_ticker, interval)` con escritura por upsert.

Los alias existentes permitían interpretar `long_ticker` como símbolo del
proveedor, `venue` como mercado y `ts` como candle timestamp, pero faltaban los
contratos temporales E1 y no existía historial append-only de revisiones.

### REQUIRED_FOR_G2

| Superficie | Faltante inicial | Resolución G2 |
|---|---|---|
| `market_candles` legacy | `bar_start`, `bar_end`, `available_at`, `is_closed`, `volume_unit`, `calendar`, `calendar_validation`, `adjustment_policy`, `depositary_ratio` | columnas nullable aditivas; ninguna fila histórica fue completada |
| observación PIT | identidad explícita, símbolo proveedor, unidades, procedencia, calidad, missingness, tiempos y code version append-only | `market_candle_observations` |
| contexto ligado al plan | owner, run, plan, portfolio, cutoff, feature v3, E2, hashes y flags shadow | `contextual_market_snapshots` |
| velas efectivamente usadas | relación auditable entre snapshot y revisión de vela | `contextual_snapshot_candles` |
| plan formal E1 | triggers v2 de identidad e inmutabilidad ausentes en el schema auditado | instalados por la migración E1 existente antes de la captura |

`owner`, `run_id` y `plan_id` ya existían en las tablas de plan. El
`feature_snapshot_v3` y `code_version` ya podían vivir en los JSON de decisión,
pero no había una relación plan-contexto versionada.

### RECOMMENDED_LATER

- registro normalizado de instrumentos y aliases con claves foráneas;
- catálogo versionado del calendario BYMA para verificar sesiones faltantes y
  hora de cierre exacta;
- catálogo contractual de unidades por proveedor;
- mapeo configurable activo-sector-benchmark;
- política Timescale de retención/compresión para observaciones;
- estado explícito de intentos de captura abortados.

El delta completo está en
[`docs/evidence/contextual-g2-schema-delta.json`](evidence/contextual-g2-schema-delta.json).

## 2. Migración aditiva

La migración es
[`migrations/20261007_contextual_g2.sql`](../migrations/20261007_contextual_g2.sql).
No borra ni renombra columnas, no actualiza velas legacy y no fabrica
`available_at`, `bar_end` ni `is_closed` históricos.

Agrega tres tablas:

1. `market_candle_observations`: una fila por revisión observada, con identidad,
   OHLCV, unidades, tiempos, calidad, missingness, procedencia, owner, run y SHA.
2. `contextual_market_snapshots`: snapshot E2 ligado al plan y al portfolio,
   con feature v3, cutoff, hashes y comparación productiva.
3. `contextual_snapshot_candles`: relación ordenada entre el snapshot y cada
   observación usada como activo, benchmark general o benchmark sectorial.

Las tres superficies son append-only. El trigger del snapshot también exige
que owner/run coincidan con el plan, que el portfolio pertenezca al owner, que
el feature sea v3 y que E2 tenga `mode=SHADOW_ONLY`,
`affects_analysis=false` y `affects_execution=false`.

La migración fue aplicada dos veces en el ensayo descartable y dos veces de
forma segura en el entorno operativo. Después de aplicarla, las 253.387 filas
legacy conservaron `NULL` en todos sus tiempos y metadatos no observados.

## 3. Validación en clon descartable

El gate previo usó TimescaleDB 2.30.2 sobre PostgreSQL 17 en un contenedor local
descartable. El ensayo reconstruyó el schema previo de 14 columnas, insertó una
vela legacy, aplicó dos veces la migración y comprobó:

- las nueve columnas E1 fueron agregadas;
- la vela legacy quedó íntegramente UNKNOWN en los campos nuevos;
- 135 observaciones del activo y 135 de SPY fueron persistidas y releídas;
- 134 velas cerradas de cada serie fueron elegibles por PIT;
- el contexto tuvo confianza HIGH y el feature persistido fue v3;
- los updates de observación y snapshot fueron rechazados;
- scores, signals, decisions, orders, quantities y cash permanecieron iguales;
- órdenes ejecutadas: cero.

Evidencia:
[`docs/evidence/contextual-g2-disposable-validation.json`](evidence/contextual-g2-disposable-validation.json).

## 4. Corrida real utilizada

| Campo | Valor |
|---|---|
| owner | `1259412316` |
| run_id | `74db5161-562c-428a-b1cd-4620b9ee106b` |
| plan_id | `286f2ca7-d363-49a2-9fa4-cf5cee550dc9` |
| portfolio_snapshot_id | `8a4276c1-3d4a-4e29-862b-6cf01450f91d` |
| cutoff / plan timestamp | `2026-10-07T18:07:54.432215+00:00` |
| code_version | `42957aa3a04eed391d3941548b4b7310b38709fd` |
| activo auditado | IREN, `BYMA:CEDEAR:IREN:ARS`, proveedor `BYMA:IREN` |
| benchmark general | SPY, `BYMA:CEDEAR:SPY:ARS`, proveedor `BYMA:SPY` |
| señal productiva IREN | SELL, score `-0.1246`, conviction `1.0` |
| plan | NORMAL, feasible, `execution-plan-v2-immutable` |
| cartera / cash | ARS 3.493.988,26 / ARS 9.798,26 |

La corrida usó el pipeline real con optimizer habilitado. Para reducir
superficie externa se omitieron LLM, sentiment y radar; el baseline y la
variante shadow usaron exactamente esa misma configuración y el mismo plan
persistido. E2 nunca fue consumido por scoring, optimizer o planner.

Se generaron cuatro intents ejecutables y dos bloqueados en el plan. Son
intenciones persistidas, no órdenes de broker. El runner no contiene una ruta
de envío de órdenes, registró cero llamadas a API de órdenes y se ejecutó con
Telegram deshabilitado.

Hubo un intento previo seguro (`b01320ca-62b9-4e37-ad0f-81dfd2ad197a`) que
capturó 484 observaciones pero abortó antes del plan por un DSN incompatible
con `asyncpg`. Las observaciones se preservaron por el contrato append-only;
ese intento no tiene `plan_id`, no se usa como evidencia G2 y no ejecutó
órdenes. El adaptador fue corregido y fijado antes de la corrida válida.

Evidencia:
[`docs/evidence/contextual-g2-real-run.json`](evidence/contextual-g2-real-run.json).

## 5. Semántica PIT de las velas reales

TradingView/BYMA entregó barras diarias de sesión regular con ajuste por
splits. G2 conserva:

- `bar_start` y `candle_timestamp`: timestamp entregado por el proveedor;
- `scraped_at`: instante real en que terminó cada descarga;
- `available_at`: ese mismo primer instante observado por Quantia, sin atribuir
  disponibilidad histórica anterior;
- `bar_end`: inicio de la siguiente barra del mismo proveedor;
- `is_closed=true`: sólo cuando existe esa siguiente barra como confirmación;
- última barra: `bar_end=NULL`, `is_closed=false`, excluida del contexto.

Esta definición es conservadora. No afirma una hora de cierre BYMA que el
payload no entrega. Para el snapshot se usaron 223 barras cerradas de IREN y
259 de SPY. En ambas series:

- identidad distinta por rol: una;
- `candle_timestamp <= cutoff`: todas;
- `bar_end <= cutoff`: todas;
- `available_at <= cutoff`: todas;
- `scraped_at <= cutoff`: todas;
- barras futuras, abiertas o sin `bar_end` usadas: cero.

Evidencia:
[`docs/evidence/contextual-g2-pit-audit.json`](evidence/contextual-g2-pit-audit.json).

## 6. Cobertura contextual real

Para IREN al cutoff:

| Dimensión | Resultado |
|---|---|
| trend_daily | `DOWNTREND` |
| trend_weekly | `MIXED` |
| RVOL | `1.4453988829734388` |
| volume_state | `NORMAL` |
| breakout_state | `IN_RANGE` |
| RS 20 vs SPY | `-0.1304171673290816` |
| RS 60 vs SPY | `0.01624760405608905` |
| RS 120 vs SPY | `-0.21854847235741182` |
| context_confidence | `1.0 / HIGH` |
| precio | `VALID` |
| volumen | `VALID` |
| procedencia | `PARTIAL` |

Los invalidadores descriptivos dieron PASS para breakout/volumen, estructura
semanal de BUY no aplicable, soporte y confirmación de volumen; fuerza relativa
20 sesiones dio WARN por deterioro. Ninguno afectó producción.

## 7. UNKNOWN preservado

No existe mapeo configurado de benchmark sectorial, por lo que quedó UNKNOWN
con motivo `NO_CONFIGURED_SECTOR_BENCHMARK_MAPPING`; no se sustituyó por QQQ ni
se infirió sector. El payload de TradingView tampoco declara unidad de volumen,
versión de calendario ni ratio depositario. Esos campos permanecen UNKNOWN.

La última barra de cada descarga no tenía una barra posterior que confirmara
su cierre. Conservó `bar_end` desconocido e `is_closed=false` y fue excluida.
Por eso la missingness del snapshot menciona `UNKNOWN_BAR_END`, aunque todas las
velas efectivamente vinculadas al snapshot tienen cierre confirmado.

## 8. No regresión

La comparación toma el `FULL_CONTEXT` productivo persistido y una copia a la
que sólo se adjunta E2. Resultado:

| Contrato | Igual |
|---|---|
| scores | sí |
| signals | sí |
| decisions | sí |
| orders/intents | sí |
| quantities | sí |
| cash | sí |

El hash de ambos payloads productivos es
`55f962067ac48c83a8597f7792f4b9d71b11ca85474f72eacb76545f6202373a`.
Los flags persistidos son `SHADOW_ONLY`, `affects_analysis=false` y
`affects_execution=false`.

Evidencia:
[`docs/evidence/contextual-g2-non-regression.json`](evidence/contextual-g2-non-regression.json).

## 9. Pruebas y límites antes de E3

Pruebas específicas:

- `tests/test_contextual_g2.py`: finitud/rangos, missingness de volumen,
  identidad, tiempos observados, cierre por siguiente barra y SQL aditivo;
- `tests/test_contextual_g2_postgres.py`: migración real, idempotencia,
  legacy UNKNOWN, append-only, roundtrip, PIT y neutralidad;
- suite contextual E1/E2 y planner pertinente: sin regresiones nuevas.

Suite completa: **801 passed, 24 skipped, 7 failed**. Los siete fallos son el
baseline ya observado y ajeno a G2: cuatro expectativas desactualizadas de UI
Telegram/radar, un mock de radar, y el challenger que no encuentra `pypfopt`.

No hay un blocker que invalide G2. Antes de E3 conviene resolver o aceptar de
forma explícita:

1. unidad de volumen y calendario versionado del proveedor;
2. mapping sectorial configurable;
3. lifecycle de capturas abortadas;
4. los siete fallos baseline de la suite completa;
5. validación visual del gráfico real si se quiere usar como artefacto de
   revisión operativo.

La recomendación es **no iniciar E3 todavía** hasta decidir estos contratos de
procedencia y limpiar el baseline de pruebas. G2 queda cerrado como PASS.
