# Contextual Analysis — G3 Immutable Market Evidence & AS-OF Replay

## Resultado

**G3: PARTIAL.** La implementación prospectiva cumple el contrato G3 en un
PostgreSQL/TimescaleDB descartable: conserva revisiones de una barra, elige la
versión correcta según el cutoff, excluye capturas abortadas, reproduce el
mismo snapshot y rechaza `UPDATE`, `DELETE` y `TRUNCATE` de evidencia. El
comportamiento productivo permanece idéntico y no se ejecutaron órdenes.

El run real de G2 puede reconstruirse exactamente y sus 482 barras cerradas
están ligadas por `observation_id`. Sin embargo, sus dos últimas barras abiertas
afectaron la missingness del snapshot y quedaron relacionadas sólo de forma
indirecta por `run_id`. Además, G2 antecede al digest persistido y al lifecycle
de captura. Esos hechos legacy no se rellenaron. La captura prospectiva ya
vincula toda observación que afecte valores o missingness, pero hace falta una
nueva corrida futura para demostrar ese cierre con evidencia real.

No se implementó E3. No cambiaron scores, señales, thresholds, optimizer,
planner productivo ni ejecución.

## 1. Arquitectura encontrada y garantías

G3 extiende las tablas de G2; no crea otro almacén de velas.

| Superficie | Estado antes de G3 | Estado después de G3 | Garantía |
|---|---|---|---|
| `market_candles` | upsert por barra | sin cambios | **MUTABLE**; sigue siendo la vista operativa legacy |
| `market_candle_observations` | múltiples observaciones y trigger contra `UPDATE/DELETE` | digest obligatorio para filas nuevas, lifecycle obligatorio y bloqueo de `TRUNCATE` | **IMMUTABLE / APPEND_ONLY** para escrituras ordinarias |
| `contextual_market_snapshots` | snapshot shadow inmutable ligado a owner/run/plan | exige captura `STARTED`, idempotencia estricta y bloqueo de `TRUNCATE` | **IMMUTABLE / APPEND_ONLY** |
| `contextual_snapshot_candles` | vínculo exacto snapshot-observación | exige evidencia `COMPLETE` o la captura activa del mismo run; colisiones dejan de ser silenciosas | **IMMUTABLE / APPEND_ONLY** |
| `market_evidence_capture_events` | ausente | eventos `STARTED`, `COMPLETE`, `ABORTED` o `FAILED`, transiciones validadas e inmutables | **IMMUTABLE / APPEND_ONLY** |
| hashes legacy de G2 | computables a posteriori | siguen sin backfill | **BEST_EFFORT / LEGACY_UNKNOWN** |

La migración aditiva está en
[`migrations/20261008_contextual_g3.sql`](../migrations/20261008_contextual_g3.sql).
Se aplicó repetidamente en el clon descartable y tres veces en el PostgreSQL local que
contiene el run G2. No borra, renombra ni completa filas históricas.

## 2. Identidades y digest

La identidad formal de una barra es:

```text
BAR_IDENTITY = (
  instrument_id,
  market,
  provider_symbol,
  interval,
  bar_start
)
```

`instrument_id` ya incluye mercado, tipo de activo, ticker y moneda. Se
mantienen también mercado y símbolo proveedor para evitar que un alias ambiguo
colapse instrumentos o feeds diferentes.

La identidad de una observación usa el diseño existente y lo refuerza con el
digest:

```text
OBSERVATION_IDENTITY = UUIDv5(
  owner,
  ingestion_run_id,
  BAR_IDENTITY,
  scraped_at,
  source,
  observation_digest
)
```

`observation_digest` es SHA-256 canónico de identidad, timestamps, estado de
cierre, OHLCV, unidades, fuente, procedencia, calidad, missingness y
`code_version`. Los números se normalizan a ocho decimales y los timestamps a
UTC. El digest se verifica antes de insertar y cada vez que la API AS-OF relee
una observación. Una colisión de ID con otro digest produce error.

## 3. Semántica AS-OF

`read_market_evidence_as_of()` recibe owner, identidad completa, fuente y
cutoff. Sólo admite una observación cuando:

```text
candle_timestamp <= cutoff
bar_start         <= cutoff
bar_end           <= cutoff
available_at      <= cutoff
scraped_at        <= cutoff
created_at        <= cutoff
is_closed         = true
capture_status    = COMPLETE
```

Para cada `BAR_IDENTITY` elige la última versión por `available_at`,
`scraped_at`, `created_at` y `observation_id`. Una captura legacy sin lifecycle
sólo puede leerse si el caller activa explícitamente
`allow_legacy_without_lifecycle`; esa excepción se usó únicamente para auditar
G2 y queda marcada en la evidencia.

La API no inventa `bar_end`, `available_at`, cierre ni calendarios. Las barras
abiertas, futuras o procedentes de una captura `ABORTED/FAILED` quedan fuera.

## 4. Corrección posterior controlada

El ensayo usó TimescaleDB 2.30.2 sobre PostgreSQL 17. Para la misma identidad de
barra `BYMA:CEDEAR:G3ASSET:ARS / BYMA:G3ASSET / 1d /
2026-06-19T14:00:00Z` se persistieron:

| Versión | observation_id | close | digest |
|---|---|---:|---|
| A, T1 | `0e1c0592-ba57-5b59-8989-ada8e8e6e734` | `120.69442164` | `fabb3844bef9545046f4820aad01a08d11716e919d27f1fe8486ff8d23187576` |
| B, T2 | `6fe6dbc8-80b1-54de-8012-b7749f5224ca` | `123.94442164` | `1192c2233578099d8f285dcd2a5bd9dc76d972057621a7a44bee4025d0f54628` |

A siguió presente, B fue una fila nueva y ambas comparten `BAR_IDENTITY`. El
cutoff `2026-10-07T21:54:30.421174Z` eligió A; el cutoff
`2026-10-07T22:04:30.421174Z` eligió B. Repetir el cutoff anterior después de
insertar B volvió a elegir A. Una tercera versión dentro de una captura
`ABORTED` no fue promovida.

Evidencia:
[`contextual-g3-append-only.json`](evidence/contextual-g3-append-only.json) y
[`contextual-g3-asof-replay.json`](evidence/contextual-g3-asof-replay.json).

## 5. Snapshot determinista

El manifiesto reproducible conserva:

- IDs y digests exactos de activo y benchmark;
- cutoff, `code_version`, versión de contexto e input hash;
- snapshot contextual completo, `snapshot_id` y hash del resultado;
- vínculo a owner, run, plan y portfolio en la persistencia.

Dos ejecuciones con el mismo código y la misma evidencia produjeron manifiestos
idénticos. Insertar la corrección futura B y repetir el cutoff anterior tampoco
cambió el resultado. La relectura del snapshot persistido fue idéntica en dos
consultas. La persistencia ahora rechaza el mismo `snapshot_id` si el payload o
los vínculos no coinciden; un retry idéntico sigue siendo idempotente.

Evidencia:
[`contextual-g3-determinism.json`](evidence/contextual-g3-determinism.json).

## 6. Lifecycle e inmutabilidad

Cada captura nueva comienza con `STARTED` y sólo puede llegar a un estado
terminal: `COMPLETE`, `ABORTED` o `FAILED`. El owner no puede cambiar entre
eventos. Una captura terminal no puede reabrirse ni recibir otro terminal.

Una captura abortada conserva las observaciones ya recibidas, pero la API
AS-OF no las selecciona y el trigger no permite ligarlas a un snapshot válido.
El runner seguro registra `FAILED` con fase y tipo de error si se interrumpe.

Los triggers bloquean:

- `UPDATE` y `DELETE` de observaciones, snapshots, vínculos y eventos;
- `TRUNCATE` de esas cuatro tablas;
- inserciones sin digest o sin captura `STARTED`;
- snapshots sin owner/run/plan/portfolio coherentes;
- vínculos con capturas no completas;
- snapshots que dejen de ser `SHADOW_ONLY` o declaren autoridad productiva.

El digest permite detectar alteración de una observación al releerla. Un
superusuario todavía puede deshabilitar triggers; endurecer roles y permisos es
una medida operativa posterior, no una segunda arquitectura de datos.

## 7. Auditoría del run real G2

| Campo | Valor |
|---|---|
| run | `74db5161-562c-428a-b1cd-4620b9ee106b` |
| plan | `286f2ca7-d363-49a2-9fa4-cf5cee550dc9` |
| portfolio snapshot | `8a4276c1-3d4a-4e29-862b-6cf01450f91d` |
| owner | `1259412316` |
| cutoff | `2026-10-07T18:07:54.432215Z` |
| code version | `42957aa3a04eed391d3941548b4b7310b38709fd` |
| snapshot | `context:85b8a976a2a47748184fac50` |

La auditoría se ejecutó en una transacción `READ ONLY, REPEATABLE READ`.
Encontró 223 observaciones ligadas de IREN y 259 de SPY. Para ambos roles:

- los ordinals son contiguos y cada barra tiene identidad única;
- la lista explícita de `observation_id` coincide con el AS-OF replay;
- no hay `bar_end`, `available_at` ni `scraped_at` posteriores al cutoff;
- no hay barras abiertas ni futuras entre los inputs numéricos;
- los input hashes cerrados coinciden con el snapshot almacenado.

Al usar las 484 observaciones del mismo run se reconstruyeron exactamente el
payload, `snapshot_id`, hash e input hashes guardados. Las dos observaciones
adicionales son la última barra abierta de IREN y SPY. Influyeron únicamente en
`UNKNOWN_BAR_END`, pero G2 no las puso en `contextual_snapshot_candles`. Se
identifican por su mismo owner/run/ticker; la relación histórica no se inventó
ni se agregó después.

Los 484 digests legacy se calcularon durante la auditoría y están marcados
`LEGACY_UNSTORED`. El lifecycle del run permanece `LEGACY_UNKNOWN`. La lista
exacta de IDs y digests está en
[`contextual-g3-real-run-audit.json`](evidence/contextual-g3-real-run-audit.json).

## 8. Neutralidad productiva

El ensayo comparó el mismo payload productivo antes y después de adjuntar el
contexto. Los hashes fueron idénticos:
`1061c5b89afd75811c53728c6c402d6cc5c44f3cafb38555aa987f95a5dfc6e1`.

| Campo | Igual |
|---|---|
| scores | sí |
| signals | sí |
| decisions | sí |
| order intents | sí |
| quantities | sí |
| cash | sí |

El contrato persistido sigue siendo `mode=SHADOW_ONLY`,
`affects_analysis=false`, `affects_execution=false`. Órdenes ejecutadas: cero.
Evidencia:
[`contextual-g3-non-regression.json`](evidence/contextual-g3-non-regression.json).

## 9. Tests

La suite focalizada ejecutó **64 passed** e incluyó G1, E2, G2 y G3 más tres
integraciones reales contra TimescaleDB descartable. Cubre:

- múltiples observaciones de la misma barra y corrección posterior;
- AS-OF antes y después de la corrección;
- cutoff de `available_at`, `scraped_at`, `created_at` y `bar_end`;
- exclusión de barra abierta, observación futura y captura abortada;
- replay y hash de snapshot deterministas;
- lifecycle terminal;
- protección contra update, delete y truncate;
- idempotencia/collisions de persistencia;
- neutralidad E2 `SHADOW_ONLY`.

La suite completa terminó con **804 passed, 26 skipped, 7 failed**. Las siete
fallas son exactamente las baseline conocidas: menú/comandos Telegram,
dependencia ausente `pypfopt`, mock de radar exploratorio, header del radar
compacto, longitud de help, menú principal y línea shadow del radar compacto.
No apareció una falla nueva de G3.

## 10. Unknowns y gaps

Se conservan explícitamente como UNKNOWN:

- benchmark sectorial no configurado;
- ratio depositario;
- unidad contractual de volumen;
- calendario BYMA versionado.

Quedan estos gaps antes de considerar promoción o E3:

1. ejecutar una nueva corrida real con G3 para demostrar que todos los inputs,
   incluidas barras abiertas que aportan missingness, quedan ligados;
2. demostrar en esa corrida digests persistidos y lifecycle `COMPLETE` de punta
   a punta;
3. definir roles PostgreSQL que impidan deshabilitar triggers fuera de la ruta
   administrativa;
4. resolver unidades, calendario y ratio depositario antes de cualquier uso
   productivo del contexto.

La recomendación es **no comenzar E3 todavía**. Primero debe cerrarse el gap
prospectivo con una captura real nueva y una auditoría AS-OF read-only.
