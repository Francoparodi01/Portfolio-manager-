# Decision Lab: auditoría de fuentes previa al replay

Corte de inspección: 2026-09-24. Base de código: `7558c61`. Checkout operativo
`cocos_copilot`, con cambios previos preservados; desarrollo aditivo en
`feature/decision-lab-pit-replay`. Esta auditoría no escribe fuentes operativas.

La fecha del hecho no demuestra cuándo Quantia conoció el dato. Toda lectura PIT
debe exigir `effective_at <= T` **y** `available_at <= T`, conservando revisión,
fuente y hash. Un dato actual no se vuelve histórico por ponerle un timestamp viejo.

| Fuente | Clasificación actual | Motivo y uso permitido |
|---|---|---|
| portfolio_snapshots / positions / raw_snapshots | APPROXIMATE | Hay scraped_at y created_at, pero save_snapshot hace upsert y reemplaza positions. Congelar payload y hashes; no certificar inmutabilidad retrospectiva. |
| market_prices | APPROXIMATE | ts de observación, sin reloj separado de ingesta/revisión; upsert. No prueba primer conocimiento ni historia de universo. |
| market_candles | RECONSTRUCTIBLE | ts + scraped_at permiten rechazar backfills posteriores a T, pero upsert elimina vintages anteriores. La admisibilidad depende además de ajuste, cierre y origen. |
| Cocos/BYMA observado | APPROXIMATE | Precio local en ARS; no existe contrato explícito de ajuste/vintage. Congelar y declarar convención. |
| Yahoo histórico | UNSAFE_FOR_REPLAY por defecto | auto_adjust/repair y eventos posteriores pueden alterar historia. Requiere RAW_AS_TRADED o POINT_IN_TIME_ADJUSTED con prueba; no inferir seguridad del nombre del proveedor. |
| FX / CCL / macro | CURRENT_STATE_ONLY | fetch_macro consulta hoy; no hay tabla de vintages macro. Los scores macro guardados no reconstruyen los inputs. No llamar fetch_macro dentro de replay. |
| técnico | RECONSTRUCTIBLE | analyze_ticker_from_frame admite frames; sólo barras cerradas, conocidas en T y con ajuste documentado. |
| riesgo | RECONSTRUCTIBLE | compute_asset_risk acepta series; necesita VIX, cartera y drawdown previos con provenance. |
| optimizer | RECONSTRUCTIBLE | Acepta history_frames. Su fillna(0) obliga a exigir previamente panel alineado y completo; no imputar huecos silenciosamente. Registrar engine/dependencias/fallback. |
| synthesis / planner | RECONSTRUCTIBLE | Funciones reutilizables con inputs explícitos. Hay que congelar versiones y configuración; no suponer que el código actual existía en T. |
| sentiment_raw/scored | RECONSTRUCTIBLE | Exigir published_at, fetched_at, created_at y scored_at <= T. Timestamp de noticia solo no basta; modelo y revisiones del texto deben justificarse. |
| sentiment_aggregated | APPROXIMATE | updated_at y upserts; agregados recalculados no representan necesariamente el bucket original. |
| noticias/eventos/earnings | APPROXIMATE | Hay publicación/created/updated en parte de las fuentes, pero observaciones actualizadas sin vintages. Rechazar versión actualizada después de T. |
| manual_market_events | RECONSTRUCTIBLE | Ventana efectiva + created_at/updated_at; excluir eventos ingresados/revisados después de T, aunque tengan fecha histórica. |
| corporate_events/effects | APPROXIMATE | effective_at no equivale a anuncio/ingesta; faltan publicaciones en eventos reales y no hay certificación de cobertura completa. |
| universo/catálogo | CURRENT_STATE_ONLY | Una aparición en market_prices demuestra observación, no listado/deslistado, permiso de operación ni catálogo completo en T. |
| decision_log | APPROXIMATE | Ledger mixto y mutable con outcomes futuros. Proyectar exclusivamente campos de decisión y feature_snapshot; jamás pasar outcomes al adapter. |
| execution_plans / order_intents | APPROXIMATE | Plan explícito, cantidades, restricciones y clocks; sirve para RECORDED_PLAN_EVALUATION. No demuestra reproducción de una estrategia histórica ni toda su configuración. |
| broker_fills / movements | APPROXIMATE | Ejecución observada, timestamps de distinta precisión y reconciliaciones posteriores. Sólo evaluación humana separada con cobertura y vínculo comprobados. |
| outcomes existentes | UNSAFE_FOR_REPLAY como input | Son etiquetas futuras. Su engine mezcla sync/escrituras y semántica por ticker; no llamarlo desde reconstrucción. |
| Radar | APPROXIMATE | Snapshots/runs con versiones; diferenciar universo evaluado del catálogo completo. No seleccionar candidatos con su outcome. |
| Shadow | APPROXIMATE | Versiones/features útiles pero no autoridad productiva; owners legacy y protocolos diferentes requieren poblaciones separadas. |
| feature snapshots | RECONSTRUCTIBLE | Hash y payload en layers.run_context/layers.feature_snapshot; hash no prueba disponibilidad de las fuentes originales. |
| estrategia/config/modelo | CURRENT_STATE_ONLY / UNKNOWN histórico | Algunos run_context tienen strategy/planner/optimizer_version='unknown'; no asociarles retrospectivamente una versión actual. |

No se certificó una fuente legacy entera como POINT_IN_TIME_SAFE. El nuevo contrato
permite ese estado para evidencia con clocks, ajuste e inmutabilidad comprobables;
no lo asigna automáticamente a estas tablas.

## Evidencia observada

- 5.244 snapshots totales desde 2026-04-15; 1.422 con owner explícito desde
  2026-08-21. La fecha ilustrativa 2026-03-03 no tiene cartera histórica disponible.
- 1.639.749 market_prices y 247.969 market_candles. Las velas incluyen backfills
  hasta 2000, pero la ingesta comienza en mayo de 2026: **no son evidencia conocida
  por Quantia en 2000**.
- 112 execution_plans y 664 order_intents desde 2026-08-10; todos los planes tienen
  owner NULL. Sólo pueden vincularse a la cuenta con verificación single-owner y
  la suposición explícita LEGACY_OWNER_INFERRED. No convertir NULL en comodín.
- Ninguno de esos 112 planes muestra updated_at posterior a created_at más de un
  minuto. Esto no prueba ausencia de mutaciones no versionadas.
- Los feature snapshots existen anidados en layers; buscar sólo una columna o una
  clave de primer nivel los omite. Se encontraron versiones históricas `unknown`.
- La validación de enlaces encontró **82 de 112 planes** vinculados a filas de
  `decision_log` sobrescritas por otro `run_id`, con contexto de cartera posterior.
  Se rechazan con `MUTATED_CROSS_RUN_DECISION_LINK`; no se usan para reconstruir T.
  Los 30 restantes permiten evaluar la propuesta registrada como evidencia LOW.
  En 11 de esos planes, los nominales/precios faltantes pertenecían a órdenes
  bloqueadas: se conservan nulos y no se ejecutan; no invalidan otras órdenes.
- `cash_before` del planner se redondea a pesos enteros. El vínculo verifica esa
  convención declarada y conserva el cash exacto del snapshot para la contabilidad;
  no amplía una tolerancia arbitraria para aceptar snapshots incompatibles.
- 62.146 noticias raw y 55.480 scores; una noticia publicada en 2017 e ingerida
  en 2026 no es input admisible de 2017.
- 2 corporate events, ambos sin source_published_at. No equivalen a un registro
  exhaustivo de dividendos, splits y ratios.

Conteos temporales, no garantías permanentes. Recibos privados en el checkout
operativo: `tmp/decision-lab/source-schema.json`, `source-profile.json`.

## Decisión de arquitectura

Separar tres poblaciones: HISTORICAL_POLICY_REPLAY, CURRENT_POLICY_ON_HISTORICAL_DATA
y RECORDED_PLAN_EVALUATION. Esta última permite evaluar un plan realmente guardado
sin afirmar que se reprodujo la estrategia que lo produjo. Datos ambiguos quedan
fuera de estadísticas primarias. Un replay estricto sin inputs suficientes devuelve
INSUFFICIENT; no se fabrica un pasado para aumentar n.

El calendario existente se reutiliza para sesiones BYMA. El engine de outcomes
legacy no se reutiliza como procedimiento porque escribe y su grain es diferente;
se conserva la convención de sesiones con entrada posterior a la decisión, usando
un evaluador puro de cartera común. Analytics v2 sigue separado.

La captura nueva en `decision_lab_plan_captures` conserva plan, cartera y evidencia
recibida al persistir planes futuros. No puede reparar los 82 vínculos históricos ni
crear vintages ausentes. Ver [validación y despliegue](decision-lab-validation.md).

Referencias técnicas: [código oficial yfinance](https://github.com/ranaroussi/yfinance/blob/main/yfinance/scrapers/history.py)
para ajustes/reparaciones; [bootstrap temporal de arch](https://arch.readthedocs.io/en/stable/bootstrap/timeseries-bootstraps.html)
para dependencia temporal. No se agrega ninguna de esas librerías al motor.
