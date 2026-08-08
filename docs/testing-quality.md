# Documento de testing y calidad

## Inventario de tests

Inventario estatico local al 2026-07-28:

- 80 archivos Python bajo [tests](../tests).
- 314 definiciones `def test_` detectadas con PowerShell.
- `python -m pytest -q` ejecuto 312 casos: 310 pasaron y 2 fueron skip.

Nota de versionado: `.gitignore` ignora `tests/*` y habilita excepciones por
archivo. Por eso parte de la suite local puede no estar versionada todavia. En
esta entrega se versiona tambien
[tests/test_broker_fill_db.py](../tests/test_broker_fill_db.py), porque fue el
test que bloqueo la validacion.

Falla observada y corregida en esta validacion:

- `tests/test_broker_fill_db.py::test_save_broker_fills_upserts_rows`.
- Motivo: el mock `_RecordingConnection.fetch()` del test acepta solo el query,
  pero `src/collector/db.py::_mark_superseded_broker_fills_for_real()` llama
  `conn.fetch(query, source, external_fill_id, SUPERSEDED_BROKER_FILL_REASON)`.
- Correccion: el test double ahora acepta `*args` y devuelve vacio para las
  consultas de marcado superseded.

## Que tests existen

| Area | Tests |
|---|---|
| Planner, nominales y rotaciones | [tests/test_execution_nominals_and_rotation.py](../tests/test_execution_nominals_and_rotation.py) |
| Decision timeline/context | [tests/test_decision_timeline.py](../tests/test_decision_timeline.py), [tests/test_decision_context.py](../tests/test_decision_context.py) |
| Universo Cocos y frescura de precios | [tests/test_cocos_universe_db.py](../tests/test_cocos_universe_db.py) |
| Cash-only portfolio | [tests/test_cash_only_portfolio.py](../tests/test_cash_only_portfolio.py) |
| Broker fills/movements/dedupe/superseded | [tests/test_broker_fill_db.py](../tests/test_broker_fill_db.py), [tests/test_broker_movement_dedupe.py](../tests/test_broker_movement_dedupe.py), [tests/test_superseded_broker_fills.py](../tests/test_superseded_broker_fills.py), [tests/test_manual_broker_run_context.py](../tests/test_manual_broker_run_context.py) |
| Viability | [tests/test_viability_audit.py](../tests/test_viability_audit.py) |
| Trend/regime/sell guards | [tests/test_trend_regime.py](../tests/test_trend_regime.py), [tests/test_trend_sell_guard.py](../tests/test_trend_sell_guard.py) |
| Shadow | [tests/test_thesis_shadow_store.py](../tests/test_thesis_shadow_store.py), [tests/test_shadow_causal.py](../tests/test_shadow_causal.py) |
| Sentiment | [tests/test_sentiment_fetcher_sources.py](../tests/test_sentiment_fetcher_sources.py), [tests/test_offhours_sentiment_policy.py](../tests/test_offhours_sentiment_policy.py) |
| Overrides/human decision | [tests/test_override_classification.py](../tests/test_override_classification.py), [tests/test_manual_market_events.py](../tests/test_manual_market_events.py), [tests/test_intraday_revalidation.py](../tests/test_intraday_revalidation.py) |
| Feature snapshots/output perf | [tests/test_feature_snapshot.py](../tests/test_feature_snapshot.py), [tests/test_output_perf.py](../tests/test_output_perf.py) |

## Que cubren

- Cash accounting, funding por ventas, nominales enteros y montos reales.
- Persistencia de precio de referencia de radar/execution plan.
- Reconciliacion y exclusiones de fills sinteticos reemplazados por reales.
- Eventos manuales que bloquean compras nuevas o marcan riesgo.
- Separacion bot-only vs manual-only en viability.
- Shadow causal fuera de `decision_log`.
- Fuente/frescura de precios Cocos.
- Decision timeline con gaps de lifecycle.
- Off-hours como exploratory/no-persist y alertas sentiment 24/7 sin modificar
  plan formal.

## Que no cubren o no queda confirmado

- No confirmado en el repo: tests end-to-end contra Cocos real.
- No confirmado en el repo: tests de carga/performance sobre DB grande.
- No confirmado en el repo: tests de seguridad completos para monitor remoto.
- No confirmado en el repo: simulacion completa multiusuario.
- Pendiente de validar: cobertura total de `scripts/telegram_bot.py`, UI estatica
  del monitor y paths de Docker Compose, aunque hay tests parciales.
- Existe `scripts/docker_smoke.py` para config, servicios, DB y endpoints. Sigue
  pendiente un E2E controlado contra Cocos real; el smoke no dispara scraping.

## Como correr tests

Suite completa:

```powershell
python -m pytest
```

Foco planner/auditoria:

```powershell
python -m pytest tests/test_execution_nominals_and_rotation.py tests/test_viability_audit.py tests/test_decision_timeline.py
```

Foco shadow/sentiment:

```powershell
python -m pytest tests/test_shadow_causal.py tests/test_thesis_shadow_store.py tests/test_sentiment_fetcher_sources.py
```

Foco broker/fills:

```powershell
python -m pytest tests/test_broker_movement_dedupe.py tests/test_superseded_broker_fills.py
```

Foco operativo y persistencia de plan:

```powershell
python -m pytest tests/test_operational_tools.py tests/test_execution_nominals_and_rotation.py
python scripts/docker_smoke.py --with-local-db --with-frontend
```

## Validaciones manuales necesarias

- Levantar Docker y confirmar `docker compose ps`.
- Confirmar login/scrape Cocos con MFA y screenshots si falla.
- Abrir monitor en `http://localhost:8010/` con token.
- Ejecutar `run_confidence_audit.py` contra DB real.
- Ejecutar `run_analysis.py --no-telegram --no-llm --no-sentiment` y revisar que
  el plan pase `validate_execution_plan()`.
- Ejecutar `run_decision_ledger.py` y confirmar que fills/movements reales se
  reconstruyen sin gaps criticos.
- Validar manualmente que Telegram no exponga secretos y que HTML sea valido
  para Telegram donde corresponda.

## Riesgos tecnicos pendientes

- Dependencia de scraping ante cambios del broker.
- Contratos JSONB en `layers` aun parcialmente informales.
- `decision_log` sigue sobrecargado, aunque plan/orden ya se persisten en tablas
  aditivas enlazadas al ledger.
- Faltan IDs normalizados para optimizer, risk y snapshot de mercado.
- Riesgo de que tests unitarios no detecten fallas de runtime Docker/Cocos.
- Riesgo de encoding en documentacion historica; nuevos docs usan ASCII.

## Validacion de sintaxis recomendada

```powershell
python -m compileall src scripts
```

Esta validacion compila modulos Python; no reemplaza tests ni smoke Docker.
