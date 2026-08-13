# Documento operativo / runbook

## Levantar el sistema

1. Crear `.env` desde [.env.example](../.env.example).

```powershell
Copy-Item .env.example .env
```

2. Completar variables obligatorias.

3. Construir imagen.

```powershell
docker compose build
```

4. Levantar con DB local.

```powershell
docker compose --profile localdb up -d db scheduler telegram_bot monitor_api
```

5. Revisar estado.

```powershell
docker compose ps
docker compose logs --tail=100 scheduler
docker compose logs --tail=100 telegram_bot
docker compose logs --tail=100 monitor_api
```

Sin DB local, configurar `DATABASE_URL` externo y levantar:

```powershell
docker compose up -d scheduler telegram_bot monitor_api
```

## Servicios

| Servicio | Evidencia | Funcion |
|---|---|---|
| `db` | [docker-compose.yml](../docker-compose.yml) | TimescaleDB/Postgres local bajo perfil `localdb`. |
| `scheduler` | [src/scheduler/runner.py](../src/scheduler/runner.py) | Jobs y loops intradia. |
| `telegram_bot` | [scripts/telegram_bot.py](../scripts/telegram_bot.py) | Interfaz Telegram. |
| `monitor_api` | [src/monitor/api.py](../src/monitor/api.py) | API/dashboard read-only en `MONITOR_API_PORT`. |

## Variables de entorno necesarias

Minimas para runtime real:

- `COCOS_USERNAME`, `COCOS_PASSWORD`, `COCOS_TOTP_SECRET`.
- `DATABASE_URL`.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` si se usa Telegram.
- `MONITOR_API_TOKEN` para iniciar `monitor_api`.
- `MONITOR_API_PORT` por defecto `8010`.
- `HEADLESS`, `RETRY_ATTEMPTS`, `TIMEOUT_MS`, `SCREENSHOT_ON_FAILURE`.

Opcionales/relevantes:

- `REDIS_URL`.
- `PORTFOLIO_REFRESH_REQUEST_POLL_SECONDS` y
  `TELEGRAM_OPERATIONAL_SYNC_TTL_SECONDS` controlan el canal de refresco
  bajo demanda entre Telegram y la sesion persistente del scheduler.
- `SENTIMENT_PIPELINE_ENABLED`, `SENTIMENT_PIPELINE_INTERVAL_SECONDS`,
  `SENTIMENT_OLLAMA_MODEL`, `OLLAMA_URL`.
- `THESIS_SHADOW_ENABLED`.
- `ISSUER_EVENT_INGESTION_ENABLED`, `ISSUER_EVENT_INGESTION_INTERVAL_SECONDS`,
  `ISSUER_EVENT_INGESTION_STARTUP_DELAY_SECONDS`, `ISSUER_EVENT_INGESTION_SOURCES`,
  `ISSUER_EVENT_RECENT_EXIT_DAYS`, `UPCOMING_EVENTS_REFRESH_SOURCES`,
  `UPCOMING_EVENTS_RECENT_EXIT_DAYS`, `UPCOMING_EVENTS_POST_BALANCE_DAYS`,
  `SEC_USER_AGENT`.
- `FMP_API_KEY`, `FINNHUB_API_KEY` para fuentes opcionales de eventos.
- `SHADOW_CAUSAL_OLLAMA_MODEL`.
- `MULTIUSER_ENABLED`, `APP_ENCRYPTION_KEY` en multiusuario.

Fuente: [.env.example](../.env.example), [.env.multiuser.example](../.env.multiuser.example),
[src/core/config.py](../src/core/config.py).

## Jobs programados

Definidos en `_scheduler_main()` de [src/scheduler/runner.py](../src/scheduler/runner.py):

| Job | Hora ART | Funcion |
|---|---:|---|
| `account_session` | continuo | Una sesion Cocos propiedad del scheduler; atiende portfolio/movimientos periodicos y pedidos de Telegram. |
| `opening_portfolio_report` | 10:31 | Refresca mediante la sesion persistente y genera el reporte de apertura. |
| `post_open_portfolio_report` | 10:45 | Marca post-open. |
| `preclose_alerts_1615` | 16:15 | Alertas pre-cierre. |
| `preclose_alerts_1645` | 16:45 | Alertas pre-cierre. |
| `portfolio_eod` | 17:02 | Portfolio, movimientos y mercado EOD mediante la misma sesion persistente. |
| `build_daily_candles` | 17:05 | Construye velas internas. |
| `verify_daily_candles` | 17:10 | Verifica cobertura de velas. |
| `daily_analysis` | 17:12 | Analisis diario formal. |
| `thesis_shadow` | 17:18 | Shadow forecasts si `THESIS_SHADOW_ENABLED`. |
| `update_outcomes_daily` | 21:30 | Actualiza outcomes. |
| `sentiment_pipeline` | intervalo | Sentiment si `SENTIMENT_PIPELINE_ENABLED`. |
| `issuer_event_ingestion` | intervalo | Evidencia Yahoo/SEC/CNV/FMP/Finnhub si `ISSUER_EVENT_INGESTION_ENABLED`. |

`/portfolio`, `/analisis`, `/analisis_full` y el refresh administrativo no
abren Playwright en `telegram_bot`. Publican una solicitud en Redis; el
`scheduler` actualiza Cocos con su navegador autenticado, persiste el snapshot
y responde cuando DB/cache ya quedaron sincronizadas. Si el canal falla, el
bot informa que sirve la ultima foto disponible y no intenta un segundo login.

## Comandos de diagnostico

```powershell
docker compose ps
docker compose logs --tail=200 scheduler
docker compose logs --tail=200 telegram_bot
docker compose logs --tail=200 monitor_api
docker compose exec -T scheduler python scripts/run_confidence_audit.py --days 180 --no-telegram
docker compose exec -T scheduler python scripts/run_performance.py --days 90 --no-telegram
docker compose exec -T scheduler python scripts/run_decision_timeline.py --days 90
docker compose exec -T scheduler python scripts/outcome_status.py
docker compose exec -T scheduler python scripts/run_issuer_event_ingestion.py --dry-run --sources yahoo
docker compose exec -T scheduler python scripts/run_upcoming_events.py --no-telegram
```

El calendario Yahoo se guarda como evidencia secundaria en
`issuer_event_observations`: conserva simbolo de origen, fecha, ratio o EPS y
siempre queda con `actionable=false`. No modifica scoring, planes ni ordenes.
Los proximos balances de la cartera se consultan tambien desde Telegram con
`/events`, `/earnings` o `/balances`. La ventana de dos ruedas queda registrada
en `decision_log.layers.earnings_window_shadow`: informa si un gate futuro
habria bloqueado una compra nueva, pero no cambia la decision actual.

Monitor:

```text
GET http://localhost:8010/api/health
GET http://localhost:8010/api/ingestion
GET http://localhost:8010/api/decisions
GET http://localhost:8010/api/fills
GET http://localhost:8010/api/logs/recent
```

Los endpoints estan registrados en `create_app()` de [src/monitor/api.py](../src/monitor/api.py).

## Como auditar una corrida

1. Identificar `run_id` en `decision_log` o salida del analisis.
2. Ejecutar timeline:

```powershell
docker compose exec -T scheduler python scripts/run_decision_timeline.py --days 90
```

3. Revisar ledger:

```powershell
docker compose exec -T scheduler python scripts/run_decision_ledger.py --days 90 --no-telegram
```

4. Revisar fills:

```powershell
docker compose exec -T scheduler python scripts/run_performance.py --days 90 --no-telegram
```

5. Si el problema es bot vs humano:

```powershell
docker compose exec -T scheduler python scripts/run_override_audit.py --days 90 --no-telegram
```

## Como detectar fallas

- Scraper/MFA: errores en logs `scheduler`, screenshots en `screenshots`, baja
  `confidence_score` o snapshots stale.
- DB: `monitor_api /api/health` o fallas de `asyncpg`.
- Candles: `run_confidence_audit.py`, `run_verify_daily_candles()` y endpoint
  `/api/candles`.
- Fills no reconciliados: `run_decision_ledger.py`, endpoint `/api/fills`.
- Mezcla de scopes: revisar `metric_scope`, `is_primary_metric` y
  `run_intent` con `run_decision_timeline.py`.
- Sentiment/Ollama: logs de `run_sentiment_pipeline_job()` y tablas
  `sentiment_raw/scored/aggregated`.

## Recuperacion

### Backup y restore de Postgres local

El procedimiento cubre la base `db` del perfil `localdb`. Genera un dump binario
en el host, lo valida con `pg_restore --list` y no imprime credenciales.

Crear y verificar un backup:

```powershell
python scripts/postgres_maintenance.py backup
```

Verificar nuevamente un archivo existente:

```powershell
python scripts/postgres_maintenance.py verify outputs/backups/portfolio-AAAAMMDDTHHMMSSZ.dump
```

Antes de restaurar, detener los servicios que escriben en la base:

```powershell
docker compose stop scheduler telegram_bot monitor_api
python scripts/postgres_maintenance.py restore outputs/backups/portfolio-AAAAMMDDTHHMMSSZ.dump --confirm-database portfolio
docker compose up -d scheduler telegram_bot monitor_api
```

`restore` usa una transaccion, `--clean --if-exists` y exige que
`--confirm-database` coincida exactamente con `--database`. No ejecutar este
comando sobre una base externa: usar el mecanismo de snapshots/restores del
proveedor correspondiente.

### Smoke Docker

Validar servicios activos y el monitor sin disparar scraping ni analisis:

```powershell
python scripts/docker_smoke.py
python scripts/docker_smoke.py --with-local-db --with-frontend
```

Si `MONITOR_API_TOKEN` esta disponible en el entorno, el smoke consulta
`/api/health` y exige `database.ok=true`. Sin token valida el endpoint publico
`/api/auth/status` y deja una advertencia explicita.

- Reiniciar servicios:

```powershell
docker compose restart scheduler telegram_bot monitor_api
```

- Reconstruir imagen si cambio codigo/dependencias:

```powershell
docker compose build
docker compose up -d scheduler telegram_bot monitor_api
```

- Reprocesar outcomes:

```powershell
docker compose exec -T scheduler python scripts/recompute_outcomes.py
```

- Sincronizar fills/movimientos:

```powershell
docker compose exec -T scheduler python scripts/run_once.py --fills --no-telegram
```

- Correr una auditoria de confianza:

```powershell
docker compose exec -T scheduler python scripts/run_confidence_audit.py --days 180 --no-telegram
```

Pendiente de definir: rotacion formal de secretos y objetivos RTO/RPO. El
backup/restore local debe probarse periodicamente en una base descartable; la
existencia de un dump no reemplaza una prueba de recuperacion.

## Logs relevantes

- Docker stdout/stderr de `scheduler`, `telegram_bot`, `monitor_api`.
- Volumen `./logs:/app/logs` para scheduler/bot/monitor segun
  [docker-compose.yml](../docker-compose.yml).
- `/api/logs/recent` redacted por [src/monitor/api.py](../src/monitor/api.py).
- `src/core/logger.py` redacciona tokens, passwords, secrets y URLs de DB/Redis.
