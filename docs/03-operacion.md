# Operacion

## Docker

Base externa:

```bash
docker compose build
docker compose up -d scheduler telegram_bot monitor_api
```

Base local:

```bash
docker compose --profile localdb up -d db scheduler telegram_bot monitor_api
```

Estado:

```bash
docker compose ps
docker compose logs --tail=100 scheduler
docker compose logs --tail=100 telegram_bot
docker compose logs --tail=100 monitor_api
```

Rebuild puntual:

```bash
docker compose up -d --build scheduler telegram_bot
docker compose up -d --build monitor_api
```

## Telegram

| Comando | Funcion |
|---|---|
| `/start`, `/menu` | menu principal |
| `/portfolio` | ultimo snapshot |
| `/analisis`, `/analysis` | plan de cartera |
| `/radar`, `/radar_full` | oportunidades |
| `/performance` | outcomes y performance |
| `/confidence`, `/confianza` | auditoria de confianza |
| `/regression`, `/regression_audit` | auditoria estadistica |
| `/calibration`, `/dcl` | decision calibration layer |
| `/status` | estado operativo |
| `/admin_scrape` | scrape admin |
| `/admin_refresh_portfolio` | refresco admin |

## CLI

Portfolio y mercado:

```bash
docker compose exec scheduler python scripts/run_once.py
docker compose exec scheduler python scripts/run_once.py --full
docker compose exec scheduler python scripts/run_once.py --fills
```

Analisis:

```bash
docker compose exec scheduler python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
docker compose exec scheduler python scripts/run_opportunity.py --no-telegram
docker compose exec scheduler python scripts/run_performance.py --no-telegram
docker compose exec scheduler python scripts/run_confidence_audit.py --no-telegram
docker compose exec scheduler python scripts/run_regression_audit.py --mode execution
docker compose exec scheduler python scripts/run_calibration.py --no-telegram
```

Outcomes:

```bash
docker compose exec scheduler python scripts/update_outcomes.py
docker compose exec scheduler python scripts/outcome_status.py
```

Fills:

```bash
docker compose exec scheduler python scripts/sync_cocos_fills.py
docker compose exec scheduler python scripts/import_broker_fills.py /app/logs/fills.csv
```

Backfill:

```bash
docker compose exec scheduler python scripts/backfill_tradingview_byma.py --asset-type ALL --bars 260
docker compose exec scheduler python scripts/backfill_tradingview_byma.py --tickers ASTS --bars 260
```

## Monitor API

URL local:

```text
http://localhost:8010/
```

Endpoints:

| Endpoint | Uso |
|---|---|
| `/api/health` | bot, scheduler, DB, mercado |
| `/api/ingestion` | ultimo portfolio, ultimo price, conteos |
| `/api/candles` | cobertura de velas |
| `/api/decisions` | decision log |
| `/api/fills` | fills y reconciliacion |
| `/api/logs/recent` | errores recientes sin secretos |

Auth:

- `Authorization: Bearer <MONITOR_API_TOKEN>`
- o `X-API-Token: <MONITOR_API_TOKEN>`
- TOTP opcional con `MONITOR_TOTP_SECRET`

El monitor es read-only.

## Variables de Entorno

| Variable | Uso |
|---|---|
| `DATABASE_URL` | conexion PostgreSQL |
| `COCOS_USERNAME` / `COCOS_EMAIL` | usuario Cocos |
| `COCOS_PASSWORD` | password Cocos |
| `COCOS_TOTP_SECRET` | MFA Cocos opcional |
| `TELEGRAM_BOT_TOKEN` | bot |
| `TELEGRAM_CHAT_ID` | destino default |
| `ADMIN_CHAT_IDS` | admins |
| `REDIS_URL` | Redis opcional |
| `MONITOR_API_TOKEN` | token monitor |
| `MONITOR_TOTP_SECRET` | TOTP monitor opcional |
| `COCOS_SYNC_FILLS` | habilita sync programado de fills |

Nunca versionar `.env`.

