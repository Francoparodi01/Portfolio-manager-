# Cocos Copilot

Cocos Copilot es un sistema cuantitativo y auditable para operar una cartera real
de Cocos Capital con disciplina de datos. No ejecuta ordenes automaticamente y no
es asesoramiento financiero: observa, analiza, propone planes, registra decisiones
y mide resultados.

El objetivo del proyecto es construir confianza operativa:

- saber que datos entraron;
- distinguir precio observado, inferencia, decision teorica y ejecucion real;
- evitar recomendaciones con datos insuficientes;
- reconciliar fills reales;
- medir outcomes, performance, regresion y calibracion.

## Estado Actual

El sistema ya cuenta con:

- scraping autenticado de portfolio, mercado, CEDEARs por apartado y movimientos;
- persistencia en PostgreSQL/TimescaleDB;
- historico de velas desde Cocos, TradingView/BYMA e snapshots internos;
- analisis de cartera con tecnico, macro, riesgo, sentiment opcional y optimizer;
- execution planner con guards operativos;
- radar de oportunidades sobre universo Cocos;
- decision log, fills, movements, outcomes, performance, regression audit y DCL;
- bot de Telegram;
- scheduler;
- API read-only de monitoreo con token y TOTP opcional;
- dashboard local en `http://localhost:8010/`.

## Principio Central

```text
El scraper observa.
El analisis interpreta.
El optimizer propone.
El execution planner decide que es operable.
Los fills confirman que paso en la realidad.
Los outcomes miden si hubo edge.
```

## Arranque Rapido

```bash
cp .env.example .env
docker compose build
docker compose up -d scheduler telegram_bot monitor_api
```

Con DB local:

```bash
docker compose --profile localdb up -d db scheduler telegram_bot monitor_api
```

Verificacion:

```bash
docker compose ps
docker compose logs --tail=100 scheduler
docker compose logs --tail=100 telegram_bot
```

## Comandos Principales

Telegram:

```text
/portfolio
/analisis
/radar
/performance
/confidence
/regression
/calibration
/status
```

CLI:

```bash
docker compose exec scheduler python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
docker compose exec scheduler python scripts/run_opportunity.py --no-telegram
docker compose exec scheduler python scripts/run_performance.py --no-telegram
docker compose exec scheduler python scripts/run_confidence_audit.py --no-telegram
docker compose exec scheduler python scripts/run_regression_audit.py --mode execution
```

## Documentacion

La documentacion profesional vive en `docs/`:

- [Indice general](docs/00-indice.md)
- [Arquitectura](docs/01-arquitectura.md)
- [Datos y persistencia](docs/02-datos-y-persistencia.md)
- [Operacion](docs/03-operacion.md)
- [Analitica y decision](docs/04-analitica-y-decision.md)
- [Mantenimiento](docs/05-mantenimiento.md)
- [Seguridad y acceso remoto](docs/06-seguridad-y-publicacion.md)

## Seguridad

Este proyecto esta pensado para uso local-first. No compartas `.env`, logs,
screenshots, dumps de DB, sesiones, cookies, fills reales ni movimientos reales.
El monitor debe quedar en `127.0.0.1` salvo que uses tunnel/proxy con auth.

Atajos historicos:

- [ARQUITECTURA.md](ARQUITECTURA.md)
- [COMANDOS.md](COMANDOS.md)

## Disclaimer

Proyecto personal de disciplina cuantitativa. No es asesoramiento financiero, no
garantiza resultados y no debe usarse como sistema autonomo de ejecucion.
