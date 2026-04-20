# REFERENCIA DE COMANDOS
### Cocos Copilot — Guía completa de CLI y Telegram

---

## Bot de Telegram

Todos los comandos están disponibles tanto como slash-commands como desde el menú de botones inline que aparece al ejecutar `/start`. Después de cada respuesta, el bot muestra el menú automáticamente.

| Comando | Descripción |
|---------|-------------|
| `/start` | Abre el menú principal con botones inline |
| `/portfolio` | Último snapshot: total, cash, posiciones con P&L y peso |
| `/analisis` | Pipeline cuantitativo completo (~2-3 min) |
| `/analisis_rapido` | Pipeline sin LLM ni sentiment (~30-60 seg) |
| `/scrape` | Scrape manual del portfolio — muestra posiciones al terminar |
| `/oportunidades` | Radar de oportunidades externas (~2-3 min) |
| `/performance` | Win rate, EV y últimas decisiones del sistema |
| `/status` | Verifica conectividad de Redis Cloud y base de datos |
| `/ayuda` | Lista completa de comandos |

> **MFA**: el sistema genera el código automáticamente via TOTP (si `COCOS_TOTP_SECRET` está en el `.env`). No se requiere intervención manual.

---

## CLI — Scrape

### run_once.py

```bash
docker compose run --rm scraper python scripts/run_once.py
docker compose run --rm scraper python scripts/run_once.py --full
docker compose run --rm scraper python scripts/run_once.py --no-db
docker compose run --rm scraper python scripts/run_once.py --json output.json
```

| Flag | Efecto |
|------|--------|
| (sin flags) | Login + scrape portfolio + guardar en DB |
| `--full` | Portfolio + precios de mercado (acciones + CEDEARs) |
| `--no-db` | Scrape sin guardar en base de datos |
| `--json FILE` | Exportar snapshot completo como JSON |
| `--no-telegram` | No enviar notificaciones Telegram |

---

## CLI — Análisis cuantitativo

### run_analysis.py

```bash
docker compose run --rm scraper python scripts/run_analysis.py
docker compose run --rm scraper python scripts/run_analysis.py --no-llm --no-sentiment
docker compose run --rm scraper python scripts/run_analysis.py --no-telegram
docker compose run --rm scraper python scripts/run_analysis.py --tickers CVX NVDA
docker compose run --rm scraper python scripts/run_analysis.py --period 1y
```

| Flag | Efecto |
|------|--------|
| `--no-llm` | Omite el razonamiento LLM |
| `--no-sentiment` | Omite análisis RSS de noticias |
| `--no-optimizer` | Omite el portfolio optimizer |
| `--no-telegram` | Imprime en consola sin enviar a Telegram |
| `--tickers A B C` | Analiza solo los tickers especificados |
| `--period 1y` | Período de historia: 1mo, 3mo, 6mo, 1y, 2y (default: 6mo) |

---

## CLI — Radar de oportunidades

### run_opportunity.py

```bash
docker compose run --rm scraper python scripts/run_opportunity.py
docker compose run --rm scraper python scripts/run_opportunity.py --top 5 --min-score 0.15 --min-rr 1.5
docker compose run --rm scraper python scripts/run_opportunity.py --universe AVGO TSM MSFT AMD
docker compose run --rm scraper python scripts/run_opportunity.py --include-portfolio
```

| Flag | Efecto |
|------|--------|
| `--universe A B C` | Evaluar solo estos tickers (default: universo Cocos ~80) |
| `--top N` | Devolver solo los N mejores setups |
| `--min-score X` | Score mínimo para aparecer en el reporte (ej: 0.15) |
| `--min-rr X` | R/R mínimo — elimina asimetrías pobres (ej: 1.5) |
| `--include-portfolio` | Incluir tickers actuales del portfolio en el análisis |
| `--period 1y` | Período de historia de precios (default: 1y) |
| `--no-sentiment` | Omitir análisis RSS |
| `--no-telegram` | Solo consola |

---

## CLI — Performance del sistema

### run_performance.py

```bash
docker compose run --rm scraper python scripts/run_performance.py
docker compose run --rm scraper python scripts/run_performance.py --days 60
docker compose run --rm scraper python scripts/run_performance.py --no-telegram
```

| Flag | Efecto |
|------|--------|
| `--days N` | Lookback en días (default: 90) |
| `--no-telegram` | Solo consola |

---

## CLI — Outcomes de decisiones

### update_outcomes.py

Rellena `outcome_5d / 10d / 20d` y `was_correct` para decisiones pasadas usando yfinance. Correr una vez por día idealmente.

```bash
docker compose run --rm scraper python scripts/update_outcomes.py
docker compose run --rm scraper python scripts/update_outcomes.py --days 60
```

---

## CLI — Base de datos

```bash
# Conectarse
docker exec -it cocos_db psql -U portfolio -d portfolio
```

---

## Docker — Operaciones

```bash
# Setup completo
docker compose build --no-cache
docker compose up -d
docker compose ps

# Rebuild de un solo servicio (después de cambiar código)
docker compose build scraper
docker compose build telegram_bot
docker compose up -d telegram_bot

# Verificar que el código nuevo quedó en la imagen
docker compose run --rm scraper grep -c "TOTP generado" src/collector/cocos_scraper.py
docker compose exec telegram_bot grep -c "action_portfolio" scripts/telegram_bot.py

# Logs
docker compose logs -f scraper
docker compose logs -f telegram_bot
docker compose logs --tail=100 scraper

# Restart / apagar  
docker compose restart telegram_bot
docker compose restart scraper
docker compose down

# Backup DB
docker exec cocos_db pg_dump -U portfolio portfolio > backup_$(date +%Y%m%d).sql
```


---

## Git — Flujo de trabajo

```bash
# Ver en qué rama estás
git branch

# Crear rama de desarrollo (una sola vez)
git checkout -b dev
git push origin dev

# Flujo diario — siempre desarrollar en dev
git checkout dev
git add .
git commit -m "feat: descripción del cambio"
git push origin dev

# Cuando está probado, mergear a main
git checkout main
git merge dev
git push origin main

# Volver a dev para seguir trabajando
git checkout dev
```

> **Regla**: `main` = lo que está corriendo en producción. Nunca commiteás directo a `main`. Todo pasa primero por `dev`.

---

## Scheduler automático

| Horario (ART) | Acción |
|---------------|--------|
| 10:30 | Scrape portfolio → guarda DB → notifica Telegram |
| 17:00 | Scrape portfolio + mercado completo → Telegram |

```bash
# Verificar que el scheduler está activo
docker compose logs -f scraper | grep "Scheduler activo"
```

---

## TOTP — MFA automático

El sistema genera el código de 6 dígitos automáticamente si `COCOS_TOTP_SECRET` está configurado en el `.env`. No se requiere intervención manual.