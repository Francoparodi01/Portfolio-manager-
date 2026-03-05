# 📊 Cocos Portfolio — Sistema Cuantitativo Automatizado

Sistema de monitoreo y análisis cuantitativo de portfolio para **Cocos Capital**, construido sobre scraping automatizado con Playwright, análisis técnico/macro/fundamental en capas, y notificaciones vía Telegram.

---

## Arquitectura general

```
┌─────────────────────────────────────────────────────────────────┐
│                        SCHEDULER (cron)                         │
│                  10:30 ART  ·  17:00 ART                        │
└────────────────────┬────────────────────────────────────────────┘
                     │
          ┌──────────▼──────────┐
          │   run_once.py       │        ┌─────────────────────┐
          │   Scraper + DB      │───────▶│   TimescaleDB       │
          └──────────┬──────────┘        │   (portfolio snaps) │
                     │                   └─────────────────────┘
          ┌──────────▼──────────┐
          │   run_analysis.py   │
          │   Pipeline multicapa│
          └──────────┬──────────┘
                     │
     ┌───────────────┼────────────────┐
     │               │                │
┌────▼─────┐  ┌──────▼──────┐  ┌─────▼──────┐
│ Technical │  │    Macro    │  │  Sentiment │
│  yfinance │  │  yfinance   │  │  RSS feeds │
└────┬──────┘  └──────┬──────┘  └─────┬──────┘
     │               │                │
     └───────────────▼────────────────┘
                      │
              ┌───────▼────────┐
              │  Risk Engine   │
              │  Kelly + VIX   │
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │   Synthesis    │
              │  blend_scores  │
              │  + Claude API  │
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │    Telegram    │
              │   Notifier     │
              └────────────────┘
```

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Scraping | Python 3.12 · Playwright (Chromium headless) |
| Base de datos | TimescaleDB (PostgreSQL 16) · asyncpg · SQLAlchemy |
| Análisis técnico | yfinance · pandas · numpy |
| Sentiment | RSS feeds · VADER / scoring custom |
| LLM synthesis | Claude API (`claude-sonnet-4-20250514`) |
| Notificaciones | Telegram Bot API |
| Infraestructura | Docker · Docker Compose |
| Scheduler | APScheduler (10:30 / 17:00 ART) |

---

## Estructura del proyecto

```
cocos_portfolio/
├── src/
│   ├── analysis/
│   │   ├── technical.py      # RSI, MACD, Bollinger, EMAs, señales
│   │   ├── macro.py          # WTI, VIX, DXY, 10Y, SP500 + régimen
│   │   ├── sentiment.py      # RSS scraping + scoring por ticker
│   │   ├── risk.py           # Volatilidad, Kelly fraccionario, drawdown
│   │   └── synthesis.py      # Blend multicapa + Claude API
│   ├── collector/
│   │   ├── cocos_scraper.py  # Playwright: login MFA + portfolio + mercado
│   │   ├── db.py             # TimescaleDB: snapshots + precios
│   │   └── notifier.py       # Telegram: alertas + reportes HTML
│   ├── core/
│   │   ├── config.py         # Configuración centralizada desde .env
│   │   └── logger.py         # Logging estructurado + decorador @timed
│   └── scheduler/
│       └── runner.py         # APScheduler: jobs automáticos
├── scripts/
│   ├── run_once.py           # Scrape manual: portfolio + mercado
│   ├── run_analysis.py       # Análisis manual on-demand
│   └── init_db.py            # Inicialización del schema
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env                      # Credenciales (no commitear)
```

---

## Pipeline de análisis

El análisis combina **4 capas independientes** con pesos configurables:

### 1. Técnico (30%)
Descarga velas OHLCV desde yfinance y evalúa:
- **RSI** (14): sobreventa <30 = bullish, sobrecompra >70 = bearish
- **MACD**: cruce de histograma y línea de señal
- **Bollinger Bands**: posición relativa del precio en las bandas
- **EMAs** (12/26): tendencia de corto plazo
- **Golden/Death Cross** (SMA 20/50): tendencia de mediano plazo
- **Volumen**: confirmación de movimientos

El score raw (-9 a +9) se normaliza a (-1, +1) y preserva la dirección incluso en señal HOLD.

### 2. Macro (30%)
Descarga indicadores globales en tiempo real y calcula impacto por ticker:
- **WTI / Brent**: favorable para CVX, adverso para manufacturas intensivas en energía
- **VIX**: ajusta sizing del risk engine
- **DXY**: adverso para empresas LatAm como MELI
- **10Y Treasury**: adverso para growth/tech (NVDA, MU)
- **SP500**: régimen general de mercado

### 3. Risk Engine (25%)
Calcula position sizing óptimo basado en:
- **Volatilidad anualizada** (ventana 6m / 1y)
- **Kelly fraccionario** (33% del Kelly óptimo): win rate × avg win / avg loss
- **VOL_TARGET = 40%**: calibrado para portfolios concentrados de 4-8 posiciones
- **Ajuste por VIX**: -25% si VIX > 25, -50% si VIX > 35
- **Drawdown del portfolio**: -40% si drawdown > -10%, stop total si > -20%

### 4. Sentiment (15%)
Analiza artículos de noticias de las últimas 48hs vía RSS:
- Score ponderado de artículos positivos y negativos
- Fuentes: Yahoo Finance, MarketWatch, Reuters, Bloomberg RSS

### Síntesis final
```
score_final = Σ (layer_score × layer_weight)

BUY        si score ≥ +0.40
ACCUMULATE si score ≥ +0.15
HOLD       si -0.15 < score < +0.15
REDUCE     si score ≤ -0.15
SELL       si score ≤ -0.40

confianza = |score| × 0.6 + consensus_entre_capas × 0.4
```

Opcionalmente, el score se enriquece con razonamiento de **Claude API** que valida la decisión del sistema contra el contexto macro real y sugiere una acción concreta.

---

## Scraper

El scraper utiliza **Playwright** con Chromium headless para autenticarse en Cocos Capital y extraer:

- **Portfolio**: posiciones, cantidades, precios, valor de mercado, tenencia valorizada total, cash ARS
- **Mercado**: precios de CEDEARs y Acciones en tiempo real (55+ instrumentos)

Maneja:
- Login con email + password
- **MFA via Telegram**: el código de 6 dígitos se recibe automáticamente por el bot
- Tablas virtualizadas (scroll para forzar render del grid)
- Extracción de totales con 5 estrategias en cascada (fallback a suma de posiciones)
- Screenshots automáticos ante errores

---

## Variables de entorno

```env
# Cocos Capital
COCOS_USERNAME=tu@email.com
COCOS_PASSWORD=tu_password

# Base de datos
POSTGRES_USER=portfolio
POSTGRES_PASSWORD=portfolio_secret
POSTGRES_DB=portfolio

# Telegram
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_CHAT_ID=-100123456789

# Claude API (para síntesis LLM)
ANTHROPIC_API_KEY=sk-ant-...

# Scraper
HEADLESS=true
TIMEOUT_MS=60000
TELEGRAM_MFA_TIMEOUT=240
SCREENSHOT_ON_FAILURE=true
```

---

## Comandos principales

```bash
# ── Levantar infraestructura ───────────────────────────────────────
docker compose up -d db

# ── Scrape manual (portfolio + mercado) ───────────────────────────
docker compose run --rm scraper python scripts/run_once.py --full

# ── Análisis on-demand ────────────────────────────────────────────
docker compose run --rm scraper python scripts/run_analysis.py

# Variantes:
# Sin LLM (más rápido, ~30s):
docker compose run --rm scraper python scripts/run_analysis.py --no-llm

# Sin sentiment:
docker compose run --rm scraper python scripts/run_analysis.py --no-sentiment

# Modo rápido (sin LLM ni noticias):
docker compose run --rm scraper python scripts/run_analysis.py --no-llm --no-sentiment

# Con más historia técnica:
docker compose run --rm scraper python scripts/run_analysis.py --period 1y

# Tickers específicos (sin leer DB):
docker compose run --rm scraper python scripts/run_analysis.py --tickers AAPL MSFT GOOGL

# Sin enviar a Telegram:
docker compose run --rm scraper python scripts/run_analysis.py --no-telegram

# ── Scheduler automático (10:30 y 17:00 ART) ──────────────────────
docker compose up -d scraper

# ── Rebuild (solo si cambia Dockerfile o requirements.txt) ────────
docker compose build --no-cache scraper
```

---

## Reporte Telegram

El sistema envía un reporte HTML formateado con:

```
╔══════════════════════════════════════╗
║    ANALISIS CUANTITATIVO COMPLETO  ║
╚══════════════════════════════════════╝

📅 05/03/2026 14:02 ART
💼 Portfolio: $1,544,790 ARS

📊 CONTEXTO MACRO
   WTI $77.2 (+3.3%) | VIX 21.8 | DXY 98.9 | SP500 +0.8%

📋 SEÑALES POR ACTIVO

🟢 CVX  → ACCUMULATE  [██░░░] 39%
   Score: +0.156   Sizing: 4.0%
   technical  ░░░░░ +0.017
   macro      ██░░░ +0.125
   🧠 El alza de WTI favorece directamente los márgenes upstream...

📌 RESUMEN EJECUTIVO
   🟢 Acumular: CVX
   🟡 Mantener: MU, MELI
   🔴 Reducir:  NVDA
```

---

## Diseño de decisiones

**¿Por qué Kelly fraccionario al 33%?**
El Kelly completo es agresivo y sensible a errores de estimación. El 33% es un punto de equilibrio entre maximizar crecimiento esperado y controlar drawdown en un portfolio de pocas posiciones concentradas.

**¿Por qué VOL_TARGET = 40%?**
Con 4 posiciones en CEDEARs tech/energy, volatilidades del 20-66% son normales. Un VOL_TARGET del 15% (el estándar académico) generaría sizing de 2-3% para todas las posiciones — irreal para este tipo de portfolio concentrado en Argentina.

**¿Por qué HOLD no es siempre score=0?**
Un activo en zona HOLD puede tener score técnico de +2 o -2 (señales débiles pero direccionales). Ignorar ese raw score pierde información. El sistema normaliza `score_raw / 9` para preservar la dirección sin cruzar los umbrales BUY/SELL.

---

## Notas de desarrollo

- Los volúmenes `./src` y `./scripts` están montados en el contenedor — cambios en código **no requieren rebuild**
- Playwright descarga Chromium en el build (~280MB) — el rebuild tarda ~10 minutos
- El scheduler corre solo si `docker compose up -d scraper` (modo daemon)
- Los `run_once.py` y `run_analysis.py` son one-shot y no activan el scheduler

---

*Sistema cuantitativo multicapa — no es asesoramiento financiero.*
