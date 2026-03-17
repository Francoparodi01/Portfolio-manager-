# COCOS COPILOT
### Sistema Cuantitativo Multicapa para Portfolio de CEDEARs
*Cocos Capital · Argentina · 2026*

---

## ¿Qué es Cocos Copilot?

Cocos Copilot es un sistema autónomo de análisis y gestión de portfolio para CEDEARs en Cocos Capital. Combina scraping automatizado, análisis técnico profesional, contexto macro local e internacional, análisis de sentimiento de noticias, gestión de riesgo y optimización matemática de portfolio — todo integrado en un bot de Telegram.

El sistema responde tres preguntas fundamentales:

- **¿Qué hago con lo que ya tengo?** → Portfolio Analyzer
- **¿Qué debería incorporar?** → Opportunity Radar
- **¿Adónde va el capital que se libera?** → Rotation Engine

> El sistema es cuantitativo puro: las decisiones las toma el modelo matemático, no el LLM. El razonamiento del LLM es solo display explicativo.

---

## Características principales

### Scraper automatizado
Login en Cocos Capital con soporte MFA via Telegram. El código de verificación de 6 dígitos se envía por el bot y se comunica con el scraper a través de Redis Cloud, eliminando race conditions entre contenedores Docker.

### Análisis técnico

| Categoría | Indicadores |
|-----------|-------------|
| Tendencia | SMA(20,50,200), EMA(12,26), ADX(14), DI+/DI− |
| Momentum | RSI(14), Estocástico(14,3), Williams %R(14) |
| MACD | (12,26,9) — línea, señal, histograma, aceleración |
| Volatilidad | Bollinger Bands(20,2), ATR(14), BB Width |
| Volumen | OBV, OBV-SMA(20), Volumen relativo |

### Contexto macro
Datos globales via yfinance: WTI, Brent, DXY, VIX, S&P 500, Merval, TNX. Datos de Argentina en tiempo real: CCL, MEP (dolarapi.com), Reservas BCRA, Riesgo País (argentinadatos.com). El sistema penaliza automáticamente el cash en ARS cuando el CCL supera ciertos umbrales.

### Risk Engine
Sizing dinámico por posición usando Kelly fraccionario (33%) y target de volatilidad (40%). Tres estados operativos controlados por VIX y drawdown del portfolio:

| Gate | Condición | Comportamiento |
|------|-----------|----------------|
| NORMAL | VIX < 28 y drawdown > −12% | Optimizer sin restricciones |
| CAUTIOUS | VIX > 28 o drawdown > −12% | Solo reducciones, no compras nuevas |
| BLOCKED | VIX > 38 o drawdown > −22% | Solo stops urgentes |

### Portfolio Optimizer
Black-Litterman con views del pipeline cuantitativo como fuente de señal. Fallback a Mínima Varianza con numpy puro si scipy no está disponible. El optimizer corre dentro del espacio permitido por el Risk Gate — nunca lo contradice.

### Opportunity Radar
Screener de 80+ tickers del universo Cocos con tres capas: filtro de liquidez/tendencia/RS, scoring multicapa con métrica de asimetría upside/downside, y clasificación COMPRABLE_AHORA / EN_VIGILANCIA / DESCARTAR.

### Rotation Engine
Decide adónde va el capital liberado de las ventas. Compara explícitamente aumentar posiciones existentes versus abrir candidatos nuevos del radar, considerando score relativo, asimetría y diversificación sectorial.

### Decision Memory & IC
Cada decisión se guarda en `decision_log` con el outcome real (retorno a 5, 10 y 20 días). El Information Coefficient (IC de Pearson y Rank IC) mide el poder predictivo histórico del sistema por horizonte temporal.

---

## Stack tecnológico

| Componente | Tecnología |
|------------|------------|
| Orquestación | Docker Compose (Python 3.12-slim) |
| Scraper | Playwright / Chromium (headless) |
| Base de datos | TimescaleDB (PostgreSQL 16 + extensión time-series) |
| MFA broker | Redis Cloud (LPUSH/BLPOP event-driven) |
| Notificaciones | python-telegram-bot >= 20.0 |
| Datos de mercado | yfinance, dolarapi.com, argentinadatos.com, BCRA API |
| Análisis técnico | ta >= 0.11 (pandas/numpy) |
| Optimización | scipy (Black-Litterman), numpy (Min-Variance) |
| Scheduler | APScheduler (10:30 ART y 17:00 ART) |
| LLM display | Claude claude-sonnet-4-20250514 via API (solo explicativo) |

---

## Instalación rápida

1. Clonar o descomprimir el proyecto en una carpeta local
2. Copiar `.env.example` → `.env` y completar las variables
3. Agregar `REDIS_URL` con la URL de Redis Cloud
4. `docker compose build --no-cache`
5. `docker compose up -d`
6. `docker compose logs -f scraper`

### Variables de entorno requeridas

| Variable | Descripción |
|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Token del bot de Telegram |
| `TELEGRAM_CHAT_ID` | Chat ID del usuario autorizado |
| `COCOS_USERNAME` | Email de Cocos Capital |
| `COCOS_PASSWORD` | Contraseña de Cocos Capital |
| `DATABASE_URL` | `postgresql+asyncpg://portfolio:portfolio_secret@cocos_db:5432/portfolio` |
| `REDIS_URL` | `redis://default:<PASSWORD>@redis-XXXX.redislabs.com:XXXX` |
| `ANTHROPIC_API_KEY` | API key de Anthropic (opcional, solo para LLM display) |

---

> ⚠️ Este sistema es una herramienta de análisis cuantitativo personal. No constituye asesoramiento financiero. Las decisiones de inversión son responsabilidad exclusiva del usuario.
