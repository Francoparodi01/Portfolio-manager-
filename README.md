# Cocos Copilot — Sistema Cuantitativo Multicapa

Sistema de análisis y optimización de portfolios de CEDEARs en Cocos Capital.
Combina análisis técnico profesional, contexto macro global y local (Argentina),
análisis de sentimiento de noticias, risk management y optimización matemática
de portfolio, todo integrado en un bot de Telegram.

---

## Arquitectura del pipeline

```
Posiciones (DB)
     │
     ├─► Macro (yfinance + APIs Argentina)     30%
     ├─► Técnico (RSI, MACD, ADX, Bollinger)  30%
     ├─► Risk Engine (Kelly, VaR, ATR)         25%
     └─► Sentiment (RSS multifuente)           15%
              │
              ▼
         Síntesis (blend determinístico)
              │
              ├─► LLM Ollama (deepseek-r1:14b)  — solo display
              │
              ▼
         Portfolio Optimizer
         (Black-Litterman / Min-Variance)
              │
              ▼
         Reporte HTML → Telegram Bot
```

---

## Indicadores técnicos implementados

| Categoría    | Indicadores                                        |
|--------------|----------------------------------------------------|
| Tendencia    | SMA(20,50,200), EMA(12,26), ADX(14), DI+/DI−     |
| Momentum     | RSI(14), Estocástico(14,3), Williams %R(14)        |
| MACD         | (12,26,9) — línea, señal, histograma, aceleración  |
| Volatilidad  | Bollinger Bands(20,2), ATR(14), BB Width           |
| Volumen      | OBV, OBV-SMA(20), Volumen relativo                 |

---

## Requisitos

- Docker Desktop (Windows, Mac o Linux)
- Ollama corriendo en Windows nativo (recomendado) con `deepseek-r1:14b`
- Cuenta en Cocos Capital
- Bot de Telegram configurado (ver `.env.example`)

---

## Instalación rápida

```bash
# 1. Clonar / descomprimir el proyecto
cd cocos_copilot

# 2. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus datos reales

# 3. Bajar modelo LLM (desde Windows, con Ollama instalado)
ollama pull deepseek-r1:14b

# 4. Levantar servicios
docker compose up -d --build

# 5. Verificar que todo está bien
docker compose logs -f scraper
```

---

## Comandos útiles

```bash
# Análisis completo (sin Telegram)
docker compose run --rm scraper python scripts/run_analysis.py --no-telegram

# Análisis rápido (sin sentiment ni optimizer)
docker compose run --rm scraper python scripts/run_analysis.py --no-sentiment --no-optimizer --no-telegram

# Análisis de un ticker específico
docker compose run --rm scraper python scripts/run_analysis.py --tickers NVDA --no-optimizer

# Backtest estándar (2 años)
docker compose run --rm scraper python scripts/backtest.py --years 2 --no-telegram

# Walk-forward validation
docker compose run --rm scraper python scripts/backtest_walkforward.py --years 3 --no-telegram

# Ver universo Cocos en DB
docker compose run --rm scraper python -c "
import asyncio
from src.core.config import get_config
from src.collector.db import PortfolioDatabase
async def f():
    db = PortfolioDatabase(get_config().database.url)
    await db.connect()
    u = await db.get_cocos_universe()
    print(f'{len(u)} tickers: {u[:10]}')
    await db.close()
asyncio.run(f())
"

# Ver modelos Ollama disponibles (si corre en Docker)
docker compose exec ollama ollama list

# Reiniciar solo el bot de Telegram
docker compose restart telegram_bot

# Deploy completo después de cambiar archivos .py
docker compose down && docker compose up -d

# Ver logs en tiempo real
docker compose logs -f telegram_bot
docker compose logs -f scraper
```

---

## Estructura del proyecto

```
cocos_copilot/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── init.sql                    ← Schema PostgreSQL inicial
├── .env.example                ← Plantilla de variables de entorno
│
├── src/
│   ├── core/
│   │   ├── config.py           ← Configuración desde env vars
│   │   ├── logger.py           ← Logger → stderr (stdout limpio para bot)
│   │   └── credentials.py      ← Credenciales Fernet por usuario
│   │
│   ├── collector/
│   │   ├── db.py               ← TimescaleDB (asyncpg)
│   │   ├── notifier.py         ← Telegram API
│   │   └── data/
│   │       └── models.py       ← Modelos de dominio
│   │
│   └── analysis/
│       ├── technical.py        ← RSI, MACD, ADX, Bollinger, OBV
│       ├── macro.py            ← Global + Argentina (CCL, MEP, Reservas, Riesgo País)
│       ├── risk.py             ← Kelly, VaR, sizing por volatilidad
│       ├── sentiment.py        ← RSS multifuente, lexicon financiero
│       ├── synthesis.py        ← Blend multicapa + LLM Ollama/Claude
│       ├── optimizer.py        ← Black-Litterman + Min-Variance + risk gate
│       └── decision_memory.py  ← Historial de decisiones + hit rate
│
└── scripts/
    ├── run_analysis.py         ← Pipeline principal (ejecutado por el bot)
    ├── telegram_bot.py         ← Bot interactivo con menú de botones
    ├── backtest.py             ← Backtest vectorizado 2y
    ├── backtest_walkforward.py ← Walk-forward validation
    ├── backtest_universe.py    ← Universe backtest (mejores combos)
    └── backtest_horizon.py     ← Comparación de frecuencias de rebalanceo
```

---

## Risk Gate

El sistema tiene tres estados operativos:

| Gate      | Condición                                    | Comportamiento                          |
|-----------|----------------------------------------------|-----------------------------------------|
| NORMAL    | VIX < 28 y drawdown > -12%                  | Optimizer opera sin restricciones       |
| CAUTIOUS  | VIX > 28 o régimen risk_off o DD > -12%     | Solo reducciones, no nuevas compras     |
| BLOCKED   | VIX > 38 o drawdown > -22%                  | Solo stops urgentes (delta < -15%)      |

---

## Conviction (% acuerdo entre capas)

La **conviction** NO es `abs(score)`. Es el porcentaje de capas activas que
apuntan en la misma dirección que el score final.

Ejemplo con CVX (score = +0.108):
- Técnico: -0.100 (bearish) → **en contra**
- Macro: +0.160 (bullish) → **a favor**
- Sentiment: +0.048 (bullish) → **a favor**
- Riesgo: 0.000 (neutral) → inactivo

Capas activas: 3. Acuerdan: 2 (macro + sentiment). Conviction = 2/3 = **67%**

Una señal con conviction 100% y score +0.40 merece tamaño máximo.
Una señal con conviction 33% aunque tenga score alto recibe tamaño reducido.

---

## Nota sobre el modelo LLM

El razonamiento del LLM (Ollama deepseek-r1:14b) es **solo display** —
no modifica el score ni la decisión cuantitativa. Es una capa explicativa
que valida o cuestiona la decisión del pipeline en lenguaje natural.

Para instalar el modelo:
```bash
ollama pull deepseek-r1:14b
```

---

*Sistema cuantitativo multicapa — no es asesoramiento financiero.*
