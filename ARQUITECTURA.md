# ARQUITECTURA DEL SISTEMA

### Cocos Copilot — Diseño técnico y decisiones de arquitectura

---

## Visión general

Cocos Copilot está organizado como un sistema cuantitativo modular que transforma datos reales de portfolio en decisiones auditables.

El sistema se compone de cuatro bloques principales:

- **Scheduler / Monitor** → mantiene datos frescos, outcomes y alertas.
- **Portfolio Analyzer** → analiza cartera actual.
- **Opportunity Radar** → busca oportunidades externas.
- **Execution Planner / Rotation Engine** → traduce targets en acciones ejecutables, bloqueadas o en observación.

Los pipelines pueden ejecutarse desde CLI o desde el bot de Telegram.

---

## Principio de arquitectura

La regla central del sistema es:

```text
El optimizer propone targets teóricos.
El Execution Planner decide si esos targets son operables.
El renderer solo muestra el Execution Plan como fuente de verdad operativa.
```

Esto evita que el sistema opere por ruido o por obedecer ciegamente al optimizer.

---

## Infraestructura Docker

| Contenedor | Imagen | Responsabilidad |
|---|---|---|
| `cocos_db` | `timescale/timescaledb:pg16` | Persistencia time-series: snapshots, posiciones, precios y decisiones |
| `cocos_scraper` | Python + Playwright | Scraping, scheduler y pipelines batch |
| `cocos_telegram_bot` | Python | Bot interactivo, comandos y ejecución desde Telegram |

Redis se utiliza como canal opcional para heartbeats, estado del monitor, flags y coordinación de eventos.

---

## Roles principales

```text
runner.py
  ├─ run_scrape()
  ├─ run_full()
  ├─ update_outcomes()
  ├─ intraday market loop
  └─ risk guard

run_analysis.py
  ├─ macro
  ├─ technical
  ├─ risk
  ├─ synthesis
  ├─ optimizer
  ├─ execution_planner
  ├─ decision_log
  └─ render Telegram

telegram_bot.py
  ├─ /portfolio
  ├─ /analisis → run_analysis.py
  ├─ /radar → run_opportunity.py
  ├─ /performance → run_performance.py
  ├─ /resumen → weekly_summary.py
  └─ /status
```

---

## Flujo general

```text
Cocos Capital
  ↓
Scraper Playwright
  ↓
TimescaleDB
  ↓
run_analysis.py
  ↓
Technical + Macro + Risk + Sentiment
  ↓
Synthesis
  ↓
Optimizer
  ↓
Execution Planner
  ↓
Decision Log
  ↓
Telegram / CLI report
```

---

## Scheduler / monitor

`src/scheduler/runner.py` mantiene vivo al sistema.

Responsabilidades:

- Scrape programado de portfolio.
- Scrape programado de mercado.
- Loop intradía de precios.
- Refresh periódico de portfolio.
- Update diario de outcomes.
- Risk guard por DB.
- Heartbeats y estado del monitor.

Tareas programadas orientativas:

| Horario ART | Acción |
|---|---|
| 10:30 | Scrape de portfolio |
| 10:31 | Inicio de loops intradía |
| 17:00 | Scrape completo EOD |
| 17:01 | Stop loops intradía |
| 21:30 | Update outcomes |

El scheduler no reemplaza a `run_analysis.py`: mantiene datos frescos y riesgo monitoreado, pero el análisis cuantitativo completo se ejecuta por `/analisis` o CLI.

---

## Bot de Telegram

`scripts/telegram_bot.py` es la interfaz del sistema.

Responsabilidades:

- Mostrar menú principal.
- Ejecutar scripts por comando.
- Mostrar reportes en Telegram.
- Separar comandos principales de comandos admin.
- Compactar el radar para no romper mensajes largos.
- Mostrar `/portfolio` limpio y sin P&L dudoso.

El bot no debe contener lógica cuantitativa. Debe actuar como router:

```text
comando → script → output → Telegram
```

---

## Pipeline de análisis de cartera

`run_analysis.py` ejecuta estas etapas:

| Etapa | Módulo | Output |
|---|---|---|
| 1. Cargar posiciones | `collector/db.py` | Último snapshot de cartera |
| 2. Macro | `analysis/macro.py` | VIX, S&P 500, petróleo, tasas, variables locales |
| 3. Técnico | `analysis/technical.py` | Señales por ticker, strength, score y razones |
| 4. Riesgo | `analysis/risk.py` | Volatilidad, sizing, drawdown y risk gate |
| 5. Sentiment | `analysis/sentiment.py` | Score RSS por ticker, opcional |
| 6. Síntesis | `analysis/synthesis.py` | Score final, capas y decisión preliminar |
| 7. Universo Cocos | `technical.py` + `synthesis.py` | Análisis de tickers fuera de cartera |
| 8. Optimizer | `analysis/optimizer.py` | Pesos objetivo teóricos |
| 9. Execution Planner | `analysis/execution_planner.py` | Órdenes ejecutables, bloqueadas o WATCH |
| 10. Decision Log | `run_analysis.py` + DB | Registro de decisiones y contexto |
| 11. IC histórico | DB / decision log | Pearson IC y Rank IC |
| 12. Render | `run_analysis.py` | HTML para Telegram / stdout |

---

## Execution Planner

El Execution Planner es la capa que convierte targets teóricos del optimizer en un plan operativo.

Puede generar:

- `BUY`
- `SELL_PARTIAL`
- `SELL_FULL`
- `HOLD`
- `WATCH`
- `BLOCKED`

### Guards principales

#### BUY_SCORE_GUARD

Bloquea compras con score negativo.

```text
BUY si score < -0.01 → BLOCKED
```

#### TRADE_QUALITY_GUARD

Evita operar señales débiles.

```text
BUY requiere score >= +0.08
score positivo débil → WATCH
score neutral → HOLD / no operar
```

#### Sell protection

Evita vender posiciones con score positivo salvo concentración o riesgo claro.

```text
Optimizer quiere vender + score positivo + sin concentración → HOLD
```

#### Neutral guard

Una señal neutral no justifica operar.

```text
score entre -0.05 y +0.05 → señal neutral / ruido
```

---

## Score, señal y alineación de capas

El reporte separa tres conceptos:

| Concepto | Qué representa |
|---|---|
| Score | Magnitud y dirección cuantitativa |
| Señal | Interpretación operativa del score |
| Alineación de capas | Coincidencia entre técnico, macro, sentiment y otras capas |

Ejemplo:

```text
Score: +0.048
Señal: NEUTRAL / RUIDO
Alineación: ALTA
Acción: WATCH
```

La alineación puede ser alta aunque el score no sea operable. Por eso no se usa como sinónimo de convicción de compra.

---

## Information Coefficient

El IC mide si los scores históricos tuvieron relación con retornos posteriores.

| IC absoluto | Interpretación |
|---|---|
| < 0.02 | Nulo |
| 0.02 – 0.05 | Débil |
| 0.05 – 0.10 | Moderado |
| > 0.10 | Fuerte |

Si el IC viene negativo, el reporte marca un régimen de cautela:

```text
Régimen IC: CAUTELA ALTA
IC negativo fuerte: evitar rotaciones con señales débiles.
```

Por ahora el IC se usa como explicación del régimen de confianza. Puede pasar a modificar thresholds en iteraciones futuras.

---

## Risk Gate

El Risk Gate define el espacio operativo antes de cualquier cálculo de pesos.

| Estado | Condición orientativa | Comportamiento |
|---|---|---|
| NORMAL | Mercado estable | Opera con restricciones normales |
| CAUTIOUS | VIX alto, drawdown o régimen defensivo | Bloquea compras nuevas o reduce agresividad |
| BLOCKED | Riesgo extremo | Solo reducciones / stops de emergencia |

---

## Optimizer

El optimizer genera pesos objetivo teóricos.

Métodos:

- Black-Litterman, cuando está disponible.
- Fallback por mínima varianza / heurísticas internas.
- Restricciones por risk gate y límites de concentración.

Importante:

```text
Los pesos del optimizer son informativos.
La acción real sale del Execution Planner.
```

---

## Opportunity Radar

El radar busca candidatos externos fuera de la cartera actual.

Etapas:

1. Screener básico de liquidez, precio, volatilidad y tendencia.
2. Score técnico/macro/riesgo/sentiment.
3. Cálculo de asimetría riesgo/retorno.
4. Clasificación:
   - compra fuerte,
   - vigilancia,
   - observación,
   - descartar.

El radar del análisis semanal se muestra compacto para no romper el mensaje de Telegram. El detalle completo vive en `/radar`.

---

## Decision Memory

Cada decisión relevante se registra en `decision_log` con:

- ticker,
- decisión,
- score,
- alineación/confidence,
- capas,
- precio al decidir,
- VIX,
- régimen,
- size,
- stop,
- target,
- horizonte.

Luego se calculan outcomes a 5, 10 y 20 días para evaluar performance.

Métricas principales:

- Win rate.
- Expected Value.
- Avg win / avg loss.
- IC y Rank IC.
- Equity curve.
- Max drawdown.

---

## Módulos legacy / soporte / experimental

### `decision_engine.py`

Módulo de soporte/legacy.

Conserva constantes y helpers de sizing, stop, target, horizonte y normalización de régimen usados por `decision_log` y reportes históricos.

La decisión operativa actual del MVP no sale de este archivo. La fuente de verdad operativa es `execution_planner.py`.

### `feature_builder.py` y `ml_model.py`

Capa experimental/post-MVP para ML.

No forman parte del runtime MVP actual. Si se conservan, deberían vivir en:

```text
src/analysis/experimental/
```

No conviene agregar `lightgbm`, `scikit-learn` o `joblib` al `requirements.txt` hasta activar esa capa.

---

## Esquema de base de datos

Tablas principales:

| Tabla | Uso |
|---|---|
| `portfolio_snapshots` | Snapshots históricos del portfolio |
| `positions` | Posiciones asociadas a cada snapshot |
| `market_prices` | Precios de mercado por ticker |
| `raw_snapshots` | Payload completo de scraping |
| `decision_log` | Registro de decisiones y outcomes |

La base usa TimescaleDB para datos time-series.

---

## Estructura del proyecto

```text
cocos_copilot/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── init.sql
├── README.md
├── ARQUITECTURA.md
├── COMANDOS.md
│
├── scripts/
│   ├── run_analysis.py
│   ├── run_opportunity.py
│   ├── run_performance.py
│   ├── update_outcomes.py
│   ├── weekly_summary.py
│   ├── run_once.py
│   └── telegram_bot.py
│
└── src/
    ├── core/
    │   ├── config.py
    │   ├── logger.py
    │   ├── redis_client.py
    │   └── credentials.py
    │
    ├── collector/
    │   ├── cocos_scraper.py
    │   ├── db.py
    │   └── notifier.py
    │
    ├── scheduler/
    │   └── runner.py
    │
    └── analysis/
        ├── technical.py
        ├── macro.py
        ├── risk.py
        ├── sentiment.py
        ├── synthesis.py
        ├── optimizer.py
        ├── execution_planner.py
        ├── validators.py
        ├── trade_lifecycle.py
        ├── decision_engine.py
        ├── decision_memory.py
        ├── opportunity_screener.py
        ├── rotation_engine.py
        └── experimental/
            ├── feature_builder.py
            └── ml_model.py
```

---

## Requirements actuales

Para el MVP actual, las dependencias principales son:

- Playwright
- python-telegram-bot
- Redis
- asyncpg
- requests / aiohttp
- python-dotenv
- cryptography
- pyotp
- pandas / numpy / yfinance
- ta
- scipy
- apscheduler
- python-dateutil / pytz

La capa ML experimental no debería inflar `requirements.txt` hasta estar activada.

---

## Archivos locales y secretos

No deberían subirse al repositorio:

```text
.env
secret_key/
secrets/
logs/
screenshots/
models/
venv/
```

Estos archivos/carpetas deben estar cubiertos por `.gitignore`.
