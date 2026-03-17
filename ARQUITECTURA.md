# ARQUITECTURA DEL SISTEMA
### Cocos Copilot — Diseño técnico y decisiones de arquitectura

---

## Visión general

El sistema está organizado en tres pipelines independientes que comparten la infraestructura base (DB + Redis + macro):

- **Portfolio Analyzer** → `run_analysis.py`
- **Opportunity Radar** → `run_opportunity.py`
- **Rotation Engine** → `rotation_engine.py` (módulo, invocado por el radar)

Los tres pipelines se invocan desde el bot de Telegram o desde CLI directamente. El scheduler automatiza `run_analysis.py` en horarios fijos.

---

## Infraestructura Docker

| Contenedor | Imagen | Responsabilidad |
|------------|--------|-----------------|
| `cocos_db` | timescale/timescaledb:latest-pg16 | Persistencia time-series (snapshots, posiciones, precios, decisiones) |
| `cocos_scraper` | python:3.12-slim + Playwright | Scheduler, scraping, análisis, optimizer |
| `cocos_telegram_bot` | python:3.12-slim | Bot interactivo, captura MFA, lanza pipelines |

Redis Cloud corre fuera de Docker. La comunicación usa `REDIS_URL` como variable de entorno en ambos contenedores que lo necesitan.

---

## Flujo MFA (anti race-condition)

El problema original: el scraper y el bot corrían en contenedores separados con filesystems aislados. La solución usa Redis Cloud como broker de eventos:

| Paso | Actor | Acción |
|------|-------|--------|
| 1 | Scraper | Detecta pantalla MFA en Cocos Capital |
| 2 | Scraper | Manda mensaje al usuario por Telegram pidiendo el código |
| 3 | Scraper | Llama `BLPOP mfa:<chat_id>` — se bloquea esperando en Redis |
| 4 | Usuario | Manda los 6 dígitos al bot de Telegram |
| 5 | Bot | Recibe el mensaje, valida 6 dígitos, ejecuta `LPUSH mfa:<chat_id>` |
| 6 | Redis | El BLPOP del scraper se desbloquea instantáneamente |
| 7 | Scraper | Recibe el código y lo ingresa en los inputs de Cocos |

> Latencia típica: 10-50ms. Sin polling, sin race conditions, funciona entre contenedores en cualquier host.

---

## Pipeline de análisis de cartera

`run_analysis.py` ejecuta estas etapas en orden:

| Etapa | Módulo | Output |
|-------|--------|--------|
| 1. Cargar posiciones | `db.py` | Lista de posiciones del último snapshot en DB |
| 2. Macro | `macro.py` | MacroSnapshot: WTI, VIX, S&P 500, CCL, MEP, Reservas, Riesgo País |
| 3. Técnico | `technical.py` | Signal por ticker: BUY/SELL/HOLD + strength + score_raw + reasons |
| 4. Risk Engine | `risk.py` | RiskMetrics: vol, Kelly, sizing, drawdown, gate state |
| 5. Sentiment | `sentiment.py` | Score RSS por ticker (Yahoo Finance + Reuters) |
| 6. Síntesis | `synthesis.py` | SynthesisResult: decision + final_score + conviction |
| 7. Universo Cocos | `technical.py` + `synthesis.py` | Análisis de tickers del mercado no en cartera |
| 8. Optimizer | `optimizer.py` | RebalanceReport: trades, pesos objetivo, gate state |
| 9. IC histórico | `decision_memory.py` + DB | Pearson IC y Rank IC de señales pasadas |
| 10. Render | `run_analysis.py` | HTML → stdout → Telegram |

---

## Motor de síntesis — Capas y pesos

| Capa | Peso | Qué mide | Cuándo penaliza |
|------|------|----------|-----------------|
| Técnico | 30% | RSI, MACD, ADX, Bollinger, OBV | Señal bajista o sin dirección |
| Macro | 30% | Tendencia de indicadores macro por sector | Indicadores adversos al sector del ticker |
| Riesgo | 25% | Volatilidad extrema y drawdown del portfolio | Solo vol>80% o drawdown activo — no penaliza vol normal de tech |
| Sentiment | 15% | Score lexicón sobre RSS de Yahoo Finance + Reuters | Noticias negativas relevantes al ticker |

### Conviction (acuerdo entre capas)

La conviction **NO es** `abs(score)`. Es el porcentaje de capas activas que apuntan en la misma dirección que el score final.

Ejemplo con CVX (score = +0.058):
- Técnico: −0.033 (bajista) → en contra
- Macro: +0.087 (alcista) → a favor
- Sentiment: +0.004 (neutral) → inactivo
- Capas activas: 2. Acuerdan: 1. **Conviction: 50%**

Una señal con conviction 100% y score alto merece tamaño máximo. Una señal con conviction 33% recibe tamaño reducido aunque el score sea alto.

---

## Risk Gate

El Risk Gate es la primera operación del optimizer. Define el espacio operativo antes de cualquier cálculo de pesos:

| Estado | Condición de entrada | Comportamiento |
|--------|----------------------|----------------|
| NORMAL | VIX < 28 y drawdown > −12% | Opera sin restricciones adicionales |
| CAUTIOUS | VIX > 28 o drawdown > −12% o régimen risk_off | Solo reducciones de posiciones con score negativo. Nuevas compras bloqueadas |
| BLOCKED | VIX > 38 o drawdown > −22% | Optimizer no corre. Solo se generan stops de emergencia (delta < −15%) |

---

## Pipeline del Opportunity Radar

### Capa 1: Screener

| Filtro | Umbral | Razón |
|--------|--------|-------|
| Precio mínimo | > $3 USD | Evita penny stocks |
| Volumen promedio | > 500,000 diario | Liquidez mínima operativa |
| Volatilidad | 8% – 120% anual | Excluye activos muertos y explosivos |
| Distancia máximos 6m | < 45% debajo | No analiza activos en caída libre |
| Distancia mínimos 6m | > 3% sobre | No compra en mínimos sin rebote |
| RS vs SPY 20d | > −10% | No peor que el mercado por más del umbral |

### Capa 2: Asimetría (métrica clave)

Para cada candidato que pasa el screener se calcula la `AsymmetryMetrics`:

- **Upside**: distancia al máximo de 6 meses (potencial de recovery)
- **Stop sugerido**: max(1.5 × ATR, 5%) — capeado a 18%
- **Risk/Reward ratio**: upside / stop_pct
- **Asymmetry ratio**: > 1.5 es MODERADA, > 2.0 es BUENA, > 3.0 es EXCELENTE

### Capa 3: Entry Engine

| Clasificación | Condiciones requeridas |
|---------------|------------------------|
| COMPRABLE_AHORA | score ≥ 0.15 + conviction ≥ 40% + asimetría ≥ MODERADA + precio sobre soporte + momentum 20d > −5% |
| EN_VIGILANCIA | score ≥ 0.07 (potencial real pero falta confirmación técnica o punto de entrada) |
| DESCARTAR | Todo lo que no cumple los umbrales anteriores |

---

## Capital allocation dinámico

El sizing sugerido no es fijo. La fórmula es:

```
sizing = base × conviction_mult × asym_mult × sector_penalty
```

| Factor | Rango | Lógica |
|--------|-------|--------|
| `base` | 2% – 9% | Deriva del score: score×25 clipeado. Score 0.20 → 5%, score 0.30 → 7% |
| `conviction_mult` | 0.50× – 1.30× | Penaliza fuerte convicción baja (<25% → 0.50×). Premia alta convicción |
| `asym_mult` | 0.70× – 1.35× | R/R < 1.2 → 0.70×. R/R ≥ 4.0 → 1.35× |
| `sector_penalty` | 0.40× – 1.00× | Si ya tenemos >30% en el mismo sector → 0.40× del tamaño calculado |

---

## Rotation Engine

El Rotation Engine responde: *"¿La plata que sale de CVX va a MU, a AVGO, o a cash?"*

Compara dos tipos de opciones:
- **Opciones internas**: posiciones existentes con decisión BUY/ACCUMULATE del pipeline
- **Opciones externas**: candidatos del radar con status COMPRABLE_AHORA

El ranking usa `score × conviction × (1 + asym_bonus)`. Se distribuye el capital entre los top 3 con límite de 15% del portfolio o 50% del capital disponible por posición. El resto va a cash.

> En gate CAUTIOUS, el Rotation Engine solo considera opciones internas. En gate BLOCKED, todo el capital liberado va a cash.

---

## Decision Memory & IC

Cada decisión del pipeline se registra en `decision_log` con: ticker, decisión, score, conviction, capas individuales, precio, VIX y régimen. El outcome (retorno a 5, 10 y 20 días) se rellena en un job posterior.

El **IC de Pearson** mide la correlación lineal entre `final_score` y el retorno real. El **Rank IC** (Spearman) es más robusto a outliers:

| IC | Interpretación |
|----|----------------|
| < 0.02 | NULO — el score no predice mejor que random |
| 0.02 – 0.05 | DÉBIL — hay alguna señal |
| 0.05 – 0.10 | MODERADO — sistema tiene valor |
| > 0.10 | FUERTE — nivel de hedge fund cuantitativo |

---

## Estructura del proyecto

```
cocos_copilot/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── init.sql
│
├── scripts/
│   ├── run_analysis.py          ← Pipeline principal — análisis de cartera
│   ├── run_opportunity.py       ← Pipeline de oportunidades externas
│   ├── run_once.py              ← Scrape manual del portfolio
│   └── telegram_bot.py          ← Bot interactivo con menú de botones
│
└── src/
    ├── core/
    │   ├── config.py            ← Configuración desde variables de entorno
    │   ├── logger.py            ← Logger centralizado → stderr
    │   ├── redis_client.py      ← Cliente Redis async singleton
    │   └── credentials.py       ← Credenciales encriptadas con Fernet
    │
    ├── collector/
    │   ├── cocos_scraper.py     ← Scraper Playwright + MFA Redis
    │   ├── db.py                ← Capa de persistencia TimescaleDB (asyncpg)
    │   └── notifier.py          ← Notificaciones Telegram (requests sync)
    │
    └── analysis/
        ├── technical.py         ← RSI, MACD, ADX, Bollinger, OBV
        ├── macro.py             ← Macro global + Argentina (APIs locales)
        ├── risk.py              ← Kelly, VaR, sizing, drawdown, gate
        ├── sentiment.py         ← RSS multifuente + lexicón financiero
        ├── synthesis.py         ← Blend multicapa + convicción + LLM display
        ├── optimizer.py         ← Black-Litterman + Min-Varianza + risk gate
        ├── decision_memory.py   ← Persistencia de decisiones + IC histórico
        ├── opportunity_screener.py ← Screener + scorer + entry engine
        └── rotation_engine.py   ← Rotation Engine — asignación de capital
```
