# ARQUITECTURA DEL SISTEMA

### Cocos Copilot — Diseño técnico y decisiones de arquitectura

---

## Visión general

Cocos Copilot está organizado como un sistema cuantitativo modular que transforma datos reales de portfolio en decisiones auditables.

El sistema se compone de tres flujos principales:

- **Portfolio Analyzer** → análisis de cartera actual.
- **Opportunity Radar** → búsqueda de oportunidades externas.
- **Execution Planner / Rotation Engine** → traducción de targets en acciones ejecutables, bloqueadas o en observación.

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

Redis se utiliza como canal de comunicación/eventos cuando aplica.

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
    └── analysis/
        ├── technical.py
        ├── macro.py
        ├── risk.py
        ├── sentiment.py
        ├── synthesis.py
        ├── optimizer.py
        ├── execution_planner.py
        ├── validators.py
        ├── decision_memory.py
        ├── opportunity_screener.py
        └── rotation_engine.py
```

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
