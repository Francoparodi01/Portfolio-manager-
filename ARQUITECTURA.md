# Arquitectura

## Propósito

Cocos Copilot transforma datos observados de una cartera real en decisiones auditables. La arquitectura está pensada para que cada etapa tenga una fuente de verdad clara y para que una sugerencia cuantitativa no se convierta automáticamente en una orden.

## Principios

1. El scraper observa; no decide.
2. El optimizer propone targets teóricos; no decide ejecución.
3. El Execution Planner es la fuente de verdad operativa.
4. Los reportes deben distinguir datos observados, hipótesis y ejecución real.
5. Los instrumentos operables se evalúan con datos de Cocos; el contexto macro puede seguir siendo global.

## Vista general

```text
                 Cocos Capital
                  /        \
                 /          \
      portfolio + market     páginas por ticker
             |                    |
             v                    v
 portfolio_snapshots        market_candles
 positions                  (COCOS + internal_snapshot)
 market_prices --------------^
      \                      /
       \                    /
        +--> run_analysis.py
               |
               +--> technical / macro / risk / sentiment
               +--> synthesis
               +--> optimizer
               +--> execution_planner
               +--> decision_log
               +--> render
```

## Infraestructura

| Servicio | Contenedor | Responsabilidad |
|---|---|---|
| `db` | `cocos_db` | PostgreSQL / TimescaleDB local opcional |
| `scheduler` | `cocos_scheduler` | scraping programado, loops intradía, outcomes |
| `telegram_bot` | `cocos_telegram_bot` | interfaz de usuario y ejecución de comandos |

`db` vive bajo el perfil `localdb`. Si `DATABASE_URL` apunta a una base externa, pueden levantarse solo `scheduler` y `telegram_bot`.

Redis es auxiliar. Se usa para MFA manual, heartbeats y flags, pero la lógica principal no depende de él para persistir decisiones.

## Fuentes de datos

### 1. Portfolio

Origen:

- `https://app.cocos.capital/capital-portfolio`

Persistencia:

- `portfolio_snapshots`
- `positions`
- `raw_snapshots`

Uso:

- cartera actual;
- cash;
- composición histórica;
- contexto para risk y planner.

### 2. Universo global de mercado

Origen:

- `https://app.cocos.capital/market/ACCIONES`
- `https://app.cocos.capital/market/CEDEARS`

Persistencia:

- `market_prices`

Uso:

- descubrir universo;
- conservar `asset_type`;
- conocer último precio y variación diaria.

`market_prices` no reemplaza una serie OHLCV histórica. Es un snapshot de mercado.

### 3. Serie canónica de velas

Fuentes físicas:

- backfill inicial oficial desde página individual del ticker + request `historic-data-extended`;
- reconstrucción diaria propia desde snapshots de `market_prices`.

Persistencia:

- `market_candles`

Uso:

- técnico;
- optimizer;
- radar;
- futuros modelos cuantitativos.

Captura:

- `scripts/capture_cocos_history.py`: un ticker;
- `scripts/backfill_cocos_history.py`: lote de activos faltantes;
- `scripts/import_cocos_history.py`: carga de JSON a DB.

Regla canónica:

- la DB puede conservar velas oficiales y reconstruidas;
- la lectura operativa devuelve una sola serie por ticker/día;
- si ambas existen, gana `COCOS`;
- `internal_snapshot` se usa solo cuando falta la oficial.

Estado actual:

- el backfill oficial inicial queda congelado;
- el scheduler reconstruye una vela diaria interna desde `market_prices`;
- las capturas de `historic-data-extended` quedan manuales/excepcionales;
- el radar externo es estricto: si un ticker no tiene historia canónica suficiente, queda `EXTERNO`.

### 4. Contexto macro

Origen:

- `analysis/macro.py`;
- `yfinance` para referencias globales;
- APIs locales para variables argentinas cuando están disponibles.

Uso:

- régimen de mercado;
- VIX;
- tasas;
- petróleo;
- dólar;
- variables argentinas.

Esto es deliberadamente distinto de usar Yahoo para el histórico de cada instrumento operable.

## Segmentación ACCIONES / CEDEARS

La segmentación no es decorativa. Atraviesa el modelo de datos y el análisis:

| Segmento | Significado |
|---|---|
| `ACCION` | acción argentina |
| `CEDEAR` | certificado local de activo extranjero |

Campos donde se preserva:

- `market_prices.asset_type`
- `market_candles.asset_type`
- universe loading en `db.py`
- carga de frames en análisis y radar.

La evaluación macro puede ser global, pero el comportamiento de los segmentos no se asume idéntico.

## Pipelines activos

### `scripts/run_analysis.py`

Pipeline principal de cartera:

| Paso | Resultado |
|---|---|
| cargar snapshot | posiciones y cash |
| cargar velas canónicas | frames históricos por ticker |
| macro | régimen y variables globales |
| técnico | señales por activo |
| riesgo | vol, sizing, warnings |
| síntesis | score final multicapa |
| universo Cocos | radar compacto externo |
| optimizer | targets teóricos |
| execution planner | órdenes o bloqueos |
| decision log | memoria auditable |
| IC | poder predictivo histórico |
| render | salida HTML para Telegram / stdout |

Notas:

- cartera actual lee la serie canónica y conoce si la historia es oficial, reconstruida o mixta;
- si falta historia canónica suficiente para un holding, se omite técnico operativo para ese activo;
- universo externo ya no cae a Yahoo: sin Cocos queda `EXTERNO`.

### `scripts/run_opportunity.py`

Pipeline de radar completo:

- carga universo tipado desde DB;
- carga velas de `market_candles`;
- filtra holdings si corresponde;
- clasifica candidatos;
- separa `COMPRABLE_AHORA`, `COMPRA_HABILITADA`, `SWAP_CANDIDATO`, vigilancia, `NO_OPERABLE` y `EXTERNO`.

### `scripts/run_performance.py`

Pipeline de auditoría:

- actualiza outcomes elegibles;
- resume dataset por `source`, `status` y `decision_type`;
- calcula win rate, EV, retornos y curva de equity;
- separa `EV histórico agregado`, planes aprobados y fills reales confirmados.

### `scripts/import_broker_fills.py`

Pipeline manual de reconciliación:

- importa fills reales confirmados desde CSV;
- persiste `broker_fills`;
- busca una decisión `execution_plan / APPROVED` compatible;
- la promociona a `EXECUTED` solo cuando existe evidencia externa real.

### `src/scheduler/runner.py`

Responsabilidades programadas:

| Hora ART | Acción |
|---|---|
| 10:30 | scrape de portfolio |
| 10:31 | inicio de loops intradía |
| 17:00 | scrape completo: portfolio + market global |
| 17:01 | fin de loops intradía |
| 17:05 | construcción de vela diaria interna |
| 21:30 | update de outcomes |

El scheduler hoy mantiene:

- snapshots de portfolio;
- precios globales de mercado;
- actualización de outcomes;
- continuidad diaria de `market_candles` desde snapshots propios.

## Núcleo de decisión

### Síntesis

`synthesis.py` combina capas:

- técnico;
- macro;
- riesgo;
- sentiment opcional.

El resultado conserva contribuciones por capa para que luego puedan auditarse.

### Risk levels

`risk_levels.py` centraliza:

- stop;
- target;
- risk/reward.

La convención de retornos SELL es direccional:

```text
si después de SELL el activo cae, el retorno direccional es positivo;
si sube, es negativo.
```

### Optimizer

`optimizer.py` produce pesos objetivo teóricos. Puede usar histórico Cocos inyectado y aplica restricciones de concentración/régimen.

### Execution Planner

`execution_planner.py` convierte targets en acciones operables:

- `BUY`
- `SELL_PARTIAL`
- `SELL_FULL`
- `HOLD`
- `WATCH`
- `BLOCKED`

Guards centrales:

- no comprar señal negativa;
- no operar señal débil;
- no vender automáticamente un activo con señal positiva si no hay razón de riesgo;
- no gastar cash inexistente.

## Radar de oportunidades

El radar externo usa:

1. screener de liquidez, tendencia, volatilidad y fuerza relativa;
2. score multicapa;
3. asimetría y risk/reward;
4. comparación contra cartera;
5. clasificación final.

Clasificaciones:

| Estado | Significado |
|---|---|
| `COMPRABLE_AHORA` | setup completo |
| `COMPRA_HABILITADA` | señal buena con alguna reserva |
| `SWAP_CANDIDATO` | mejora relativa contra un holding |
| `VIGILANCIA_A/B/C` | interés decreciente |
| `NO_OPERABLE` | señal presente pero setup inválido |
| `EXTERNO` | no hay velas Cocos suficientes para evaluar |

`C.I.` se conserva como caso `EXTERNO`: la ruta de Cocos no entrega histórico utilizable.

## Persistencia

Tablas principales:

| Tabla | Uso |
|---|---|
| `portfolio_snapshots` | snapshots históricos de cartera |
| `positions` | posiciones por snapshot |
| `market_prices` | snapshots globales de mercado |
| `market_candles` | velas OHLCV oficiales Cocos + velas reconstruidas internas |
| `raw_snapshots` | payloads crudos |
| `decision_log` | decisiones, capas, outcomes y auditoría |
| `broker_fills` | fills reales confirmados e importados para reconciliación |
| `bot_users` | usuarios del bot |
| `ml_decision_features` | stub de capa ML |
| `ml_model_registry` | stub de capa ML |

Protecciones importantes:

- claves únicas por snapshot/posición;
- unicidad de velas por `(ts, long_ticker, interval)`;
- unicidad de decisiones diarias equivalentes en `decision_log`;
- unicidad de fills por `(source, external_fill_id)`;
- DDL concentrado en `init.sql`.

## Módulos

```text
scripts/
  run_analysis.py
  run_opportunity.py
  run_performance.py
  import_broker_fills.py
  run_once.py
  weekly_summary.py
  update_outcomes.py
  capture_cocos_history.py
  backfill_cocos_history.py
  import_cocos_history.py
  telegram_bot.py

src/collector/
  cocos_scraper.py
  cocos_history.py
  db.py
  notifier.py

src/analysis/
  technical.py
  macro.py
  risk.py
  risk_levels.py
  synthesis.py
  optimizer.py
  execution_planner.py
  opportunity_screener.py
  trade_lifecycle.py
  decision_engine.py
  regression_audit.py
  validators.py

src/scheduler/
  runner.py
```

## Módulos de soporte y legado

- `decision_engine.py`: conserva contratos y helpers usados por compatibilidad y auditoría.
- `trade_lifecycle.py`: soporte auxiliar de lifecycle y convención de riesgo, aún no integrado como fuente operativa principal.
- `rotation_engine.py`: eliminado; el camino principal de rotación y funding vive en `execution_planner.py`.
- `core.credentials.py`: eliminado; la configuración activa usa `core.config` + `.env`.

## Deuda técnica conocida

- monitorear calidad de velas `internal_snapshot`;
- definir política de refresco manual/excepcional para altas nuevas o reparaciones;
- terminar de decidir el destino de módulos huérfanos heredados;
- migrar usos de `datetime.utcnow()` a fechas timezone-aware;
- seguir acumulando outcomes de ejecución real antes de sacar conclusiones fuertes.
