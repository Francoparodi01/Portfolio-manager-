# Datos y Persistencia

## Fuentes

| Fuente | Uso | Persistencia |
|---|---|---|
| Cocos portfolio | cartera, cash, posiciones | `portfolio_snapshots`, `positions`, `raw_snapshots` |
| Cocos market ACCIONES | universo local, precios actuales | `market_prices` |
| Cocos market CEDEARs | universo CEDEAR por Top/ETF/Otros/Nuevos | `market_prices` |
| Cocos movements | actividad de instrumentos/caja | `broker_movements`, `broker_fills` |
| Cocos historic-data | backfill oficial/manual | `market_candles` con `COCOS` |
| TradingView/BYMA | backfill historico comparable en ARS | `market_candles` con `TRADINGVIEW_BYMA` |
| Snapshots propios | continuidad diaria | `market_candles` con `internal_snapshot` |
| Macro/yfinance/APIs | contexto de mercado | no es fuente canonica de precios operables |

Crypto en CEDEARs queda excluido por decision del proyecto.

## Prioridad de Velas

Cuando hay mas de una vela para el mismo ticker/dia:

1. `COCOS`
2. `TRADINGVIEW_BYMA`
3. `internal_snapshot`
4. otras fuentes

Esto permite rellenar espalda historica con TradingView/BYMA sin desplazar a Cocos
cuando exista dato oficial de Cocos.

## Tablas Principales

| Tabla | Funcion |
|---|---|
| `portfolio_snapshots` | snapshot historico de cartera |
| `positions` | posiciones por snapshot |
| `raw_snapshots` | payload crudo de portfolio |
| `market_prices` | snapshots de mercado por ticker |
| `market_candles` | OHLCV canonico |
| `decision_log` | decisiones, bloqueos, ejecuciones, outcomes |
| `broker_fills` | fills reales reconciliables |
| `broker_movements` | actividad Cocos de instrumentos/caja |
| `bot_users` | usuarios y credenciales cifradas |
| `ml_decision_features` | feature store experimental |
| `ml_model_registry` | registro experimental de modelos |

## Fills y Movements

`broker_movements` conserva la actividad observada en Cocos. Cuando un movimiento
tiene ticker, lado, cantidad, precio y fecha, puede convertirse en fill.

`broker_fills` se reconcilia contra `decision_log`:

- match estricto contra `execution_plan / APPROVED` => `EXECUTED`;
- si no hay plan aprobado compatible, se materializa como `EXECUTED_MANUAL`;
- nada queda pendiente si se pudo asociar a una operacion real.

Esta separacion evita atribuir al bot decisiones que fueron manuales.

## Outcomes

Los outcomes se calculan contra `market_candles`:

- `outcome_5d`
- `outcome_10d`
- `outcome_20d`

Convencion:

- BUY gana si el precio sube.
- SELL gana si el precio baja.

Los outcomes se actualizan solo cuando existen velas suficientes para medir el horizonte.

## TradingView/BYMA

El script `scripts/backfill_tradingview_byma.py` usa simbolos `BYMA:{ticker}` por defecto.
Esto mantiene comparabilidad en ARS con Cocos.

Aliases conocidos:

| Cocos | TradingView/BYMA |
|---|---|
| `BA.C` | `BAC` |
| `BRKB` | `BRKB` |
| `C.I.` | `C` |

Uso:

```bash
python scripts/backfill_tradingview_byma.py --asset-type ALL --bars 260
python scripts/backfill_tradingview_byma.py --tickers ASTS --bars 260
```

No se usan NYSE/NASDAQ para comparar CEDEARs operables en ARS.

## Estado de Cobertura

El radar ya no llama "externos" a CEDEARs detectados en Cocos. El estado correcto es:

```text
EN COCOS / SIN HISTORICO OPERABLE
```

Significa que el ticker existe en Cocos, pero aun no tiene suficientes velas para
analisis operativo. No depende de fills; los fills solo aparecen si hubo operaciones reales.

