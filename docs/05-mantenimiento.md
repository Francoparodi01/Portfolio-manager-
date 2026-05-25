# Mantenimiento

## Backfill Cocos

Requiere Chrome con debugging remoto:

```powershell
Start-Process chrome.exe -ArgumentList '--remote-debugging-port=9222','--user-data-dir=C:\Temp\cocos-cdp-profile','https://app.cocos.capital'
```

Captura puntual:

```bash
python scripts/capture_cocos_history.py CEDEARS NVDA --output logs/nvda_history.json
python scripts/import_cocos_history.py logs/nvda_history.json
```

Batch:

```bash
python scripts/backfill_cocos_history.py --import-db
```

## Backfill TradingView/BYMA

Recomendado para ampliar cobertura rapidamente cuando Cocos ya lista el ticker.

```bash
python scripts/backfill_tradingview_byma.py --asset-type ALL --bars 260
python scripts/backfill_tradingview_byma.py --tickers ASTS --bars 260
```

La fuente queda marcada como `TRADINGVIEW_BYMA`. Cocos sigue teniendo prioridad.

## Validacion de Precios

```bash
python scripts/validate_byma_prices.py --tickers GGAL MELI ASTS
python scripts/validate_byma_prices.py --asset-type CEDEAR --limit 20
```

Se compara Cocos DB contra `BYMA:{ticker}` para mantener ARS vs ARS.

## Fills

Sincronizacion Cocos:

```bash
python scripts/sync_cocos_fills.py
```

CSV manual:

```bash
python scripts/import_broker_fills.py logs/fills.csv
```

Despues de fills:

```bash
python scripts/update_outcomes.py
python scripts/run_performance.py --no-telegram
```

## Calidad de Datos

Checklist:

```bash
python scripts/run_confidence_audit.py --no-telegram
python scripts/outcome_status.py
python scripts/run_opportunity.py --no-telegram
```

Consultar cobertura:

```sql
SELECT asset_type, COUNT(*) FROM market_prices GROUP BY asset_type;

SELECT ticker, source, COUNT(*)
FROM market_candles
GROUP BY ticker, source
ORDER BY ticker, source;
```

## Limpieza Futura

No borrar archivos sin antes clasificarlos:

| Categoria | Accion |
|---|---|
| runtime activo | conservar |
| script operativo manual | conservar o mover a `scripts/maintenance` |
| experimento no usado | archivar o eliminar |
| output generado | ignorar/eliminar |
| tests sin uso | eliminar solo si no hay CI ni valor de regresion |

`tests/` actualmente esta ignorado y no tiene archivos trackeados. La decision de
eliminarlo puede tomarse en el paso de limpieza estructural.

## Politica de Limpieza Recomendada

1. Inventariar archivos por uso real.
2. Confirmar imports con `rg`.
3. Verificar entrypoints Docker/Telegram/scheduler.
4. Mover lo dudoso a una rama o commit separado.
5. Eliminar outputs generados.
6. Recompilar y correr smoke tests.

Smoke tests minimos:

```bash
python -m py_compile scripts/run_analysis.py scripts/run_opportunity.py scripts/run_performance.py scripts/telegram_bot.py
docker compose exec scheduler python scripts/run_confidence_audit.py --no-telegram
docker compose exec scheduler python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
```

