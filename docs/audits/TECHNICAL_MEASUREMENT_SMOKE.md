# Quantia — verificación acotada de cálculo técnico (18/10/2026)

**Objetivo:** comprobar indicadores y ranking con series sintéticas; **no** auditar de nuevo la base, calcular PnL ni desarrollar otro shadow.

## Hallazgo corregido en esta rama

- En `src/analysis/technical.py::_rsi`, 14 ganancias y ninguna pérdida daban `NaN` (la división por pérdida cero se convertía en `NaN`). Esto omitía la contribución RSI de sobrecompra en `generate_signals`.
- La fórmula vigente es **RSI de medias móviles simples** de cambios absolutos de precio, no RSI Wilder. Esta corrección **no cambia su suavizado**: define 100 cuando hay ganancias sin pérdidas, 0 con pérdidas sin ganancias y 50 sin variación; antes del período mínimo se mantiene `NaN`.
- La corrección **puede modificar score técnico, motivos y señales futuras** en casos extremos. No reescribir observaciones históricas ni integrar sin revisar el impacto.

## Qué cubren las pruebas

RSI ascendente/descendente/plano/mixto; SMA, EMA, MACD, Bollinger, ATR y volumen en una serie sintética; transmisión de RSI a reversión técnica; rechazo de barra posterior al cutoff; ranking de Radar independiente de outcomes históricos. La prueba de orden usa el algoritmo productivo, no un score alternativo.

## Diferencia detectada y pendiente deliberado

`opportunity_screener.py::_compute_screener_metrics` calcula `m.rsi` sobre *retornos porcentuales*, mientras `technical.py::_rsi` calcula RSI sobre *variaciones absolutas*. Además, en una ventana plana, el screener lo interpreta como 100. **No se modifican esas fórmulas ni los thresholds del Radar en este PR**: requieren una decisión explícita sobre la definición canónica y una comparación de impacto con capturas congeladas antes de cambiar el ranking.

## Límites y gate

Las pruebas validan aritmética básica y casos frontera **sin datos de BYMA**, instrumentos, CEDEAR/CCL, cobertura de volumen ni PnL. No demuestran capacidad predictiva ni ventaja respecto de HOLD.

```powershell
python -m pytest tests/test_technical_measurement_contract.py tests/test_radar_discovery.py -q --override-ini addopts=''
```

Mantener el PR en Draft hasta que los tests pasen y se revise el posible cambio de señales sobre fixtures congelados. Sin cambios de DB, scheduler, órdenes, scoring del Radar, optimizer, planner ni PRs #25/#28.
