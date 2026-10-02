# Dashboard de estadísticas del bot

Dashboard local de investigación, limitado al owner configurado, que reconstruye retornos de precio desde tablas crudas. No usa `outcome_*`, estadísticas previas ni agregados del ledger. No ejecuta órdenes ni modifica PostgreSQL, estrategia, guards o configuración.

## Ejecutar sin copiar secretos

Requiere Python, `asyncpg`, `python-dotenv` y `tzdata` (en Windows). Desde el checkout de esta rama:

```powershell
python scripts/bot_stats_dashboard/server.py --env-file C:\ruta\al\checkout\Quantia\.env
```

El archivo existente sólo se lee. Variables ya presentes en el proceso tienen precedencia. El owner se toma de `OWNER_CHAT_ID` o, en su ausencia, `TELEGRAM_CHAT_ID`. No se acepta un owner desde la URL del navegador. No se debe ampliar el scope cuando una muestra está vacía.

Abrir http://127.0.0.1:8765. `--port` permite elegir otro puerto libre. Sólo escucha en loopback. Cada consulta usa `default_transaction_read_only=on`, transacción `readonly=True`, aislamiento `repeatable_read`, timeout y límites de extracción; un exceso falla sin publicar una muestra truncada. No importa los clientes de aplicación que crean esquemas.

## Definiciones y exclusiones

- Población: `execution_plans.source='execution_plan'`, plan factible, intención BUY/SELL ejecutable y `was_blocked IS FALSE`, del owner exacto. El conteo de planes es independiente del join a intenciones para conservar planes sin órdenes.
- Unidad: primera intención elegible por fecha ART/ticker/lado. No significa duplicados idénticos ni episodios independientes: puede haber solapamiento entre días y ambos lados del mismo ticker.
- Calendario: sesiones BYMA previstas, nunca la próxima fila disponible. Entrada en la sesión posterior a la fecha del plan; salida al cierre H incluyendo entrada como sesión 1. Se exige cobertura completa de las H sesiones para detectar discontinuidades. Faltantes no desplazan fechas.
- Rango de calendario revisado para este dashboard: 01/01/2026 a 02/10/2026, contra [BYMA](https://www.byma.com.ar/mercado/calendario-bursatil). Jornadas con negociación pero sin liquidación sí cuentan. Fuera de ese rango, los horizontes maduros quedan `calendar_unverified`; extenderlo requiere otra revisión, sin tocar el calendario operativo.
- Velas: `1d`, ARS, BYMA, fecha UTC. Se selecciona una fuente e instrumento completos por horizonte, primero COCOS y después TRADINGVIEW_BYMA. Fuentes o instrumentos no se mezclan dentro del horizonte. Yahoo e `internal_snapshot` quedan excluidos. Conflictos en una misma serie invalidan ese día de forma independiente del orden de filas. Otro proveedor completo puede servir como fallback y se registra en la evidencia.
- Corte conservador: sólo sesiones anteriores al día corriente ART; también se excluyen capturas realizadas antes de las 18:00 ART del día de la vela, o posteriores al corte de consulta. Se informa por separado corte de consulta y fecha máxima permitida de precios. Esa fecha máxima no acredita frescura por ticker.
- Eventos: ventanas con eventos corporativos activos registrados o discontinuidades diarias mayores al 30% quedan sin evaluar. No hay rebasing automático. La heurística puede excluir movimientos genuinos y no detecta todos los eventos. Un registro vacío no certifica ausencia de eventos. Sin las tablas del registro, el cálculo falla cerrado por horizonte.
- BUY bruto = cierre/apertura - 1; SELL bruto = 1 - cierre/apertura (caída evitada contra mantener, no rentabilidad de una venta ejecutada). Neto = bruto - costo_pb/10000, descontado **una vez**. Los 75 pb por defecto son un supuesto total, no una comisión real por lado.
- EV = media por señal evaluable; acierto = neto > 0 / señales evaluables; cobertura = evaluables / señales únicas. Con denominador vacío, estas métricas son `null` / N/D. n=0 sí es un conteo real.
- Es retorno nominal de precio, no retorno total, DVA, cartera ejecutable ni PnL realizado. No incorpora dividendos, flujos, tamaño, restricciones, impuestos o costos observados. No es un backtest PIT: los registros pueden haber sido actualizados o backfilled.

## Verificar y conservar evidencia local

```powershell
python -m pytest scripts/bot_stats_dashboard -q
python scripts/bot_stats_dashboard/audit.py --env-file C:\ruta\al\checkout\Quantia\.env
```

`audit.py` guarda `output/bot-stats/raw-audit.json`, sin owner ID ni credenciales. El perfil de mercado usa exclusivamente tickers presentes en el `decision_log` del mismo owner y queda etiquetado como contexto, no como muestra bot. Es una consulta adicional posterior al snapshot del dashboard y registra su propio corte. Los SELECT de reconciliación de denominadores están en `server.py`; se ejecutan en el mismo snapshot transaccional que los datos del cálculo.

En el navegador: ventanas 90/180/365 días, costo editable, descarga JSON completa de la vista y “Imprimir / guardar vista en PDF”. La exportación usa el mismo HTML y sus estilos de impresión. Si existe `output/pdf/quantia-bot-stats.pdf`, `/report.pdf` sirve ese informe auditado fijo, identificado con su fecha/ventana/costo; no cambia al mover filtros. Para un PDF nuevo, imprimir la vista actual. Conservar las evidencias de cuenta localmente; no agregarlas al commit.

## Resultado de validación del 02/10/2026

43 pruebas focalizadas aprobadas, compilación Python, conexión a la PostgreSQL usada por el scheduler y cotejo SQL de sólo lectura. Para el owner configurado en el `.env` activo no hay planes formales, ni siquiera fuera de la ventana. Por eso los cuatro horizontes tienen n=0 y EV, acierto y cobertura N/D. No se rellenó esa ausencia con Radar o actividad manual.

Se revisaron escritorio/móvil, ventanas, costo, JSON y PDF renderizado. La evidencia de precios real permite auditar fuentes/moneda/timestamps para tickers del owner, pero **no valida retornos reales por señal del bot**, porque no existe esa muestra. Los casos positivos y los cuatro horizontes se validaron con fixtures sintéticos explícitos. El PnL realizado de cuenta queda sin medir: los conteos de fills/snapshots no sustituyen una conciliación de lotes, costos y flujos.
