# Dashboard de estadísticas del bot

Dashboard local y de solo lectura para reconstruir resultados desde `execution_plans`, `order_intents` y `market_candles` de PostgreSQL. No borra ni reinicia tablas: "volver a cero" significa ignorar los agregados y recalcular desde registros crudos. No lee métricas precomputadas (`outcome_*`, ledger, viability), ni actividad manual, radar o swaps.

## Ejecución en el equipo que tiene acceso a la BD

Colocar esta carpeta en `scripts/bot_stats_dashboard/` del proyecto Quantia. Usar el entorno Python del proyecto (`asyncpg` ya está en `requirements.txt`). Configurar `DATABASE_URL` y `OWNER_CHAT_ID` en variables de entorno del proceso, sin pegarlas en el código ni en el navegador. En Windows PowerShell, desde la raíz del repositorio:

```powershell
python scripts/bot_stats_dashboard/server.py
```

Abrir `http://127.0.0.1:8765`. Si `DATABASE_URL` usa el hostname Docker `db`, el proceso debe correr en una red que pueda resolver ese nombre o usar una URL local/externa con acceso de lectura. El servidor escucha solo en loopback y usa transacciones PostgreSQL read-only.

## Definiciones

- Población: intenciones `BUY`/`SELL` ejecutables, no bloqueadas, de planes factibles del usuario (`owner_chat_id`). No implica que el usuario haya ejecutado la recomendación.
- Duplicados: una señal por fecha argentina, ticker y lado; gana la primera intención del día. Días distintos pueden representar el mismo trade económico y solaparse: **n no es un número de episodios independientes**.
- Precios: velas diarias ARS de BYMA. La fecha de la vela usa día UTC como los joins de velas del proyecto. Entrada: apertura de la próxima sesión disponible luego del día del plan. Salida: cierre de la sesión H contando la entrada como sesión 1. Si el precio de entrada o salida es ambiguo o no existe, el resultado queda pendiente.
- Retorno: `BUY = cierre_salida / apertura_entrada - 1`; `SELL = 1 - cierre_salida / apertura_entrada` (caída evitada frente a HOLD). EV neto resta el costo supuesto (por defecto 75 puntos básicos) una vez por señal. Es un **contrafactual direccional**, no PnL de cuenta ni simulación ejecutable con cash, sizing y restricciones.
- Cobertura: n, pendientes, velas inválidas/conflictivas y señales excluidas a la vista. Si la consulta excede el límite de seguridad, falla en vez de mostrar una muestra como si fuese el universo.

## Comprobaciones necesarias con BD real

Antes de usar resultados para decisiones, confirmar `currency`/`venue` y calendario de `market_candles`, ajustes por eventos corporativos, volumen de filas, fecha de la última vela, conteo de planes elegibles y exclusiones. No hubo acceso a la BD desde el entorno de desarrollo, por lo que el dashboard aún no tiene cifras verificadas contra datos productivos.
