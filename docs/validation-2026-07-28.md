# Snapshot de validacion - 2026-07-28

Este snapshot registra la validacion hecha para la entrega documental. No cambia
logica productiva, thresholds, schemas ni comportamiento operativo.

## Checkout

- Repo: `C:\Users\Franco\OneDrive\Escritorio\backend\cocos_copilot`
- Branch: `main`
- Motivo: memoria y filesystem confirman que este es el checkout vivo; no se uso
  `cocos_copilot_1-copia`.

## Validacion local

| Comando | Resultado |
|---|---|
| Validacion de links Markdown internos | OK |
| Validacion de rutas de codigo/config referenciadas | OK |
| `python -m compileall -q src scripts` | OK |
| `python -m pytest -q` | `310 passed, 2 skipped` |
| `python scripts/* --help` para entrypoints documentados | OK |

## Ajuste de test

Se corrigio el test double en
[tests/test_broker_fill_db.py](../tests/test_broker_fill_db.py):

- Falla inicial: `_RecordingConnection.fetch()` aceptaba solo `statement`.
- Codigo real: `PortfolioDatabase.save_broker_fills()` termina llamando
  `_mark_superseded_broker_fills_for_real(conn, source, external_fill_id)`, que
  usa `conn.fetch(statement, source, external_fill_id, reason)`.
- Correccion: el mock acepta `*args` y devuelve `[]` para consultas de marcado
  `UPDATE broker_fills synthetic`.
- Resultado: suite local completa pasa.

## Smoke Docker

| Check | Resultado |
|---|---|
| `docker compose version` | Docker Compose v5.1.0 |
| `docker compose config --quiet` | OK |
| Servicios vivos por `docker ps` | `cocos_scheduler`, `cocos_telegram_bot`, `cocos_monitor_api`, `cocos_pg_db` |
| Import dentro de `scheduler` | OK |
| Import dentro de `monitor_api` | OK |
| Import dentro de `telegram_bot` | OK, `BOT_COMMAND_SPECS=18` |
| `GET /api/health` con token local | HTTP 200, `ok=True`, `database.ok=True`, `redis.ok=True` |

Anomalia Docker local: `docker compose ps --all` falla por una referencia stale
a un contenedor muerto sin nombre (`No such container: eeae...`). `docker ps`
lista los servicios vivos correctamente y los imports/health checks pasan. No se
removieron volumenes.

## Auditoria viva

### Confidence audit

Comando:

```powershell
docker compose exec -T scheduler python scripts/run_confidence_audit.py --days 180 --no-telegram
```

Resultado principal:

- Veredicto: confiable para auditoria estadistica inicial.
- Portfolio reciente: 2026-07-28 17:02, valor aproximado `$1,741,521`.
- Market prices recientes: 2026-07-28 17:03, 178,322 filas 7d, 355 tickers.
- Candles canonicas: velas 2026-07-28, 318 activos, faltantes hoy 0.
- `decision_log` 180d: 653 filas.
- Execution plan: 364; approved/executable: 88; executed bot: 31;
  executed manual: 123.
- Blocked: 245; optimizer: 27; radar: 139.
- Outcomes cerrados: 5d 528; 10d 500; 20d 439.
- Movimientos/fills Cocos: 167 total, 167 reconciliados, 0 pendientes.

### Performance 90d

Comando:

```powershell
docker compose exec -T scheduler python scripts/run_performance.py --days 90 --no-telegram
```

Resultado principal:

- Muestra operativa 5d: 115 outcomes cerrados.
- Acierto: 49% (56 ganadoras / 59 perdedoras).
- EV operativo 5d: +0.1%, marginal.
- Ganancia promedio al acertar: +7.8%.
- Perdida promedio al fallar: -7.3%.
- Pendientes operativos: 13.
- Fuentes usadas: `broker_movement/EXECUTED_MANUAL` 91,
  `execution_plan/EXECUTED` 24.
- Equity curve: 100 -> 95.5; retorno acumulado -4.5%; max drawdown -11.1%.
- Lectura: la muestra cerrada viene apenas positiva; seguir midiendo.

### Viability audit 180d

Comando:

```powershell
docker compose exec -T scheduler python scripts/run_viability_audit.py --days 180 --no-telegram
```

Resultado principal:

- Costo usado: 0.75%.
- Muestra minima: 30.
- Bot-only 5d: n=24, win 50.0%, EV neto +0.3%, MaxDD -38.7%, IC +0.285.
- Manual-only 5d: n=100, win 45.0%, EV neto -0.7%, MaxDD -90.0%.
- Gates: IC positivo, EV neto positivo, EV bot > manual y drawdown bot menor
  que manual pasan; muestra bot-only 5d no pasa porque n=24 < 30.
- Lectura: viable como proyecto, edge bot no validado por falta de muestra
  bot-only cerrada.
- No se aflojaron thresholds ni guards.

## Estado de incertidumbre despues de validar

- Edge bot todavia no validado: falta muestra bot-only cerrada.
- Performance 90d operativa es marginal, no evidencia fuerte de edge.
- Docker tiene una anomalia local de metadata/compose ps, aunque servicios vivos
  y health check pasan.
- No se valido backup/restore, despliegue remoto ni multiusuario end-to-end.
