# Backtest configurable por Telegram

`/backtest` muestra ayuda y botones para iniciar 20 o 50 ruedas, consultar estado y descargar el último resultado. El comando está separado de `/analisis_full` operativo: calcula una cartera ficticia, no publica planes operativos ni envía órdenes al broker.

```
/backtest hasta=26/10/2026
/backtest desde=01/08/2026 hasta=26/10/2026 capital=3m medir=todo
/backtest ruedas=50 hasta=26/10/2026 tickers=AMD,NVDA,PYPL,AXP,MU medir=riesgo,operaciones
/backtest estado
/backtest cancelar
/backtest resultado
/backtest historial
```

Los tres últimos comandos aceptan el ID completo obtenido del historial. Cada usuario sólo accede a sus propias corridas. Fechas `DD/MM/YYYY` o ISO; `hasta=hoy` usa la última rueda completa si aún no cerró. Se usa el calendario argentino del proyecto, hasta 120 ruedas, y entre uno y seis cortes por rueda.

| Parámetro | Predeterminado | Significado |
| --- | --- | --- |
| desde / ruedas | 20 ruedas | Alternativas para fijar inicio |
| hasta | última rueda completa | Fecha final; no admite futuro |
| capital | 3000000 | Efectivo ficticio inicial ARS |
| tickers | auto | Universo histórico disponible, o lista separada por coma |
| horas | 11,13,16 | Hora argentina, entre 10:30 y 16:59 |
| medir | todo | rentabilidad, riesgo, operaciones, efectividad, activos, horizontes |
| horizontes | 5,10,20 | Resultados acumulados desde T0; fuera del período queda no evaluable |
| comision | 0.6 | Porcentaje por lado en fills simulados |
| deslizamiento | 0.15 | Deslizamiento adverso porcentual por lado en fills simulados |
| arranque | nativo | `nativo` y `radar` mantienen compatibilidad; ambos habilitan el arranque desde efectivo solo en backtest |

## Semántica y evidencia

Se ejecuta el motor actual `scripts.run_analysis.main` con radar, optimizer y sentimiento habilitados, LLM deshabilitado, `no_persist=True`, `no_telegram=True`, `run_intent=exploratory`. Un adaptador aporta cartera ficticia, histórico y reloj por corte. No se modifican las reglas de decisión o guardas nativas. Los costos elegidos afectan el ledger; el planner conserva sus propios supuestos de costos.

Desde 2026-10-28, el replay completa el `ExecutionPlan` cuando hay efectivo y no hay posiciones ni plan previo. Usa únicamente candidatos elegibles del radar y el reconciliador nativo, con sus umbrales, riesgo, costos, nominales y bloqueos por eventos/calidad. Sin candidatos elegibles puede conservar efectivo. La política `ELIGIBLE_RADAR_V1`, variante `FULL_CASH_START`, se registra en los parámetros y el informe; es exclusiva del backtest y no modifica `/analisis_full` operativo. El worker normaliza también solicitudes de versiones anteriores del bot antes de capturar datos. Los resultados históricos conservados no se reescriben. El adaptador no reescribe código fuente y rechaza llamadas persistentes u operativas.

El ledger procesa cronológicamente la primera cotización ARS registrada al menos 60 segundos después del análisis y anterior al siguiente corte o cierre. Una venta posterior no financia una compra anterior. No hay cortos ni saldo negativo. Los fills son modelados, sin profundidad ni garantía de ejecución. No hay dividendos en efectivo, impuestos, retorno del efectivo o benchmark HOLD validado.

Las velas deben ser de sesiones anteriores y haber sido capturadas antes del corte; no se utilizan cierres futuros. El macro usa series públicas reconstruidas de días anteriores, no el archivo original intradiario. Se retienen restricciones temporales de sentimiento y eventos, pero sus revisiones pueden faltar. Earnings sin archivo completo e information coefficient usan fallbacks nativos. Este replay no acredita rentabilidad real ni reconstruye necesariamente la política histórica.

Efectividad significa proporción de ciclos cerrados ganadores, después de costos modelados. Una posición se cierra cuando vuelve a cero; las ventas parciales acumulan PnL en el mismo ciclo. Sin ciclos, la proporción es nula/no evaluable. Los ciclos no se suponen estadísticamente independientes. Drawdown se calcula sobre valuaciones de cierre diario incluyendo capital inicial, no sobre máximos intradiarios.

Los resultados `COMPLETE`, `PARTIAL` y `NOT_EVALUABLE` describen ejecución/cobertura. Todos conservan `economic_verification=NOT_VERIFIED`. Los resultados parciales indican su fecha real de valuación. Fallas de aislamiento o cortes sin universo/historial técnico mínimo impiden publicar retorno. Las alertas de calidad y precios anteriores se conservan.

## Operación

### Variantes de rotación de la simulación

`/backtest` acepta `delta_band_mult=1|2|3`, `radar_buy_tiers=ALL|SIN_ESPERAR_FUNDING|SOLO_ABRIR_POSICION`, `redeploy_cooldown_sessions=0|3|5` y `min_hold_sessions=0|3|5|10` (un valor por parámetro). Se guardan como `rotation_overrides` en el spec y en el manifest, incluidos en el hash del spec. La confirmación y el estado muestran los filtros pedidos.

Sin parámetros adicionales, el replay conserva el control. Los filtros solo se instalan dentro del proceso aislado de simulación; el primer corte desde efectivo y las excepciones de riesgo siguen protegidos. No modifican el scoring, el planner productivo ni el scheduler.

Ejemplo de banda triple:

```text
/backtest desde=15/07/2026 hasta=09/09/2026 capital=3000000 tickers=auto horas=11,13,16 medir=todo horizontes=5,10,20 comision=0.6 deslizamiento=0.15 delta_band_mult=3
```

La captura SQL se escribe por lotes de 5.000 filas dentro de una única transacción read-only/repeatable-read. Las cotizaciones intradiarias no se recortan por un máximo fijo de filas: se conservan todas las del período, con presupuesto de 512 MiB para el archivo de captura. Los demás límites de conjuntos y los controles de precios conflictivos siguen vigentes. El archivo se publica atómicamente sólo al completar y validar la captura; un error elimina el temporal y conserva cualquier captura anterior. Superar el presupuesto devuelve `CAPTURE_SIZE_LIMIT`, sin muestreo ni truncamiento silencioso.

El bot sólo valida y encola. Un worker dedicado ejecuta una corrida por vez, con 0.75 CPU / 3 GiB; máximo cinco corridas activas y una por usuario. La cola y resultados están bajo `/app/outputs/backtests`. El worker requiere sólo `DATABASE_URL` y el token de Telegram: sin secretos del broker ni Docker socket. Captura SQL en transacción explícitamente read-only; el hijo de replay elimina credenciales y bloquea red/persistencia. Límites: captura 15 minutos y replay 6 horas. Un reinicio marca corridas activas `INTERRUPTED`, sin reanudación ambigua. Cancelación termina el grupo de procesos.

Bot y worker deben usar el mismo usuario de sistema (`scraper`, UID/GID 1000:1000). La imagen final y el servicio del worker lo fijan explícitamente; no debe heredar `root` del paso de instalación de dependencias. `/app/outputs/backtests`, sus locks, subdirectorios y archivos deben pertenecer a ese UID. No hace falta ampliar permisos a otros usuarios. La prueba de despliegue debe encolar desde el proceso del bot, sobre la cola compartida real, esperar el resultado del worker y abrir los archivos desde el bot. Probar cada proceso con carpetas propias no detecta este problema.

El worker entrega PDF y ZIP con CSV, parámetros, resultado JSON y hashes SHA-256 de fuentes/código. `analisis_por_activo.csv` conserva señales; `decisiones.csv` conserva decisiones del ExecutionPlan; `cortes.csv` distingue ausencia de plan. Incluye órdenes, fills, motivos, costos, activos y cartera diaria. Fuentes capturadas y trazas completas quedan en el servidor, separadas por corrida. El estado no depende de tener abierta la conversación. Fallas de entrega tienen hasta tres intentos; `/backtest resultado` permite recuperar los archivos.

No se emiten mensajes de prueba a usuarios reales. La validación de entrega usa el handler real con transporte capturado; `get_my_commands` verifica registro en Telegram. El primer uso del usuario valida la recepción efectiva en su teléfono.

Despliegue 2026-10-27: imagen derivada del bot exacto en ejecución, preservando FIL, configuración y montajes existentes; scheduler y motor nativo sin modificaciones. Evidencia y procedimiento de rollback en `C:\Users\Franco\Documents\Codex\audits\quantia-telegram-backtest-20261027`. No aplicar un `compose up` genérico con `.env` sin reconciliar el drift documentado de las variables en ejecución.

Corrección de permisos 2026-10-27: el worker inicial había creado la cola como root, bloqueando `/backtest` desde el bot. Se reparó exclusivamente la propiedad de esa cola y se reemplazó el worker por `quantia-telegram-backtest:20261027-v2` con usuario `scraper`, conservando entorno, montajes y recursos. El bot y scheduler no se reiniciaron. Evidencia y prueba de acceso entre ambos procesos en `C:\Users\Franco\Documents\Codex\audits\quantia-backtest-permissions-20261027`.
