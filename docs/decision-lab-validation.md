# Decision Lab: validación y despliegue del 24/09/2026

La implementación permite reproducir contrafactuales de planes registrados y
rechazar reconstrucciones sin evidencia suficiente. **No valida que Quantia genere
valor económico**, ni completa Analytics v2.

## Datos reales inspeccionados

Período de decisiones: 10/08/2026–23/09/2026. Corte de outcomes:
24/09/2026 11:00 UTC. Exportación PostgreSQL read-only con provider de velas diario
`TRADINGVIEW_BYMA`, moneda ARS y fechas de sesión UTC del provider. No se descargaron
precios para rellenar huecos ni se ejecutó el pipeline productivo para este replay.

| Medida | Resultado |
|---|---:|
| Planes inspeccionados | 112 |
| Excluidos por enlace sobrescrito de otro run | 82 |
| Propuestas registradas evaluables | 30 |
| Registros de cartera / planes / features / barras exportados | 30 / 30 / 165 / 3.381 |
| Episodios con calidad HIGH elegibles para inferencia primaria | 0 |

| Horizonte | Pares maduros descriptivos | n efectivo conservador |
|---|---:|---:|
| 5 sesiones | 24 | 5 |
| 10 sesiones | 21 | 3 |
| 20 sesiones | 11 | 1 |
| 40 sesiones | 0 | 0 |

Estos n efectivos son proxies de ventanas de cuenta no solapadas, no estimaciones
de independencia demostrada. Los resultados se clasifican LOW y no reciben
intervalos inferenciales. A 40 sesiones siguen inmaduros; no se imputan retornos cero.
En 5 sesiones hay además un cierre faltante: se conserva UNAVAILABLE, separado de
los cinco episodios PENDING. En 10/20/40 sesiones quedan 9/19/30 PENDING.

El registro operativo es `RECORDED_PLAN_EVALUATION`, no reejecución de una versión
histórica acreditada. Las versiones productivas originales, el universo completo,
vintages macro/FX y ajustes históricos no se recuperaron de manera exacta.

## Recibos reproducibles y comprobación independiente

Kernel numérico congelado en commit `1541193` y manifest de implementación/runtime.
Los commits posteriores de consultas y documentación no cambian ese kernel.

- Run de un episodio:
  `df5f7d67aa1c3fd2a2e8fe05d152e7279a1eadbb542291a7753f0751c370ecb7`.
- Run de 30 episodios:
  `737951b6dd461527f8274af304b7fa25d03c74297bb9c41f1721b45c3341d59d`.
- Ambos pasaron validación de todos los hashes y recálculo completo con el mismo
  runtime de Python 3.13 en Windows. Sus manifests conservan versiones exactas.
- Tiempos observados, incluyendo recálculo: 0,630 s y 2,134 s. Son mediciones locales,
  no un límite duro de CI ni una extrapolación a otro volumen de datos.

Se recalcularon independientemente los episodios del 11, 13 y 20 de agosto, a cinco
sesiones. Se verificaron cartera inicial, nominales, próxima apertura, cierre de la
quinta sesión, costos y ausencia de evidencia posterior al corte. Para estos planes
de reducción se utilizó la identidad:

```text
delta_ARS = sum(nominales_vendidos * (open_entrada - close_salida)) - fees
DVA = delta_ARS / capital_comun
PLAN_NAV = HOLD_NAV + delta_ARS
```

Las tres comprobaciones coinciden con el motor a tolerancia de 1e-10 en retorno.
La política simulada aplica 75 bps por lado ejecutado, PRICE_ONLY, ARS nominal sin
intereses. No representa costos observados ni PnL realmente ejecutado.

Los datos de cuenta y retornos detallados permanecen en artefactos privados ignorados
por Git: `outputs/decision-lab/<run_id>/` y
`tmp/decision-lab/release-validation.json`. Incluyen precios/legs de la comprobación,
exclusiones, tablas, Markdown, manifest y feed para Analytics v2. No se publican
carteras, identificadores de propietario ni fills en el repositorio.

## Tests

| Verificación | Resultado |
|---|---|
| Suite completa final del checkout de trabajo | 490 passed, 4 failed |
| Cuatro fallos reproducidos sobre baseline `7558c61` | Preexistentes: menú/help/Radar |
| Integración local Lab + Analytics v2 + agente | 176 passed |
| Analytics v2, compatibilidad del helper estadístico | 60 passed |
| Regresión final de routing/agente/Telegram en checkout operativo | 111 passed |
| Fixture de adapter core usando funciones productivas y socket prohibido | PASS |
| PostgreSQL aislado: idempotencia, owner y rechazo UPDATE/DELETE | PASS; rollback del schema de test |
| Persistencia real del run, repetida | Una fila de run; 1.499 objetos inmutables |
| Captura en imagen del scheduler, dataclass y DB | PASS; rollback del dato sintético |
| Flujo Telegram con sink falso, cuatro preguntas y trazas | COMPLETE; sin mensajes reales de prueba |
| Consulta desde imagen Telegram ya activada | PASS; run y DVA almacenados correctos |

Los cuatro fallos antiguos son:

- `test_native_command_menu_keeps_only_primary_workflows`
- `test_compact_radar_parses_header_with_cash_lines`
- `test_help_text_is_mobile_compact_and_scope_safe`
- `test_compact_radar_preserves_shadow_context_line`

No se declara suite global verde ni cobertura global >=90%: ese umbral no fue
certificado para este checkout. No se cambiaron tests viejos para ocultar fallos.

```powershell
python -m pytest tests/test_decision_lab.py tests/test_decision_lab_statistics.py tests/test_decision_lab_persistence.py tests/test_decision_lab_agent.py -q
python scripts/check_decision_lab_db.py
python scripts/run_decision_lab.py validate --folder outputs/decision-lab/<run_id> --recompute
```

La comprobación con `--recompute` requiere el código y dependencias del manifest.
El mismo dataset bajo otro runtime se registra como otro experimento/run, no se
sobrescribe evidencia previa. La validación de hashes sola no requiere recalcular.
El artefacto también se copió al checkout operativo y pasó `validate --recompute`
allí con el mismo entorno Python de validación.

## Despliegue operativo

Migración aditiva ejecutada: `decision_lab_objects`, `decision_lab_runs`,
`decision_lab_plan_captures`, índices y triggers que rechazan UPDATE/DELETE.
Las tablas de señales, planes, órdenes y fills no cambian. El run real quedó
persistido y las consultas del agente exigen owner exacto; otro owner devolvió
INSUFFICIENT sin datos.

Se activaron las imágenes locales el 24/09/2026 a las 21:49 UTC:

| Servicio | Imagen/tag | Base preservada |
|---|---|---|
| Telegram | `quantia-telegram-decision-lab:65632bb` | Imagen operativa previa; sólo archivos del cambio |
| Scheduler | `quantia-scheduler-decision-lab:1541193` | Imagen anterior propia; sólo hook de captura y Pydantic 2.13.5 |

El scheduler tenía una versión anterior de `run_analysis.py`; se agregó únicamente
el hook de captura a esa versión. Ambos arrancaron sin errores de importación ni
reinicios en la verificación posterior. No se disparó un análisis formal para
fabricar una captura: el próximo plan formal utilizará el hook.

El checkout operativo conserva sus cambios locales previos. Los commits se hicieron
en `feature/decision-lab-pit-replay`, basado en `feature/agentic-loop-orchestrator`.
La rama anterior y el PR #1 no se modificaron.

Recibos locales de imágenes/archivos y override de Compose:
`tmp/decision-lab/deploy/`, `deployment-receipt.json` y
`telegram-smoke-before-activation.json`. Para recrear esas imágenes locales:

```powershell
docker compose -f docker-compose.yml -f tmp/decision-lab/deploy/compose.yaml up -d --no-deps --no-build telegram_bot scheduler
```

Las imágenes anteriores se conservaron con tags
`quantia-telegram-before-decision-lab:20260924` y
`quantia-scheduler-before-decision-lab:20260924`. Un rollback de código no elimina
las tablas ni evidencia append-only. Estos tags son locales, no un registry público.

## Qué falta para afirmar valor económico

Se necesitan episodios prospectivos con fuentes, universo y corporate actions
completos; versiones congeladas; cobertura de madurez; muestra suficiente de ventanas
comparables; costos observados o escenarios preregistrados; holdout no visto e
intervalos que sostengan el efecto. La captura agregada es un paso de trazabilidad,
no una certificación automática HIGH.

La comparación REAL_HUMAN_EXECUTION sigue NOT_APPLICABLE por falta de cobertura y
atribución completa. El adapter de política moderna cubre el core de cartera, no
todo Radar/guards del pipeline. Las comparaciones entre versiones están implementadas
y probadas con fixtures, pero este run real sólo contiene una versión registrada:
no existe un ganador empírico A/B que Telegram pueda afirmar.
