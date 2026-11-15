# Estado del feature — 13 de noviembre de 2026

**Piloto USD ampliado a 50 acciones:** 46 con publicaciones en el archivo SEC,
92 decisiones (14 BUY / 78 CASH), 51 series de precios completas y replay offline
de las 92 decisiones. [Uso, resultados y límites](thesis-desk-usd-pilot.md).
La muestra principal tiene 60 eventos sin solapamiento: retorno medio por decisión
con costos de −0,30% a 5 ruedas y −0,27% a 10; a 20 ruedas, −0,76% sobre 35 maduros
y 25 pendientes. Son retornos por evento, no PnL de cartera. Diez compras
principales a 5/10 ruedas y dos alertas de calidad: no demuestra una ventaja consistente.
La corrida de 25 acciones conserva sus decisiones, precios y resultados idénticos.
Validación: 193 pruebas y 232 retornos maduros recalculados con aritmética decimal.

## Estado previo del desk CEDEAR

**PAPER_ONLY, con validación parcial.** La revisión completa de la cola CEDEAR
y el backtest integral BASE_V1/RISK_V2 siguen pendientes; el piloto USD es otro experimento.
Última corrida PAPER confirmada: `20261107T210015-4d9db4d3`, **7/11, 18:04:34 ART**. Replay `REPLAY_MATCH`, sin cambios
de código durante la ejecución. Se llegó a un punto de pausa: modelo descargado y servidor
CPU propio detenido. No hubo commit, push, deploy ni operaciones reales de este desk.

## Recuperación histórica completada el 12 de noviembre

El faltante principal era de implementación: el colector anterior sólo buscaba publicaciones
recientes y el backtest esperaba un archivo histórico externo. El nuevo colector recorre
el período solicitado, conserva originales y fechas, retoma checkpoints y verifica hashes.

| Archivo histórico | Resultado comprobado |
|---|---|
| Catálogo recorrido | 290 acciones identificadas; 243 con al menos un documento recuperado. Las identidades actuales se usan para descubrir fuentes, no como prueba del universo histórico. |
| Documentos | 1.588 únicos: 1.523 SEC y 65 PDF de CNV. 1.332 publicados del 1/9 al 31/10, y 256 entre el 18/8 y el 31/8 para el período previo de 14 días. Todos pendientes de revisión. |
| SEC | 257 emisores mapeados, 1.097 presentaciones 8-K/6-K y enmiendas. 255 recorridos sin errores de recuperación; BMNR e INFY tienen cuatro adjuntos sin texto extraíble. 33 acciones no tienen mapeo SEC. Esto no equivale a cobertura exhaustiva de noticias. |
| IVV | 10 snapshots con fecha exacta, del 31/8 al 30/10; 504–505 posiciones de equity por snapshot. No se infiere la identidad histórica del instrumento local. |
| Macro | 50 cierres diarios VIX, incluyendo días previos, y dos blackouts de empleo con calendario BLS fechado. La hora original de publicación del VIX sigue desconocida. |
| Recursos y reanudación | Caché configurada hasta 1 GB; la reanudación real conservó idénticos documentos, evidencia y cobertura, con 150 lecturas de disco, una reutilización en RAM y cero solicitudes de red. Pico de cuerpos en caché en esa reanudación: 34.654.221 bytes. Cuatro conexiones como máximo; cero llamadas LLM, cero APIs pagas. |
| Integridad | 4.078 artefactos verificados, sin diferencias de hashes ni cambios de código durante la ejecución. Los 65 documentos CNV provienen de PDF, no de pantallas de carga. |
| Backtest ejecutado | BASE_V1 y RISK_V2: 43 ruedas, 129 cortes, 42.999 cotizaciones; dos blackouts incorporados. Ambas variantes siguen NOT_EVALUABLE y metrics=null. |

La descarga no produce automáticamente tesis aprobadas. Quedan por completar su revisión,
el universo histórico mapeado, el riesgo fechado y los calendarios de balances.
Las 74.294 velas originales tienen cero registros con linaje temporal completo
y cero con política RAW explícita; no se rellenaron esos campos con supuestos.
La cola PAPER del 7/11 y este corpus histórico se mantienen como cohortes distintas.

### Referencia de precios recuperada de la base local

La consulta nueva confirmó 1.673.374 cotizaciones ARS de 403 símbolos entre mayo y octubre.
El runner ahora puede recuperar la última cotización registrada entre 16:30 y 17:00
de cada día, la misma referencia utilizada en PAPER, mediante `--recorded-closes`.
No se presenta como precio de cierre oficial. La captura fue SELECT / READ ONLY.

Se capturaron 24.516 filas por instrumento/día y se admitieron **24.363 referencias
de 383 símbolos**, descartando 153 registros fuera del calendario definido.
El motor pudo calcular una zona de 20 observaciones para **175/290 acciones el 1/9,
173/290 el 1/10 y 189/290 el 30/10**. Es una prueba de disponibilidad de precios con
fechas de anuncio hipotéticas, no son tesis, señales ni operaciones reales.
El origen conserva timestamp y hash de la cotización; la disponibilidad de la
agregación se reconstruye al finalizar la ventana a las 17:00.

Corrida: `outputs/thesis-desk-sep-oct-2026-recorded-20261112/`; comprobación:
`outputs/thesis-recorded-price-validation-20261112.json`. Sin diferencias de hashes.
Se ejecutaron ambas variantes: 43 ruedas, 129 cortes y 42.999 cotizaciones utilizables.
Siguen `NOT_EVALUABLE` por falta de hechos/universo/riesgo completos; los precios ya
no son un bloqueo generalizado para las acciones con suficientes referencias.
Tests tras esta integración: `python -m pytest (Get-ChildItem tests/test_thesis_desk_*.py).FullName -q`
→ **171 passed**. La opción queda explícita y no modifica el modo predeterminado.

Archivos: `outputs/thesis-history-sep-oct-2026-v2/summary.json`, `coverage.csv` (por acción),
`documents.json`, `evidence.json`, `manifest.json`; validación en
`outputs/thesis-history-validation-20261112.json`; ejecución en
`outputs/thesis-desk-sep-oct-2026-history-20261112/result.md`.
Comando de tests: `python -m pytest (Get-ChildItem tests/test_thesis_desk_*.py).FullName -q`:
**162 passed**. Comando reproducible y reanudación: [backtest](thesis-desk-backtest.md#uso).

## Checkpoint PAPER del 7 de noviembre

| Componente | Estado comprobado |
|---|---|
| Universo | 427 registros, 425 símbolos únicos; 290 acciones identificadas, 57 ETF excluidos, 74 identidades desconocidas y 6 registros en conflicto. |
| Fuentes | 511 documentos únicos. Feed oficial acotado completo para 240/290 acciones; calendario de balances para 246/290. No significa cobertura exhaustiva. |
| Research → Contrarian → PM | Qwen 3.5 9B local en CPU, sin API paga. 69/511 documentos revisados. Los roles usan el mismo modelo y no son verificaciones independientes. |
| Cola durable | 441 pendientes de Research y 1 de Contrarian: 442 documentos. Cero decisiones PM pendientes para los candidatos habilitados. |
| Watchlist | VZ por guidance elevado y GGB por adquisición completada. Son hipótesis citadas por el modelo, sin ejecución ni recomendación de inversión. |
| Motor paper | Cero fills, posiciones y ciclos cerrados. Caja y NAV ficticios: ARS 3.000.000. Retorno y aciertos nulos. |
| Medición | Cuatro señales históricas registradas: dos cumplen los controles actuales y dos están excluidas. Las dos incluidas esperan su primera cotización posterior; cero resultados evaluados a 5/20 ruedas. |
| Reporte | HTML offline con búsqueda, filtros, CSV/JSON y hora argentina explícita. Ver `outputs/thesis-desk-all-local/runs/20261107T210015-4d9db4d3/view.html`. |

## Correcciones terminadas

Contrarian debe elegir contrapruebas literales vinculadas a documentos posteriores del
mismo emisor. Research rechaza ratings crediticios como guidance, avisos de participaciones,
recompras —incluidas las aceleradas— y financiación como acuerdos operativos. La cita debe
expresar el estado del acuerdo. Un evento legal necesita una acción o resolución explícita;
los riesgos hipotéticos y las coincidencias dentro de otras palabras no bastan. Estos controles
son condiciones necesarias; no prueban por sí solos la materialidad de una tesis.

La antigua señal de rating de GGB y la recompra de AMAT conservan sus registros originales,
pero quedan excluidas del denominador. La tesis actual de GGB corresponde a otra fuente y
a otro hecho: la adquisición de DFESA. Los reportes archivados anteriores conservan sus
resultados originales y sus defectos conocidos, registrados en `validation.json`.

La migración final revalidó y conservó 70 documentos con sus fechas originales; dos quedaron
rechazados y pendientes de Research. No se borró la cartera ni se copiaron decisiones PM.
El reporte de migración identifica los contratos de origen y verifica hashes de las fuentes.
Los conteos de migraciones sucesivas no deben sumarse como documentos distintos.

Los límites operativos quedan en `runtime.json`, separados de la configuración inmutable
de cartera: un documento por lote local, timeout de 1800 segundos y reserva configurable
para PM. La captura final destinó su única llamada a PM (107,1 segundos); el aviso
`FREE_BUDGET_EXHAUSTED` de Research refleja esa reserva y conserva los pendientes.

## Validación

```powershell
python -m pytest tests/test_thesis_desk_universe.py tests/test_thesis_desk_workflow.py tests/test_thesis_desk_backtest.py tests/test_agent_openrouter.py -q
```

**160 passed**. Tras explicitar la zona horaria en la vista, el test relacionado del
dashboard pasó y el navegador verificó `18:04:34 ART`, dos filas en el filtro Watchlist,
cohorte válida 2 y excluidas 2 en ambos horizontes, sin errores de consola. Whitespace y
sintaxis PowerShell del iniciador CPU sin hallazgos.

Prueba real con Qwen CPU: los tres documentos de AMAT/BMNR/LVS terminaron sin tesis
admitidas y el control positivo de VZ terminó `SUPPORTED`; cuatro llamadas de
59,76 / 54,68 / 26,47 / 193,43 segundos. Una prueba previa validó los casos de rating GGB
y notificación VOD. La última corrección de palabras completas revalidó offline esas salidas
y sus fuentes, además de los tests. PM se ejecutó nuevamente con el contrato final.
La API confirmó `size_vram: 0`; estas pruebas verifican integración, no rentabilidad.

En el checkpoint del 7/11, Windows registraba su último inicio el 6 de noviembre a las 19:13:54 ART. El apagado
espontáneo anterior sigue con causa indeterminada; no se lo atribuyó a la GPU. No hubo
otro reinicio observado. Se mantuvo el mismo modelo en CPU sin modificar drivers ni
variables persistentes; esto no garantiza estabilidad futura.

## Pendientes y reanudación

1. Completar los 442 documentos pendientes; puede llevar horas en CPU. Los emisores con
   revisión pendiente siguen bloqueados para nuevas entradas.
2. Resolver o conservar explícitas las brechas de cobertura: 15 acciones identificadas
   sin fuente vinculada, JPM bloqueada, feeds parciales e identidades incompletas.
3. Capturar cotizaciones y resultados futuros maduros para medir las cohortes. No existe
   todavía evidencia económica evaluable. EXTENDED no acredita el mandato SMID histórico.

Para continuar, iniciar `./scripts/start_thesis_desk_model.ps1` en una terminal y ejecutar
en otra `python -m scripts.run_thesis_desk --local --once --max-model-calls 4`.
La cola y la cartera se conservan. La última captura se consulta sin inferencia con
`python -m scripts.run_thesis_desk --local --report`.

El backtest del **01/09–31/10/2026** sigue `NOT_EVALUABLE`: 43 ruedas, 129 cortes y
42.999 cotizaciones utilizables en la captura archivada, sin evidencia oficial histórica
suficiente de tesis, universo, riesgo y eventos. Ambas variantes tienen `metrics: null`.
La captura actual no reconstruye decisiones pasadas y cero fills no significa retorno 0%.

Evidencia: `outputs/thesis-desk-all-local/validation.json`, `validation-operating-cpu.json`,
`validation-final-admission.json`, `cache-migration-final-20261107.json` dentro del mismo
directorio; corrida `20261107T210015-4d9db4d3` y
`outputs/thesis-desk-sep-oct-2026-validated-20261106/`.
Guía completa: [thesis-desk.md](thesis-desk.md).
