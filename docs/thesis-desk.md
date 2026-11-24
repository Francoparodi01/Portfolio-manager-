# Desk de tesis: universo completo, modelos abiertos y medición paper

Implementa Research → Contrarian → PM → técnica/riesgo → simulación persistente,
inspirado en los [dos](https://x.com/MatiasScalbi/status/2097105195415097524)
[posts](https://x.com/MatiasScalbi/status/2118142082275577874), con ARS 3.000.000 ficticios.
Lee mercado público de Quantia y escribe archivos locales; no tiene órdenes, cuentas,
Telegram ni conexión con el scheduler de producción.

Estado de implementación, validaciones y pendientes: [thesis-desk-status.md](thesis-desk-status.md).

## Uso

Desde la raíz principal `cocos_copilot`, con `cocos_db` y Ollama iniciados:

```powershell
python -m scripts.run_thesis_desk --local --once
python -m scripts.run_thesis_desk --local --once --max-model-calls 4
python -m scripts.run_thesis_desk --local --watch --poll-seconds 300
python -m scripts.run_thesis_desk --local --watch --max-model-calls 8 --poll-seconds 60
python -m scripts.run_thesis_desk --local --report
python -m scripts.run_thesis_desk --replay outputs/thesis-desk-all-local/runs/RUN_ID
python -m pytest tests/test_thesis_desk_universe.py tests/test_thesis_desk_workflow.py tests/test_thesis_desk_backtest.py tests/test_agent_openrouter.py -q
```

`outputs/thesis-desk-all-local/latest.json` señala la última corrida confirmada.
Abrir su `report.html` muestra cobertura, decisiones, bloqueos, cartera y resultados a 5/20 ruedas.
Tiene búsqueda, filtros y exportación CSV/JSON. Funciona sin servidor, CDN ni conexión.
`--report` genera `view.html` sin alterar el reporte original incluido en el manifiesto.
El panel es una captura; volver a abrirlo después de cada corrida. `--watch` es un proceso
de primer plano, termina con Ctrl+C y no instala tareas ni servicios.

La primera revisión de cientos de documentos puede tardar bastante en el equipo local.
Cada documento se guarda al terminar Research y Contrarian; una interrupción conserva ese
trabajo. La siguiente corrida retoma pendientes y documentos nuevos. La cartera se confirma
en una transacción separada al finalizar; una falla no confirma fills. Los mensajes
`OFFICIAL_SOURCE_SWEEP` y `MODEL_REVIEW` muestran el progreso real.
`--max-model-calls 8` confirma reportes por tandas cortas y continúa toda la cola en las
siguientes corridas. Cambia el presupuesto de esa ejecución, sin cambiar la cartera ni
el universo. Los emisores pendientes siguen bloqueados; no se declara revisión completa.
Cada pasada reserva por defecto una llamada para PM cuando permite al menos dos llamadas:
Research/Contrarian dejan ese espacio para decidir sobre los emisores ya revisados. Con
una sola llamada no hay reserva automática. `--pm-reserve-calls N` permite ajustar esta
reserva; igualar el presupuesto total sirve para completar exclusivamente PM pendiente.
Los límites efectivos quedan en `runtime.json`, incluido en el manifiesto, separados de
la configuración inmutable de cartera. Se conserva la caché validada al variar estos límites.
Si un lote devuelve un contrato inválido, sus documentos pendientes se intentan individualmente
una vez; los fallos restantes quedan visibles y bloquean nuevas entradas del emisor.
Contrarian recibe los documentos capturados del mismo emisor, incluidos los posteriores a la
tesis. Una publicación nueva invalida la caché de revisión, conservando la extracción previa.

## Modelos, claves y costo

`--local` usa **Qwen 3.5 9B**, instalado, por `http://127.0.0.1:11434/api/chat`, sin key.
En otro equipo hay que instalar Ollama y descargar el mismo modelo con `ollama pull qwen3.5:9b`,
además de las dependencias de `requirements.txt`. No hace falta crear ni guardar una clave local.
Si el equipo se apaga durante la carga, detener la corrida y revisar la estabilidad; un
apagado inesperado no identifica por sí solo la causa. Para ejecutar el mismo modelo sin
GPU, cerrar Ollama desde la bandeja y ejecutar `./scripts/start_thesis_desk_model.ps1` en
una terminal separada. El script usa las [opciones oficiales para desactivar GPU](https://docs.ollama.com/gpu),
no modifica variables persistentes ni instala servicios. `ollama ps` debe mostrar CPU;
la API `/api/ps` informa `size_vram: 0`. Después se usan los mismos comandos `--local`.
Este modo puede ser más lento; no es un diagnóstico ni una garantía de estabilidad del equipo.
La ejecución local usa por defecto un documento por lote y un timeout de respuesta de
1800 segundos, porque el lote de ocho documentos excedió los 300 segundos en la prueba
CPU del 6 de noviembre. `--documents-per-batch N` permite reducir el lote configurado y
`--model-timeout-seconds N` admite entre 60 y 3600 segundos. Contrarian sigue recibiendo
los extractos de todos los documentos capturados del emisor, sin un recorte adicional. El límite
es por llamada, no por corrida; una pasada con varias llamadas puede durar bastante más.
En remoto se mantienen ocho documentos y 300 segundos por defecto. La revisión masiva
CPU y su cobertura se verifican en el estado enlazado al inicio de esta guía.
Los tres roles son llamadas separadas al mismo modelo; no equivalen a verificación independiente.
La variante local usa [salidas estructuradas de Ollama](https://docs.ollama.com/capabilities/structured-outputs)
con los IDs de documentos/citas permitidos por rol; después aplica las mismas validaciones
de contenido, emisor y fechas. Un JSON válido no prueba que la interpretación sea correcta.
Cada respuesta local es un objeto con una clave obligatoria por documento o hallazgo;
Research usa `null` cuando no encuentra una tesis. Se rechazan claves repetidas, faltantes
o ajenas antes de convertir al contrato común. Así un documento no puede producir dos
tesis ni desaparecer silenciosamente de la revisión de un lote.
Para un veto, la salida estructurada vincula cada documento posterior del mismo emisor
con sus citas literales permitidas. Contrarian elige una cita existente; no puede componer
una frase nueva. Se sigue validando la cita contra el texto y la cronología de la fuente.
Una calificación crediticia o su perspectiva no cuentan como guía operativa. Avisos de
derechos de voto y recompras propias no cuentan como acuerdos de crecimiento; la cita de
un acuerdo debe expresar explícitamente su firma, cierre o cancelación, según la clasificación.
También se excluyen las recompras aceleradas y las cancelaciones de financiación. Un hecho
legal adverso exige una acción o resolución explícita; una lista de riesgos hipotéticos no
basta. Son condiciones necesarias de admisión, no una prueba automática de materialidad.
Sin `--local`, se usa **nvidia/nemotron-3.5-super-vl:free** de OpenRouter y la key existente
`OPENROUTER_API_KEY` o `openrouter` del `.env` ignorado por Git. No se imprimen claves ni se
modifica la configuración del agente de producción. Esta variante usa `outputs/thesis-desk-all/`.

`FREE_ONLY` rechaza modelos pagos. No hay fallback automático de proveedor. Los límites
son 300 intentos/corrida y 1000/día en local, 40/50 en OpenRouter; la API puede imponer más
restricciones. Los errores de cuota conservan la cola, nunca habilitan gasto.

Para reutilizar capturas públicas entre experimentos nuevos:
`python -m scripts.run_thesis_desk --local --once --seed-cache outputs/thesis-desk-all`.
Se verifican hashes y se conserva la fecha original; no se copian decisiones ni saldos.

## Universo y fuentes

`ALL_RECORDED_EQUITIES` descubre todos los registros `(ticker, asset_type, currency)`
CEDEAR/ACCION en ARS de `market_prices`, sin excluir por antigüedad. No representa todas
las acciones del mundo ni prueba que la base incluya todo BYMA. Los tres instrumentos del
JSON de configuración son fuentes IR de respaldo, no un límite del universo.

- Identidad: programas de [Comafi](https://www.comafi.com.ar/custodiaglobal/programas.aspx),
  [Caja de Valores](https://cajadevalores.com.ar/Servicios/Cedears) y paneles públicos BCBA.
  Se usa el ticker de origen explícito. ETF, tipos inciertos, símbolos antiguos sin mapeo
  y conflictos quedan visibles y bloqueados; no se completan por semejanza de nombres.
  Dos programas CEDEAR que apuntan al mismo subyacente/mercado requieren verificación y
  quedan en conflicto: no se elige automáticamente cuál de los dos catálogos es correcto.
- Fuentes: SEC 8-K/6-K de los últimos 14 días, hasta cinco presentaciones por emisor y
  dos anexos por presentación; feeds IR y CNV. Se guardan texto, URL, fechas, hashes,
  límites y errores. La caché no rejuvenece documentos; el colector limita solicitudes.
  Los documentos se identifican por instrumento, URL, publicación y texto. Las
  presentaciones CNV repetidas se deduplican antes de aplicar el límite por emisor.
- CNV puede no cubrir los 14 días ni contener los documentos completos. Sigue `PARTIAL`
  y bloquea entradas locales hasta tener evidencia suficiente. Emisores extranjeros sin
  fuente oficial mapeada también quedan pendientes.
- CORE exige membresía IVV fechada. EXTENDED incluye otras acciones verificadas, con
  tamaño no CORE de 4% y stop de 15%. **Es una ampliación para todo el universo; no es
  el mandato SMID de USD 3–70 mil millones del post.** No se inventa capitalización.

Se cuentan instrumentos y símbolos únicos por separado: el histórico tiene símbolos
bajo distintos tipos. Esos conflictos no se mezclan en precios ni cartera. El denominador
de cobertura son las acciones identificadas, con los excluidos informados aparte.

## Admisión y ejecución

Research propone guía elevada/reafirmada, acuerdos operativos firmados/cerrados o hechos
adversos oficiales. Selecciona una cita exacta de 3–25 palabras preparada desde el documento;
no puede reescribirla, inventar fuentes o fijar fechas. La guía necesita un verbo explícito
de cambio/reafirmación en la cita. Tablas de proyecciones y financiación rutinaria no bastan;
los rechazos se conservan. Se usan fragmentos: la extracción puede omitir hechos relevantes.
Contrarian revisa hallazgos; un veto necesita contraprueba citada. PM decide WATCHLIST/PASS.
El modelo no tiene herramientas, autoridad de ejecución ni permiso para fijar cantidades.

La cola depende de documento, fuente y versión del contrato/modelo. Una revisión pendiente,
fuente incompleta o calendario faltante bloquea compras. Los hechos adversos admitidos se
conservan para salidas. Respuestas truncadas o inválidas son errores, nunca decisiones válidas.

La actualización de clasificación del 7 de noviembre invalida la caché anterior. Para
los contratos anteriores auditados, la migración local conserva únicamente documentos y
revisiones que siguen pasando los validadores, con sus fechas originales de disponibilidad:

```powershell
python -m scripts.migrate_thesis_desk_cache --state-dir outputs/thesis-desk-all-local --from-run outputs/thesis-desk-all-local/runs/20261107T145816-1c1c1554 --output outputs/thesis-desk-all-local/cache-migration-final-20261107.json
```

Verifica hashes de código y artefactos; rechaza versiones desconocidas y corridas cuyo
código cambió durante la ejecución. No migra decisiones de PM, no consulta modelos y no
reinicia la cartera. Las clasificaciones rechazadas vuelven a Research. También recupera
las revisiones de la regresión real del contrato intermedio auditado, revalidándolas contra
las mismas fuentes archivadas y registrando el contrato de origen de cada grupo. Conserva
la evidencia original de señales existentes, incluso de las inválidas, para excluirlas
de la medición sin borrar el historial. No ejecutar durante una corrida del desk.

Zona técnica: ±2% del promedio de 20 últimas cotizaciones diarias de 16:30–17:00 ART,
anteriores al hecho. **No son cierres oficiales BYMA**. La reconstrucción se conoce desde
la captura actual y sólo usa factores corporativos documentados. RISK_V2 conserva starter
70%, cupo semanal, concentración máxima 12% y freno al caer 8% desde el máximo observado.
VIX usa una sesión previa completa: amarillo desde 25, rojo desde 35. Son parámetros fijos
de investigación, junto con comisión ficticia 0,6% y slippage adverso 0,15% por lado.

BLS bloquea desde 24 horas antes hasta una hora después del empleo. Los balances usan
observaciones recientes de Quantia y el [calendario público Yahoo mediante yfinance](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Calendars.html).
Sólo una fecha futura explícita establece cobertura; ausencia de evento no prueba ausencia
de riesgo. Las fechas del proveedor pueden cambiar y no son confirmación oficial empresarial.
Se bloquean el día previo y el día del evento. La captura conserva su límite de paginación
y expira a las seis horas.

Fills: rueda BYMA, 10:30–17:00 ART, nueva cotización de hasta cinco minutos, timestamp de
mercado posterior a la latencia de 60 segundos. Compras vencen a 15 minutos o al cierre.
Los stops reaccionan a cotizaciones observadas; no son protección continua. No se inventan
fills durante pausas. La configuración de cartera es inmutable: cambiarla requiere otro
`--state-dir`. Reglas y supuestos completos: [backtest](thesis-desk-backtest.md).
Un `FACT_VETO` validado conserva la cita oficial posterior y la tesis específica afectada
en `evidence.json:vetoes`. Bloquea esa entrada y permite solicitar la salida de una posición
paper existente, aunque falte evidencia para nuevas compras; el fill espera la siguiente
cotización válida y la latencia. No se convierte el veto en una compra o venta real.
La evidencia adversa de posiciones abiertas queda guardada, incluso si se captura fuera
de rueda y la fuente falta en la siguiente corrida.

## Métricas y evidencia

`metrics.json` conserva cobertura, corridas, NAV/caja ficticios, comisiones, PnL realizado,
fills, posiciones, ciclos cerrados, aciertos por ciclo, retorno total, drawdown actual,
curva diaria observada y alertas de calidad. Sin fills/ciclos suficientes, retorno o aciertos
son nulos; no se presentan como rentabilidad del 0%. El control es efectivo nominal sin
interés; no se afirma superioridad frente a HOLD.

Cohorte prospectiva: una primera WATCHLIST por tesis, anclada a la primera cotización observada
posterior. A 5/20 ruedas se busca el primer precio desde la misma hora local y dentro de
60 minutos. Se distinguen sin ancla, inmaduras, maduras, evaluadas y faltantes. Retorno neto
hipotético: `salida*0.9985*0.994/(entrada*1.0015*1.006)-1`. No es PnL de fills, efecto causal
ni una muestra de operaciones independientes.

Cada señal guarda la evidencia de su primera admisión. `observed_cohort` cuenta todas las
señales registradas; `cohort` incluye sólo las que conservan procedencia y cumplen las reglas
de clasificación actuales. `excluded` y `exclusion_reasons` explican las demás. La ausencia
de procedencia se informa como `SOURCE_PROVENANCE_UNKNOWN`; nunca se convierte en un
resultado nulo o en una pérdida. Un cambio de criterio conserva el registro original y su
exclusión explícita. El reporte muestra por separado la cohorte válida y las exclusiones.

Cada corrida archiva configuración/política, originales `raw/`, `sources.json`, universo,
SQL de lectura, `market.json`, `research.json`, `evidence.json`, `paper-input.json`,
`state-before.json`, `result.json`, `metrics.json`, reportes HTML/Markdown y `manifest.json`.
El manifiesto registra hashes de entradas/código y cambios de código durante la corrida.
SQLite separa cartera, caché durable y presupuesto. El replay verifica integridad y repite
la transición determinista; no verifica calidad del modelo ni rentabilidad.

No se modelan dividendos en efectivo, impuestos, profundidad, interés ni inflación.
Todo resultado es `PAPER_ONLY` / `NOT_VERIFIED`, con denominadores y faltantes explícitos.

## Límite histórico

El período **01/09–31/10/2026** sigue `NOT_EVALUABLE` por falta de tesis, universo y velas
con disponibilidad histórica acreditada. El motor y comandos están en
[thesis-desk-backtest.md](thesis-desk-backtest.md). Capturar publicaciones hoy o usar un modelo
actual no las convierte en decisiones conocidas entonces. La ampliación actual recolecta
y mide evidencia prospectiva sin inventar un resultado histórico.
