# Telegram como evidencia secundaria de decisiones — v1 (21/10/2026)

## Objetivo

Registrar qué decisiones, propuestas y estados de cartera **publicó** Quantia entre 12/03 y 21/10/2026, sin confundir mensajes con el origen canónico de decisiones ni con operaciones reales. Esta fuente es **append-only/offline**, para la auditoría AS-IS. **No modificar** `decision_log`, `execution_plans`, `order_intents`, `broker_fills`, `broker_movements`, outcomes, scores, planner ni performance.

La exportación original son 11 HTML personales. Los datos reconstruidos, la base SQLite y los archivos de conciliación deben permanecer **fuera de este repositorio público**, en almacenamiento privado con hash SHA256, acceso controlado y conservación de la fuente.

## Fuente, cobertura y límites

- Exportación de Telegram: 10.961 mensajes, 11 HTML, período 12/03 a 21/10/2026.
- Propuestas BUY/SELL extraídas: 776 (687 publicaciones de propuesta y **89 simulaciones contextuales**, no operables por sí mismas).
- Estados de posición extraídos de análisis de cartera: 1.973; incluyen **572 HOLD** y **692 WATCH**. Son estados comunicados; pueden repetirse entre reportes y **no representan 1.973 decisiones independientes**.
- Avisos Cocos comunicados por el bot: 236 líneas, 183 filas candidatas tras deduplicación tentativa. Los avisos no son liquidaciones comprobadas.
- Cruces temporales de propuestas con avisos de igual ticker/lado: 159 pares candidatos. **Cero cruces certificados por esta fuente**; proximidad, ticker y lado no demuestran causalidad.
- Cobertura incompleta: informes partidos entre mensajes, adjuntos multimedia ausentes, formato cambiante, fecha de aviso distinta de fecha de operación, versiones múltiples y capturas reescritas en otras fuentes.

## Jerarquía de evidencia

1. **Decisión formal registrada:** `execution_plans` + `order_intents`, owner explícito, contexto de plan y captura inmutable cuando existe. Aplican las categorías HIGH/MEDIUM/LOW/UNRECOVERABLE de `scripts/historical_reconstruction`. `decision_log` ayuda a diagnosticar linaje; nunca reemplaza la identidad del plan.
2. **Evidencia secundaria del mensaje:** HTML original de Telegram con ID de mensaje, timestamp ART, archivo de origen, hash de archivo, acción publicada y texto original. Registrar si fue plan, alerta, lectura, WATCH/HOLD o simulación.
3. **Ejecución almacenada:** `broker_fills`/`broker_movements`, revisando `external_fill_id`/`external_movement_id`, owner y relojes. Estar en base **no implica** confirmación directa del broker.
4. **Certificación económica:** liquidaciones, extractos de cuenta/caja, comisiones, costos históricos y corporate actions de Cocos, cotejados por propietario. Ausencia de esta verificación deja el resultado en PARTIAL/RECOVERABLE/NOT_IDENTIFIABLE; nunca VERIFIED por analogía de ticker o fecha.

## Importación/cotejo seguro

Los archivos privados pueden prepararse fuera del repo con un script local, sin importar al esquema operativo:

- `decisiones_telegram_trazables.csv`: propuestas, source message ID y clasificación de simulación.
- `estados_cartera_comunicados.csv`: posiciones HOLD/WATCH/BUY/SELL tal como se publicaron.
- `cruces_propuesta_aviso_candidatos.csv`: coincidencias exploratorias, `causal_link_verified=NO`.
- `resumen_cruce.json`: cobertura y SHA256 del SQLite, estado real de las fuentes.

Para continuar, exportar **por separado** y **en transacción READ ONLY / REPEATABLE READ** las tablas `execution_plans`, `order_intents` y `broker_fills` desde cada fuente disponible. Para `broker_movements` conservar IDs originales, estados y payloads, sin derivar BUY/SELL de texto ambiguo. Los exports se guardan privadamente, no se commitean ni se envían a CI. Cotejar el archivo de Supabase **como corte independiente** del PostgreSQL local, sin asumir equivalencia ni restaurar uno sobre otro.

**Reglas de match:** no aceptar solamente tickers y fechas; exigir compatibilidad de owner, fuente, instrumento, sentido, cantidad, monto bruto/neto, identificador externo y tiempo con la precisión realmente disponible. Todos los matches temporales son CANDIDATE hasta atestación por IDs/comprobantes. Duplicados o discrepancias conservan las dos versiones, nunca corrección silenciosa.

## Gates y pruebas

- Revisar que los conteos y SHA256 originales sean reproducibles; no publicar los HTML.
- No mutar la DB ni disparar órdenes, análisis operables o notificaciones.
- No sumar simulaciones a planes formales ni usar estados WATCH/HOLD como fills.
- No calcular PnL/retornos a partir de valores de Telegram ni deducir performance de coincidencias de propuestas.
- Generar reporte explícito por fuente: ACCESSED/UNAVAILABLE y calidad de conciliación; fallar cerrado ante falta de owner, ID o fechas.
- Antes de introducir métricas operativas, conciliar extractos Cocos, caja y comprobantes, con revisión manual de ambiguos.

## Estado de este PR

Sólo documenta el contrato de incorporación; **no se integraron datos privados a GitHub**, **no se conectó a Cocos ni a Supabase**, **no se modificó el runtime del bot**. El cotejo exportable se entrega como artefacto privado independiente. Esta documentación es preparatoria y no certifica PnL ni mejora económica.
