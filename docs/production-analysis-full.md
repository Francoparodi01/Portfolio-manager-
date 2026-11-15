# Análisis completo operativo

Desde el cambio del 27/10/2026, `/analisis_full` es el flujo principal del bot.
`/analisis`, `/analysis`, `/analisis_semanal` y el botón de análisis conservan
compatibilidad y llaman al mismo flujo. Cada pedido refresca cartera y universo,
incluye radar, sentimiento y optimizador, y genera el ExecutionPlan nativo.

Durante una sesión válida, las decisiones se guardan con el propietario explícito
y `layers.analysis_profile = analisis_full_operational_v1`. Las mediciones
posteriores consumen esas decisiones mediante los contratos existentes de
ExecutionPlan, decision_log y outcomes. No se recalcula ni se reclasifica el
historial anterior como si proviniera del análisis completo.

El scheduler captura el análisis completo a las 11:00, 13:00 y 16:00 ART de días
de rueda. Estas capturas actualizan la evidencia y el reporte cacheado sin mandar
reportes adicionales. Se omiten si falla el refresco o el mercado está cerrado.
El informe de las 17:12 también incluye el radar, pero mantiene su carácter
exploratorio posterior al cierre y no genera evidencia operativa.

`/analisis_test` y `/analisis_debug` siguen sin persistir planes. Se conservan las
restricciones de mercado, frescura, financiación y riesgo del motor. Este cambio
no agrega ejecución de órdenes ni incorpora el puente de cartera ficticia usado
en el backtest.

El contrato de argumentos compartido vive en `src/core/analysis_profile.py`.
La clave de caché incorpora el perfil para invalidar informes del flujo anterior.
Las pruebas funcionales están en `tests/test_production_analysis_full.py` y se
pueden ejecutar con unittest dentro de las imágenes de cada servicio.

El despliegue conserva por separado las imágenes y configuración previas de cada
servicio. Las variantes del motor que ya tenían el bot y el scheduler no se
actualizan mutuamente: el cambio unifica el modo completo y la persistencia,
no es una migración del resto de los módulos instalados.
