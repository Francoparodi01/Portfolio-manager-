# Telegram, ledger y recomendaciones históricas de Quantia

Revisión: 2026-10-22. Alcance: interpretación del histórico importado y sus mediciones.

## Historial Telegram: qué conserva el ledger
La reconstrucción de chats conserva mensajes, fecha, ticker, acción, contexto y
trazabilidad del archivo original. Incluye recomendaciones y bloqueos cuando hay
evidencia. BUY/SELL/REDUCE/INCREASE/HOLD/WAIT/WATCH son observaciones del bot; una
recomendación escrita no acredita una operación ejecutada.

El runtime con la extensión histórica utiliza `telegram_history_imports`,
`telegram_history_observations` y `telegram_history_measurements`. Mantener IDs y
hashes de fuentes y mediciones. No copiar observaciones a `decision_log` ni
presentarlas como decisiones operativas nuevas. La documentación no reimporta datos.

## Medir el movimiento posterior a una recomendación
Sí se puede cruzar una recomendación fechada con precios posteriores, si existen
datos suficientes y un protocolo explícito. Es evidencia histórica direccional,
distinta de fills y ganancias reales. Si el bot decide después de rueda, conservar
la regla de entrada de la medición, como el cierre de la próxima sesión; no usar
retrospectivamente el precio previo al mensaje.

El artefacto debe identificar corte, mercado, moneda, calendario, horizonte,
precio inicial/final, motivo de exclusión y mensaje o decisión original. No
cambiar silenciosamente una ventana de días calendario por sesiones bursátiles.

## Repeticiones, tamaño de muestra y rendimiento
Una misma idea puede aparecer en varios mensajes. Separar todas las observaciones,
el último estado diario, episodios y ventanas sin solapamiento por ticker. Esas
unidades tienen distintos denominadores. HOLD/WAIT/WATCH no son automáticamente
aciertos o errores direccionales. Una caída después de SELL es un movimiento a
favor de la señal, no evidencia de una venta real ni de un short ejecutado.

Informar aciertos/casos maduros, media de magnitud y pendientes. No sumar retornos
de señales para reconstruir una cartera ni llamar DVA a una tasa direccional.
La pregunta «¿mejor que mantener?» requiere la comparación canónica PLAN vs HOLD
y sus supuestos; el porcentaje de subidas o bajadas por sí solo no la responde.

## Acceso del agente y ausencia de datos
`get_decision_ledger` es la entrada registrada para leer el ledger. Verificar qué
bloques devuelve ese run: las extensiones disponibles dependen del despliegue y
la salida puede estar acotada. El SQL genérico no expone `telegram_history_*`.
Si un bloque histórico no aparece, no concluir que se perdió: distinguir ausencia,
recorte de salida, falta de acceso y ausencia real de medición.

`get_bot_directional_outcomes` consulta su propia metodología de episodios; no
asumir que reproduce automáticamente toda la reconstrucción de chats. Para
resultados fechados usar el artefacto original y su hash cuando estén disponibles.

## Implementación y referencias
Extensión desplegada: `src/analysis/telegram_history.py` y
`src/analysis/telegram_history_measurements.py`. Ledger: `src/analysis/decision_ledger.py`.
Acceso: `src/agentic/harness/tools_ext.py`. Los módulos de la extensión pueden no
existir en un checkout anterior: comprobar la versión antes de prometer su acceso.
