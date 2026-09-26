# Validación de Analytics v2 — 2026-09-22

Checkout: `C:\Users\Franco\OneDrive\Escritorio\backend\cocos_copilot`.
HEAD base: `67d30cf739ff6deb6936d74e2541ae6391d2b22d`.
Los cambios permanecen locales; había modificaciones anteriores en scheduler,
Telegram, Shadow, scraper y tests. No se incluyeron en esta implementación.

## Pruebas y benchmark

| Verificación | Resultado |
|---|---|
| Analytics v2 unitarias e integración | 35 passed |
| Cobertura package, líneas y ramas | 96,97%; mínimo exigido 90% |
| Viability + decision market audit + planner | 22 passed |
| Suite completa del checkout, antes del último test offline adicional | 618 passed, 2 skipped, 9 failed |
| Reproducción de los módulos con fallos, sin cargar test_analytics_v2 | mismos 9 fallos; 24 passed |
| Descriptivas sobre 250.000 outcomes sintéticos | 4,585 s; 250.000 contribuciones de salida |
| Build del ejemplo con 5.000 resamples | completado |
| validate: hashes, inventario y replay | VALID |
| Revisión visual | siete gráficos generados; costos, gates y waterfall inspeccionados y corregidos |

El benchmark excluye inferencia, lectura/escritura y gráficos. No se midió pico
de memoria y no se convierte el tiempo de esta máquina en hard fail de CI.

Runtime de pruebas: Windows, Python 3.13.7, NumPy 2.1.2, pandas 2.3.3,
Pydantic 2.13.5, pytest 9.0.3. La suite completa usó un venv temporal con
system-site-packages e instalación aislada de scikit-learn; no se cambiaron los
containers. No equivale a una validación de todas las combinaciones permitidas
por requirements.txt ni a certificar la cobertura global de Quantia.

## Fallos ajenos al nuevo package

Se reprodujeron sin ejecutar los tests nuevos:

1. `test_multiuser_runner_scope::test_run_opportunity_loads_owner_scoped_portfolio`: fixture sin `cfg.scraper`.
2. `test_radar_exploratory::test_followed_watchlist_combines_sources_and_keeps_latest_per_ticker`: mock no admite la consulta adicional de precios.
3. `test_run_analysis_report_layout::test_report_leads_with_clear_no_buy_recommendation_when_unfunded`: contrato de texto esperado distinto.
4. `test_run_analysis_report_layout::test_report_expands_main_action_with_recommendation_context`: contrato de texto esperado distinto.
5. `test_shadow_telegram_menu::test_main_menu_exposes_shadow_report_button_and_command`: texto/comando esperado distinto.
6. `test_telegram_output_quality::test_compact_radar_parses_header_with_cash_lines`: formato esperado distinto.
7. `test_telegram_output_quality::test_help_text_is_mobile_compact_and_scope_safe`: longitud esperada <1200, actual 2331 bytes.
8. `test_telegram_output_quality::test_main_menu_prioritizes_common_actions_and_groups_secondary_views`: menú esperado distinto.
9. `test_telegram_radar_compact::test_compact_radar_preserves_shadow_context_line`: falta el texto esperado en compact.

No se modificaron esos módulos para maquillar el resultado de esta entrega.

## Ejemplo validado

Run con 5.000 resamples: `76b61c17d09fbcd988baad3c`.

[Reporte sintético](../../outputs/analytics/76b61c17d09fbcd988baad3c/report.md)
· [Manifest](../../outputs/analytics/76b61c17d09fbcd988baad3c/manifest.json)

La compra sintética a 100,50, venta a 120 y fees de 1 reconcilian PnL neto 18,50.
Shortfall -1,50 no se descuenta otra vez. Las recomendaciones repetidas quedan
en enlaces sin multiplicar episodios; la madurez 40D y ambigüedad siguen visibles.
Ni este PnL ni los EV del ejemplo describen el resultado real de Quantia.

## Integración Telegram — 2026-09-23

Activados `/analytics`, `/analytics_v2` y Auditoría → Analytics v2. La API de
Telegram confirmó `/analytics` dentro de los 16 comandos registrados, con menú
de tipo `commands`. El contenedor inició sin reinicios; scheduler conservó su
imagen y su hora de inicio.

Validación del puente:

- 79 tests pasaron: motor, puente Telegram, viability, decision-market y menú ticker.
  Cobertura conjunta del motor y el puente: 96,78% de líneas y ramas combinadas.
- Después de incorporar la identidad del entorno numérico, los 25 tests del
  puente pasaron, incluido el nuevo test de cambio de run ID al cambiar runtime.
- Se ejecutó el handler real contra PostgreSQL con un receptor local de mensajes:
  HTML válido, ZIP generado, hashes verificados y limpieza temporal comprobada.
  La prueba no envió mensajes de prueba a Telegram.
- La captura real pudo recalcularse exactamente dentro del entorno registrado.
  Diferentes plataformas mostraron variaciones de ~1e-16 en skew; por eso el
  manifest y run ID incluyen versiones de librerías, Python y plataforma.
- Los SHA256 del bot, script y puente desplegados coinciden con el checkout.

Imagen activada:
`sha256:b73e1bccc79225759a37d222a0c71a15edba595835c36ad8a55813dcee7ee59c`.
Se construyó sobre la imagen previamente desplegada, copiando únicamente el
package Analytics, el puente, su script y el bot, más Pydantic. Esto preservó
los cambios ajenos a esta entrega. La imagen anterior queda como
`cocos-telegram-before-analytics-20260922:local` para rollback del servicio.

La consulta live es observacional y de sólo lectura. No certifica madurez/PIT
históricos, n efectivo, contabilidad económica ni matching exacto donde falta
evidencia. Véase [contrato del comando](telegram.md).

## Frontera de aceptación

Aceptado como motor offline determinístico con inputs canónicos explícitos y
comando Telegram observacional desplegado. Los adaptadores de evidencia real
sellada y la contabilidad completa del broker continúan pendientes; están detallados
en README.md. No se declara terminada la totalidad del roadmap ni validado edge.
