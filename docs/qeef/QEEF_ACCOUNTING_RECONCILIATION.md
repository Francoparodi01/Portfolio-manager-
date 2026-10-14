# QEEF Phase 2 — conciliación económica

El servicio `accounting.reconcile` utiliza Decimal sobre snapshots y movimientos observados. La validación final reutiliza `Analytics v2.economic_pnl` mediante `realized_value`. Costos hipotéticos sólo pertenecen al contrafactual; un `AccountEvent` SIMULATED es rechazado por la contabilidad observada.

## Identidades

```text
NAV = cash convertido + suma(cantidad × precio × FX válido)
PnL económico neto = NAV final − NAV inicial − aportes netos
PnL por componentes = realizado bruto + cambio no realizado
                      − fees − impuestos − otros costos − financiamiento
```

Se exige coincidencia de ambas identidades, límites NAV observados y cobertura declarada de fills, flujos, acciones corporativas, costos, bases y financiamiento. Tolerancia monetaria: 0,01 de la moneda de reporte; cantidades deben coincidir exactamente. Los gaps se reportan sin corregir los registros originales.

| Input | Datos | Regla |
|---|---|---|
| AccountSnapshot | Owner, snapshot_id, clocks, cash por moneda, holdings, NAV observado | Identidad exacta, valuaciones no futuras, instrumentos únicos |
| Holding | Instrumento, ticker, moneda, cantidad, precio, costo medio, clocks, fuente | No infiere security master del ticker |
| AccountEvent | ID fuente, BUY/SELL/flujo/dividendo/split/ratio/tasa/comisión | Duplicados idénticos una vez; conflicto de ID rechazado |
| FXQuote | Par, tasa, effective_at, available_at, fuente | Ambos clocks anteriores a la valuación; no usa CCL futuro |
| Coverage | Owner, período, banderas de completitud y referencias | Ausencia de movimiento no demuestra ausencia de flujo |

Compras actualizan costo medio ponderado; ventas parciales realizan sólo la cantidad vendida. Splits/cambios de ratio ajustan cantidad y costo unitario, sin cambiar el costo total ni fabricar PnL. Dividendos observados ingresan al cash y componente realizado. Una venta mayor a la cantidad visible bloquea certificación. Se comparan cash y cantidades con el snapshot final observado.

Fees, impuestos y otros costos ausentes no valen cero. `fee_status=UNKNOWN` cuando corresponda y `economic_pnl_net=null`. Los cambios posteriores de comisiones son nuevas versiones con su reloj real de observación. La extensión aditiva `ObservedAccountCost` en Analytics v2 representa cargos de cuenta no asociados a trades: se suma al componente correspondiente, se filtra as-of y se incluye en el hash, sin fabricar ejecuciones.

## Flujos y retornos

Sin flujos o con todos los flujos exactamente al cierre del período, el retorno ajustado es `(NAV final − flujos)/NAV inicial − 1`. Con flujos intraperíodo, el PnL monetario puede ser verificable pero el retorno permanece null hasta contar con subperíodos valuados. Esto también aplica cuando aportes y retiros se compensan y su suma neta es cero.

No se calcula drawdown de cuenta encadenando señales solapadas. La salida mantiene resultados de contabilidad y contrafactual separados. El riesgo incremental requiere una curva continua reconciliada; sigue no identificable sin ella. Las estadísticas anualizadas no se deducen de un único período.

## Monedas e instrumentos

La valuación acepta cash/posiciones en varias monedas con FX conocido al corte. La atribución de PnL de operaciones en moneda distinta a la de reporte sigue NOT_IDENTIFIABLE (`FX_COMPONENT_ATTRIBUTION_REQUIRED`): falta separar componente de instrumento y FX. La selección de cotización toma la última elegible; el proveedor debe controlar frescura, ya que no se ha inventado una caducidad universal.

El adapter nativo usa una clave local `BROKER_TICKER:<currency>:<ticker>`, explícitamente distinta de una identidad verificada del instrumento. No mezcla cotizaciones de acciones estadounidenses con CEDEARs. La cobertura corporativa y ratios debe venir de la fuente; no se supone un ratio por similitud de ticker.

## Fuentes nativas actuales

`portfolio_snapshots.total_value_ars` contiene valor invertido; NAV observado añade `cash_ars`. El adapter conserva cantidades, precios y bases reportadas. Los fills aportan la comisión observada cuando existe; no aportan un contrato completo de impuestos/otros costos. Los vínculos `recorded_plan_links` conservan lo almacenado, pero un match previo por cercanía no pasa a ser causalidad exacta de ejecución.

Las conciliaciones automáticas recorren pares adyacentes de snapshots, incluidas ventanas atrasadas, hasta 100 entradas nuevas por ciclo. Una revisión de fills puede generar otra versión para el mismo período. Coverage nativo permanece no atestado: coincidir numéricamente no basta para certificar exhaustividad. Los movimientos no reconocidos quedan en `unresolved_movements`, sin descartarlos silenciosamente ni transformarlos en flujos conocidos.

## Auditoría real reproducida

Corte: 2026-10-14 04:53:59.810706 UTC, capture SHA256 `814fac6d86339bce6bf31ec2c2053c6fcafb51354b31f01e9866c18170e3c935`.

| Denominador | Resultado observado |
|---|---|
| 301 casos / 72 tickers | 0 tesis congeladas; 0 casos económicos VERIFIED |
| 13 planes / 32 captures | 2 FULL_CONTEXT; 22 casos vinculados a esos contextos |
| 301 casos | 48 vínculos exactos item/HOLD→plan; 37 vínculos almacenados decisión→fill; 0 atribuciones formales plan→ejecución |
| 109 fills | 109 fees UNKNOWN; 0 IDs externos duplicados |
| 2.233 snapshots / 28.503 posiciones | 2.166 sumas cantidad×precio coincidentes; 67 diferencias >0,01 |
| 2.232 pares adyacentes | 1.872 cantidades visibles coincidentes; 328 gaps; 32 con timestamps de fills insuficientes |
| 28.615 observaciones de mercado | 28.615 PARTIAL según el contrato nuevo |

Estas son pruebas aritméticas de filas visibles. No certifican cash externo completo, corporate actions, bases, financiamiento o PnL neto. No se atribuye a Quantia alpha ni pérdidas reales a partir de estos gaps. Ninguna fila real fue modificada.
