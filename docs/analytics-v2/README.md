# Quantia Analytics v2

Capa adicional offline en `src/analysis/analytics_v2`. El contrato está en
[ADR 001](../adr/001-analytics-v2-measurement-contract.md). Las tablas y sus hashes
son la evidencia; los gráficos sólo la muestran. No cambia `/viability`, Ledger,
Telegram, scheduler, órdenes ni resultados de Agentic Portfolio Manager.

## Ejecución reproducible

Desde `C:\Users\Franco\OneDrive\Escritorio\backend\cocos_copilot`:

```powershell
python -m pip install -r requirements-analytics.txt
python examples/analytics_v2/sample.py
python -m src.analysis.analytics_v2.cli build --input outputs/analytics_example_input/input.json --policy outputs/analytics_example_input/policy.json
python -m src.analysis.analytics_v2.cli report outputs/analytics/<run_id>
python -m src.analysis.analytics_v2.cli validate outputs/analytics/<run_id>
```

El ejemplo es **sintético**, con 200 resamples para revisión rápida; los defaults
productivos son 5.000. Incluye repeats, FOLLOW/IGNORE/CONTRARY, fill peor que
referencia, costos, Radar, Swap repetido, ambigüedad y 40D pendiente. El ejemplo
económico compra una unidad a 100,50 y vende a 120, paga 1 en fees y termina con
PnL neto 18,50. Shortfall -1,50 se muestra aparte, no se vuelve a restar.

`build` consume JSON validado por `Dataset` y `AnalyticsPolicy`. Se exige cutoff
timezone-aware y se normaliza a UTC. Las agrupaciones de fecha usan fecha UTC.
`report` consume las tablas del bundle construido; no consulta precios ni red.
`validate` verifica inventario, hashes, metadatos, código y replay de cálculos.
Un conflicto dentro del mismo run aborta en lugar de sobrescribir evidencia.
El código sin commit queda identificado por el hash de todos los módulos, además
del HEAD de Git. Para replay histórico conservar el checkout correspondiente.

## Evidencia de entrada

Los modelos frozen son la especificación ejecutable. Un snapshot contiene:

- Recomendaciones con cuenta, módulo, instrumento, score de convicción alineado,
  serie de precios, decisión, elegibilidad y ambigüedad.
- Calendario explícito; OHLC por serie/instrumento/sesión/revisión con moneda y
  base de ajuste consistentes. No mezclar series de proveedores.
- Relojes `available_at`, `ingested_at`, `sealed_at` y `source_id` por evidencia.
- Acciones humanas enlazadas exactamente al ancla de la oportunidad.
- Fills con costos observados nullable. Cero sólo si el costo fue observado cero.
- NAV reconciliado, flujo externo y componentes brutos del período económico,
  `coverage_confirmed` y referencias a la evidencia de cobertura.

La cobertura económica es una declaración del adaptador/contador que proporciona
el snapshot; el motor verifica límites de NAV, costos de fills y reconciliación,
pero no certifica por sí solo que el broker haya entregado todos los movimientos.
Snapshots de pantalla o `total_value_ars` por sí solos no satisfacen el contrato.
`source_id` conserva provenance del proveedor y el pipeline calcula hashes de fila.

Para matching, el score/ventana pertenecen al ancla. Una clasificación de FOLLOW
registrada varios días después no se trata como decisión contemporánea. Es
`INFORMATION_SET_MISMATCH` si se cambia decision_as_of. UNKNOWN/MODIFIED requieren
evidencia adicional y no entran al primary. IGNORE usa cash local=0; no modela
oportunidades alternativas del capital humano.

La atribución de ejecución soporta round-trip largo identificado, cantidades
balanceadas, compras antes de ventas, costos completos y referencias contemporáneas
`EXECUTION_MARKET`. Publica retorno real y deltas timing/execution/cost sobre
capital de referencia. `sizing_delta` permanece null sin contrafactual identificable.
No intenta inventar una descomposición aditiva a partir de notionals arbitrarios.

## Políticas y lectura

- Costos: 0/75/150/250/400 bps; BASE=150; stress=250. El costo real es observado.
- Swaps: costo incremental round-trip de reemplazar frente a mantener. El
  benchmark original no paga ese costo de reemplazo; su costo histórico es hundido.
- IC: Spearman score alineado/alpha direccional. Benchmark ausente implica IC
  ausente y evita confirmar un edge de ranking.
- `n_raw`: recomendaciones representadas por episodios maduros; `n_recommendations`:
  todas las recomendaciones; `n_episodes`: episodios maduros. Los counts de estados
  son por episodio/horizonte, nunca por escenario agregado.
- `n_effective`: proxy conservador, mínimo de clusters por instrumento con
  ventanas solapadas y floor(n_dates/max(horizonte,bloque)). No prueba independencia.
- Bootstrap circular preserva fechas completas. Si n_dates<2*bloque no da CI;
  una muestra degenerada no se convierte en confirmación. Test unilateral
  centrado es una aproximación bootstrap, no una prueba IID exacta.
- BH sólo tiene familia si se declararon nombres `cuenta/módulo/cohorte/H/ev`
  e `.../ic` antes de la primera decisión. Faltantes cuentan en family_size.
- Radar ALL es la unión de los episodios OPERABLE/WATCHLIST; no sumar ALL y
  sus subconjuntos ni preregistrarlos como oportunidades independientes.
- Gates: IMMATURE/OBSERVE/PROVISIONAL_GUARDED/CONFIRMED/FAIL/DISABLED_SHADOW.
  La tabla de requisitos y el mapeo legacy están en el ADR.
- Confirmación estadística y elegibilidad para revisión de autoridad son campos
  distintos. `capital_authority` siempre false: este package sólo audita.
- Curvas shadow deben declarar cuenta, cohorte, horizonte, moneda y sizing.
  El gate de DD usa esa curva; sin ella no recomienda autoridad de capital.
- Sharpe HAC Bartlett, Sortino y volatilidad sólo se publican para una curva
  con frecuencia, risk-free, MAR y retornos ajustados por flujos verificables.
  NONE y PERIOD_END se verifican; flujos intraperíodo requieren dividir la serie
  en subperíodos. No se infiere un TWR de dos NAV con flujos intermedios.

## Artefactos

`outputs/analytics/<analysis_run_id>/` contiene manifest.json, derived.json,
summary.json, report.md, tablas CSV y siete PNG. Incluye ventanas, enlaces,
outcomes, fills/shortfall, PnL, curvas, métricas, matching, CI por bloque, gates,
hipótesis/BH, costos, outliers, madurez, ECDF, calibración, pares Swap y persistencia.
El primer cambio de signo es entre horizontes observados, no un tiempo de evento
continuo ni un Kaplan-Meier. No se atribuyen ceros a horizontes pendientes.

## Validación

```powershell
python -m pytest tests/test_analytics_v2.py --cov=src.analysis.analytics_v2 --cov-report=term-missing --cov-fail-under=90
python -m pytest tests/test_viability_audit.py tests/test_decision_market_audit.py tests/test_execution_nominals_and_rotation.py
```

La exigencia `pyproject` de cobertura global 90% pertenecía a Agentic Portfolio
Manager. Quantia no tiene ese pyproject. Se mide y exige 90% sobre el nuevo package;
la suite legacy se valida por separado, sin atribuirle esa cobertura.

## Pendientes explícitos

1. Adaptadores de captura prospectiva desde PostgreSQL/broker con snapshots sellados
   y cobertura verificable. No convertir las tablas mutables legacy en historia PIT.
2. Libro completo de posiciones, FX, corporate actions, impuestos, financiación y
   movimientos externos desde las fuentes reales. El motor consume su reconciliación.
3. Atribución de sizing y round-trips parciales, cortos, multi-leg o con financiación;
   sus campos quedan null con motivos cuando no son identificables.
4. Captura canónica PIT para Ledger/Viability v2, UI, migraciones y scheduling.
   Telegram incorpora `/analytics` como lectura observacional de las tablas actuales;
   no sustituye la CLI canónica ni los reportes legacy. Ver [Telegram](telegram.md).
5. Generación de carteras shadow con sizing preregistrado; se admiten curvas
   explícitas, no se fabrican encadenando outcomes solapados.
6. Calibración prospectiva de políticas e inferencia de 5.000 resamples en datasets
   grandes. El benchmark descriptivo de 250k rows está en
   `scripts/benchmark_analytics_v2.py`; no incluye bootstrap ni exportación/plots.

Estos límites impiden declarar terminado el rollout operativo o validado el edge
de Quantia sólo por haber implementado el motor y pasado fixtures sintéticos.
