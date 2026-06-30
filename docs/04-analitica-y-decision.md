# Analitica y Decision

## Analysis

`scripts/run_analysis.py` es el pipeline principal de cartera.

Pasos:

1. carga portfolio y cash;
2. carga velas canonicas;
3. calcula tecnico;
4. calcula macro;
5. calcula riesgo;
6. agrega sentiment si esta habilitado;
7. sintetiza score multicapa;
8. ejecuta optimizer;
9. pasa por execution planner;
10. guarda eventos auditables;
11. muestra reporte.

## Cambio operativo - lunes 15 de junio de 2026

- `run_analysis.py` acepta `--no-persist` para pruebas sin guardar eventos en
  `decision_log`.
- `/analisis` sigue siendo el plan formal. `/analisis_test` corre el mismo
  pipeline sin persistir.
- `/radar` y `/radar_full` son exploratorios desde Telegram y usan
  `--no-persist`; las ideas de radar no entran al EV principal hasta que haya
  decision/fill real.
- Sentiment/noticias queda habilitado por defecto en analisis y radar. El flag
  `--no-sentiment` queda reservado para debugging tecnico.
- La capa de noticias es auditable/contextual: puede advertir o bloquear, pero
  no debe convertir por si sola una lectura en compra.
- `scripts/run_market_context.py` y `/mercado` ejecutan en una sola salida:
  macro, fuentes confiables, noticias recientes, scoring contextual y lectura
  de soporte. No escriben `decision_log` ni modifican EV, planner o thresholds.
- Desde el mismo corte operativo, el optimizer intenta usar Black-Litterman real
  con PyPortfolioOpt. Si PyPortfolioOpt no está disponible o no converge, el
  sistema cae explícitamente a `FALLBACK_MAX_SHARPE` y lo informa en el reporte.

Un holding sin 60 velas queda como `NO_EVALUABLE`. No entra al optimizer ni a
senal operativa hasta tener historico suficiente.

## Radar

`scripts/run_opportunity.py` evalua universo Cocos excluyendo holdings por defecto.

Estados:

| Estado | Significado |
|---|---|
| `COMPRABLE_AHORA` | setup completo |
| `COMPRA_HABILITADA` | buena senal con reservas |
| `SWAP_CANDIDATO` | mejora relativa contra cartera |
| `VIGILANCIA_A/B/C` | seguimiento |
| `NO_OPERABLE` | senal presente pero R/R o setup invalido |
| `EXTERNO` interno | nombre tecnico heredado; en reporte se muestra como sin historico operable |

El radar no opera un ticker solo porque tenga score alto: requiere asimetria, R/R,
cash/funding o swap posible.

## Optimizer

`src/analysis/optimizer.py` propone pesos objetivo teoricos. No ejecuta.

Motor actual: `BLACK_LITTERMAN` con PyPortfolioOpt cuando está disponible. El
campo `Motor real` indica si corrió Black-Litterman real o fallback numpy.

El optimizer puede sugerir aumentar una posicion, pero el execution planner puede
bloquearla si:

- score no supera umbral de BUY;
- no hay cash;
- el R/R no compensa;
- hay gates de calidad;
- la orden no supera minimo operativo.

## Execution Planner

`src/analysis/execution_planner.py` es la fuente de verdad operativa.

Decisiones:

- `BUY`
- `SELL_PARTIAL`
- `SELL_FULL`
- `HOLD`
- `WATCH`
- `BLOCKED`

El planner busca que el reporte final sea accionable y honesto:

- "comprar" solo si es ejecutable;
- "watch" si hay idea teorica pero no hay calidad/cash;
- "blocked" si hay razon explicita para no operar.

## Performance

`scripts/run_performance.py` mide:

- dataset por source/status/decision_type;
- outcomes 5d/10d/20d;
- win rate;
- expected value;
- promedio win/loss;
- curva de equity;
- diferencia entre teorico, aprobado, bloqueado y ejecutado.

La lectura correcta separa:

- performance total operativa;
- fills manuales;
- decisiones aprobadas por planner;
- ideas teoricas del optimizer/radar.

## Regression Audit

`scripts/run_regression_audit.py` analiza si las variables y capas del modelo
explican outcomes posteriores.

Modos:

```bash
python scripts/run_regression_audit.py --mode signal
python scripts/run_regression_audit.py --mode optimizer
python scripts/run_regression_audit.py --mode execution
python scripts/run_regression_audit.py --mode blocked
python scripts/run_regression_audit.py --mode all
```

Hasta que el dataset tenga mas muestra, regression debe leerse como auditoria de
calidad, no como verdad estadistica definitiva.

## Confidence Audit

`scripts/run_confidence_audit.py` revisa:

- DB y tablas clave;
- frescura de datos;
- cobertura de velas;
- fills reconciliados;
- outcomes cerrados;
- feriados/mercado cerrado;
- consistencia entre precios y velas.

## DCL

`src/analysis/dcl/` es la capa experimental de calibracion:

- carga decisiones enriquecidas;
- evalua seguridad de muestra;
- ejecuta auditoria estadistica;
- genera reporte de calibracion.

No debe usarse para cambiar thresholds automaticamente hasta tener muestra robusta.
