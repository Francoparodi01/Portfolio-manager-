# QEEF: especificación de métricas v1.0.0

## Extensión Phase 2: denominadores prospectivos

`qeef-prospective-v1` mantiene los estimandos v1 y agrega métricas de captura por owner y corte: fuentes únicas esperadas/ausentes desde activación; versiones capturadas; jobs pendientes, RETRY y DEAD_LETTER; duplicados de entrega; lag; tesis sin vínculo y contratos PIT completos. UNKNOWN no se transforma en cero. Para movimientos históricos sin owner, el denominador recuperable es UNKNOWN.

Cada evaluación usa el Episode congelado de CASE y fija horizontes 5/10/20 sesiones más horizonte propio en sesiones. La diferencia monetaria contrafactual es diferencia de retornos por el capital inicial común. No es PnL realizado. `evaluations_mature` requiere PLAN verificable en todos los horizontes pedidos; las alternativas restantes tienen sus gates propios.

La conciliación separa PnL monetario, realizado bruto, cambio no realizado, fees, cash, cantidades y NAV. Los cargos no asociados a fills usan `Analytics v2.ObservedAccountCost`; se agregan por componente sin inventar trades. Retornos de períodos con flujos intraperíodo requieren valuaciones intermedias incluso si el flujo neto es cero. La atribución FX de PnL multimoneda permanece no identificable. Detalle en [QEEF_ACCOUNTING_RECONCILIATION.md](QEEF_ACCOUNTING_RECONCILIATION.md).

Las versiones de conciliación no son ventanas independientes. No se publican curvas, alpha, probabilidades calibradas ni métricas de promoción a partir de los fixtures sintéticos.

Implementación normativa: [counterfactuals.py](../../src/analysis/qeef/counterfactuals.py), [metrics.py](../../src/analysis/qeef/metrics.py), [historical.py](../../src/analysis/qeef/historical.py), y núcleos reutilizados de Decision Lab/Analytics v2. Los resultados usan fracciones decimales: 0,01 = 1%; diferencias de retornos se expresan en puntos porcentuales al multiplicar por 100. Un punto básico = 0,0001. Moneda contrafactual v1: ARS/BYMA; no se convierte subyacente USD a CEDEAR por inferencia.

## Identidad del estimando

Todo agregado debe especificar owner, estrategia/versión, fuente, scope, estado de origen, calidad, horizonte/unidad, régimen, definición de benchmark y costos. Un mismo ticker no hace compatibles dos cohortes. Se reportan por separado decisiones, oportunidades canónicas, episodios de portfolio y ejecuciones. `recurrence_proxy_id` es una agrupación exploratoria, no `opportunity_id` ni tamaño efectivo demostrado.

Resultados distintos: `COUNTERFACTUAL`, `OBSERVED_EXECUTION`, `REALIZED_ACCOUNT`; los registros legacy conservan la etiqueta de diagnóstico direccional. Recomendación emitida y plan aprobado no son evidencia de fill. Sin cobertura completa, ausencia de fills es `UNOBSERVED`, no `NOT_EXECUTED`.

## Horizontes y censura

5D/10D/20D permanecen disponibles; 40D legacy también se preserva. En `decision_log` significan días calendario más primera barra disponible; en el motor QEEF significan **N sesiones BYMA** posteriores a la decisión. Nunca se comparan silenciosamente ambos significados.

El motor acepta cualquier horizonte entero positivo; la tesis puede declarar sesiones, días calendario o evento. Cierre por target/invalidación/cambio de régimen/posición/expiración se registra mediante evento, con motivo y evidencia. La persistencia de tendencia se expresa con condiciones/eventos objetivos definidos antes del resultado, no alargando retroactivamente una tesis ganadora. El tiempo transcurrido se obtiene de los relojes de apertura/cierre; una tesis abierta se reporta `RIGHT_CENSORED_AT_AS_OF`.

El evaluador de retornos v1 usa sesiones; no traduce automáticamente un cierre por evento a una valoración monetaria sin barra y cobertura de esa fecha. En historia, ausencia de outcome se conserva como faltante o pendiente, no pérdida ni cero. Cobertura usa el total elegible como denominador. No se interpreta la media de maduros como estimación insesgada si la falta de madurez depende del resultado.

## A. Detección de oportunidades

`detection_quality` recibe universo U congelado en t, selección S⊆U, score y label binario posterior. La definición debe registrar riesgo máximo, política de costos, liquidez, horizonte, referencia y completitud del universo. Un label económico debe aplicarse a **todos** los elegibles con la misma regla; no basta precio posterior creciente. El código verifica el contrato y los relojes; no demuestra por sí solo que el productor haya etiquetado correctamente el universo.

- Precision = positivos seleccionados / seleccionados, sólo si todos los seleccionados están etiquetados.
- Recall = positivos seleccionados / positivos del universo, sólo con universo y labels completos y denominador positivo.
- Falsos positivos = seleccionados etiquetados negativos; omisiones = positivos del universo no seleccionados.
- Cobertura = labels disponibles al corte / |U|. Desconocidos permanecen en U.
- NDCG@k = suma(label_i/log2(i+1)) / suma ideal para k=|S|, orden score descendente y ticker para empates; `null` sin positivos/score válido.
- Comparador aleatorio: selección sin reemplazo, mismo k, universo ordenado, semilla 42 registrada; se compara precision bajo la misma definición.

Anticipación, persistencia emergente, volumen, liquidez y rendimiento condicionado necesitan labels/path y referencias específicos. V1 no calcula estos efectos desde la captura actual ni los reemplaza por un score. `executable_return=null` separa detección de una estrategia invertible.

## B. Investigación y tesis

`thesis_quality` verifica referencias y disponibilidad anterior a la apertura, faltantes y relaciones SUPPORTS/CONTRADICTS. Calidad de datos, coherencia, poder predictivo y desempeño económico son campos separados. Contar indicadores no aumenta automáticamente calidad. Técnica, fundamentals, earnings, valuación, macro/noticias y catalizadores deben aportar período/publicación/disponibilidad/versión cuando corresponda. No se usa una revisión fundamental publicada después para justificar una tesis anterior.

La coherencia queda PARTIAL: registrar soporte no valida causalmente la tesis. Los valores predictivo y económico quedan ausentes sin evaluación independiente.

## C. Decisión y contrafactuales

Sea V0 = cash0 + Σ(q_i P_i0), reconciliado exactamente con el snapshot inicial. Para cada alternativa a, el kernel simula cantidades, efectivo y costos bajo el mismo calendario/capital. El retorno es R_a,h = V_a,h / V0 − 1; valor incremental frente a b: Δ_a,b,h = R_a,h − R_b,h. No es alpha factorial ni PnL observado.

Alternativas QEEF:

| Nombre | Definición congelada |
|---|---|
| PLAN | Órdenes del plan del episodio |
| NO_TRADE | No modificar cartera ni efectivo |
| HOLD_EXISTING | Mantener cartera y efectivo existentes; numéricamente igual a NO_TRADE sin otra política |
| CASH | Liquidar **todo el portfolio** con costos/lotes y misma regla temporal; no sólo sleeve afectado |
| REBALANCE_BASELINE | Órdenes de política/estado/owner registrados antes de t |
| ELIGIBLE_ALTERNATIVE | Órdenes de selección ex ante dentro del conjunto elegible congelado |

El CASH histórico del kernel Decision Lab puede referirse al sleeve afectado: QEEF versiona explícitamente la diferencia y no recalcula su historia con la nueva definición. HOLD no se penaliza por no haber comprado al ganador futuro. Cash no obtiene una tasa estimada si no está definida en el kernel/modelo.

La simulación comienza en una apertura de sesión estrictamente posterior a la decisión; es una convención conservadora común, también para señales intradiarias. No presume compra al cierre de una señal postcierre. Requiere barras ARS/BYMA RAW_AS_TRADED, cierre confirmado, linaje PIT, identidad/ratio y cobertura de acciones corporativas. Dividendos/splits/ratio deben reflejarse con eventos explícitos y cantidades congruentes. Faltante de barra de una tenencia o alternativa bloquea la comparación común; no se suprime el ticker para mejorar el retorno.

Las comisiones/spread/slippage son **supuestos de simulación versionados** con mismo modelo entre alternativas, no fees observadas. Costos que superan ganancia bruta producen retorno negativo. Lotes y financiación pueden reducir ejecución; no se crea apalancamiento para completar cantidades. Escenarios de costos y benchmarks se informan todos, con sus hashes; elegir el más favorable después no constituye validación.

`freeze_alternatives` rechaza benchmark definido/seleccionado después de t, owner/estado incompatibles y órdenes fuera del universo o bloqueadas. Los hashes de política son procedencia declarada, no sustituyen auditoría de cómo se eligió esa política. Sin definición ex ante, resultado `NOT_IDENTIFIABLE`.

## D. Asignación y riesgo ex ante

Pesos long-only finitos, incluyendo CASH, deben sumar 1. Concentración HHI = Σw_i² (incluye cash); utilización de capital = 1−w_cash; exposición sectorial agrega pesos por sector, con UNKNOWN explícito. Límite por instrumento se compara sólo cuando se aporta `max_weight`.

Con matriz Σ simétrica, finita, PSD y alineada a los pesos:

- σ_p = sqrt(w'Σw), en la unidad temporal de la covarianza suministrada.
- Riesgo marginal i = (Σw)_i / σ_p.
- Contribución i = w_i(Σw)_i / σ_p; su suma = σ_p.
- Sin σ positivo, contribuciones ausentes; sin matriz, riesgo ausente.
- Cambio de asignación = w_target−w_initial.
- Reasignación unilateral incluyendo cash = 0,5 Σ|Δw_i|.
- Notional de trades de activos / NAV = Σ_activos|Δw_i|. No se denomina costo.

El caller debe garantizar corte PIT y período de la matriz; la función comprueba propiedades numéricas, no linaje externo. Cash drag, factores, FX, liquidez y costos de rebalanceo quedan indeterminados sin datos/regla adicionales. Pesos distintos no prueban mejora económica: se requiere comparar carteras valorizadas bajo el mismo contrafactual.

## E. Valor económico realizado

Se reutiliza `analytics_v2.pnl.economic_pnl` con owner exacto y datos visibles al corte:

PnL_net = NAV_final − NAV_inicial − flujos_externos_netos.

Debe reconciliar, con tolerancia monetaria 0,01:

PnL_net = PnL_realizado + cambio_PnL_no_realizado − fees − impuestos − financiación − otros_costos.

Requiere cobertura atestada, referencias, NAV extremos reconciliados y suma de fees/impuestos/otros costos de fills consistente con el período. Componentes desconocidos no son cero. `return_on_beginning_nav` sólo se calcula con flujos externos cero y NAV inicial positivo. El retorno sobre capital desplegado usa su denominador explícito si fue provisto.

Una curva válida exige convención temporal consistente, puntos reconciliados y retornos ajustados por flujos que satisfagan r_t=(NAV_t−F_t)/NAV_(t−1)−1, para flujos NONE o PERIOD_END. Flujos intraperíodo sin valoración de subperíodos no se convierten en TWR válido. TWR=Π(1+r_t)−1. El [manual GIPS](https://www.gipsstandards.org/standards/gips-standards-for-firms/gips-standards-handbook-for-firms/) sustenta la separación de flujos y rendimiento; **no se declara cumplimiento GIPS**.

Sobre una curva continua válida, nunca sobre señales solapadas:

- Equity normalizada E0=1, E_t=Π(1+r_s); MDD=min(E_t/max_(u≤t)E_u−1).
- Volatilidad anualizada = std_muestral(r)√A, A=períodos/año declarado.
- Sharpe HAC = media(r−rf) / sqrt(varianza Newey-West con pesos Bartlett) × √A; máximo 5 lags y n−1. Ajuste por dependencia no vuelve independiente una población sesgada. La relación entre autocorrelación y Sharpe está documentada en [NBER w9571](https://www.nber.org/papers/w9571).
- Sortino = (media(r)−MAR) / sqrt(media(min(r−MAR,0)²)) × √A; sin downside, `null`.
- Calmar = CAGR/|MDD|, sólo con al menos A intervalos reconciliados y drawdown no nulo. No anualizar un episodio breve para publicarlo como Calmar.
- Turnover de Analytics = Σ|cantidad_fill×precio_fill| / NAV_promedio ponderado por tiempo. El nombre heredado `turnover_one_way` conserva esa fórmula; no es 0,5 de compras+ventas. No mezclar con reasignación por pesos.
- Exposición bruta/neta promedio: notional/NAV ponderado por tiempo entre puntos observados; máximo bruto separado.

`execution_quality` mide cantidad realmente ejecutada y fill_ratio, fees conocidas y costo de shortfall = Σ sign(side) q(P_fill−P_ref), BUY=+1/SELL=−1; positivo significa peor precio. Analytics `execution_shortfall` usa convención opuesta de contribución al PnL: no sumar ambos signos como métricas distintas. No se imputa cantidad faltante. Unidades de cantidad/precio/fees deben compartir moneda y base; el productor debe aportar referencia congelada comparable.

Profit factor de trades cerrados requeriría PnL neto y matching defendible; no está implementado como métrica realizada QEEF v1. `profit_factor_unit_notional` existente en Analytics es un diagnóstico de retornos por oportunidad, no PF de cuenta. Excess return sólo se publica como delta de contrafactuales aquí. Alpha estimado permanece `null` sin modelo factorial/benchmark congelado y muestra válida. La captura actual no permite ninguno de esos claims realizados.

## F. Confianza, cobertura y múltiples hipótesis

`paired_confidence` exige una cohorte exacta y un valor por oportunidad; duplicado idéntico es idempotente, duplicado conflictivo se rechaza. n_raw, n_opportunities, n_mature y n_ticker_clusters se reportan separados. Cobertura = maduros / oportunidades totales, incluyendo censuradas/faltantes.

El proxy no solapado se construye por finalización creciente y admite la siguiente ventana sólo si decision_at > exit_at de la anterior. Comienza en la **decisión**, no la próxima apertura. Es una cota operativa conservadora de ventanas disjuntas, no una estimación probada de n IID.

V1 no calcula intervalos si faltan ≥30 ventanas no solapadas, ≥90% cobertura, ≥2 tickers o calidad HIGH en todas las observaciones aceptadas. Aun pasando esos criterios sólo produce PARTIAL:

1. Bootstrap temporal por bloques circulares de fechas observadas; conserva sección transversal. Longitud max(horizonte,20), seed42, 2.000 resamples, CI95%; requiere al menos dos bloques de fechas y 90% draws válidos.
2. Bootstrap separado por clusters ticker, remuestreando clusters completos y promediando las observaciones resultantes.

No es bootstrap conjunto de dos vías. Dos tickers siguen siendo pocos clusters; ningún intervalo autoriza promoción. Media y win_rate_incremental pueden mostrarse descriptivamente con muestra pequeña, junto a denominador y ausencia de inferencia. Regímenes distintos se evalúan por separado; no se elige retrospectivamente el favorable.

`multiple_testing` reutiliza Benjamini-Hochberg: hipótesis preregistradas sin p-value cuentan como p=1 en el tamaño de familia y conservan q reportado `null`. La corrección no repara data snooping ni políticas seleccionadas tras observar outcomes. No hubo inferencia múltiple, validación OOS ni ablación empírica válida nueva con esta captura; las funciones están disponibles para evidencia prospectiva.

## Versionado y tratamiento de desconocidos

`qeef-v1.0.0` no reemplaza métricas legacy. Se conserva outcome crudo, reloj, fórmula/unidad y fuente. En el adaptador de esta auditoría, los campos económicos son `null` deliberadamente porque no hay tesis canónica, precios/acciones enlazados ni reconciliación suficiente. Este adaptador **no es un buscador universal capaz de demostrar que ninguna oportunidad válida existe en otras fuentes**. El conteo cero significa cero certificado por este protocolo/captura.

No hay score global. Calidad del dato, confianza y retorno pueden discrepar. Una simulación bien construida puede perder; una ganancia observada puede no ser atribuible a una decisión; una defensa puede rendir menos en términos absolutos y cumplir su objetivo de riesgo.
