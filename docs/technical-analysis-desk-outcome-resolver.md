# Technical Desk — resolver de resultados (`technical-desk-outcome-resolver-v1`)

Módulo: `src/technical_desk/outcome_resolver.py`. Puro y determinista: no lee base, reloj, red ni modelos.
Implementa las reglas de evaluación de `technical-analysis-desk-design.md` para UN escenario (`ScenarioOutput`).
No valida capacidad predictiva: solo decide, con reglas congeladas, qué pasó con cada escenario registrado.

## Reglas congeladas
- **Origen:** la primera sesión evaluable es la primera sesión del calendario cuya apertura es *estrictamente posterior* al origen (`ledger_committed_at` para predicciones del sistema, `published_at` para utilidad comunicada; dos cohortes, dos corridas).
- **Horizonte:** `horizon_sessions` rueda(s) del calendario del instrumento, nunca días corridos. Si el calendario no cubre el horizonte: `NOT_EVALUABLE`.
- **Vela faltante:** `MISSING_BAR`. No se corre el vencimiento a la observación siguiente. Los eventos ocurridos antes de la primera vela faltante sí cuentan.
- **Madurez:** `data_through` es la última sesión con dato completo. Las posteriores no se miran (sin look-ahead) y el escenario queda `IMMATURE` salvo que ya haya un evento terminal.
- **Orden dentro de una vela:** los eventos que dependen del cierre (`CLOSE`, o `HIGH`/`LOW` con operador de "nunca tocó") ocurren al cierre; un toque (`HIGH` GT/GE, `LOW` LT/LE) ocurre dentro de la vela, o a la apertura si la apertura ya lo cumple (gap). Mismo orden y decisivos distintos (confirmación/invalidación/objetivo) ⇒ `AMBIGUOUS_SAME_BAR`. Nunca se resuelve a favor de la tesis.
- **Objetivos:** requieren confirmación previa (`TARGETS_REQUIRE_PRIOR_CONFIRMATION`). Un objetivo tocado antes se registra en `targets_touched_before_confirmation` y no puntúa; puede puntuar si se vuelve a tocar después. Dirección `NEUTRAL`: objetivos no evaluados (`target_note`).
- **Caducidad:** un escenario no confirmado al terminar su horizonte es `EXPIRED_UNCONFIRMED`.
- **Persistencia:** `persistence_bars` cuenta velas consecutivas del timeframe del escenario; un corte reinicia la cuenta. Solo cuentan velas posteriores al origen.
- **Semanal (W):** solo semanas completas dentro de la ventana; el evento se fecha en la última rueda de la semana; una rueda faltante bloquea la vela semanal (`MISSING_BAR`). Sin ninguna semana completa: `NOT_EVALUABLE`.
- **HIGH/LOW ausentes:** una condición o un objetivo que los necesita no se evalúa con el cierre: `NOT_EVALUABLE`.
- **Ajustes:** un evento de ajuste (split) dentro de la ventana ⇒ `NOT_COMPARABLE` (no hay protocolo de transformación de niveles).
- **Etiqueta de sesión en día no hábil dentro de la ventana:** `NOT_EVALUABLE`.

Estados: `TARGET_REACHED`, `INVALIDATED_BEFORE_CONFIRMATION`, `INVALIDATED_AFTER_CONFIRMATION`, `CONFIRMED_TARGET_NOT_REACHED`, `CONFIRMED_NO_TARGETS`, `EXPIRED_UNCONFIRMED`, `AMBIGUOUS_SAME_BAR`, `MISSING_BAR`, `NOT_COMPARABLE`, `NOT_EVALUABLE`, `IMMATURE` (único no final).

## Qué no hace todavía
- No escribe en el ledger: `Resolution.to_payload()` devuelve el dict JSON-seguro para registrarlo como evaluación nueva (revisión), nunca como reemplazo.
- No arma el calendario ni las velas: las recibe (`expected_sessions`, `Bar`, `bars_from_rows`). La cobertura de `read_market_evidence_as_of` sigue sin verificarse.
- Sin métrica de dirección/rango ni zona neutral (requiere parámetros congelados antes de evaluar), ni agregación por cohorte, baselines o calibración.
- La ambigüedad intrabar no se desempata con datos intradía.
- No sustituye a `update_outcomes`/`decision_log`: son semánticas distintas (días corridos, primera vela posterior).
