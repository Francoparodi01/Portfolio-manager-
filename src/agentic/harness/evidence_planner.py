from __future__ import annotations

from .schemas import EvidenceClaim, TaskSpec


# Claim definitions are intentionally deterministic. The LLM may decide whether
# optional evidence is worth consulting, but it does not get to redefine which
# facts make a financial answer well-grounded.
_CLAIMS: dict[str, list[tuple[str, str, tuple[str, ...]]]] = {
    "portfolio_review": [
        ("portfolio_state", "Estado y composición actual de la cartera.", ("get_portfolio_snapshot",)),
        ("current_decision", "Última evidencia de decisión disponible para la cartera.", ("get_persisted_decision_evidence",)),
    ],
    "position_analysis": [
        ("portfolio_state", "Exposición actual del activo dentro de la cartera.", ("get_portfolio_snapshot",)),
        ("current_decision", "Señal y plan actual del motor para el activo.", ("get_decision_evidence",)),
        ("historical_edge", "Evidencia económica histórica frente a alternativas como HOLD.", ("get_decision_value_added",)),
        ("market_context", "Contexto macro relevante para interpretar la señal.", ("get_macro_context",)),
    ],
    "decision_explanation": [
        ("current_decision", "Señal y plan actual que originan la decisión.", ("get_decision_evidence",)),
        ("historical_edge", "Evidencia histórica que respalda o debilita la decisión.", ("get_decision_value_added",)),
        ("instrument_context", "Análisis específico del instrumento cuando esté disponible.", ("analyze_ticker",)),
    ],
    "position_comparison": [
        ("portfolio_state", "Exposición actual de los activos comparados.", ("get_portfolio_snapshot",)),
        ("current_decision", "Señales y plan actual de los activos comparados.", ("get_decision_evidence",)),
        ("historical_edge", "Comparación de evidencia histórica de decisión.", ("get_decision_value_added",)),
    ],
    "opportunities": [
        ("portfolio_state", "Restricciones y exposición actual de la cartera.", ("get_portfolio_snapshot",)),
        ("opportunity_set", "Oportunidades observadas por el radar vigente.", ("scan_opportunities",)),
        ("current_decision", "Compatibilidad de las oportunidades con el plan actual.", ("get_decision_evidence",)),
    ],
    "performance": [
        ("economic_result", "Resultado económico observado en el ledger canónico.", ("get_decision_ledger",)),
    ],
    "bot_follow_pnl": [
        ("bot_counterfactual", "Contrafactual económico de seguir los planes del bot.", ("get_bot_follow_pnl", "get_normalized_bot_follow_pnl")),
    ],
    "net_performance": [
        ("net_result", "Resultado neto por decisión según el reporte canónico.", ("get_net_decision_report",)),
    ],
    "analytics_v2": [
        ("analytics", "Métricas observacionales de Analytics v2.", ("get_analytics_v2",)),
    ],
    "viability": [
        ("viability", "Auditoría de viabilidad vigente.", ("get_viability_audit",)),
    ],
    "regression_audit": [
        ("regression", "Auditoría de regresión vigente.", ("get_regression_audit",)),
    ],
    "calibration_audit": [
        ("calibration", "Auditoría de calibración vigente.", ("get_calibration_audit",)),
    ],
    "decision_history": [
        ("decision_history", "Resultados maduros de decisiones históricas.", ("get_decision_ledger",)),
        ("historical_edge", "Valor agregado histórico de las decisiones.", ("get_decision_value_added",)),
    ],
    "decision_lab": [
        ("plan_vs_hold", "Comparación PLAN vs HOLD para la cohorte solicitada.", ("compare_plan_vs_hold",)),
        ("decision_value_added", "DVA observado para la decisión o cohorte.", ("get_decision_value_added",)),
        ("counterfactuals", "Contrafactuales disponibles para la decisión.", ("get_decision_counterfactuals",)),
        ("replay_quality", "Calidad de reconstrucción point-in-time.", ("get_replay_evidence_quality",)),
        ("historical_comparables", "Episodios históricos comparables.", ("get_similar_historical_episodes",)),
    ],
    "meta_policy": [
        ("meta_policy_state", "Estado y reglas observadas de Economic Meta Policy.", ("get_meta_policy",)),
        ("current_decision", "Decisión base sobre la que Meta Policy está evaluando.", ("get_decision_evidence",)),
        ("historical_edge", "Evidencia histórica usada para exigir edge económico.", ("get_decision_value_added",)),
    ],
    "market_context": [
        ("macro_context", "Estado macro y de mercado vigente.", ("get_macro_context",)),
        ("portfolio_exposure", "Cómo ese contexto se traduce a exposición de la cartera.", ("get_macro_exposure",)),
    ],
    "system_status": [
        ("system_health", "Salud técnica de los componentes de Quantia.", ("get_system_status",)),
    ],
    "evidence_provenance": [
        ("previous_sources", "Fuentes auditadas de la respuesta anterior.", ("get_run_evidence_provenance",)),
    ],
}


class EvidencePlanner:
    """Translate a semantic task into explicit claims and evidence candidates."""

    def plan(
        self,
        *,
        task: TaskSpec,
        available_tools: set[str],
        required_tools: list[str],
    ) -> list[EvidenceClaim]:
        required = set(required_tools)
        claims: list[EvidenceClaim] = []
        for claim_id, description, tools in _CLAIMS.get(task.intent, []):
            candidates = list(tools)
            available = [name for name in candidates if name in available_tools]
            claims.append(EvidenceClaim(
                claim_id=claim_id,
                description=description,
                tools=candidates,
                available_tools=available,
                required=bool(required.intersection(candidates)),
            ))

        # Unknown/new intents still expose the deterministic requirements chosen
        # by ContextSelector, so observability never loses the reason a tool was
        # mandatory merely because the claim library has not been extended yet.
        covered = {tool for claim in claims for tool in claim.tools}
        for tool in required_tools:
            if tool in covered:
                continue
            claims.append(EvidenceClaim(
                claim_id=f"required_{tool}",
                description=f"Evidencia canónica requerida por la política de contexto: {tool}.",
                tools=[tool],
                available_tools=[tool] if tool in available_tools else [],
                required=True,
            ))
        return claims


__all__ = ["EvidencePlanner"]
