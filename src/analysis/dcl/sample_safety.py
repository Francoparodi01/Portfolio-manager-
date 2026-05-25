from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from src.analysis.dcl.outcome_loader import EnrichedDecision


@dataclass(frozen=True)
class SampleSafety:
    n_total: int
    n_auditable: int
    n_by_regime: dict[str, int]
    n_by_ticker: dict[str, int]
    min_n_for_threshold_opt: int = 30
    min_n_for_layer_attribution: int = 40
    min_n_for_regime_split: int = 20
    warnings: list[str] = field(default_factory=list)
    can_run_threshold_opt: bool = False
    can_run_layer_attribution: bool = False
    can_run_regime_analysis: bool = False

    @classmethod
    def evaluate(
        cls,
        decisions: list[EnrichedDecision],
        *,
        min_n_for_threshold_opt: int = 30,
        min_n_for_layer_attribution: int = 40,
        min_n_for_regime_split: int = 20,
    ) -> "SampleSafety":
        auditable = [d for d in decisions if d.is_auditable]
        n_total = len(decisions)
        n_auditable = len(auditable)
        n_by_regime = dict(Counter(d.market_regime or "unknown" for d in auditable))
        n_by_ticker = dict(Counter(d.ticker for d in auditable if d.ticker))
        decision_types = {d.decision_type for d in auditable if d.decision_type}

        warnings: list[str] = []
        if n_total == 0:
            warnings.append("No hay decisiones en decision_log para el periodo.")
        if n_auditable < 20:
            warnings.append(
                f"Muestra insuficiente: {n_auditable} decisiones auditables. "
                "Solo corresponde lectura descriptiva."
            )
        elif n_auditable < 50:
            warnings.append(
                f"Muestra chica: {n_auditable} decisiones auditables. "
                "Toda recomendacion debe mostrar intervalos de confianza."
            )

        if len(n_by_ticker) < 3:
            warnings.append("Diversidad insuficiente: se necesitan al menos 3 tickers.")
        if len(decision_types) < 2:
            warnings.append("Diversidad insuficiente: se necesitan al menos 2 tipos de decision.")

        has_diversity = len(n_by_ticker) >= 3 and len(decision_types) >= 2
        can_threshold = n_auditable >= min_n_for_threshold_opt and has_diversity
        can_layer = n_auditable >= min_n_for_layer_attribution and has_diversity
        can_regime = any(n >= min_n_for_regime_split for n in n_by_regime.values())

        return cls(
            n_total=n_total,
            n_auditable=n_auditable,
            n_by_regime=n_by_regime,
            n_by_ticker=n_by_ticker,
            min_n_for_threshold_opt=min_n_for_threshold_opt,
            min_n_for_layer_attribution=min_n_for_layer_attribution,
            min_n_for_regime_split=min_n_for_regime_split,
            warnings=warnings,
            can_run_threshold_opt=can_threshold,
            can_run_layer_attribution=can_layer,
            can_run_regime_analysis=can_regime,
        )
