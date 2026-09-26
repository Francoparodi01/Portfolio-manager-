"""Read-only projection of objects already computed by the analysis pipeline."""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import math
from pathlib import Path


def _safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def decision_evidence(*, results, execution_plan, macro_snap, portfolio_snapshot,
                      total_ars, cash_ars, analysis_run_id):
    from src.analysis import execution_planner as planner
    signals = []
    for result in results:
        signals.append({"ticker": result.ticker, "final_score": result.final_score,
                        "decision": result.decision, "layers": [asdict(layer) for layer in result.layers],
                        "technical_regime": result.technical_regime, "trend_score": result.trend_score,
                        "warnings": result.warnings,
                        "technical_candle_source_mode": result.technical_candle_source_mode})
    return _safe({"schema_version": "agent-decision-evidence-v1", "analysis_run_id": analysis_run_id,
                  "evaluated_at": datetime.now(timezone.utc).isoformat(),
                  "snapshot_as_of": (portfolio_snapshot or {}).get("scraped_at"),
                  "snapshot_stale_reason": (portfolio_snapshot or {}).get("_stale_reason"),
                  "total_value_ars": total_ars, "cash_ars": cash_ars,
                  "signals": signals, "plan": asdict(execution_plan) if execution_plan is not None else None,
                  "macro": macro_snap.to_dict(),
                  "buy_policy": {"negative_block": planner.SCORE_BUY_BLOCK_NEG, "minimum": planner.SCORE_BUY_MIN},
                  "planner_sha256": hashlib.sha256(Path(planner.__file__).read_bytes()).hexdigest(),
                  "scope": "CURRENT_PROPOSED_PLAN_NOT_EXECUTION_NOT_RETURN"})
