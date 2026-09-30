"""Version metadata attached to analysis/audit payloads without destructive migrations."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from typing import Any

OPTIMIZER_VERSION = "optimizer-v1-budget-safe"
PLANNER_VERSION = "execution-planner-v2-target-contract"
SYNTHESIS_VERSION = "synthesis-v1-legacy-thresholds"
RISK_POLICY_VERSION = "risk-policy-v1"
CALIBRATION_VERSION = "calibrated-optimizer-v2-shadow-2"


def config_hash(config: dict) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def analysis_config_snapshot(extra: dict | None = None) -> dict[str, Any]:
    """Return only decision-relevant, non-secret configuration.

    This intentionally avoids serializing the global application config because
    it can contain API tokens and credentials. Imports are lazy to avoid cycles.
    """
    from src.analysis import execution_planner as planner
    from src.analysis import optimizer as optimizer
    from src.analysis import synthesis as synthesis

    snapshot: dict[str, Any] = {
        "synthesis": {
            "layer_weights": dict(synthesis.LAYER_WEIGHTS),
            "buy_threshold": 0.40,
            "accumulate_threshold": 0.15,
            "reduce_threshold": -0.15,
            "sell_threshold": -0.40,
        },
        "optimizer": {
            "w_min": optimizer.W_MIN,
            "w_max": optimizer.W_MAX,
            "rebalance_threshold": optimizer.REBALANCE_THRESH,
            "risk_free_annual": optimizer.RF_ANNUAL,
            "risk_aversion": optimizer.RISK_AVERSION,
            "tau": optimizer.TAU,
            "min_history_days": optimizer.MIN_HISTORY_DAYS,
            "vix_cautious": optimizer.VIX_CAUTIOUS,
            "vix_blocked": optimizer.VIX_BLOCKED,
            "dd_cautious": optimizer.DD_CAUTIOUS,
            "dd_blocked": optimizer.DD_BLOCKED,
        },
        "planner": {
            "min_weight_delta": planner.MIN_WEIGHT_DELTA,
            "min_trade_ars": planner.MIN_TRADE_ARS,
            "fee_pct": planner.FEE_PCT,
            "slippage_pct": planner.SLIPPAGE_PCT,
            "sell_full_threshold": planner.SELL_FULL_THRESH,
            "score_buy_strong": planner.SCORE_BUY_STRONG,
            "score_buy_min": planner.SCORE_BUY_MIN,
            "score_buy_block_neg": planner.SCORE_BUY_BLOCK_NEG,
            "score_neutral_high": planner.SCORE_NEU_HIGH,
            "score_neutral_low": planner.SCORE_NEU_LOW,
            "score_neg_weak_low": planner.SCORE_NEG_DEBIL_LOW,
            "max_weight_concentration": planner.MAX_WEIGHT_CONC,
            "max_weight_hard_concentration": planner.MAX_WEIGHT_HARD_CONC,
        },
    }
    if extra:
        snapshot["runtime"] = extra
    return snapshot


def code_version() -> str:
    for name in ("GIT_SHA", "CODE_VERSION", "GITHUB_SHA"):
        value = str(os.getenv(name, "") or "").strip()
        if value:
            return value
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=1.0,
        ).strip()
    except Exception:
        return "unknown"


def version_manifest(config: dict | None = None) -> dict:
    effective_config = analysis_config_snapshot() if config is None else config
    return {
        "optimizer_version": OPTIMIZER_VERSION,
        "planner_version": PLANNER_VERSION,
        "synthesis_version": SYNTHESIS_VERSION,
        "risk_policy_version": RISK_POLICY_VERSION,
        "calibration_version": CALIBRATION_VERSION,
        "config_hash": config_hash(effective_config),
        "code_version": code_version(),
    }
