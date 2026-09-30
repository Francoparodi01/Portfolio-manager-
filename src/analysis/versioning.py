"""Version metadata attached to analysis/audit payloads without destructive migrations."""
from __future__ import annotations
import hashlib, json, os

OPTIMIZER_VERSION = "optimizer-v1-budget-safe"
PLANNER_VERSION = "execution-planner-v2-target-contract"
SYNTHESIS_VERSION = "synthesis-v1-legacy-thresholds"
RISK_POLICY_VERSION = "risk-policy-v1"
CALIBRATION_VERSION = "calibrated-optimizer-v2-shadow-1"

def config_hash(config: dict) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

def version_manifest(config: dict | None = None) -> dict:
    return {
        "optimizer_version": OPTIMIZER_VERSION,
        "planner_version": PLANNER_VERSION,
        "synthesis_version": SYNTHESIS_VERSION,
        "risk_policy_version": RISK_POLICY_VERSION,
        "calibration_version": CALIBRATION_VERSION,
        "config_hash": config_hash(config or {}),
        "code_version": os.getenv("GIT_SHA") or os.getenv("CODE_VERSION") or "unknown",
    }
