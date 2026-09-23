"""Quantia post-agentic/JEV training and evaluation harness.

This package is intentionally downstream from the agentic loop and JEV.
It never places orders, mutates deterministic risk controls, or promotes a
model to production by itself.
"""

from .contracts import PromotionGateResult, TrainingExample, TrainingManifest
from .dataset import build_training_dataset, temporal_split
from .gates import PromotionGateConfig, evaluate_promotion_gate
from .preflight import PreflightResult, run_preflight

__all__ = [
    "PreflightResult",
    "PromotionGateConfig",
    "PromotionGateResult",
    "TrainingExample",
    "TrainingManifest",
    "build_training_dataset",
    "evaluate_promotion_gate",
    "run_preflight",
    "temporal_split",
]
