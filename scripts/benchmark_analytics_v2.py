"""Opt-in synthetic benchmark, never a wall-clock CI gate or live evidence."""
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from time import perf_counter

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.analysis.analytics_v2.metrics import cohort_metrics
from src.analysis.analytics_v2.models import AnalyticsPolicy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=250000)
    args = parser.parse_args()
    start = datetime(2000, 1, 1, tzinfo=timezone.utc)
    policy = AnalyticsPolicy(experiment_id="SYNTHETIC-BENCHMARK", evaluated_as_of=datetime(2040, 1, 1, tzinfo=timezone.utc))
    rng = np.random.default_rng(1729)
    values = rng.normal(.002, .03, args.rows)
    rows = []
    for i, value in enumerate(values):
        at = start + timedelta(days=i // 50)
        rows.append({"account_id": "SYNTHETIC", "source_module": "CORE", "cohort": "BOT", "horizon_days": 5,
                     "cost_scenario": "RESEARCH_BASE", "cost_bps": 150, "outcome_status": "MATURE",
                     "episode_id": str(i), "instrument_id": str(i % 50), "ticker": str(i % 50),
                     "original_instrument_id": None, "entry_at": at.isoformat(),
                     "maturity_at": (at + timedelta(days=5)).isoformat(), "decision_as_of": at.isoformat(),
                     "recommendation_count": 1, "net_return": float(value), "net_alpha": float(value-.001),
                     "gross_return": float(value+.015), "gross_alpha": float(value+.014), "reference_notional": 100.,
                     "score": float(i % 10)})
    begun = perf_counter()
    metrics, _, outliers, _ = cohort_metrics(rows, policy, inference_enabled=False)
    print(json.dumps({"label": "SYNTHETIC DESCRIPTIVE BENCHMARK, inference excluded", "rows": args.rows,
                      "seconds": perf_counter()-begun, "output_contributions": len(outliers),
                      "n_episodes": metrics[0]["n_episodes"], "wall_clock_is_ci_gate": False}))


if __name__ == "__main__":
    main()
