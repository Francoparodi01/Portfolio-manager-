"""Render the reproducible controlled E2 diagnostic used in documentation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.validate_contextual_g1 import _fixture_candles
from src.analysis.contextual_market import build_contextual_snapshot, render_contextual_diagnostic
from src.collector.cocos_history import candles_to_frame


def build_example(output_json: Path, output_png: Path) -> dict:
    candles, cutoff = _fixture_candles()
    asset = candles_to_frame(candles)
    asset.attrs["cutoff"] = cutoff
    steps = np.arange(len(asset), dtype=float)
    asset_close = 100 + steps * .08 + np.sin(steps / 5) * 3
    asset["Open"] = asset_close - .3
    asset["High"] = asset_close + 1.0
    asset["Low"] = asset_close - 1.0
    asset["Close"] = asset_close
    benchmark = asset.copy()
    benchmark.attrs = dict(asset.attrs)
    benchmark_close = 100 + steps * .08 + np.sin(steps / 7)
    benchmark["Open"] = benchmark_close - .3
    benchmark["High"] = benchmark_close + .8
    benchmark["Low"] = benchmark_close - .8
    benchmark["Close"] = benchmark_close
    benchmark["ProviderSymbol"] = "SPY-CONTROLLED-PROXY"
    benchmark.attrs["series_identity"] = {
        **asset.attrs["series_identity"],
        "ticker": "SPY", "instrument_id": "BYMA:CEDEAR:SPY:ARS",
    }
    benchmark.attrs["provider_symbols"] = ["SPY-CONTROLLED-PROXY"]
    snapshot = build_contextual_snapshot(
        asset, cutoff=cutoff, signal_action="HOLD", benchmarks={"SPY": benchmark},
    )
    render_contextual_diagnostic(asset, snapshot, output_png)
    result = {
        "evidence_kind": "CONTROLLED_GENERATED_FIXTURE",
        "not_market_observation": True,
        "definition": "Reproducible diagnostic rendering; values are not claimed as historical market data.",
        "snapshot": snapshot.to_dict(),
        "diagnostic": str(output_png),
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-png", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build_example(args.output_json, args.output_png), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
