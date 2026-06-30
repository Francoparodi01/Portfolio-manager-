"""Print the latest Black-Litterman OptimizationError diagnostic."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_PATH = Path("logs/optimizer_diagnostics_latest.json")


def _format_matrix(payload: dict) -> str:
    universe = payload["universe"]
    covariance = payload["covariance"]
    width = max(10, max(len(ticker) for ticker in universe) + 2)
    lines = ["".ljust(width) + "".join(t.rjust(width) for t in universe)]
    for row in universe:
        values = "".join(
            f"{float(covariance[row][col]):.6f}".rjust(width)
            for col in universe
        )
        lines.append(row.ljust(width) + values)
    return "\n".join(lines)


def render(payload: dict) -> str:
    required = {
        "universe", "scores", "lower_bounds", "upper_bounds", "covariance",
        "sum_lower_bounds", "sum_upper_bounds", "cash_floor",
    }
    missing = sorted(required - payload.keys())
    if missing:
        raise ValueError(f"Diagnostic missing keys: {', '.join(missing)}")

    lines = [
        "BLACK-LITTERMAN DIAGNOSTIC",
        f"generated_at: {payload.get('generated_at')}",
        f"error: {payload.get('error_type')}: {payload.get('error')}",
        f"universe: {', '.join(payload['universe'])}",
        f"sum(lower_bounds): {float(payload['sum_lower_bounds']):.4f}",
        f"sum(upper_bounds): {float(payload['sum_upper_bounds']):.4f}",
        f"cash_floor: {float(payload['cash_floor']):.4f}",
        "",
        "ticker score lower upper",
    ]
    for ticker in payload["universe"]:
        lines.append(
            f"{ticker} "
            f"{float(payload['scores'][ticker]):+.4f} "
            f"{float(payload['lower_bounds'][ticker]):.4f} "
            f"{float(payload['upper_bounds'][ticker]):.4f}"
        )
    lines.extend(["", "covariance:", _format_matrix(payload)])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print universe, scores, bounds and covariance from the latest BL failure."
    )
    parser.add_argument("path", nargs="?", type=Path, default=DEFAULT_PATH)
    parser.add_argument("--json", action="store_true", help="Print raw JSON")
    args = parser.parse_args()

    if not args.path.exists():
        parser.error(f"diagnostic not found: {args.path}")
    payload = json.loads(args.path.read_text(encoding="utf-8"))
    print(json.dumps(payload, indent=2, ensure_ascii=False) if args.json else render(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
