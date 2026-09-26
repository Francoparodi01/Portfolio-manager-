"""Run a fixed legacy analysis script with process-local DB write guards."""
import runpy
import sys
from pathlib import Path

from .read_only import install_process_guards


def main():
    allowed = {"scripts/run_analysis.py", "scripts/run_opportunity.py"}
    if len(sys.argv) < 2 or sys.argv[1] not in allowed:
        raise ValueError("unregistered analysis script")
    script = sys.argv[1]
    required = {"--no-persist", "--no-telegram"}
    if script == "scripts/run_analysis.py":
        required.add("--no-llm")
    if not required.issubset(sys.argv[2:]):
        raise ValueError("required no-write/no-send flags missing")
    install_process_guards()
    sys.argv = sys.argv[1:]
    root = Path(__file__).resolve().parents[2]
    runpy.run_path(str(root / script), run_name="__main__")


if __name__ == "__main__":
    main()
