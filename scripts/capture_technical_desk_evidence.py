"""Capture only the five preregistered public market series into new local files."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.technical_desk.evidence_capture import canonical, content_hash, fetch_public_evidence


async def run(selection: Path, output: Path, bars: int):
    payload = json.loads(selection.read_text(encoding="utf-8"))
    tickers = payload["selected_tickers"]
    if len(tickers) != 5 or tickers != sorted(set(tickers)):
        raise ValueError("EXACTLY_FIVE_PREREGISTERED_TICKERS_REQUIRED")
    output.mkdir(parents=True, exist_ok=False)
    manifest = dict(schema="technical-desk-evidence-manifest-v1",
                    selection_path=str(selection.resolve()),
                    selection_sha256=hashlib.sha256(selection.read_bytes()).hexdigest(),
                    owner_token=payload["owner_token"], selected_tickers=tickers,
                    started_at=datetime.now(timezone.utc).isoformat(), captures=[])
    for ticker in tickers:
        try:
            capture = await fetch_public_evidence(ticker, bars=bars)
            digest = content_hash(capture)
            filename = f"{ticker}-{digest[:16]}.json"
            with (output / filename).open("xb") as handle:
                handle.write(canonical(capture) + b"\n")
            item = dict(ticker=ticker, status="CAPTURED", file=filename, content_hash=digest,
                        rows=len(capture["bars"]), latest_bar=capture["bars"][-1]["bar_start"],
                        latest_close_status=capture["bars"][-1]["close_status"],
                        available_at=capture["available_at"])
        except Exception as exc:
            # Error type only; do not leak transport headers or future credentials.
            item = dict(ticker=ticker, status="SOURCE_BLOCKED", error_type=type(exc).__name__)
        manifest["captures"].append(item)
        print(json.dumps(item), flush=True)
    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["status"] = "CAPTURED" if all(x["status"] == "CAPTURED" for x in manifest["captures"]) else "PARTIAL"
    source = Path(__file__).resolve().parents[1] / "src/technical_desk/evidence_capture.py"
    manifest["capture_source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    with (output / "manifest.json").open("xb") as handle:
        handle.write(canonical(manifest) + b"\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--bars", type=int, default=400)
    args = parser.parse_args()
    result = asyncio.run(run(args.selection, args.output, args.bars))
    raise SystemExit(0 if result["status"] == "CAPTURED" else 2)


if __name__ == "__main__":
    main()
