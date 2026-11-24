"""Reuse verified raw downloads when rebuilding a historical archive with new code."""
import json
from pathlib import Path
import shutil

from src.thesis_desk.contracts import digest, dump


def reuse_downloads(source, target):
    source, target = source.resolve(), target.resolve()
    if source == target:
        raise ValueError("Archive reuse requires distinct directories")
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("code_changed_during_run"):
        raise ValueError("Cannot reuse an archive whose code changed during capture")
    artifacts = manifest["artifacts"]
    (target / "raw").mkdir(parents=True, exist_ok=True)
    (target / "receipts").mkdir(parents=True, exist_ok=True)
    count = 0
    for path in sorted((source / "receipts").glob("*.json")):
        if digest(path.read_bytes()) != artifacts[str(path.relative_to(source))]:
            raise ValueError("Reuse receipt integrity failure")
        receipt = json.loads(path.read_text(encoding="utf-8"))
        raw = Path(receipt["raw_path"]).resolve()
        if not raw.is_relative_to(source / "raw"):
            raise ValueError("Reuse raw path outside archive")
        if digest(raw.read_bytes()) != receipt["sha256"] or artifacts[str(raw.relative_to(source))] != receipt["sha256"]:
            raise ValueError("Reuse raw integrity failure")
        destination = target / "raw" / (receipt["sha256"] + ".bin")
        if destination.exists():
            if digest(destination.read_bytes()) != receipt["sha256"]:
                raise ValueError("Existing destination raw integrity failure")
        else:
            shutil.copyfile(raw, destination)
        receipt["raw_path"] = str(destination)
        destination_receipt = target / "receipts" / path.name
        if destination_receipt.exists():
            if json.loads(destination_receipt.read_text(encoding="utf-8")) != receipt:
                raise ValueError("Existing destination receipt differs")
        else:
            dump(destination_receipt, receipt)
        count += 1
    return count
