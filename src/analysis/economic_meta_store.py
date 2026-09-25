"""Append-only persistence for Economic Meta Policy v1 shadow records."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from src.analysis.economic_meta_policy import MetaDecisionRecord, assert_shadow_only


DEFAULT_SHADOW_PATH = Path("outputs/economic_meta_policy/shadow_decisions.jsonl")


class EconomicMetaShadowStore:
    """Minimal append-only JSONL store isolated from production execution state."""

    def __init__(self, path: str | Path = DEFAULT_SHADOW_PATH) -> None:
        self.path = Path(path)

    def append(self, record: MetaDecisionRecord) -> None:
        assert_shadow_only(record)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            record.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        # One write keeps readers from observing a JSON fragment between the
        # payload and newline while the Telegram bot and watcher share the file.
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    def append_many(self, records: Iterable[MetaDecisionRecord]) -> int:
        count = 0
        for record in records:
            self.append(record)
            count += 1
        return count

    def read_all(self) -> list[dict]:
        if not self.path.exists():
            return []
        rows: list[dict] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if text:
                    rows.append(json.loads(text))
        return rows


__all__ = ["DEFAULT_SHADOW_PATH", "EconomicMetaShadowStore"]
