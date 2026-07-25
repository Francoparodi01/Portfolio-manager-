from __future__ import annotations

import statistics
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, Sequence


@dataclass(frozen=True)
class StageTiming:
    name: str
    elapsed_ms: float
    metadata: dict[str, object] = field(default_factory=dict)


@contextmanager
def stage_timer(name: str, sink: list[StageTiming], **metadata: object) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        sink.append(
            StageTiming(
                name=name,
                elapsed_ms=(time.perf_counter() - started) * 1000.0,
                metadata=dict(metadata),
            )
        )


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def summarize_measurements(
    *,
    name: str,
    kind: str,
    durations_ms: Sequence[float],
    sizes_bytes: Sequence[int] | None = None,
    line_counts: Sequence[int] | None = None,
    statuses: Sequence[str | int] | None = None,
    query_count: int | None = None,
) -> dict[str, object]:
    durations = [float(value) for value in durations_ms]
    sizes = [int(value) for value in (sizes_bytes or [])]
    lines = [int(value) for value in (line_counts or [])]
    return {
        "name": name,
        "kind": kind,
        "runs": len(durations),
        "status": sorted({str(value) for value in statuses or []}),
        "avg_ms": round(statistics.mean(durations), 2) if durations else 0.0,
        "median_ms": round(statistics.median(durations), 2) if durations else 0.0,
        "p95_ms": round(percentile(durations, 95), 2) if durations else 0.0,
        "max_ms": round(max(durations), 2) if durations else 0.0,
        "bytes_avg": round(statistics.mean(sizes), 1) if sizes else 0.0,
        "lines_avg": round(statistics.mean(lines), 1) if lines else 0.0,
        "query_count": query_count,
    }
