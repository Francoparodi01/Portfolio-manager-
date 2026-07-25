from src.core.output_perf import percentile, summarize_measurements
from scripts.benchmark_outputs import render_table


def test_percentile_uses_bounded_sorted_values():
    assert percentile([10, 30, 20], 0) == 10
    assert percentile([10, 30, 20], 50) == 20
    assert percentile([10, 30, 20], 100) == 30


def test_summarize_measurements_reports_size_latency_and_queries():
    summary = summarize_measurements(
        name="status",
        kind="renderer",
        durations_ms=[10.0, 20.0, 30.0],
        sizes_bytes=[100, 200, 300],
        line_counts=[5, 6, 7],
        statuses=["ok"],
        query_count=1,
    )

    assert summary["median_ms"] == 20.0
    assert summary["bytes_avg"] == 200.0
    assert summary["lines_avg"] == 6.0
    assert summary["query_count"] == 1


def test_benchmark_render_table_formats_status_values():
    table = render_table(
        [
            summarize_measurements(
                name="help",
                kind="renderer",
                durations_ms=[1.0],
                sizes_bytes=[42],
                line_counts=[2],
                statuses=["ok"],
                query_count=0,
            )
        ]
    )

    assert "| help | renderer | 1 |" in table
    assert "| ok |" in table
