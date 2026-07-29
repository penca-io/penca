"""Performance tests for lifecycle operations (persist, snapshot, compact).

Measures the throughput of moving data between storage tiers.  Prints
markdown summary tables for docs/performance.md.

Run via ``just perf-test``.
"""

from __future__ import annotations

import time

import pytest

from .performance_helpers import (
    ROW_COUNT,
    PerfResult,
    insert_and_commit,
    make_client,
    setup_performance_schema,
)


def _persist(client, context: dict[str, str]):
    client.persist(
        schema_uuid=context["schema_uuid"],
        table_uuid=context["table_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def _snapshot(client, context: dict[str, str]):
    client.snapshot(
        schema_uuid=context["schema_uuid"],
        table_uuid=context["table_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def _compact_persist_segments(client, context: dict[str, str]):
    client.compact_persist_segments(
        catalog_uuid=context["catalog_uuid"],
        branch_uuid=context["main_branch_uuid"],
        table_uuid=context["table_uuid"],
    )


class TestLifecyclePerformance:
    """Throughput of lifecycle operations."""

    def test_persist(self, perf_recorder):
        """Time to persist 100k rows from hot to cold."""
        client = make_client()
        context = setup_performance_schema(client)
        insert_and_commit(client, context, offset=0, count=ROW_COUNT)

        start = time.perf_counter()
        _persist(client, context)
        elapsed = time.perf_counter() - start

        perf_recorder.record(
            PerfResult("persist", "hot_to_cold", ROW_COUNT, elapsed, unit="persist")
        )

    def test_snapshot(self, perf_recorder):
        """Time to create a snapshot after persist."""
        client = make_client()
        context = setup_performance_schema(client)
        insert_and_commit(client, context, offset=0, count=ROW_COUNT)
        _persist(client, context)

        start = time.perf_counter()
        _snapshot(client, context)
        elapsed = time.perf_counter() - start

        perf_recorder.record(
            PerfResult("snapshot", "after_persist", ROW_COUNT, elapsed, unit="snapshot")
        )

    @pytest.mark.parametrize("segment_count", [2, 5, 10])
    def test_compact_persist_segments(self, segment_count: int, perf_recorder):
        """Compact log segments created by multiple persists."""
        rows_per_segment = ROW_COUNT // segment_count
        client = make_client()
        context = setup_performance_schema(client)

        for segment_index in range(segment_count):
            offset = segment_index * rows_per_segment
            insert_and_commit(client, context, offset=offset, count=rows_per_segment)
            _persist(client, context)

        start = time.perf_counter()
        _compact_persist_segments(client, context)
        elapsed = time.perf_counter() - start

        perf_recorder.record(
            PerfResult(
                "compact_persist_segments",
                f"{segment_count}_segments",
                ROW_COUNT,
                elapsed,
                unit="compact",
            )
        )

    def test_full_lifecycle_pipeline(self, perf_recorder):
        """End-to-end: write -> persist -> snapshot."""
        client = make_client()
        context = setup_performance_schema(client)

        pipeline_results: dict[str, float] = {}

        start = time.perf_counter()
        insert_and_commit(client, context, offset=0, count=ROW_COUNT)
        pipeline_results["write"] = time.perf_counter() - start

        start = time.perf_counter()
        _persist(client, context)
        pipeline_results["persist"] = time.perf_counter() - start

        start = time.perf_counter()
        _snapshot(client, context)
        pipeline_results["snapshot"] = time.perf_counter() - start

        for operation, elapsed in pipeline_results.items():
            perf_recorder.record(
                PerfResult(
                    f"pipeline_{operation}",
                    "end_to_end",
                    ROW_COUNT,
                    elapsed,
                    unit=operation,
                )
            )
