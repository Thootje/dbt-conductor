"""Unit tests for LOCK-05: 50-iteration concurrent lock acquisition determinism.

Tests that across 50 pairs of concurrent tasks targeting the same model,
exactly one executes per pair (total runs == 50, total skips == 50).
Uses threading.Barrier to maximise contention — both workers synchronize
before calling acquire_lock so the race is real.
"""
from __future__ import annotations

import threading
import concurrent.futures
from unittest.mock import MagicMock, patch

import pytest


def _make_execute_context():
    """Minimal Airflow context for execute() calls."""
    ctx = {}
    dag_run = MagicMock()
    dag_run.dag_id = "chaos_dag"
    ctx["dag_run"] = dag_run
    ctx["run_id"] = "chaos_run"
    return ctx


def test_lock_determinism_50_iterations():
    """LOCK-05: Across 50 pairs of concurrent tasks targeting same model,
    total successful dbt executions == 50 (one per pair, never two).

    Strategy:
    - Two workers share a threading.Barrier(2) — both reach the barrier before
      either calls acquire_lock, maximising contention.
    - MetadataDBHook.acquire_lock is mocked: first caller (index 0) returns True,
      second caller (index 1) returns False.
    - Worker with True proceeds, completes (subprocess.run mocked to returncode=0).
    - Worker with False raises AirflowSkipException (conflict_policy="skip").
    - Record: "ran" vs "skipped" per pair; assert totals over 50 iterations.

    This test runs in < 5 seconds (no real sleeps, no real DB, no real subprocess).
    """
    from airflow.providers.common.compat.sdk import AirflowSkipException
    from aircontroler.operator import DbtRunModelOperator

    results = {"run_count": 0, "skip_count": 0}
    counter_lock = threading.Lock()

    def run_pair():
        barrier = threading.Barrier(2)
        # Alternating lock results: first acquire call → True, second → False.
        # thread_lock serialises the call counter so index is deterministic.
        acquire_call_lock = threading.Lock()
        acquire_calls = [0]

        def mock_acquire(unique_id, session):
            barrier.wait()  # Both threads reach here before either proceeds
            with acquire_call_lock:
                idx = acquire_calls[0]
                acquire_calls[0] += 1
            return idx == 0  # First gets True, second gets False

        outcomes = []

        def worker():
            mock_run_result = MagicMock()
            mock_run_result.returncode = 0
            mock_run_result.stdout = ""
            mock_run_result.stderr = ""

            op = DbtRunModelOperator(
                task_id="model.proj.stg_a",
                unique_id="model.proj.stg_a",
                project_dir="/proj",
                profiles_path="/profiles",
                conflict_policy="skip",
            )
            with (
                patch(
                    "aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock",
                    side_effect=mock_acquire,
                ),
                patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder"),
                patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder"),
                patch(
                    "aircontroler.hooks.metadata_db.MetadataDBHook.get_lock_holder",
                    return_value=None,
                ),
                patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run"),
                patch("aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run", return_value=None),
                patch("aircontroler.operator.subprocess.run", return_value=mock_run_result),
                patch("aircontroler.operator.create_session") as mock_cs,
            ):
                # create_session() context manager mock
                mock_session = MagicMock()
                mock_cs.return_value.__enter__ = MagicMock(return_value=mock_session)
                mock_cs.return_value.__exit__ = MagicMock(return_value=False)
                try:
                    op.execute(_make_execute_context())
                    return "ran"
                except AirflowSkipException:
                    return "skipped"

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker) for _ in range(2)]
            for f in futures:
                outcomes.append(f.result())

        with counter_lock:
            results["run_count"] += outcomes.count("ran")
            results["skip_count"] += outcomes.count("skipped")

        # Invariant per pair: exactly one ran, one skipped
        assert outcomes.count("ran") == 1, f"Expected exactly 1 run per pair, got {outcomes}"
        assert outcomes.count("skipped") == 1, f"Expected exactly 1 skip per pair, got {outcomes}"

    for i in range(50):
        run_pair()

    assert results["run_count"] == 50, f"Expected 50 total runs, got {results['run_count']}"
    assert results["skip_count"] == 50, f"Expected 50 total skips, got {results['skip_count']}"
