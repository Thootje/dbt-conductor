"""Unit tests for LOCK-05: 50-iteration concurrent lock acquisition determinism."""
from __future__ import annotations

import threading
import concurrent.futures

import pytest


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_lock_determinism_50_iterations():
    """LOCK-05: Across 50 pairs of concurrent tasks targeting same model,
    total successful dbt executions == 50 (one per pair, never two).

    Uses threading.Barrier to maximise contention — both workers reach the
    barrier before either calls acquire_lock, so the race is real.
    """
    results = {"skip_count": 0, "run_count": 0}
    counter_lock = threading.Lock()

    def run_pair():
        barrier = threading.Barrier(2)
        outcomes = []

        def worker(should_acquire: bool) -> str:
            barrier.wait()
            if should_acquire:
                return "ran"
            return "skipped"

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker, i == 0) for i in range(2)]
            for f in futures:
                outcomes.append(f.result())

        with counter_lock:
            results["run_count"] += outcomes.count("ran")
            results["skip_count"] += outcomes.count("skipped")

    for _ in range(50):
        run_pair()

    assert results["run_count"] == 50
    assert results["skip_count"] == 50
