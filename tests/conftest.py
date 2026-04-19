"""Shared pytest fixtures for aircontroler test suite."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def manifest_v11_data() -> dict:
    """Load the hand-crafted v11 manifest fixture (5-node diamond)."""
    return json.loads((FIXTURES_DIR / "manifest_v11.json").read_text())


@pytest.fixture
def manifest_v12_data() -> dict:
    """Load the hand-crafted v12 manifest fixture (same topology, v12 schema)."""
    return json.loads((FIXTURES_DIR / "manifest_v12.json").read_text())


@pytest.fixture
def run_results_v6_data() -> dict:
    """Load the hand-crafted v6 run_results fixture (one stg_a result entry)."""
    return json.loads((FIXTURES_DIR / "run_results_v6.json").read_text())


@pytest.fixture
def manifest_with_meta_data() -> dict:
    """Load v12 manifest fixture with meta.aircontroler on the mart node (EXEC-01)."""
    return json.loads((FIXTURES_DIR / "manifest_v12_with_meta.json").read_text())


@pytest.fixture
def mock_dag():
    """Return a minimal Airflow DAG mock suitable for unit tests.

    Note: Actual DAG instantiation requires airflow to be installed.
    Tests that need a real DAG must import from airflow.models directly.
    """
    dag = MagicMock()
    dag.dag_id = "test_dag"
    dag.task_ids = set()
    dag.task_dict = {}
    return dag


@pytest.fixture
def mock_context():
    """Return a minimal Airflow task execution context mock."""
    return {
        "run_id": "test_run_id",
        "task_instance": MagicMock(),
    }


@pytest.fixture
def manifest_with_freshness_data() -> dict:
    """Load v12 manifest fixture with meta.aircontroler.freshness_hours (FRESH-01)."""
    return json.loads((FIXTURES_DIR / "manifest_v12_with_freshness.json").read_text())
