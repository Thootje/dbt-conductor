"""Integration tests — full pipeline with real dbt-duckdb. Covers CORE-01 through CORE-09.

These tests require:
  - pip install -e ".[test]" (installs dbt-duckdb and pytest)
  - No cloud credentials needed (dbt-duckdb uses in-memory DuckDB)

Run: pytest tests/integration/ -x -m integration
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "dbt_project"

pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="implementation pending — Wave 4 (after factory.py complete)")
def test_full_pipeline_wires_correctly(tmp_path):
    """CORE-01..09: Parse manifest, resolve selector, wire DAG, verify topology.

    Steps:
    1. Run 'dbt parse' on the fixture dbt-duckdb project to generate manifest.json
    2. Call build_dbt_dag() with manifest_path, 'dbt build --select +mart', project_dir, profiles_path
    3. Assert result['tasks'] has 5 entries
    4. Assert result['entry_tasks'] has 2 tasks (stg_a, stg_b)
    5. Assert result['exit_tasks'] has 1 task (mart)
    6. Assert mart task has int_a and int_b in its upstream_list
    7. Assert each task_id equals the model's unique_id (dotted format works)
    """
    pytest.skip("implementation pending — Wave 4 (after factory.py complete)")


@pytest.mark.skip(reason="implementation pending — Wave 4")
def test_model_execution_via_subprocess(tmp_path):
    """CORE-06: DbtRunModelOperator.execute() runs real dbt build via subprocess.

    Steps:
    1. Generate manifest via 'dbt parse' on fixture project
    2. Instantiate DbtRunModelOperator for 'model.fixture_project.stg_a'
    3. Call operator.execute({})
    4. Assert no exception raised (returncode == 0)
    5. Assert DuckDB :memory: table was created
    """
    pytest.skip("implementation pending — Wave 4")
