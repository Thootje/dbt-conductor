"""Integration tests — full pipeline with real dbt-duckdb. Covers CORE-01 through CORE-09.

These tests require dbt-duckdb to be installed:
    pip install -e ".[test]"

No cloud credentials required (dbt-duckdb uses in-memory DuckDB).

Run: pytest tests/integration/ -x -m integration
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "dbt_project"


def _dbt_available() -> bool:
    """Check if dbt is available on PATH."""
    return shutil.which("dbt") is not None


@pytest.fixture(scope="module")
def generated_manifest(tmp_path_factory):
    """Generate a real manifest.json from the fixture dbt project using dbt parse.

    Uses module scope so dbt parse runs only once per test session.
    The target directory is placed in tmp_path to avoid collisions and gitignore concerns.
    """
    if not _dbt_available():
        pytest.skip("dbt not found on PATH — install via pip install -e '.[test]'")
    tmp_target = tmp_path_factory.mktemp("dbt_target")
    # Run dbt parse: generates target/manifest.json in the project dir.
    # We override target-path to use tmp_path so tests don't write to the fixture dir.
    result = subprocess.run(
        [
            "dbt", "parse",
            "--project-dir", str(FIXTURE_DIR),
            "--profiles-dir", str(FIXTURE_DIR),
            "--target-path", str(tmp_target),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"dbt parse failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")
    manifest_path = tmp_target / "manifest.json"
    assert manifest_path.exists(), f"manifest.json not found at {manifest_path}"
    return str(manifest_path)


def test_full_pipeline_wires_correctly(generated_manifest, tmp_path):
    """CORE-01..09: Parse manifest, resolve selector, wire DAG, verify topology.

    Covers:
    - CORE-01: manifest loaded from file path
    - CORE-03: SelectorResolver invokes real dbt ls against fixture project
    - CORE-04: one task per model; task_id == unique_id (dotted format)
    - CORE-05: edges match dbt lineage; entry/exit tasks correct
    """
    if not _dbt_available():
        pytest.skip("dbt not available")
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    from aircontroler.operator import DbtRunModelOperator
    with DAG("integration_test_dag", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=generated_manifest,
            dbt_command="dbt build --select +mart",
            project_dir=str(FIXTURE_DIR),
            profiles_path=str(FIXTURE_DIR),
        )
    # CORE-04: 5 models, each a DbtRunModelOperator
    assert len(result["tasks"]) == 5, (
        f"Expected 5 tasks, got {len(result['tasks'])}: {list(result['tasks'].keys())}"
    )
    for uid, task in result["tasks"].items():
        assert isinstance(task, DbtRunModelOperator)
        assert task.task_id == uid, f"task_id {task.task_id!r} != unique_id {uid!r}"
    # CORE-05: entry and exit classification
    assert len(result["entry_tasks"]) == 2, (
        f"Expected 2 entry tasks, got {len(result['entry_tasks'])}"
    )
    assert len(result["exit_tasks"]) == 1, (
        f"Expected 1 exit task, got {len(result['exit_tasks'])}"
    )
    entry_names = {t.task_id for t in result["entry_tasks"]}
    assert any("stg_a" in uid for uid in entry_names), f"stg_a not in entry_tasks: {entry_names}"
    assert any("stg_b" in uid for uid in entry_names), f"stg_b not in entry_tasks: {entry_names}"
    exit_names = {t.task_id for t in result["exit_tasks"]}
    assert any("mart" in uid for uid in exit_names), f"mart not in exit_tasks: {exit_names}"
    # CORE-05: mart is downstream of int_a and int_b
    mart_uid = next(uid for uid in result["tasks"] if "mart" in uid)
    mart_task = result["tasks"][mart_uid]
    upstream_ids = {t.task_id for t in mart_task.upstream_list}
    assert any("int_a" in uid for uid in upstream_ids), (
        f"int_a not upstream of mart: {upstream_ids}"
    )
    assert any("int_b" in uid for uid in upstream_ids), (
        f"int_b not upstream of mart: {upstream_ids}"
    )


def test_model_execution_via_subprocess(generated_manifest):
    """CORE-06: DbtRunModelOperator.execute() runs real dbt build via subprocess.

    Runs dbt build --select stg_a against the in-memory DuckDB fixture.
    Verifies no exception is raised (exit code == 0).

    Covers:
    - CORE-06: dbt build subprocess executed with correct args
    - CORE-07: profiles_path passed as --profiles-dir
    - CORE-08: operator importable and instantiable (compat shim works)
    """
    if not _dbt_available():
        pytest.skip("dbt not available")
    from aircontroler.operator import DbtRunModelOperator
    # Find the stg_a unique_id from the manifest
    import json
    with open(generated_manifest) as fh:
        manifest_data = json.load(fh)
    stg_a_uid = next(
        uid for uid in manifest_data["nodes"]
        if "stg_a" in uid and manifest_data["nodes"][uid]["resource_type"] == "model"
    )
    op = DbtRunModelOperator(
        task_id=stg_a_uid,
        unique_id=stg_a_uid,
        project_dir=str(FIXTURE_DIR),
        profiles_path=str(FIXTURE_DIR),
    )
    # Should not raise — dbt build stg_a against DuckDB :memory: succeeds
    op.execute({})
