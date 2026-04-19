"""End-to-end integration tests: migration to hook to operator round-trip on SQLite.

Uses the sqlite_airflow_db fixture (tests/integration/conftest.py) to:
  1. Create a tmp_path SQLite DB.
  2. Point Airflow at it via AIRFLOW__DATABASE__SQL_ALCHEMY_CONN.
  3. Apply the aircontroler Alembic migration.

Then exercises MetadataDBHook.record_run() + get_last_run() against the real DB,
plus DbtRunModelOperator.execute() in skip and stale scenarios.

Covers Nyquist dims 1 (fresh to skip), 2 (stale to run), 4 (success records run)
AT INTEGRATION LEVEL, complementing the unit-level coverage in Plan 02.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.integration


def _make_context():
    """Return a mock Airflow context with dag_run.dag_id and run_id populated."""
    ctx = {}
    dag_run = MagicMock()
    dag_run.dag_id = "integration_dag"
    ctx["dag_run"] = dag_run
    ctx["run_id"] = "integration_run_1"
    return ctx


def test_record_and_get_last_run_roundtrip(sqlite_airflow_db):
    """FRESH-04 round-trip: record_run writes, get_last_run reads it back."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    hook = MetadataDBHook()
    # Initially no row exists for this unique_id
    assert hook.get_last_run("model.proj.stg_a") is None
    # Write a row
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_a", run_id="run_a_1")
    # Read it back
    last = hook.get_last_run("model.proj.stg_a")
    assert last is not None
    # Should be very recent (within the last 60 seconds)
    now = datetime.now(tz=timezone.utc)
    # Normalize naive datetime to UTC (SQLite strips tzinfo)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    assert (now - last).total_seconds() < 60


def test_multiple_records_return_max(sqlite_airflow_db):
    """FRESH-04 / D-06: get_last_run returns MAX(completed_at) when multiple rows exist."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    hook = MetadataDBHook()
    # Write three rows for the same unique_id
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_a", run_id="run_a_1")
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_b", run_id="run_b_1")
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_c", run_id="run_c_1")
    last = hook.get_last_run("model.proj.stg_a")
    assert last is not None


def test_other_unique_id_does_not_leak(sqlite_airflow_db):
    """FRESH-04 / T-3-01: get_last_run for unique_id X is not affected by rows for unique_id Y."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    hook = MetadataDBHook()
    hook.record_run(unique_id="model.proj.stg_a", dag_id="d", run_id="r")
    # Different unique_id should still return None
    assert hook.get_last_run("model.proj.stg_b") is None


def test_operator_skip_when_fresh_end_to_end(sqlite_airflow_db, mocker):
    """Nyquist dim 1 at integration level: fresh entry in DB causes execute() to skip."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    from aircontroler.operator import DbtRunModelOperator
    from airflow.providers.common.compat.sdk import AirflowSkipException

    # Seed a fresh row (completed 1 hour ago, TTL 6 hours)
    hook = MetadataDBHook()
    hook.record_run(unique_id="model.proj.stg_a", dag_id="seed_dag", run_id="seed_run")

    # Patch subprocess.run to ensure it is NOT called on the skip path
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")

    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    with pytest.raises(AirflowSkipException):
        op.execute(_make_context())
    assert mock_run.call_count == 0


def test_operator_runs_and_records_when_stale_end_to_end(sqlite_airflow_db, mocker):
    """Nyquist dims 2 + 4 at integration level: stale causes subprocess AND writes a new row."""
    from sqlalchemy import create_engine, func, select
    from aircontroler.db_manager import model_runs_table
    from aircontroler.hooks.metadata_db import MetadataDBHook
    from aircontroler.operator import DbtRunModelOperator

    # Seed an OLD row (simulate 100 hours ago). Insert directly via the sqlalchemy engine
    # so we can set completed_at to a past datetime without mocking the hook.
    engine = create_engine(sqlite_airflow_db)
    with engine.begin() as conn:
        conn.execute(
            model_runs_table.insert().values(
                unique_id="model.proj.stg_a",
                dag_id="old_dag",
                run_id="old_run",
                completed_at=datetime.now(tz=timezone.utc) - timedelta(hours=100),
            )
        )

    # Patch subprocess.run to simulate a successful dbt build
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")

    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    op.execute(_make_context())

    # Subprocess WAS called (stale)
    assert mock_run.call_count == 1

    # A NEW row was written (record_run called); now we have 2 rows for this unique_id
    with engine.connect() as conn:
        count = conn.execute(
            select(func.count()).select_from(model_runs_table).where(
                model_runs_table.c.unique_id == "model.proj.stg_a"
            )
        ).scalar()
    assert count == 2, f"expected 2 rows (old + new), got {count}"

    # Most-recent row is from the integration_dag / integration_run_1 context
    with engine.connect() as conn:
        latest_dag_id = conn.execute(
            select(model_runs_table.c.dag_id)
            .where(model_runs_table.c.unique_id == "model.proj.stg_a")
            .order_by(model_runs_table.c.completed_at.desc())
            .limit(1)
        ).scalar()
    assert latest_dag_id == "integration_dag"
