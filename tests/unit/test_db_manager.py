"""Unit tests for aircontroler.db_manager — FRESH-06."""
from __future__ import annotations


def test_version_table_not_alembic_version():
    """FRESH-06: version_table_name must NOT be 'alembic_version' (RunDBManager._validate check)."""
    from aircontroler.db_manager import AircontrolerDBManager
    assert AircontrolerDBManager.version_table_name != "alembic_version"
    assert AircontrolerDBManager.version_table_name == "aircontroler_alembic_version"


def test_revision_heads_map():
    """FRESH-06 / LOCK-04: revision_heads_map maps package version to migration revision id."""
    from aircontroler.db_manager import AircontrolerDBManager
    assert AircontrolerDBManager.revision_heads_map == {"1.0.0": "0001", "1.1.0": "0002"}


def test_model_runs_table_schema():
    """FRESH-06: model_runs_table has exactly the columns required by D-05."""
    from aircontroler.db_manager import model_runs_table
    col_names = {c.name for c in model_runs_table.columns}
    assert col_names == {"id", "unique_id", "dag_id", "run_id", "completed_at"}


def test_model_runs_table_indexes():
    """FRESH-06: model_runs_table has indexes on unique_id and completed_at (D-06 query path)."""
    from aircontroler.db_manager import model_runs_table
    index_names = {idx.name for idx in model_runs_table.indexes}
    assert "ix_acr_unique_id" in index_names
    assert "ix_acr_completed_at" in index_names


def test_supports_table_dropping():
    """FRESH-06: supports_table_dropping=True enables `airflow db drop` teardown."""
    from aircontroler.db_manager import AircontrolerDBManager
    assert AircontrolerDBManager.supports_table_dropping is True
