"""Integration tests for Alembic migration round-trip. Covers FRESH-06."""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


def _alembic_config(migrations_dir, db_url):
    """Build an alembic Config with script_location pointed at our migrations dir."""
    from alembic.config import Config
    cfg = Config()
    cfg.set_main_option("script_location", migrations_dir)
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _migrations_dir():
    return os.path.join(
        os.path.dirname(__file__), "..", "..", "aircontroler", "migrations"
    )


def test_migration_upgrade_creates_table(tmp_path):
    """FRESH-06: Alembic upgrade() creates aircontroler_model_runs with expected columns."""
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    insp = inspect(engine)
    assert "aircontroler_model_runs" in insp.get_table_names()
    cols = {c["name"] for c in insp.get_columns("aircontroler_model_runs")}
    assert cols == {"id", "unique_id", "dag_id", "run_id", "completed_at"}


def test_migration_upgrade_creates_indexes(tmp_path):
    """FRESH-06: Alembic upgrade() creates indexes on unique_id and completed_at."""
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    insp = inspect(engine)
    index_names = {i["name"] for i in insp.get_indexes("aircontroler_model_runs")}
    assert "ix_acr_unique_id" in index_names
    assert "ix_acr_completed_at" in index_names


def test_migration_downgrade_drops_table(tmp_path):
    """FRESH-06: Alembic downgrade() removes aircontroler_model_runs table."""
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")
    insp = inspect(engine)
    assert "aircontroler_model_runs" not in insp.get_table_names()


def test_version_table_name_is_not_default(tmp_path):
    """FRESH-06: Version tracking uses aircontroler_alembic_version (not default alembic_version).

    This protects against collision with Airflow's own alembic_version table when both
    run against the same DB.
    """
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert "aircontroler_alembic_version" in tables
    assert "alembic_version" not in tables


# ── Phase 4: LOCK-04 migration stubs ──────────────────────────────────────


@pytest.mark.integration
def test_migration_creates_lock_holders_table(tmp_path):
    """LOCK-04: Alembic upgrade head creates aircontroler_lock_holders with expected columns."""
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test_lock.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    insp = inspect(engine)
    assert "aircontroler_lock_holders" in insp.get_table_names()
    cols = {c["name"] for c in insp.get_columns("aircontroler_lock_holders")}
    assert cols == {"unique_id", "dag_id", "run_id", "locked_at"}


@pytest.mark.integration
def test_migration_downgrade_removes_lock_holders_table(tmp_path):
    """LOCK-04: Alembic downgrade base removes aircontroler_lock_holders table."""
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test_lock_down.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")
    insp = inspect(engine)
    assert "aircontroler_lock_holders" not in insp.get_table_names()
    assert "aircontroler_model_runs" not in insp.get_table_names()


@pytest.mark.integration
def test_migration_downgrade_to_0001_removes_only_lock_holders(tmp_path):
    """LOCK-04: Downgrade to 0001 removes lock_holders but preserves model_runs."""
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test_partial_down.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0001")
    insp = inspect(engine)
    assert "aircontroler_lock_holders" not in insp.get_table_names()
    assert "aircontroler_model_runs" in insp.get_table_names()
