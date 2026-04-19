"""Integration-test-scoped conftest: wires Airflow to a tmp_path SQLite DB.

Pattern:
  - Set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN to sqlite:///<tmp_path>/airflow_test.db
    BEFORE airflow.settings is imported (via monkeypatch.setenv).
  - Reload airflow.settings and airflow.utils.session so create_session returns
    sessions bound to the test SQLite engine.
  - Apply the aircontroler Alembic migration to create the model_runs table.
  - Yield the sqlalchemy url for any assertion needs.
  - Teardown: tmp_path handles file removal; monkeypatch restores env vars.

Do NOT import airflow at module top level; that would fix the DB URL before the
env var is set. All airflow imports happen inside the fixture body.
"""
from __future__ import annotations

import importlib
import os

import pytest


def _migrations_dir() -> str:
    return os.path.join(
        os.path.dirname(__file__), "..", "..", "aircontroler", "migrations"
    )


@pytest.fixture
def sqlite_airflow_db(tmp_path, monkeypatch):
    """Point Airflow at a tmp_path SQLite DB and apply the aircontroler migration.

    Yields the sqlalchemy url string so tests can inspect the DB directly if needed.
    """
    db_path = tmp_path / "airflow_test.db"
    db_url = f"sqlite:///{db_path}"

    # Set env var BEFORE any Airflow import. monkeypatch guarantees teardown.
    monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", db_url)
    monkeypatch.setenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN", db_url)

    # Reload airflow.settings so the new URL takes effect for subsequent create_session calls.
    # Airflow 3.x stores Session in settings.Session (initially None); configure_vars() reads
    # AIRFLOW__DATABASE__SQL_ALCHEMY_CONN and configure_orm() builds the scoped_session factory.
    # A plain importlib.reload() does NOT call configure_vars/configure_orm — we must do so
    # explicitly after the reload to populate settings.Session.
    import airflow.settings as airflow_settings
    importlib.reload(airflow_settings)
    airflow_settings.configure_vars()
    airflow_settings.configure_orm()
    import airflow.utils.session as airflow_session
    importlib.reload(airflow_session)

    # Apply the aircontroler Alembic migration; creates aircontroler_model_runs + indexes.
    from alembic import command
    from alembic.config import Config
    cfg = Config()
    cfg.set_main_option("script_location", _migrations_dir())
    cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(cfg, "head")

    yield db_url
