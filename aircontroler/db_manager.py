"""AircontrolerDBManager — BaseDBManager subclass registered via db-managers key (FRESH-06).

Airflow 3.x: registered via get_provider_info()["db-managers"]; `airflow db migrate`
instantiates it with a session and calls initdb() which runs the Alembic migration.

Airflow 2.x: BaseDBManager does not exist. Module still importable (fallback to object).
Table creation on Airflow 2.x is performed via `aircontroler db init` CLI (Plan 03).

Also defines SQLAlchemy Table objects used by MetadataDBHook for parameterized reads
and writes — SQL injection mitigation for T-3-01 and T-4-02:
  - aircontroler_model_runs (migration 0001): freshness state tracking
  - aircontroler_lock_holders (migration 0002): cross-DAG conflict lock identity rows
"""
from __future__ import annotations

import os

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
)

try:
    from airflow.utils.db_manager import BaseDBManager
    _AIRFLOW3 = True
except ImportError:
    # Airflow 2.x: module does not exist. Class defined but not registered as db-manager.
    BaseDBManager = object  # type: ignore[assignment,misc]
    _AIRFLOW3 = False

metadata = MetaData()

model_runs_table = Table(
    "aircontroler_model_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("unique_id", String(255), nullable=False),
    Column("dag_id", String(250), nullable=False),
    Column("run_id", String(250), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=False),
    Index("ix_acr_unique_id", "unique_id"),
    Index("ix_acr_completed_at", "completed_at"),
)

lock_holders_table = Table(
    "aircontroler_lock_holders",
    metadata,
    Column("unique_id", String(255), primary_key=True),
    Column("dag_id", String(250), nullable=False),
    Column("run_id", String(250), nullable=False),
    Column("locked_at", DateTime(timezone=True), nullable=False),
)


class AircontrolerDBManager(BaseDBManager):  # type: ignore[misc,valid-type]
    """Registered via db-managers key in get_provider_info() on Airflow 3.x.

    Constructor inherited from BaseDBManager: __init__(self, session).
    initdb() inherited — runs Alembic upgrade to head using the revision chain:
      0001_add_model_runs_table.py  (1.0.0 → freshness state tracking)
      0002_add_lock_holders_table.py (1.1.0 → cross-DAG conflict lock identity rows)
    """

    metadata = metadata
    migration_dir = os.path.join(os.path.dirname(__file__), "migrations")
    alembic_file = os.path.join(os.path.dirname(__file__), "migrations", "alembic.ini")
    version_table_name = "aircontroler_alembic_version"  # MUST NOT be 'alembic_version' (RunDBManager._validate)
    supports_table_dropping = True
    revision_heads_map = {"1.0.0": "0001", "1.1.0": "0002"}
