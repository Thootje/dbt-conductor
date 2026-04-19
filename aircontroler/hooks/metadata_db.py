"""MetadataDBHook — Airflow metadata DB access for the aircontroler_model_runs state table.

FRESH-04: runtime reads and writes use `create_session` from `airflow.utils.session`
(same import path on Airflow 2.7+ and 3.x per RESEARCH Pitfall 1). This satisfies
the CLAUDE.md rule that workers must not open raw SQLAlchemy sessions from execute().

Security (T-3-01): all queries use SQLAlchemy ORM Core expressions against the
`model_runs_table` imported from aircontroler.db_manager — unique_id is bound as a
parameter, never formatted into a SQL string.
"""
from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session


class MetadataDBHook(BaseHook):
    """Hook for aircontroler state table access via Airflow's metadata DB session."""

    conn_name_attr = "aircontroler_conn_id"
    default_conn_name = "aircontroler_default"
    conn_type = "aircontroler"
    hook_name = "Aircontroler"

    def get_conn(self):
        """Not supported — MetadataDBHook uses create_session for all DB access."""
        raise NotImplementedError(
            "MetadataDBHook uses create_session (airflow.utils.session), not get_conn()."
        )

    def get_last_run(self, unique_id: str) -> datetime | None:
        """Return MAX(completed_at) for the given unique_id, or None if never recorded.

        FRESH-04 / T-3-01: uses parameterized ORM `sa.select(...).where(col == value)`.
        The unique_id is bound as a parameter; no string formatting is used.
        """
        from aircontroler.db_manager import model_runs_table
        with create_session() as session:
            result = session.execute(
                sa.select(sa.func.max(model_runs_table.c.completed_at))
                .where(model_runs_table.c.unique_id == unique_id)
            ).scalar()
        return result

    def record_run(self, unique_id: str, dag_id: str, run_id: str) -> None:
        """Insert a success row with completed_at=now(UTC) for this model run.

        D-04: append-only — never updates in place. D-05: writes all four non-id columns.
        """
        from aircontroler.db_manager import model_runs_table
        with create_session() as session:
            session.execute(
                model_runs_table.insert().values(
                    unique_id=unique_id,
                    dag_id=dag_id,
                    run_id=run_id,
                    completed_at=datetime.now(tz=timezone.utc),
                )
            )
