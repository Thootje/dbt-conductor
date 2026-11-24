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

import inspect

import sqlalchemy as sa
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session

_CREATE_SESSION_SUPPORTS_SCOPED = "scoped" in inspect.signature(create_session).parameters


def _unscoped_session():
    """create_session(scoped=False) on Airflow 3+, plain create_session() on 2.x."""
    if _CREATE_SESSION_SUPPORTS_SCOPED:
        return create_session(scoped=False)
    return create_session()


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

    def get_last_run(self, unique_id: str, session=None) -> datetime | None:
        """Return MAX(completed_at) for the given unique_id, or None if never recorded.

        D-08: when session is provided, uses it directly (no new create_session opened).
        When session=None (default), opens own create_session — backward-compatible.
        FRESH-04 / T-3-01: unique_id is bound as a parameter; no string formatting.
        """
        from aircontroler.db_manager import model_runs_table
        if session is not None:
            return session.execute(
                sa.select(sa.func.max(model_runs_table.c.completed_at))
                .where(model_runs_table.c.unique_id == unique_id)
            ).scalar()
        with create_session() as session:
            return session.execute(
                sa.select(sa.func.max(model_runs_table.c.completed_at))
                .where(model_runs_table.c.unique_id == unique_id)
            ).scalar()

    def record_run(self, unique_id: str, dag_id: str, run_id: str, session=None) -> None:
        """Insert a success row with completed_at=now(UTC) for this model run.

        D-08: when session is provided, uses it directly (caller commits).
        When session=None (default), opens own create_session — backward-compatible.
        D-04: append-only. D-05: writes all four non-id columns.
        """
        from aircontroler.db_manager import model_runs_table
        stmt = model_runs_table.insert().values(
            unique_id=unique_id,
            dag_id=dag_id,
            run_id=run_id,
            completed_at=datetime.now(tz=timezone.utc),
        )
        if session is not None:
            session.execute(stmt)
            return
        with create_session() as session:
            session.execute(stmt)

    def acquire_lock(self, unique_id: str, session) -> bool:
        """Attempt to acquire a Postgres advisory transaction lock for unique_id.

        D-03: uses pg_try_advisory_xact_lock (non-blocking). Returns True if lock
        acquired, False if already held by another session.
        D-04 (hash): lock_id = zlib.adler32(unique_id.encode()) & 0x7FFFFFFF.
        Stays within Postgres int4 range; stable across processes (not PYTHONHASHSEED).
        D-13: on non-Postgres dialect, logs WARNING and returns True (no-op).
        T-4-01: unique_id is only used as input to zlib.adler32() — the resulting
        integer is the bind param. The raw string never appears in SQL.

        Args:
            unique_id: dbt model unique_id (e.g. 'model.proj.stg_a').
            session: An open SQLAlchemy session from the caller's create_session() block.
                The advisory lock is transaction-scoped — held until session commits/rolls back.
        """
        import zlib
        dialect = session.get_bind().dialect.name
        if dialect != "postgresql":
            self.log.warning(
                "acquire_lock: dialect=%s is not PostgreSQL — "
                "conflict_policy has no effect (lock always acquired)",
                dialect,
            )
            return True
        lock_id = zlib.adler32(unique_id.encode()) & 0x7FFFFFFF
        result = session.execute(
            sa.text("SELECT pg_try_advisory_xact_lock(:h)"),
            {"h": lock_id},
        ).scalar()
        return bool(result)

    def upsert_lock_holder(self, unique_id: str, dag_id: str, run_id: str) -> None:
        """Commit identity row for this task into aircontroler_lock_holders.

        D-11 (micro-transaction): uses create_session(scoped=False) so this commits
        independently of any active scoped session on the current thread. The row is
        visible to all other tasks immediately after this method returns.
        T-4-02: dag_id, run_id inserted via pg_insert().values() — parameterized, not
        string-formatted SQL.

        On PostgreSQL: INSERT ... ON CONFLICT (unique_id) DO UPDATE (upsert semantics,
        handles stale rows from crashes — D-10).
        On SQLite (dev): falls back to DELETE + INSERT (no ON CONFLICT DO UPDATE syntax
        available via SQLAlchemy Core across dialects).
        """
        from aircontroler.db_manager import lock_holders_table
        locked_at = datetime.now(tz=timezone.utc)
        with _unscoped_session() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                stmt = (
                    pg_insert(lock_holders_table)
                    .values(
                        unique_id=unique_id,
                        dag_id=dag_id,
                        run_id=run_id,
                        locked_at=locked_at,
                    )
                    .on_conflict_do_update(
                        index_elements=["unique_id"],
                        set_=dict(dag_id=dag_id, run_id=run_id, locked_at=locked_at),
                    )
                )
                session.execute(stmt)
            else:
                # SQLite dev fallback: delete existing row then insert fresh
                session.execute(
                    sa.delete(lock_holders_table).where(
                        lock_holders_table.c.unique_id == unique_id
                    )
                )
                session.execute(
                    lock_holders_table.insert().values(
                        unique_id=unique_id,
                        dag_id=dag_id,
                        run_id=run_id,
                        locked_at=locked_at,
                    )
                )

    def delete_lock_holder(self, unique_id: str) -> None:
        """Remove identity row from aircontroler_lock_holders for unique_id.

        D-12 / T-4-04: called from execute() finally block so cleanup happens on all
        exit paths (success, skip, conflict skip, conflict fail, dbt failure).
        Uses create_session(scoped=False) — independent micro-transaction that commits
        even if the main session has already been rolled back.
        """
        from aircontroler.db_manager import lock_holders_table
        with _unscoped_session() as session:
            session.execute(
                sa.delete(lock_holders_table).where(
                    lock_holders_table.c.unique_id == unique_id
                )
            )

    def get_lock_holder(self, unique_id: str, session) -> "sa.engine.Row | None":
        """Return the lock_holders row for unique_id, or None if no row exists.

        D-11: uses the caller's open session so the read is consistent within the
        same transaction that just failed to acquire the advisory lock. Row has
        .dag_id and .run_id attributes used in conflict error messages.
        """
        from aircontroler.db_manager import lock_holders_table
        return session.execute(
            sa.select(lock_holders_table).where(
                lock_holders_table.c.unique_id == unique_id
            )
        ).fetchone()
