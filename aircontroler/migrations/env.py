"""Alembic env.py for the aircontroler provider migrations.

Critical constraint: RunDBManager._validate() in Airflow 3.x AST-parses this file
and raises AirflowException if any configure call lacks a non-default
version_table kwarg. Both branches below include the required kwarg.

Do NOT import airflow.settings at module top level — the alembic CLI imports this
file without Airflow initialized. BaseDBManager.get_alembic_config() injects the
active connection via context.config.attributes["connection"].
"""
from __future__ import annotations

from alembic import context


def run_migrations_online():
    conn = context.config.attributes.get("connection", None)
    if conn is None:
        # Standalone alembic CLI / integration tests: engine built from sqlalchemy.url
        from sqlalchemy import engine_from_config
        cfg = context.config.get_section(context.config.config_ini_section, {})
        connectable = engine_from_config(cfg, prefix="sqlalchemy.")
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=None,
                version_table="aircontroler_alembic_version",
                render_as_batch=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    else:
        # BaseDBManager path (Airflow 3.x initdb): session-scoped connection injected
        context.configure(
            connection=conn,
            target_metadata=None,
            version_table="aircontroler_alembic_version",
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise RuntimeError("Offline mode not supported for aircontroler migrations")
else:
    run_migrations_online()
