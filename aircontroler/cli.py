"""aircontroler command-line interface.

Primary use case: `aircontroler db init` creates the aircontroler_model_runs table
on Airflow 2.x deployments where BaseDBManager (Airflow 3.x only) is not available
to run migrations via `airflow db migrate`.

All Airflow imports are deferred inside command functions so `aircontroler --help`
works without a fully-initialized Airflow context.
"""
from __future__ import annotations

import click


@click.group()
def cli():
    """aircontroler management commands."""


@cli.group("db")
def db_group():
    """Database management subcommands."""


@db_group.command("init")
def db_init():
    """Create aircontroler_model_runs table in the Airflow metadata DB (Airflow 2.x).

    On Airflow 3.x, prefer `airflow db migrate` — the provider's AircontrolerDBManager
    is registered via the db-managers key in get_provider_info() and runs the same
    Alembic migration automatically.
    """
    from airflow.utils.session import create_session
    from aircontroler.db_manager import AircontrolerDBManager
    with create_session() as session:
        manager = AircontrolerDBManager(session)
        manager.initdb()
    click.echo("aircontroler_model_runs table created.")


if __name__ == "__main__":  # pragma: no cover
    cli()
