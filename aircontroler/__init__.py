"""aircontroler — Map each dbt model to a native Airflow task."""
from __future__ import annotations

__version__ = "0.1.0"


def get_provider_info() -> dict:
    """Return Airflow provider metadata.

    Required by the [project.entry-points."apache_airflow_provider"] entry-point.
    Keys must conform to Airflow's provider_info.schema.json.

    db-managers (FRESH-06): on Airflow 3.x, `airflow db migrate` discovers this list,
    instantiates each class with a session, and calls .initdb() — which runs the
    Alembic migration in aircontroler/migrations/. On Airflow 2.x, this key is
    ignored by Airflow; users must run `aircontroler db init` (see cli.py).
    """
    return {
        "package-name": "airflow-provider-aircontroler",
        "name": "aircontroler",
        "description": (
            "Map each dbt model to a native Airflow task with "
            "accurate upstream/downstream wiring."
        ),
        "versions": [__version__],
        "db-managers": ["aircontroler.db_manager.AircontrolerDBManager"],
    }
