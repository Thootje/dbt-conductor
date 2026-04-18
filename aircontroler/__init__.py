"""aircontroler — Map each dbt model to a native Airflow task."""
from __future__ import annotations

__version__ = "0.1.0"


def get_provider_info() -> dict:
    """Return Airflow provider metadata.

    Required by the [project.entry-points."apache_airflow_provider"] entry-point.
    Keys must conform to Airflow's provider_info.schema.json.
    """
    return {
        "package-name": "airflow-provider-aircontroler",
        "name": "aircontroler",
        "description": (
            "Map each dbt model to a native Airflow task with "
            "accurate upstream/downstream wiring."
        ),
        "versions": [__version__],
    }
