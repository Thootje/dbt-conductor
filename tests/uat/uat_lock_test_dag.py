"""UAT DAG: Phase 4 cross-DAG conflict locking — concurrent skip behavior.

Drop this file into your composer-dev DAGs folder. Set two Airflow Variables:
  AIRCONTROLER_UAT_PROJECT_DIR   — absolute path to a dbt project inside the
                                    composer-dev container (e.g. the fixture
                                    project mounted at /home/airflow/gcs/data/...)
  AIRCONTROLER_UAT_PROFILES_PATH — path to the directory containing profiles.yml

UAT scenario 1 — concurrent skip:
  Trigger this DAG twice in quick succession from the Airflow UI.
  Expected: one run completes (dbt executes), the other raises AirflowSkipException
  with a message like "conflict: model.X already locked by dag_Y/run_Z".

UAT scenario 2 — pg_locks inspection:
  While this DAG is in the "running" state, trigger uat_pg_locks_check_dag.
  Expected: pg_locks shows a row with locktype='advisory' whose objid matches
  adler32(unique_id) & 0x7FFFFFFF.  After both runs finish the row is gone.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable

with DAG(
    dag_id="aircontroler_uat_lock_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    max_active_runs=2,
    catchup=False,
    tags=["aircontroler", "uat"],
) as dag:
    # Import here so the DAG can still be parsed even if aircontroler isn't
    # installed yet — Airflow logs an ImportError at task-run time, not parse time.
    try:
        from aircontroler.operator import DbtRunModelOperator

        DbtRunModelOperator(
            task_id="stg_a__conflict_test",
            unique_id="model.fixture_project.stg_a",
            project_dir=Variable.get("AIRCONTROLER_UAT_PROJECT_DIR", default_var="/opt/airflow/dags/fixture_project"),
            profiles_path=Variable.get("AIRCONTROLER_UAT_PROFILES_PATH", default_var="/opt/airflow/dags/fixture_project"),
            conflict_policy="skip",
        )
    except ImportError:
        from airflow.operators.python import PythonOperator

        def _missing_package(**_):
            raise RuntimeError(
                "aircontroler is not installed in this Airflow environment. "
                "Install it with: pip install airflow-provider-aircontroler"
            )

        PythonOperator(task_id="install_aircontroler_first", python_callable=_missing_package)
