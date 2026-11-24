"""UAT DAG: inspect Postgres advisory locks held by aircontroler.

Trigger this DAG from the Airflow UI WHILE uat_lock_test_dag has a run in
the "running" state. The task logs will show whether an advisory lock row
exists in pg_locks, along with the expected objid for verification.

Expected output while a lock is held:
  Advisory lock HELD — 1 row(s)
  pid=<N>  classid=0  objid=<hash>  granted=True
  Expected objid for model.fixture_project.stg_a: <hash>
  MATCH: advisory lock objid matches adler32 hash of unique_id

Expected output after all runs complete:
  No advisory locks currently held for aircontroler tasks.
"""
from __future__ import annotations

import zlib
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_UNIQUE_IDS_TO_CHECK = [
    "model.fixture_project.stg_a",
]


def check_advisory_locks(**_):
    import sqlalchemy as sa
    from airflow.utils.session import create_session

    expected = {
        uid: zlib.adler32(uid.encode()) & 0x7FFFFFFF
        for uid in _UNIQUE_IDS_TO_CHECK
    }

    with create_session() as session:
        try:
            rows = session.execute(
                sa.text(
                    "SELECT pid, locktype, classid, objid, granted "
                    "FROM pg_locks WHERE locktype = 'advisory'"
                )
            ).fetchall()
        except Exception as exc:
            print(f"pg_locks query failed (not on Postgres?): {exc}")
            return

    held_objids = {row.objid for row in rows}

    if rows:
        print(f"Advisory lock HELD — {len(rows)} row(s)")
        for row in rows:
            print(f"  pid={row.pid}  classid={row.classid}  objid={row.objid}  granted={row.granted}")
    else:
        print("No advisory locks currently held for aircontroler tasks.")

    print()
    for uid, objid in expected.items():
        print(f"Expected objid for {uid}: {objid}")
        if objid in held_objids:
            print(f"  MATCH: advisory lock objid matches adler32 hash — lock is held")
        else:
            print(f"  NOT FOUND: lock not currently held (task may have finished already)")


with DAG(
    dag_id="aircontroler_uat_pg_locks_check",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aircontroler", "uat"],
) as dag:
    PythonOperator(
        task_id="check_pg_locks",
        python_callable=check_advisory_locks,
    )
