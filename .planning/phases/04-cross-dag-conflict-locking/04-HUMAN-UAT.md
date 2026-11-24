---
status: partial
phase: 04-cross-dag-conflict-locking
source: [04-VERIFICATION.md]
started: 2026-04-19T11:12:00Z
updated: 2026-04-20
---

## Setup (composer-dev)

**Files:** `tests/uat/uat_lock_test_dag.py` and `tests/uat/uat_pg_locks_check_dag.py`

1. Copy both DAG files into your composer-dev DAGs folder:
   ```
   cp tests/uat/uat_lock_test_dag.py   <composer-dev-dags-dir>/
   cp tests/uat/uat_pg_locks_check_dag.py <composer-dev-dags-dir>/
   ```

2. Install aircontroler in the composer-dev environment:
   ```
   composer-dev run-airflow-command pip install -e /path/to/aircontroler
   ```
   Or add it to your composer-dev `requirements.txt`.

3. Set Airflow Variables (Admin → Variables in the UI):
   - `AIRCONTROLER_UAT_PROJECT_DIR` — path to the dbt project **inside** the container  
     (e.g. if you mount the repo: `/home/airflow/gcs/dags/fixture_project`)
   - `AIRCONTROLER_UAT_PROFILES_PATH` — path to the folder containing `profiles.yml`

   Quick way via CLI:
   ```
   composer-dev run-airflow-command variables set AIRCONTROLER_UAT_PROJECT_DIR /opt/airflow/dags/fixture_project
   composer-dev run-airflow-command variables set AIRCONTROLER_UAT_PROFILES_PATH /opt/airflow/dags/fixture_project
   ```

---

## Tests

### 1. Live concurrent DAG behavior (Postgres)

**Steps:**
1. Open Airflow UI → `aircontroler_uat_lock_test`
2. Trigger a run → immediately trigger a second run before the first task starts executing
3. Watch both runs in the Grid view

**Expected:**
- One run: task completes green, dbt subprocess ran
- Second run: task fails with `AirflowSkipException` — logs show `"conflict: model.fixture_project.stg_a already locked by aircontroler_uat_lock_test/<run_id>"`
- `aircontroler_lock_holders` table has zero rows after both runs finish

**Result:** [pending]

---

### 2. pg_locks advisory lock inspection (Postgres)

**Steps:**
1. Trigger `aircontroler_uat_lock_test` (single run is enough)
2. While the task is in **running** state, trigger `aircontroler_uat_pg_locks_check`
3. Check the `check_pg_locks` task logs

**Expected output while lock is held:**
```
Advisory lock HELD — 1 row(s)
  pid=<N>  classid=0  objid=<hash>  granted=True
Expected objid for model.fixture_project.stg_a: <hash>
  MATCH: advisory lock objid matches adler32 hash
```

**Expected output after run completes:**
```
No advisory locks currently held for aircontroler tasks.
```

**Result:** [pending]

---

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
