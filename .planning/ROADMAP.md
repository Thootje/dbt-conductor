# Roadmap: aircontroler

**Created:** 2026-04-17
**Granularity:** standard
**Core Value:** A dbt DAG where every model is a first-class Airflow task, with cross-DAG freshness policies and conflict resolution ensuring no model runs more times than necessary.

## Phases

- [x] **Phase 1: Walking Skeleton** - User can render a dbt project as a per-model Airflow DAG running on Composer + BigQuery
- [x] **Phase 2: Execution Hardening** - User gets production-grade per-task overrides, structured run results, and parse-time safety guards
- [x] **Phase 3: Freshness Policies** - User can define time-based TTLs that skip already-fresh models across DAGs (D1 differentiator)
- [ ] **Phase 4: Cross-DAG Conflict Locking** - User can configure skip/fail conflict policies that prevent concurrent DAGs from double-running the same model (D2 differentiator)
- [ ] **Phase 5: Cloud Run + Distribution** - User can deploy aircontroler on Cloud Run executor with per-model GCP service accounts

## Milestone 1: v1.0

### Phase 1: Walking Skeleton
**Goal:** User can render a dbt project as an Airflow DAG where every model is a separate task with dbt-accurate upstream/downstream wiring, installable from PyPI on Airflow 2.10+ or 3.x.
**Depends on:** Nothing (first phase)
**Requirements:** CORE-01, CORE-02, CORE-03, CORE-04, CORE-05, CORE-06, CORE-07, CORE-08, CORE-09
**Success Criteria** (what must be TRUE):
  1. User pip-installs `airflow-provider-aircontroler` and sees the provider registered in Airflow's Providers list on both 2.10+ and 3.x.
  2. User passes `manifest_path=...` (or opts into compile-on-parse) plus a `dbt build --select ...` command and the resulting DAG contains one Airflow task per resolved dbt model, each `task_id` equal to the model's `unique_id`.
  3. User opens Airflow's Graph view and sees the dbt lineage graph (task edges exactly mirror upstream/downstream relationships from the manifest).
  4. A triggered DAG run executes each model task as a `dbt build --select <model>` subprocess using credentials from a user-supplied `profiles.yml`, with a configurable `dbt_executable_path`.
**Plans:** 5 plans

Plans:
- [ ] 01-01-PLAN.md — Wave 0: pyproject.toml, test stubs, JSON fixtures
- [ ] 01-02-PLAN.md — Wave 1: manifest.py, selector.py, __init__.py, hooks stub
- [ ] 01-03-PLAN.md — Wave 2: operator.py (DbtRunModelOperator)
- [ ] 01-04-PLAN.md — Wave 3: factory.py (build_dbt_dag)
- [ ] 01-05-PLAN.md — Wave 4: dbt-duckdb fixture project, integration tests

### Phase 2: Execution Hardening
**Goal:** User gets production-grade operator ergonomics — per-model overrides, visibility into dbt execution results, and a parse-time guard rail that prevents runaway DAGs.
**Depends on:** Phase 1
**Requirements:** EXEC-01, EXEC-02, EXEC-03, EXEC-04
**Success Criteria** (what must be TRUE):
  1. User sets `meta.aircontroler.retries`, `retry_delay`, `pool`, and `sla` in `schema.yml` and the resulting Airflow tasks honor those values (verified by task config inspection and a forced-failure retry test).
  2. After each model task runs, the task log contains structured timing, row counts, and adapter response parsed from `run_results.json`.
  3. When a user's selector resolves to more than the configured cardinality limit (default 800), DAG parsing fails hard at parse time with an actionable error — no silent runaway.
**Plans:** 4 plans

Plans:
- [x] 02-01-PLAN.md — Wave 0: test fixtures (run_results_v6.json, manifest_v12_with_meta.json) + conftest fixtures
- [x] 02-02-PLAN.md — Wave 1: cardinality guard (EXEC-03) + max_active_tasks (EXEC-04) in factory.py
- [x] 02-03-PLAN.md — Wave 2: per-model meta overrides (EXEC-01) — operator constructor expansion + factory meta extraction loop
- [x] 02-04-PLAN.md — Wave 3: run_results.json parsing (EXEC-02) — operator _log_run_results + integration test

### Phase 3: Freshness Policies
**Goal:** User can define time-based freshness TTLs per model so that tasks skip execution when the model was already built recently in any DAG — the first of two headline differentiators over Cosmos.
**Depends on:** Phase 2
**Requirements:** FRESH-01, FRESH-03, FRESH-04, FRESH-06 (FRESH-02 and FRESH-05 deferred per D-11)
**Success Criteria** (what must be TRUE):
  1. User sets `meta.aircontroler.freshness_hours: N` in `schema.yml` (or overrides per-DAG in Python) and subsequent task runs within the TTL window are skipped without invoking dbt.
  2. After a successful model run, the `aircontroler_model_runs` state row is written via a dedicated hook (no direct SQLAlchemy from worker code) and is readable across DAGs.
  3. When two DAGs declare different TTLs for the same model, the longest TTL wins — verified by a cross-DAG test where DAG_A (1h) and DAG_B (6h) both observe the same skip window.
  4. On Airflow 3.x the `aircontroler_model_runs` table is created via `BaseDBManager`; on Airflow 2.x it is created via `aircontroler db init` — both paths ship an Alembic migration.
**Plans:** 4 plans

Plans:
- [x] 03-01-PLAN.md — Wave 0: db_manager.py + Alembic migration + test fixtures & stubs (FRESH-04, FRESH-06)
- [x] 03-02-PLAN.md — Wave 1: MetadataDBHook full impl + operator freshness skip/record (FRESH-03, FRESH-04)
- [x] 03-03-PLAN.md — Wave 1: factory meta extraction + provider_info db-managers + `aircontroler db init` CLI (FRESH-01, FRESH-03, FRESH-06)
- [x] 03-04-PLAN.md — Wave 2: SQLite integration tests (round-trip + operator end-to-end) (FRESH-03, FRESH-04, FRESH-06)

### Phase 4: Cross-DAG Conflict Locking
**Goal:** User can configure conflict policies so that when multiple DAGs target the same model concurrently, only one runs and the others skip or fail deterministically — the second headline differentiator over Cosmos.
**Depends on:** Phase 3
**Requirements:** LOCK-01, LOCK-02, LOCK-03, LOCK-04, LOCK-05
**Success Criteria** (what must be TRUE):
  1. With conflict policy `skip`, a chaos test firing two DAGs simultaneously at the same model results in exactly one dbt execution and one skipped task — never two concurrent `dbt build` subprocesses for the same model.
  2. With conflict policy `fail`, the losing task raises and marks itself failed with a clear "conflict: model X already locked by DAG Y" error.
  3. The locking path uses `pg_advisory_xact_lock(hashtext(unique_id))` acquired before the freshness check, released automatically at transaction end (verified by inspecting `pg_locks` during a held lock).
  4. When a task requires multiple locks (parent + child model), locks are acquired in sorted `unique_id` order — a deadlock-provocation test runs clean across 50 iterations.
**Plans:** TBD

### Phase 5: Cloud Run + Distribution
**Goal:** User can deploy aircontroler on Google Cloud Composer with the Cloud Run executor, using a per-model GCP service account for BigQuery access — completing the target deployment story.
**Depends on:** Phase 4
**Requirements:** CLOUD-01, CLOUD-02, CLOUD-03
**Success Criteria** (what must be TRUE):
  1. User builds the reference Dockerfile (shipped in the repo) and gets a container image with the dbt project and `profiles.yml` baked in, runnable by the Cloud Run executor end-to-end on a sample DAG.
  2. User sets `meta.aircontroler.service_account: svc-x@project.iam.gserviceaccount.com` in `schema.yml` (or overrides in Python) and the corresponding model task runs under that identity — verified by a BigQuery audit log showing the impersonated principal.
  3. When `service_account` is set, the dbt subprocess acquires credentials via Workload Identity Federation + ADC impersonation — no long-lived key files on disk.
**Plans:** TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Walking Skeleton | 5/5 | Complete | 2026-04-18 |
| 2. Execution Hardening | 4/4 | Complete | 2026-04-19 |
| 3. Freshness Policies | 4/4 | Complete | 2026-04-19 |
| 4. Cross-DAG Conflict Locking | 0/0 | Not started | - |
| 5. Cloud Run + Distribution | 0/0 | Not started | - |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CORE-01 | Phase 1 | Complete |
| CORE-02 | Phase 1 | Complete |
| CORE-03 | Phase 1 | Complete |
| CORE-04 | Phase 1 | Complete |
| CORE-05 | Phase 1 | Complete |
| CORE-06 | Phase 1 | Complete |
| CORE-07 | Phase 1 | Complete |
| CORE-08 | Phase 1 | Complete |
| CORE-09 | Phase 1 | Complete |
| EXEC-01 | Phase 2 | Complete |
| EXEC-02 | Phase 2 | Complete |
| EXEC-03 | Phase 2 | Complete |
| EXEC-04 | Phase 2 | Complete |
| FRESH-01 | Phase 3 | Planned |
| FRESH-02 | Phase 3 | Deferred (per D-11 — post-v1) |
| FRESH-03 | Phase 3 | Planned |
| FRESH-04 | Phase 3 | Planned |
| FRESH-05 | Phase 3 | Deferred (per D-11 — post-v1) |
| FRESH-06 | Phase 3 | Planned |
| LOCK-01 | Phase 4 | Pending |
| LOCK-02 | Phase 4 | Pending |
| LOCK-03 | Phase 4 | Pending |
| LOCK-04 | Phase 4 | Pending |
| LOCK-05 | Phase 4 | Pending |
| CLOUD-01 | Phase 5 | Pending |
| CLOUD-02 | Phase 5 | Pending |
| CLOUD-03 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 26 total (24 planned for v1 + 2 deferred post-v1 per D-11)
- Mapped to phases: 26
- Unmapped: 0 ✓
- Duplicates: 0 ✓

---
*Last updated: 2026-04-19 — Phase 3 planned (4 plans across 3 waves); FRESH-02 and FRESH-05 marked deferred per D-11.*
