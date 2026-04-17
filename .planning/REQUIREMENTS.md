# Requirements: aircontroler

**Defined:** 2026-04-17
**Core Value:** A dbt DAG where every model is a first-class Airflow task, with cross-DAG freshness policies and conflict resolution ensuring no model runs more times than necessary.

## v1 Requirements

### Core Operator

- [ ] **CORE-01**: User can point the operator at a pre-built `manifest.json` path (recommended production pattern)
- [ ] **CORE-02**: User can enable compile-on-parse mode where the operator runs `dbt parse` at DAG parse time (development opt-in; not the default)
- [ ] **CORE-03**: User can pass a full `dbt build --select ...` or `dbt run --select ...` command string and the operator resolves the model graph by delegating to `dbt ls`
- [ ] **CORE-04**: Each resolved dbt model becomes a separate Airflow task with its own `task_id` equal to the model's unique_id
- [ ] **CORE-05**: Task upstream/downstream dependencies mirror the dbt model DAG (i.e. Airflow Graph view shows the dbt lineage graph)
- [ ] **CORE-06**: Each model task executes via CLI subprocess (`dbt build --select <model_name>`) with configurable `dbt_executable_path`
- [ ] **CORE-07**: Operator reads credentials from a `profiles.yml` path supplied by the user
- [ ] **CORE-08**: Package is compatible with Airflow 2.7+ and Airflow 3.x using a single wheel (via `apache-airflow-providers-common-compat` shim)
- [ ] **CORE-09**: Package is published to PyPI as an Airflow provider (`airflow-provider-aircontroler`) with `get_provider_info` entry point

### Execution Hardening

- [ ] **EXEC-01**: User can configure per-model `retries`, `retry_delay`, `pool`, and `sla` via `meta.aircontroler.*` in `schema.yml`
- [ ] **EXEC-02**: Operator parses `run_results.json` after each model task and exposes timing, row counts, and adapter response in task logs
- [ ] **EXEC-03**: Operator validates selector cardinality at DAG parse time and raises a hard error if the resolved model count exceeds a configurable limit (default: 800)

### Freshness Policies

- [ ] **FRESH-01**: User can define a time-based freshness TTL per model in `schema.yml` under `meta.aircontroler.freshness_hours`
- [ ] **FRESH-02**: User can override model-level freshness TTL per-DAG in Python when constructing the operator
- [ ] **FRESH-03**: When a task starts, the operator checks if the model ran successfully within its TTL; if so, the task is skipped without executing dbt
- [ ] **FRESH-04**: Model execution state (last successful run timestamp per model unique_id) is persisted in the Airflow metadata DB via a dedicated hook (not direct SQLAlchemy — required for Airflow 3 worker isolation)
- [ ] **FRESH-05**: When two DAGs define different TTLs for the same model, the most permissive (longest) TTL applies
- [ ] **FRESH-06**: Operator ships an Alembic migration for the `aircontroler_model_runs` state table; Airflow 3 uses `BaseDBManager`, Airflow 2.x uses a manual `aircontroler db init` CLI command

### Conflict Handling

- [ ] **LOCK-01**: User can configure a conflict policy per model (`skip` or `fail`) for when multiple DAGs attempt to run the same model concurrently
- [ ] **LOCK-02**: Conflict detection uses Postgres advisory locks (`pg_advisory_xact_lock`) acquired before the freshness check; locks are released automatically at transaction end
- [ ] **LOCK-03**: When conflict policy is `skip`, a task that finds another DAG already running the same model marks itself as skipped and does not execute dbt
- [ ] **LOCK-04**: When conflict policy is `fail`, a task that finds another DAG already running the same model raises an exception and marks itself as failed
- [ ] **LOCK-05**: Lock acquisition order is deterministic (sorted by model unique_id) to prevent deadlocks when a task needs multiple locks

### Cloud Run Support

- [ ] **CLOUD-01**: Repository ships a reference Dockerfile that bakes the dbt project and `profiles.yml` into a container image suitable for the Cloud Run executor
- [ ] **CLOUD-02**: Operator accepts a `service_account` parameter on a per-model basis (set via `meta.aircontroler.service_account` in `schema.yml` or overridden in Python)
- [ ] **CLOUD-03**: Operator supports Workload Identity Federation impersonation — when `service_account` is set, the subprocess runs with the specified GCP identity via ADC impersonation

## v2 Requirements

### Conflict Handling — Wait Mode

- **LOCK-V2-01**: User can set conflict policy to `wait`, causing a task to block (via deferrable Trigger) until the in-flight model completes, then re-check freshness before deciding whether to run
- **LOCK-V2-02**: Wait mode has a configurable timeout; if the in-flight model does not complete within the timeout, the waiting task fails

### Observability

- **OBS-V2-01**: Operator exposes a counter of skipped model runs (freshness hits) per DAG run, accessible as an Airflow metric
- **OBS-V2-02**: Operator ships a `aircontroler db cleanup --older-than <days>` CLI command to prune stale rows from `aircontroler_model_runs`
- **OBS-V2-03**: Dry-run mode: operator resolves the model graph and freshness state but does not execute dbt — reports what would run and what would be skipped

### Adapter Support

- **ADAPT-V2-01**: Support dbt-snowflake adapter in addition to dbt-bigquery
- **ADAPT-V2-02**: Support dbt-redshift adapter

### Credentials

- **CRED-V2-01**: Support Airflow connection-based profile generation as an alternative to `profiles.yml` on disk

## Out of Scope

| Feature | Reason |
|---------|--------|
| `wait` conflict mode | Requires deferrable Trigger infrastructure; deferred to v2 |
| Airflow connection-based credentials | profiles.yml sufficient for v1; connection abstraction deferred |
| dbt-core Python API execution | Unstable across minor versions; unsafe for parallel Airflow workers |
| KubernetesExecutor / CeleryExecutor | Not needed for target Composer + Cloud Run deployment |
| dbt adapters beyond BigQuery | Focus on target deployment; other adapters in v2 |
| `on_warning_callback` | Deferred; structured run results (EXEC-02) covers the data |
| dbt Cloud integration | Separate product; out of scope |
| Watcher mode (single subprocess, all models) | Breaks per-task isolation, retries, and per-model service accounts |
| Source freshness as a separate task type | Deferred; dbt build includes test execution |
| Airflow 3 Assets integration | Deferred; experimental in Airflow 3.x |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CORE-01 | Phase 1: Walking Skeleton | Pending |
| CORE-02 | Phase 1: Walking Skeleton | Pending |
| CORE-03 | Phase 1: Walking Skeleton | Pending |
| CORE-04 | Phase 1: Walking Skeleton | Pending |
| CORE-05 | Phase 1: Walking Skeleton | Pending |
| CORE-06 | Phase 1: Walking Skeleton | Pending |
| CORE-07 | Phase 1: Walking Skeleton | Pending |
| CORE-08 | Phase 1: Walking Skeleton | Pending |
| CORE-09 | Phase 1: Walking Skeleton | Pending |
| EXEC-01 | Phase 2: Execution Hardening | Pending |
| EXEC-02 | Phase 2: Execution Hardening | Pending |
| EXEC-03 | Phase 2: Execution Hardening | Pending |
| FRESH-01 | Phase 3: Freshness Policies | Pending |
| FRESH-02 | Phase 3: Freshness Policies | Pending |
| FRESH-03 | Phase 3: Freshness Policies | Pending |
| FRESH-04 | Phase 3: Freshness Policies | Pending |
| FRESH-05 | Phase 3: Freshness Policies | Pending |
| FRESH-06 | Phase 3: Freshness Policies | Pending |
| LOCK-01 | Phase 4: Cross-DAG Conflict Locking | Pending |
| LOCK-02 | Phase 4: Cross-DAG Conflict Locking | Pending |
| LOCK-03 | Phase 4: Cross-DAG Conflict Locking | Pending |
| LOCK-04 | Phase 4: Cross-DAG Conflict Locking | Pending |
| LOCK-05 | Phase 4: Cross-DAG Conflict Locking | Pending |
| CLOUD-01 | Phase 5: Cloud Run + Distribution | Pending |
| CLOUD-02 | Phase 5: Cloud Run + Distribution | Pending |
| CLOUD-03 | Phase 5: Cloud Run + Distribution | Pending |

**Coverage:**
- v1 requirements: 26 total
- Mapped to phases: 26
- Unmapped: 0 ✓
- Duplicates: 0 ✓

---
*Requirements defined: 2026-04-17*
*Last updated: 2026-04-17 after roadmap creation — traceability phase names reconciled with ROADMAP.md*
