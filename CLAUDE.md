# aircontroler

An open-source Airflow provider package for dbt that maps each dbt model to a native Airflow task, with cross-DAG freshness policies and conflict resolution.

## Project Context

- Planning docs: `.planning/`
- Roadmap: `.planning/ROADMAP.md`
- Requirements: `.planning/REQUIREMENTS.md`
- Project context: `.planning/PROJECT.md`
- Current state: `.planning/STATE.md`

## GSD Workflow

This project uses the GSD (Get Shit Done) workflow.

**To work on the next phase:**
```
/gsd-discuss-phase <N>   # gather context and clarify approach
/gsd-plan-phase <N>      # create execution plan
/gsd-execute-phase <N>   # execute the plan
```

**To check progress:**
```
/gsd-progress
```

## Key Decisions

- **Manifest loading**: Accept pre-built `manifest.json` path (default) or compile via `dbt parse` at parse time (dev opt-in). Never block the scheduler with `dbt compile`.
- **Task generation**: Static Python loop at parse time (`for node in graph.nodes`), not Airflow `.expand()` — preserves per-model graph wiring and lineage in Graph view.
- **Selector resolution**: Delegate to `dbt ls --select ... --output json` subprocess — full grammar support, no reimplementation.
- **Execution**: CLI subprocess only (`dbt build --select <model>`). Never import `dbtRunner` — unsafe for parallel workers.
- **State/locking**: Airflow metadata DB via dedicated hook (never direct SQLAlchemy from workers — required for Airflow 3). Postgres advisory locks for distributed mutex.
- **Credentials**: `profiles.yml` on disk. No Airflow connection abstraction in v1.
- **Compatibility**: Single wheel for Airflow 2.7+ and 3.x via `apache-airflow-providers-common-compat` shim.

## Stack

- Python, `hatchling` build backend
- `apache-airflow>=2.7,<4.0` + `apache-airflow-providers-common-compat>=1.7.0`
- `dbt-core>=1.7,<1.12` + `dbt-bigquery`
- Postgres advisory locks (`pg_advisory_xact_lock`) for conflict handling
- Alembic migrations for `aircontroler_model_runs` state table

## Critical Pitfalls to Avoid

1. **Never call `dbt compile` at DAG parse time as the default** — causes 50–100s scheduler stalls (the Cosmos trap)
2. **Never use direct SQLAlchemy sessions from worker `execute()` methods** — Airflow 3 workers have no DB access; use a dedicated hook
3. **Never use `SELECT FOR UPDATE` for locking** — use `pg_advisory_xact_lock` with sorted acquisition order to prevent deadlocks
4. **Never hardcode manifest field paths** — read `manifest_version` first and dispatch to a version-specific parser (v11/v12)
5. **Validate cardinality at parse time** — fail hard above 800 resolved models
