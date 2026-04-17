# aircontroler

## What This Is

An open-source Airflow provider package for dbt that maps each dbt model to a native Airflow task via dynamic task mapping, giving users full lineage visibility in Airflow's Graph view. It targets data teams running dbt on BigQuery with Airflow (LocalExecutor or Cloud Run), and is designed to outcompete Cosmos by solving cross-DAG model deduplication and conflict handling — problems Cosmos ignores entirely.

## Core Value

A dbt DAG where every model is a first-class Airflow task, with cross-DAG freshness policies and conflict resolution ensuring no model runs more times than necessary.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Operator accepts a pre-built `manifest.json` path (recommended production pattern) OR compiles via `dbt parse` at DAG parse time (development opt-in only)
- [ ] Operator resolves model graph from a full `dbt build/run --select ...` command using dbt selector syntax (delegated to `dbt ls` subprocess)
- [ ] Each resolved model becomes a separate statically-generated Airflow task at parse time (Python loop, not Airflow `.expand()` — preserves per-model graph wiring and lineage)
- [ ] Task dependencies mirror the dbt model DAG (upstream/downstream wiring)
- [ ] dbt models execute via CLI subprocess (`dbt run --select model_name`)
- [ ] Credentials supplied via `profiles.yml` on disk
- [ ] Compatible with Airflow 2.x and 3.x
- [ ] Compatible with LocalExecutor and Google Cloud Run executor
- [ ] Published as an installable PyPI package (Airflow provider)
- [ ] Time-based freshness policies configurable per model (dbt `meta` as default, overridable per-DAG in Python)
- [ ] Conflict handling configurable per policy (wait / skip / fail) when multiple DAGs target the same model
- [ ] Model execution state persisted in Airflow metadata DB for cross-DAG freshness checks and distributed locking
- [ ] Per-model service account selection (design TBD)

### Out of Scope

- KubernetesExecutor / CeleryExecutor — not needed for target deployments
- Airflow connection-based credential management — profiles.yml is sufficient for v1
- dbt-core Python API execution — CLI subprocess only; API is unstable
- dbt adapters beyond BigQuery at launch — Snowflake/Redshift/Postgres deferred
- Single-operator mode (one Airflow task for all models) — defeats lineage goal

## Context

- Primary motivation: Cosmos does not handle conflicts when multiple DAGs include the same model, causing redundant runs. It also has no freshness policy system to deduplicate across DAGs.
- Target deployment: Google Cloud Composer (managed Airflow) with BigQuery. Cloud Run executor means each Airflow task runs as an isolated Cloud Run job — how dbt project files and `profiles.yml` reach the container is TBD.
- The Airflow metadata DB (Postgres/MySQL backing Cloud Composer) is the coordination layer for both freshness state and distributed locking — no additional infrastructure required.
- Per-model service account selection is a BigQuery-specific requirement (different models may need access to different datasets via different GCP identities). Implementation approach is TBD.

## Constraints

- **Tech stack**: Python, Airflow provider pattern, dbt CLI subprocess
- **Adapters**: BigQuery only at launch (dbt-bigquery)
- **Airflow**: Must support both 2.x and 3.x APIs
- **Credentials**: profiles.yml on disk — no Airflow connection abstraction in v1
- **Execution**: CLI only — dbt-core Python API explicitly excluded

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Static task generation at parse time (one task per model, Python loop not `.expand()`) | `.expand()` collapses all model tasks into one Graph node, destroying lineage; static loop preserves per-model wiring | — Pending |
| CLI subprocess execution | Stable across dbt versions; dbt Python API changes between minor versions | — Pending |
| Airflow metadata DB for state/locking via dedicated hook | No extra infra; Postgres advisory locks handle concurrent DAG conflicts; hook abstraction required for Airflow 3 worker isolation | — Pending |
| profiles.yml on disk for credentials | Simplest path; Airflow connection abstraction deferred | — Pending |
| BigQuery-only at launch | Focus on target deployment; other adapters added in future milestones | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-17 after research — manifest-as-input clarified, static task generation vs .expand() resolved, locking approach updated*
