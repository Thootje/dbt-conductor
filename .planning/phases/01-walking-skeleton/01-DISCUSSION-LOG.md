# Phase 1: Walking Skeleton - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-18
**Phase:** 01-walking-skeleton
**Areas discussed:** User-facing DAG API, Select command interface, Test infrastructure, service_account stub, Package layout, Provider metadata, dbt project-dir handling, Manifest parser internals

---

## User-facing DAG API

| Option | Description | Selected |
|--------|-------------|----------|
| Factory function | User owns the DAG, calls build_dbt_dag(dag, ...) to add tasks. Flexible — other operators can be wired before/after. | ✓ |
| DbtDag class | Operator/class that auto-creates the whole DAG. Less boilerplate but less composable. | |
| TaskGroup only | DbtTaskGroup creates tasks inside a group. Good middle ground but returns a group not individual tasks. | |

**User's choice:** Factory function
**Notes:** User confirmed the preview showing `start >> dbt['entry_tasks']` and `dbt['exit_tasks'] >> notify` patterns.

### Return value follow-up

| Option | Description | Selected |
|--------|-------------|----------|
| Dict with entry/exit + all tasks | `{'entry_tasks': [...], 'exit_tasks': [...], 'tasks': {unique_id: task}}` | ✓ |
| Flat dict by unique_id only | `{unique_id: task}` — user finds entry/exit themselves | |

**User's choice:** Dict with entry/exit + all tasks

---

## Select Command Interface

| Option | Description | Selected |
|--------|-------------|----------|
| Full dbt command string | User passes `'dbt build --select tag:daily+ --profile prod'`. We parse selector flags for `dbt ls`. | ✓ |
| Separate params | `command=`, `select=`, `exclude=`, `profile=`, `target=` as distinct params. | |

**User's choice:** Full dbt command string

---

## Test Infrastructure

| Option | Description | Selected |
|--------|-------------|----------|
| Unit + DAG parse tests | Unit tests + Airflow DAG instantiation tests. No actual dbt execution in CI. | |
| Unit tests only | Mock subprocess only. Fastest CI but no real Airflow instantiation. | |
| Unit + full integration | Real Airflow instance + real dbt execution in CI with sample project. | ✓ |

**User's choice:** Unit + full integration

### Adapter follow-up

| Option | Description | Selected |
|--------|-------------|----------|
| DuckDB | Zero infra, no cloud credentials, easy contributor onboarding. | ✓ |
| BigQuery only | Most faithful to production but gates contributors on GCP access. | |
| SQLite | Zero infra but fewer dbt adapter features. | |

**User's choice:** DuckDB

---

## service_account Stub

| Option | Description | Selected |
|--------|-------------|----------|
| Accept + warn | Parameter accepted, logs WARNING about future support. | ✓ |
| Accept silently (no-op) | Parameter accepted, nothing logged. | |
| Raise NotImplementedError | Crashes immediately if used. | |

**User's choice:** Accept + warn

---

## Package Layout

| Option | Description | Selected |
|--------|-------------|----------|
| Flat modules | `operator.py`, `factory.py`, `manifest.py`, `selector.py`, `hooks/`. | ✓ |
| Nested by concern | `operators/`, `graph/`, `manifest/`, `selector/`, `hooks/`. | |

**User's choice:** Flat modules

---

## Provider Metadata

| Option | Description | Selected |
|--------|-------------|----------|
| aircontroler (lowercase) | Package = airflow-provider-aircontroler, provider name = 'aircontroler'. | ✓ |
| Aircontroler (capitalized) | Capitalized display name in Airflow UI. | |
| Air Controller (two words) | More readable but inconsistent with package slug. | |

**User's choice:** aircontroler (lowercase)

---

## dbt project-dir Handling

| Option | Description | Selected |
|--------|-------------|----------|
| Separate params on factory | `project_dir` and `profiles_path` as explicit params, validated at parse time. | ✓ |
| User puts in dbt_command | `--project-dir` and `--profiles-dir` embedded in command string. Zero extra params, no parse-time validation. | |

**User's choice:** Separate params on factory

---

## Manifest Parser Internals

| Option | Description | Selected |
|--------|-------------|----------|
| Dataclasses | `ManifestNode` dataclass, typed fields, IDE autocomplete, immutable. | ✓ |
| TypedDict | Lighter, dict-compatible. Less IDE support for attribute access. | |
| Plain dicts | No static type checking. Manifest schema drift goes undetected. | |

**User's choice:** Dataclasses

---

## Claude's Discretion

- CI matrix specifics (exact Airflow minor versions in test matrix)
- Internal naming of the selector resolver class
- Default value for `dbt_executable_path` (PATH lookup vs required param)
- Exact fields on `ManifestNode` beyond minimum for graph wiring

## Deferred Ideas

None — discussion stayed within phase scope.
