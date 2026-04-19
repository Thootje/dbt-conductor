---
phase: 02-execution-hardening
plan: "02"
subsystem: factory
tags: [factory, cardinality-guard, concurrency, dag-parse-time, airflow, exec-03, exec-04]

requires:
  - phase: 02-execution-hardening
    plan: "01"
    provides: "test fixture infrastructure (manifest_with_meta_data, run_results_v6_data)"

provides:
  - "aircontroler/factory.py: build_dbt_dag() with cardinality guard (EXEC-03) and max_active_tasks param (EXEC-04)"
  - "tests/unit/test_factory.py: 8 new unit tests covering cardinality guard and max_active_tasks"

affects:
  - "02-03 (Wave 2): factory.py signature is the integration surface for pool/pool_slots/target_path params"
  - "02-04 (Wave 3): integration tests will call build_dbt_dag() with the new params"

tech-stack:
  added: []
  patterns:
    - "os.environ.get with int() conversion and no try/except (fail-loud for misconfigured env vars)"
    - "Strict greater-than cardinality check (len > limit, not >=) preserves exact-at-limit behavior"
    - "dag.max_active_tasks duck-typed assignment (no Airflow version branch needed)"

key-files:
  created: []
  modified:
    - "aircontroler/factory.py"
    - "tests/unit/test_factory.py"

key-decisions:
  - "EXEC-03: cardinality guard uses strict > (not >=) so exactly-at-limit does not raise"
  - "EXEC-03: env var read via int(os.environ.get(...)) with no try/except — ValueError propagates naturally for misconfigured values (fail loud, per CONTEXT.md D-13 specifics)"
  - "EXEC-04: dag.max_active_tasks assigned only when explicitly set; None preserves Airflow's default of 16"
  - "TDD protocol followed: RED commit (4427d34) with 8 failing tests, then GREEN commit (0da7593) with implementation"

metrics:
  duration: "~2 min"
  completed: "2026-04-19"
  tasks: 2
  files_modified: 2
---

# Phase 2 Plan 02: Cardinality Guard and DAG Concurrency Control Summary

**Added cardinality guard (EXEC-03) and max_active_tasks concurrency param (EXEC-04) to build_dbt_dag() via two precise edits to factory.py and 8 new unit tests covering all precedence/edge cases.**

## Performance

- **Duration:** ~2 min
- **Started:** 2026-04-19T05:25:05Z
- **Completed:** 2026-04-19T05:27:04Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

### Task 1: Add cardinality guard and max_active_tasks to build_dbt_dag() (TDD GREEN)

Modified `/workspace/aircontroler/aircontroler/factory.py` with three precise edits:

**Edit A — `import os` at module level** (line 24):
```python
import os
```

**Edit B — expanded function signature** (lines 37–47):
```python
def build_dbt_dag(
    dag: "DAG",
    manifest_path: str,
    dbt_command: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str | None = None,
    service_account: str | None = None,
    cardinality_limit: int | None = None,   # new — EXEC-03
    max_active_tasks: int | None = None,    # new — EXEC-04
) -> dict:
```

**Edit C — cardinality guard + concurrency block** (lines 102–119, inserted after `resolver.resolve()` call):
```python
# EXEC-03: cardinality guard at DAG parse time.
# Precedence: kwarg > env var > hardcoded default 800.
effective_limit: int = (
    cardinality_limit
    if cardinality_limit is not None
    else int(os.environ.get("AIRCONTROLER_MAX_MODELS", 800))
)
if len(resolved_ids) > effective_limit:
    raise ValueError(
        f"Selector resolved to {len(resolved_ids):,} models "
        f"(limit: {effective_limit:,}). "
        "Narrow your --select filter or raise cardinality_limit=N in build_dbt_dag()."
    )

# EXEC-04: DAG-level concurrency control.
# Only mutate when explicitly set; None preserves Airflow's default (16).
if max_active_tasks is not None:
    dag.max_active_tasks = max_active_tasks
```

### Task 2: Add unit tests for cardinality guard and max_active_tasks (TDD RED then GREEN)

Appended 8 new test functions to `/workspace/aircontroler/tests/unit/test_factory.py`:

| Test name | Behavior tested |
|-----------|----------------|
| `test_cardinality_default_limit` | 801 ids with no kwarg/env raises ValueError with "801" in message |
| `test_cardinality_kwarg_override` | cardinality_limit=3 with 5 ids raises ValueError matching "5.*(limit: 3)" |
| `test_cardinality_env_var` | AIRCONTROLER_MAX_MODELS=10 used when no kwarg; 11 ids raises |
| `test_cardinality_kwarg_overrides_env` | kwarg=20 wins over env=5; 10 ids does NOT raise |
| `test_cardinality_at_limit_passes` | count == limit (5 == 5) does NOT raise (strict >) |
| `test_cardinality_error_message` | 1,500 and 1,000 comma-formatted; "Narrow your --select filter" and "cardinality_limit=N" in message |
| `test_max_active_tasks_set` | max_active_tasks=5 sets dag.max_active_tasks to 5 |
| `test_max_active_tasks_default_none` | max_active_tasks=None leaves dag.max_active_tasks unchanged at Airflow default (16) |

## Task Commits

1. **Task 2 RED: Failing tests** - `4427d34` (test) — 8 failing tests for cardinality guard and max_active_tasks
2. **Task 1 GREEN: Implementation** - `0da7593` (feat) — factory.py with cardinality guard and max_active_tasks

## Files Created/Modified

- `/workspace/aircontroler/aircontroler/factory.py` — added `import os`, two new params, cardinality guard block, dag.max_active_tasks assignment
- `/workspace/aircontroler/tests/unit/test_factory.py` — 8 new unit tests appended after existing tests

## build_dbt_dag() Final Signature

```python
def build_dbt_dag(
    dag: "DAG",
    manifest_path: str,
    dbt_command: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str | None = None,
    service_account: str | None = None,
    cardinality_limit: int | None = None,
    max_active_tasks: int | None = None,
) -> dict:
```

## AIRCONTROLER_MAX_MODELS env var

Read via `int(os.environ.get("AIRCONTROLER_MAX_MODELS", 800))` with **no try/except**. Per CONTEXT.md specifics: "ValueError if set to non-integer is acceptable (misconfiguration, fail loud)." Python's natural ValueError propagates — this is the intended behavior.

## Total Unit Test Count

**29 tests pass** (21 Phase 1 tests + 8 new Phase 2 tests). Full suite: `uv run --extra test pytest tests/unit/ -x -q` exits 0.

## Decisions Made

- TDD protocol followed strictly: RED commit (`4427d34`) written with 8 failing tests, then GREEN commit (`0da7593`) with minimal implementation to pass all tests.
- No REFACTOR phase needed — factory.py implementation is clean as-is with proper comments and no dead code.
- The `test_cardinality_kwarg_overrides_env` test calls `build_dbt_dag()` with 10 fake ids and `cardinality_limit=20`; it does NOT assert a return value because the fake_ids don't match nodes in the manifest, so `tasks` dict is empty — but the function returns successfully (no ValueError), which is what the test verifies.

## Deviations from Plan

None — plan executed exactly as written. All three edits applied verbatim. All 8 test functions appended verbatim with no modifications.

## Known Stubs

None — both `cardinality_limit` and `max_active_tasks` are fully wired. The cardinality guard and concurrency control are fully functional, not stubs.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes introduced. The `AIRCONTROLER_MAX_MODELS` env var threat surface was already analyzed in the plan's STRIDE threat register (T-2-02: accepted, operator/admin trust boundary).

## Self-Check: PASSED

All files exist and all commits are present in git history.
