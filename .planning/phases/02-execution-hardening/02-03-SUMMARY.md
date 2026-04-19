---
phase: 02-execution-hardening
plan: "03"
subsystem: operator-factory
tags: [operator, factory, per-model-overrides, meta-aircontroler, retries, pool, target_path, exec-01]

requires:
  - phase: 02-execution-hardening
    plan: "01"
    provides: "manifest_with_meta_data pytest fixture (mart node with meta.aircontroler)"
  - phase: 02-execution-hardening
    plan: "02"
    provides: "build_dbt_dag() with cardinality_limit and max_active_tasks params"

provides:
  - "aircontroler/operator.py: DbtRunModelOperator with retries/retry_delay/pool/target_path explicit kwargs and conditional BaseOperator injection"
  - "aircontroler/factory.py: build_dbt_dag() with pool/pool_slots/target_path params and ac_meta extraction loop"
  - "tests/unit/test_operator.py: 7 new unit tests for per-model override kwargs"
  - "tests/unit/test_factory.py: 8 new unit tests for meta forwarding, pool precedence, freshness-key-ignored"

affects:
  - "02-04 (Wave 3): operator.py is the integration point for _log_run_results(); target_path is already wired"
  - "No further factory.py changes expected after this plan"

tech-stack:
  added: []
  patterns:
    - "Conditional kwargs injection: only non-None values forwarded to BaseOperator (Pitfall 3 fix)"
    - "retry_delay int (seconds) converted to timedelta(seconds=N) inside __init__ (Pitfall 4 fix)"
    - "Deferred import: from datetime import timedelta inside __init__ (lazy-import style)"
    - "Per-model pool precedence: ac_meta.get('pool') or pool or None (D-16)"
    - "Unknown meta keys silently ignored via .get() — no reflection or eval (D-04)"

key-files:
  created: []
  modified:
    - "aircontroler/operator.py"
    - "aircontroler/factory.py"
    - "tests/unit/test_operator.py"
    - "tests/unit/test_factory.py"

key-decisions:
  - "Conditional kwargs injection pattern: if retries is not None: kwargs['retries'] = retries — preserves BaseOperator defaults when not set"
  - "retry_delay stored as int on operator constructor, converted to timedelta before passing to BaseOperator"
  - "pool_slots only applied when task_pool is truthy: pool_slots if task_pool else 1"
  - "TDD protocol followed for both tasks: RED commits before GREEN commits"

metrics:
  duration: "~3 min"
  completed: "2026-04-19"
  tasks: 2
  files_modified: 4
---

# Phase 2 Plan 03: Per-Model Overrides (EXEC-01) Summary

**Per-model retries, retry_delay, pool, and target_path wired from meta.aircontroler in schema.yml to BaseOperator via conditional kwargs injection — with 15 new unit tests covering all override and default-preservation scenarios.**

## Performance

- **Duration:** ~3 min
- **Started:** 2026-04-19T05:30:06Z
- **Completed:** 2026-04-19T05:33:20Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

### Task 1: Expand DbtRunModelOperator constructor

Modified `aircontroler/operator.py` with two edits:

**Edit A — constructor expansion** (lines 37–67): Added `target_path`, `retries`, `retry_delay`, `pool` as explicit keyword-only args after `dbt_executable_path`. Deferred `from datetime import timedelta` import inside `__init__`. Conditional injection pattern ensures only non-None values are forwarded to `BaseOperator.__init__()` — preserving Airflow's own defaults (retries=0, retry_delay=timedelta(seconds=300), pool="default_pool") when kwargs are omitted.

**Edit B — template_fields update** (line 35): Appended `"target_path"` to expose it for Jinja templating.

**Final DbtRunModelOperator `__init__` signature:**
```python
def __init__(
    self,
    *,
    unique_id: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str = "dbt",
    target_path: str | None = None,
    retries: int | None = None,
    retry_delay: int | None = None,
    pool: str | None = None,
    **kwargs,
) -> None:
```

### Task 2: Expand build_dbt_dag() with meta extraction loop

Modified `aircontroler/factory.py` with two edits:

**Edit A — signature expansion**: Added `pool: str | None = None`, `pool_slots: int = 1`, `target_path: str | None = None` after `max_active_tasks`.

**Edit B — task creation loop replacement**: Replaced the bare operator construction with the `ac_meta` extraction loop. Per-model pool takes precedence over DAG-level pool (D-16). Unknown meta keys (e.g., `freshness`) are silently ignored.

**Final `build_dbt_dag()` signature (post-PLAN-03):**
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
    pool: str | None = None,
    pool_slots: int = 1,
    target_path: str | None = None,
) -> dict:
```

## Pool Precedence Resolution Rule (D-16)

```
task_pool = ac_meta.get("pool") or pool or None
```

Priority order: **per-model meta.aircontroler.pool > DAG-level pool param > unset (BaseOperator default)**

`pool_slots` is applied only when `task_pool` is truthy: `pool_slots if task_pool else 1`.

## New Tests

### Operator tests (7 new, tests/unit/test_operator.py)

| Test name | Behavior tested |
|-----------|----------------|
| `test_retries_from_meta` | retries=3 kwarg → op.retries == 3 |
| `test_retry_delay_conversion` | retry_delay=300 (int) → op.retry_delay == timedelta(seconds=300) |
| `test_pool_from_meta` | pool='heavy_models' → op.pool == 'heavy_models' |
| `test_target_path_stored` | target_path='/custom/target' → op.target_path == '/custom/target' |
| `test_target_path_default_none` | no target_path kwarg → op.target_path is None |
| `test_no_meta_uses_defaults` | no override kwargs → op.retries == 0 (BaseOperator default preserved) |
| `test_template_fields_includes_target_path` | 'target_path' in DbtRunModelOperator.template_fields |

### Factory tests (8 new, tests/unit/test_factory.py)

| Test name | Behavior tested |
|-----------|----------------|
| `test_meta_forwarding_to_operator` | mart node meta → retries=3, retry_delay=timedelta(300), pool='heavy_models' |
| `test_no_meta_node_uses_defaults` | stg_a (no meta) → retries == 0 (BaseOperator default) |
| `test_per_model_pool_overrides_dag_pool` | mart meta pool wins over DAG-level pool (D-16) |
| `test_dag_pool_applied_to_all` | DAG-level pool='dag_pool' → all tasks get that pool |
| `test_dag_pool_none_leaves_unset` | pool=None (default) → tasks at BaseOperator default ('default_pool' or None) |
| `test_pool_slots_applied_when_pool_set` | pool_slots=4 flows to all pooled tasks |
| `test_target_path_forwarded_to_all_tasks` | target_path='/custom/target' → all operator instances have it |
| `test_freshness_key_ignored` | meta.aircontroler.freshness=24 → no crash, retries == 0 (key silently ignored) |

## Total Unit Test Count

**44 tests pass** (21 Phase 1 + 8 PLAN-02 + 7 operator new + 8 factory new).

Full suite: `uv run --extra test pytest tests/unit/ -x -q` exits 0. factory.py coverage: 100%. operator.py coverage: 97%.

## Task Commits

1. **Task 1 RED: Failing operator tests** — `bc51717` (test)
2. **Task 1 GREEN: Operator constructor expansion** — `096396f` (feat)
3. **Task 2 RED: Failing factory tests** — `ef1f209` (test)
4. **Task 2 GREEN: Factory meta extraction loop** — `28650a4` (feat)

## Files Created/Modified

- `/workspace/aircontroler/aircontroler/operator.py` — constructor expanded; template_fields updated
- `/workspace/aircontroler/aircontroler/factory.py` — signature expanded; task loop now extracts ac_meta per node
- `/workspace/aircontroler/tests/unit/test_operator.py` — 7 new tests appended
- `/workspace/aircontroler/tests/unit/test_factory.py` — 8 new tests appended

## PLAN 04 Readiness

PLAN 04 (Wave 3) will add `_log_run_results()` to `operator.py` and integrate it into `execute()` after the subprocess call. No further changes to `factory.py` are expected:
- `target_path` is already stored on the operator instance (ready for `_log_run_results()` path resolution)
- `run_results_v6_data` fixture is already in `tests/conftest.py` (from PLAN 01)
- The only change surface is `operator.py::execute()` and one new integration test

## Decisions Made

- Conditional kwargs injection pattern (not explicit `retries=None` passthrough) is the load-bearing design for preserving BaseOperator defaults. Passing `retries=None` to BaseOperator explicitly sets retries to None, which is not the same as omitting it (Pitfall 3 from RESEARCH.md).
- Deferred `from datetime import timedelta` import inside `__init__` matches the lazy-import style established in the project.
- TDD protocol followed strictly for both tasks: RED commits written before GREEN commits, all tests confirmed failing before implementation.

## Deviations from Plan

None — plan executed exactly as written. All edits applied verbatim per plan specifications. All 15 new tests pass on first GREEN implementation attempt.

## Known Stubs

None — all four kwargs (retries, retry_delay, pool, target_path) are fully wired through the operator-factory chain. The pre-existing `service_account` stub in factory.py is from Phase 1 (Plan 01) and is out of scope for this plan.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes introduced. The threat register items T-2-01-T1 through T-2-01-T5 were reviewed:
- T-2-01-T3 (target_path information disclosure): mitigated — this plan only stores the string on the instance, no file I/O. PLAN 04 addresses the I/O risk.
- T-2-01-T4 (unknown meta keys): mitigated — `test_freshness_key_ignored` enforces the `.get()`-only extraction contract.

## Self-Check: PASSED
