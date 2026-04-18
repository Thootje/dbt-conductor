---
phase: 01-walking-skeleton
plan: "04"
subsystem: factory
tags: [factory, parse-time, dag-wiring, entry-exit, service-account-stub, tdd]
dependency_graph:
  requires:
    - "01-02"   # parse_manifest, ManifestNode, SelectorResolver
    - "01-03"   # DbtRunModelOperator
  provides:
    - aircontroler.factory (build_dbt_dag)
  affects:
    - "01-05"   # DAG integration test calls build_dbt_dag() end-to-end
tech_stack:
  added:
    - logging.getLogger (module-level logger for aircontroler.factory)
  patterns:
    - static Python loop at parse time (one operator per resolved node — not .expand())
    - set-difference entry/exit classification (avoids has_upstream dict bug from RESEARCH.md)
    - D-11 WARNING stub pattern (service_account: log + continue, no crash)
key_files:
  created:
    - aircontroler/factory.py (159 lines)
  modified:
    - tests/unit/test_factory.py (4 stubs replaced with real tests)
decisions:
  - "Set-difference entry/exit classification: all_resolved_deps set built from node.depends_on values in resolved set; exit_tasks = tasks whose uid is not in that set"
  - "SelectorResolver.resolve() receives profiles_path as profiles_dir — matches existing selector.py signature"
  - "dbt_executable_path defaults to 'dbt' via `executable = dbt_executable_path or 'dbt'` inside factory body"
metrics:
  duration: "~2 minutes"
  completed: "2026-04-18T08:08:39Z"
  tasks_completed: 1
  tasks_total: 1
  files_created: 1
  files_modified: 1
  tests_passed: 21
  tests_total: 21
---

# Phase 01 Plan 04: build_dbt_dag() Factory Summary

**One-liner:** build_dbt_dag() wires manifest parsing, dbt-ls selector resolution, and static-loop operator creation into the public API — all 4 factory tests and full 21-test unit suite green.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 RED | Failing factory tests | `d22947b` | `tests/unit/test_factory.py` |
| 1 GREEN | Implement factory.py | `5ced049` | `aircontroler/factory.py` |

## Module Implemented

| File | Lines | Exports |
|------|-------|---------|
| `aircontroler/factory.py` | 159 | `build_dbt_dag()` |

### build_dbt_dag() Signature

```python
def build_dbt_dag(
    dag: "DAG",
    manifest_path: str,
    dbt_command: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str | None = None,
    service_account: str | None = None,
) -> dict:
    # Returns: {'entry_tasks': [...], 'exit_tasks': [...], 'tasks': {uid: operator}}
```

## Test Results

### Factory unit tests (test_factory.py)

```
tests/unit/test_factory.py::test_task_per_model PASSED
tests/unit/test_factory.py::test_edge_wiring PASSED
tests/unit/test_factory.py::test_entry_exit_classification PASSED
tests/unit/test_factory.py::test_service_account_warning PASSED

4 passed in 2.13s
```

### Full unit suite (tests/unit/)

```
tests/unit/test_factory.py ....
tests/unit/test_init.py ..
tests/unit/test_manifest.py .....
tests/unit/test_operator.py .....
tests/unit/test_selector.py .....

21 passed in 2.10s
```

## Acceptance Criteria Verified

```
grep 'def build_dbt_dag' aircontroler/factory.py
→ 36:def build_dbt_dag(

grep 'service_account is not yet implemented' aircontroler/factory.py
→ 79:            "service_account is not yet implemented — "

grep '^from aircontroler' aircontroler/factory.py
→ 26:from aircontroler.manifest import ManifestNode, parse_manifest
→ 27:from aircontroler.operator import DbtRunModelOperator
→ 28:from aircontroler.selector import SelectorResolver

grep 'shell=True' aircontroler/factory.py
→ no output (CLEAN — zero matches)
```

## Security Properties Confirmed

All STRIDE threat mitigations applied:
- **T-01-04-01**: `dbt_command` used only for selector flag extraction in `SelectorResolver._extract_selector_args()`; never passed verbatim to any subprocess; actual subprocess commands built from individual operator constructor fields at execute time
- **T-01-04-02**: Phase 2 cardinality guard (EXEC-03, max 800 tasks) accepted as known risk in Phase 1; no runaway protection yet
- **T-01-04-03**: `service_account` repr-logged (email address, not a credential); no secret material involved since feature is unimplemented

## TDD Gate Compliance

- **RED commit:** `d22947b` — `test(01-04): add failing factory tests` — confirmed `ModuleNotFoundError: No module named 'aircontroler.factory'` before implementation
- **GREEN commit:** `5ced049` — `feat(01-04): implement build_dbt_dag() factory function` — all 4 factory tests pass; full 21-test suite green

## Decisions Made

- **Entry/exit classification**: Set-difference approach rather than `has_upstream` dict (RESEARCH.md Pattern 3 note: the has_upstream dict approach had a latent bug). `entry_tasks` = tasks with no `depends_on` entry in the resolved set. `exit_tasks` = tasks whose `uid` is not in `all_resolved_deps` (the union of all `depends_on` values for nodes in the resolved set).
- **profiles_path forwarding**: Factory parameter `profiles_path` forwarded to `SelectorResolver.resolve()` as `profiles_dir` — matches the existing `selector.py` method signature exactly.

## Deviations from Plan

None — plan executed exactly as written. The factory code from the plan's `<action>` block was implemented verbatim (with minor docstring additions). All 4 tests passed on first run after implementation.

## Known Stubs

None — `build_dbt_dag()` is fully implemented. The `service_account` parameter is intentionally stubbed (D-11) with a WARNING log; this stub is the Phase 1 intended behavior, not an omission.

## Status

COMPLETE — factory.py implemented with correct D-03 signature, D-02 return dict, D-11 warning, entry/exit classification, and edge wiring. All 4 factory unit tests pass. Full unit suite green: 21 passed.

## Self-Check: PASSED

Files exist:
- `aircontroler/factory.py` — FOUND (159 lines)
- `tests/unit/test_factory.py` — FOUND (4 real tests)
- `.planning/phases/01-walking-skeleton/01-04-SUMMARY.md` — FOUND (this file)

Commits exist:
- `d22947b` — FOUND (test(01-04): add failing factory tests)
- `5ced049` — FOUND (feat(01-04): implement build_dbt_dag() factory function)
