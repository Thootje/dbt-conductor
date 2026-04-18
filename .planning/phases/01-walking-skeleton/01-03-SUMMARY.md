---
phase: 01-walking-skeleton
plan: "03"
subsystem: operator
tags: [airflow, operator, subprocess, common-compat, dbt, security, tdd]
dependency_graph:
  requires:
    - "01-02"   # ManifestNode/parse_manifest contracts; common-compat BaseHook pattern
  provides:
    - aircontroler.operator (DbtRunModelOperator)
  affects:
    - "01-04"   # DbtTaskFactory creates DbtRunModelOperator instances
    - "01-05"   # DAG integration test exercises DbtRunModelOperator.execute()
tech_stack:
  added:
    - subprocess.run as list[str] (dbt build, no shell passthrough)
    - airflow.providers.common.compat.sdk.BaseOperator (CORE-08 compat shim)
    - airflow.providers.common.compat.sdk.AirflowException
  patterns:
    - template_fields declaration for Airflow template resolution
    - Credential-safe subprocess: path passed as flag, never file contents
    - Security comments avoid literal "shell=True" to prevent grep false positives
key_files:
  created:
    - aircontroler/operator.py (82 lines)
  modified:
    - tests/unit/test_operator.py (5 stubs replaced with real tests)
key_decisions:
  - "template_fields declared for unique_id, project_dir, profiles_path, dbt_executable_path — required for Airflow template resolution on all operator params"
  - "Docstrings avoid literal 'shell=True' substring (use 'no shell passthrough') to prevent grep-based security checks from producing false positives"
patterns-established:
  - "Security grep gate: operator docstrings describe subprocess safety as 'no shell passthrough' not 'never shell=True'"
requirements-completed: [CORE-06, CORE-07, CORE-08]
duration: ~5min
completed: "2026-04-18"
---

# Phase 01 Plan 03: DbtRunModelOperator Summary

**DbtRunModelOperator(BaseOperator) via common-compat shim — subprocess dbt build with list[str] cmd, --profiles-dir credential path flag, and AirflowException on non-zero exit; 5 unit tests green.**

## Performance

- **Duration:** ~5 min
- **Started:** 2026-04-18T08:00:00Z
- **Completed:** 2026-04-18T08:04:41Z
- **Tasks:** 1 (TDD: 2 commits — RED test, GREEN implementation)
- **Files modified:** 2

## Accomplishments

- `DbtRunModelOperator(BaseOperator)` implemented with full `execute()` using `subprocess.run` as `list[str]` — no shell passthrough
- Profiles path passed as `--profiles-dir` directory flag; file contents never read or logged (T-01-03-02)
- `AirflowException` raised on non-zero dbt exit containing `unique_id` and exit code (CORE-06)
- `BaseOperator` imported from `airflow.providers.common.compat.sdk` for Airflow 2.7+/3.x compatibility (CORE-08)
- All 5 operator unit tests pass; full unit suite at 17 passed, 4 skipped (factory stubs)

## Task Commits

1. **Task 1 RED: Failing operator tests** - `4bcf0b3` (test)
2. **Task 1 GREEN: DbtRunModelOperator implementation** - `ef18687` (feat)

## Files Created/Modified

- `aircontroler/operator.py` (82 lines) — `DbtRunModelOperator(BaseOperator)` with `__init__` accepting `unique_id`, `project_dir`, `profiles_path`, `dbt_executable_path`; `execute()` builds and runs list[str] command; raises `AirflowException` on failure
- `tests/unit/test_operator.py` — 5 real tests replacing skipped stubs: `test_import_compat`, `test_subprocess_command`, `test_custom_executable`, `test_failure_raises`, `test_profiles_path`

## Key Method Signatures

```python
class DbtRunModelOperator(BaseOperator):
    template_fields = ("unique_id", "project_dir", "profiles_path", "dbt_executable_path")

    def __init__(
        self,
        *,
        unique_id: str,
        project_dir: str,
        profiles_path: str,
        dbt_executable_path: str = "dbt",
        **kwargs,
    ) -> None: ...

    def execute(self, context: dict) -> None:
        # cmd = [executable, "build", "--select", unique_id,
        #         "--project-dir", project_dir, "--profiles-dir", profiles_path]
        # subprocess.run(cmd, capture_output=True, text=True)
        # raises AirflowException on returncode != 0
```

## Test Results

```
tests/unit/test_operator.py::test_import_compat PASSED
tests/unit/test_operator.py::test_subprocess_command PASSED
tests/unit/test_operator.py::test_custom_executable PASSED
tests/unit/test_operator.py::test_failure_raises PASSED
tests/unit/test_operator.py::test_profiles_path PASSED

5 passed in 2.11s
```

## Full Unit Suite

```
tests/unit/test_factory.py ssss     (4 skipped — Wave 2 stubs)
tests/unit/test_init.py ..
tests/unit/test_manifest.py .....
tests/unit/test_operator.py .....
tests/unit/test_selector.py .....

17 passed, 4 skipped in 2.14s
```

## Security Properties Confirmed

```
grep -n 'shell=True' aircontroler/operator.py
→ no output (exit code 1 — zero matches) [T-01-03-01]

grep -n 'from airflow.providers.common.compat' aircontroler/operator.py
→ 15:from airflow.providers.common.compat.sdk import AirflowException, BaseOperator [CORE-08]

grep -n '\-\-profiles\-dir' aircontroler/operator.py
→ 28:            Passed as --profiles-dir to dbt subprocess.
   67:            "--profiles-dir", self.profiles_path,  [CORE-07]
```

All STRIDE threat mitigations applied:
- **T-01-03-01**: `cmd` is `list[str]` constructed from individual fields; no shell passthrough
- **T-01-03-02**: `profiles_path` passed as directory path flag only; file contents never read, never appear in subprocess `env` dict or logs
- **T-01-03-03**: `capture_output=True` — accepted buffered approach for Phase 1; Phase 2 can add streaming

## Decisions Made

- `template_fields` declared for all operator params — required for Airflow's template resolution machinery to work on `unique_id`, `project_dir`, `profiles_path`, `dbt_executable_path`
- Docstrings avoid literal "shell=True" substring to prevent grep-based security gate false positives (same pattern as 01-02 manifest.py fix)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Docstring contained literal 'shell=True' causing security grep false positive**
- **Found during:** Task 1 GREEN acceptance criteria verification
- **Issue:** Module docstring and `execute()` docstring contained "shell=True" substring — `grep 'shell=True' operator.py` returned a match, violating the acceptance criteria that mandates NO match
- **Fix:** Rephrased "never shell=True" to "no shell passthrough" — functionally identical security guidance, grep-clean. Identical pattern previously applied in 01-02 manifest.py
- **Files modified:** `aircontroler/operator.py` (lines 7 and 59)
- **Verification:** `grep 'shell=True' aircontroler/operator.py` returns exit code 1 (no matches); all 5 tests still pass
- **Committed in:** `ef18687` (included in GREEN commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 - bug: security grep false positive)
**Impact on plan:** Cosmetic docstring change only. No logic or behavior change. Security property strengthened.

## Issues Encountered

None — implementation matched plan specification exactly. The "shell=True" docstring issue is a known pattern established in 01-02 and resolved identically.

## TDD Gate Compliance

- **RED commit:** `4bcf0b3` — `test(01-03): add failing operator tests` — confirmed `ModuleNotFoundError: No module named 'aircontroler.operator'` before implementation
- **GREEN commit:** `ef18687` — `feat(01-03): implement DbtRunModelOperator` — all 5 tests pass

## Known Stubs

None — `DbtRunModelOperator` is fully implemented. `execute()` runs the actual dbt subprocess.

## Next Phase Readiness

- `DbtRunModelOperator` is ready for `DbtTaskFactory` (Plan 04) to instantiate at parse time
- Factory will call `DbtRunModelOperator(task_id=node.unique_id, unique_id=node.unique_id, project_dir=..., profiles_path=..., dag=dag)`
- No blockers for Plan 04

---
*Phase: 01-walking-skeleton*
*Completed: 2026-04-18*

## Self-Check: PASSED

Files exist:
- `aircontroler/operator.py` — FOUND (82 lines)
- `tests/unit/test_operator.py` — FOUND (5 real tests)
- `.planning/phases/01-walking-skeleton/01-03-SUMMARY.md` — FOUND (this file)

Commits exist:
- `4bcf0b3` — FOUND (test(01-03): add failing operator tests)
- `ef18687` — FOUND (feat(01-03): implement DbtRunModelOperator)
