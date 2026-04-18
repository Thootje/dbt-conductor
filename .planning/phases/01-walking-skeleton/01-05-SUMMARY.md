---
phase: 01-walking-skeleton
plan: "05"
subsystem: testing
tags: [integration-tests, dbt-duckdb, fixture-project, dbt-parse, subprocess, dag-wiring]
dependency_graph:
  requires:
    - "01-02"  # ManifestLoader, parse_manifest, ManifestNode
    - "01-03"  # DbtRunModelOperator
    - "01-04"  # build_dbt_dag() factory
  provides:
    - tests/fixtures/dbt_project (5-model diamond dbt-duckdb fixture project)
    - tests/integration/test_dag_wiring.py (2 integration tests replacing stubs)
  affects:
    - Phase 2 (Execution Hardening) — integration suite serves as regression baseline
tech_stack:
  added:
    - dbt-duckdb>=1.8.0 (installed via [test] extra, dbt=1.11.8 in .venv)
    - .gitignore (excludes dbt target/, logs/, .user.yml from commits)
  patterns:
    - module-scoped pytest fixture for dbt parse (run once per session, tmp_path isolation)
    - _dbt_available() guard: tests skip gracefully when dbt not on PATH
    - --target-path tmp_path: dbt parse writes to tmp dir, not fixture dir (T-01-05-02 mitigated)
key_files:
  created:
    - tests/fixtures/dbt_project/dbt_project.yml
    - tests/fixtures/dbt_project/profiles.yml
    - tests/fixtures/dbt_project/models/stg_a.sql
    - tests/fixtures/dbt_project/models/stg_b.sql
    - tests/fixtures/dbt_project/models/int_a.sql
    - tests/fixtures/dbt_project/models/int_b.sql
    - tests/fixtures/dbt_project/models/mart.sql
    - .gitignore
  modified:
    - tests/integration/test_dag_wiring.py (stubs replaced with real implementations)
    - .planning/phases/01-walking-skeleton/01-VALIDATION.md (all Status cells updated to green)
key-decisions:
  - "module-scoped generated_manifest fixture: dbt parse runs once per session; --target-path tmp_path prevents fixture dir pollution"
  - "_dbt_available() via shutil.which('dbt'): tests skip gracefully when dbt absent; no test failures in environments without dbt"
  - ".gitignore added to exclude dbt-generated artifacts (target/, logs/, .user.yml) from version control"
requirements-completed:
  - CORE-01
  - CORE-02
  - CORE-03
  - CORE-04
  - CORE-05
  - CORE-06
  - CORE-07
  - CORE-08
  - CORE-09
duration: ~4min
completed: "2026-04-18"
---

# Phase 01 Plan 05: Integration Tests + dbt-duckdb Fixture Project Summary

**dbt-duckdb fixture project (5-model diamond) and 2 integration tests that exercise the full parse-manifest → dbt-ls → DAG-wiring → real-subprocess pipeline against in-memory DuckDB — 23 tests pass (21 unit + 2 integration), 90% coverage.**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-04-18T08:11:15Z
- **Completed:** 2026-04-18T08:14:49Z
- **Tasks:** 2
- **Files created:** 9 (7 fixture files + .gitignore + SUMMARY.md)
- **Files modified:** 2 (test_dag_wiring.py, 01-VALIDATION.md)

## Accomplishments

- Created 5-model diamond dbt project using dbt-duckdb (in-memory DuckDB, no cloud credentials needed) at `tests/fixtures/dbt_project/`
- Replaced both pytest.skip stub functions in `test_dag_wiring.py` with real integration tests
- `test_full_pipeline_wires_correctly`: calls build_dbt_dag() with real manifest and real dbt ls; asserts 5 tasks, 2 entry (stg_a, stg_b), 1 exit (mart), mart upstream contains int_a and int_b
- `test_model_execution_via_subprocess`: instantiates DbtRunModelOperator for stg_a and calls execute({}); verifies no exception (dbt build exit 0 against DuckDB :memory:)
- Full suite: 23 passed, 90% coverage — Phase 1 walking skeleton complete

## Task Commits

1. **Task 1: Create dbt-duckdb fixture project** - `9c2d151` (feat)
2. **Task 2: Implement integration tests** - `e58adfb` (feat)

## Test Results

### Integration tests

```
tests/integration/test_dag_wiring.py::test_full_pipeline_wires_correctly PASSED
tests/integration/test_dag_wiring.py::test_model_execution_via_subprocess PASSED

2 passed in 12.33s
```

### Full suite with coverage

```
tests/integration/test_dag_wiring.py::test_full_pipeline_wires_correctly PASSED
tests/integration/test_dag_wiring.py::test_model_execution_via_subprocess PASSED
tests/unit/test_factory.py::test_task_per_model PASSED
tests/unit/test_factory.py::test_edge_wiring PASSED
tests/unit/test_factory.py::test_entry_exit_classification PASSED
tests/unit/test_factory.py::test_service_account_warning PASSED
tests/unit/test_init.py::test_provider_info PASSED
tests/unit/test_init.py::test_version_exported PASSED
tests/unit/test_manifest.py::test_parse_v11 PASSED
tests/unit/test_manifest.py::test_parse_v12 PASSED
tests/unit/test_manifest.py::test_unknown_version_raises PASSED
tests/unit/test_manifest.py::test_manifest_path_validated_at_parse_time PASSED
tests/unit/test_manifest.py::test_compile_on_parse PASSED
tests/unit/test_operator.py::test_import_compat PASSED
tests/unit/test_operator.py::test_subprocess_command PASSED
tests/unit/test_operator.py::test_custom_executable PASSED
tests/unit/test_operator.py::test_failure_raises PASSED
tests/unit/test_operator.py::test_profiles_path PASSED
tests/unit/test_selector.py::test_extract_select PASSED
tests/unit/test_selector.py::test_extract_exclude PASSED
tests/unit/test_selector.py::test_extract_selector PASSED
tests/unit/test_selector.py::test_parse_ls_output PASSED
tests/unit/test_selector.py::test_ls_failure_raises PASSED

================================ tests coverage ================================
Name                                Stmts   Miss  Cover   Missing
-----------------------------------------------------------------
aircontroler/__init__.py                4      0   100%
aircontroler/factory.py                28      0   100%
aircontroler/hooks/__init__.py          0      0   100%
aircontroler/hooks/metadata_db.py       9      9     0%   7-26
aircontroler/manifest.py               62      7    89%   75, 119, 121, 125-127, 146
aircontroler/operator.py               20      0   100%
aircontroler/selector.py               39      1    97%   102
-----------------------------------------------------------------
TOTAL                                 162     17    90%
============================= 23 passed in 12.61s ==============================
```

## Files Created/Modified

- `tests/fixtures/dbt_project/dbt_project.yml` — Minimal dbt project config (fixture_project, test_profile)
- `tests/fixtures/dbt_project/profiles.yml` — dbt-duckdb in-memory profile (type: duckdb, path: :memory:)
- `tests/fixtures/dbt_project/models/stg_a.sql` — Source model: `select 1 as id, 'foo' as val`
- `tests/fixtures/dbt_project/models/stg_b.sql` — Source model: `select 2 as id, 'bar' as val`
- `tests/fixtures/dbt_project/models/int_a.sql` — Intermediate: refs stg_a
- `tests/fixtures/dbt_project/models/int_b.sql` — Intermediate: refs stg_b
- `tests/fixtures/dbt_project/models/mart.sql` — Diamond apex: refs int_a and int_b via join
- `tests/integration/test_dag_wiring.py` — Stubs replaced with 2 real integration tests
- `.gitignore` — Excludes dbt-generated artifacts (target/, logs/, .user.yml, .coverage)

## VALIDATION.md Sign-Off

All 19 per-task verification items updated from ⬜ pending to ✅ green:
- manifest-parse-v11, manifest-parse-v12, manifest-unknown-version, manifest-compile-on-parse
- selector-extract-select, selector-extract-exclude, selector-extract-selector, selector-parse-ndjson, selector-ls-failure
- factory-task-per-model, factory-edge-wiring, factory-entry-exit
- operator-subprocess, operator-custom-executable, operator-failure-raises, operator-profiles-path, operator-compat-import
- provider-info
- integration-full-pipeline

`nyquist_compliant: true`, `wave_0_complete: true` set in frontmatter.

## Decisions Made

- **module-scoped generated_manifest fixture**: dbt parse runs once per test session, not once per test. The target directory uses `tmp_path_factory.mktemp()` so parallel test runs don't collide and the fixture directory stays clean.
- **_dbt_available() guard**: Integration tests skip gracefully (not fail) when dbt is not on PATH. Enables running unit suite in environments without dbt installed.
- **.gitignore added**: No `.gitignore` existed in the repo. Added to exclude dbt-generated files (`target/`, `logs/`, `.user.yml`) and Python artifacts (`.coverage`, `__pycache__`, `.venv`). This is Rule 2 (missing critical functionality — generated files must not be committed).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added .gitignore to prevent dbt artifacts from being committed**
- **Found during:** Task 2 (integration test verification)
- **Issue:** Running `dbt ls` manually during verification wrote `target/`, `logs/`, and `.user.yml` into the fixture project directory. No `.gitignore` existed in the repo, so these would be tracked by git.
- **Fix:** Created `.gitignore` excluding dbt-generated output directories and standard Python artifacts before committing task 2.
- **Files modified:** `.gitignore` (created)
- **Verification:** `git status --short` shows only expected files after adding .gitignore
- **Committed in:** `e58adfb` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Necessary for correct repo hygiene. No scope creep — the fix only prevents unintended file tracking.

## Issues Encountered

- dbt is not on the bare system PATH; it is available only inside the uv virtual environment (`.venv/bin/dbt`). Integration tests work correctly because pytest runs via `uv run python -m pytest`, which activates the venv and puts `.venv/bin/dbt` on PATH. `shutil.which("dbt")` returns the correct path in this context.

## Known Stubs

None — all integration test stubs replaced with real assertions. `DbtRunModelOperator.execute()` runs a real dbt subprocess. `build_dbt_dag()` calls real dbt ls.

## Next Phase Readiness

Phase 1 Walking Skeleton is complete. All 9 CORE requirements (CORE-01 through CORE-09) are verified by passing tests. Phase 2 (Execution Hardening: EXEC-01 cardinality guard, EXEC-02 error propagation, EXEC-03 retry policy) can begin immediately.

- Integration test suite serves as regression baseline for Phase 2
- `aircontroler/hooks/metadata_db.py` is a stub with 0% coverage — Phase 2 will implement the MetadataDBHook fully

## Threat Flags

No new trust boundary surface introduced beyond what was planned in the threat model.

- T-01-05-02 (Tampering: dbt parse writes target/ to fixture dir): MITIGATED — `--target-path tmp_path` used in generated_manifest fixture; fixture directory stays clean.

---

*Phase: 01-walking-skeleton*
*Completed: 2026-04-18*

## Self-Check: PASSED

Files exist:
- `tests/fixtures/dbt_project/dbt_project.yml` — FOUND
- `tests/fixtures/dbt_project/profiles.yml` — FOUND
- `tests/fixtures/dbt_project/models/stg_a.sql` — FOUND
- `tests/fixtures/dbt_project/models/stg_b.sql` — FOUND
- `tests/fixtures/dbt_project/models/int_a.sql` — FOUND
- `tests/fixtures/dbt_project/models/int_b.sql` — FOUND
- `tests/fixtures/dbt_project/models/mart.sql` — FOUND
- `tests/integration/test_dag_wiring.py` — FOUND (stubs replaced)
- `.gitignore` — FOUND
- `.planning/phases/01-walking-skeleton/01-05-SUMMARY.md` — FOUND (this file)

Commits exist:
- `9c2d151` — FOUND (feat(01-05): create dbt-duckdb fixture project)
- `e58adfb` — FOUND (feat(01-05): implement integration tests replacing stubs)
