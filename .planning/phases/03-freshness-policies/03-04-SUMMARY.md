---
phase: 03-freshness-policies
plan: "04"
subsystem: testing
tags: [sqlite, airflow, integration-test, round-trip, alembic, sqlalchemy]

# Dependency graph
requires:
  - phase: 03-freshness-policies plan 01
    provides: Alembic migration creating aircontroler_model_runs table
  - phase: 03-freshness-policies plan 02
    provides: MetadataDBHook.record_run() and get_last_run() contracts
  - phase: 03-freshness-policies plan 03
    provides: DbtRunModelOperator freshness_hours wiring through factory

provides:
  - Integration test conftest fixture sqlite_airflow_db that provisions a real SQLite DB
  - 5 end-to-end integration tests covering FRESH-03, FRESH-04, and FRESH-06 requirements
  - All 8 Nyquist dimensions from VALIDATION.md verified by automated tests
  - Full Phase 3 test suite green at 101 tests (88 unit + 13 integration)

affects:
  - gsd-verify-work Phase 3 gate
  - Any future Phase 4+ plans that add integration tests

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Airflow 3.x integration test pattern: set env var, reload settings, call configure_vars() + configure_orm() explicitly to wire scoped_session"
    - "pytest sqlite_airflow_db fixture: monkeypatch + Alembic upgrade against tmp_path"
    - "Direct engine insertion for seeding stale rows without mocking the hook"

key-files:
  created:
    - tests/integration/conftest.py
    - tests/integration/test_freshness_roundtrip.py
  modified: []

key-decisions:
  - "Airflow 3.x create_session requires settings.Session != None; importlib.reload() alone does not call configure_vars()/configure_orm(). Must call both explicitly after reload."
  - "Two env vars set defensively: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN (Airflow 3) and AIRFLOW__CORE__SQL_ALCHEMY_CONN (Airflow 2 legacy) for cross-version compatibility."
  - "Stale-row seeding in test_operator_runs_and_records_when_stale_end_to_end uses create_engine(sqlite_airflow_db) directly rather than mocking the hook — real DB insertion to set completed_at to 100h ago."

patterns-established:
  - "Integration test pattern: yield db_url from fixture so tests can bind sqlalchemy engines directly for inspection"
  - "Operator integration test: mock subprocess.run, let the real hook and session machinery execute against SQLite"

requirements-completed:
  - FRESH-03
  - FRESH-04
  - FRESH-06

# Metrics
duration: 15min
completed: 2026-04-19
---

# Phase 3 Plan 04: Integration Round-Trip Tests Summary

**SQLite-backed integration tests covering MetadataDBHook round-trip + DbtRunModelOperator skip/stale paths, with Airflow 3.x configure_orm fix making all 8 Nyquist dimensions green**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-04-19T08:26:23Z
- **Completed:** 2026-04-19T08:35:00Z
- **Tasks:** 3 (Tasks 1, 2, 3 complete)
- **Files modified:** 2

## Accomplishments

- Created `tests/integration/conftest.py` with the `sqlite_airflow_db` fixture that provisions a tmp_path SQLite DB, reloads and reconfigures Airflow settings, and applies the aircontroler Alembic migration
- Created `tests/integration/test_freshness_roundtrip.py` with 5 integration tests covering FRESH-03, FRESH-04, and FRESH-06 at the real-DB level
- Full suite is green at 101 tests (88 unit + 13 integration); no regressions

## Integration Conftest Design

### Env Var Reload Pattern

The `sqlite_airflow_db` fixture uses the following pattern to wire Airflow to a tmp_path SQLite DB:

1. `monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", db_url)` — sets before any Airflow import
2. `monkeypatch.setenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN", db_url)` — defensive Airflow 2.x compat
3. `importlib.reload(airflow.settings)` — re-reads module, but Session is still None
4. `airflow_settings.configure_vars()` — reads AIRFLOW__DATABASE__SQL_ALCHEMY_CONN into SQL_ALCHEMY_CONN
5. `airflow_settings.configure_orm()` — builds the scoped_session factory and sets Session
6. `importlib.reload(airflow.utils.session)` — picks up updated settings.Session reference
7. `alembic command.upgrade(cfg, "head")` — creates aircontroler_model_runs on the test DB

### Why the Plan's Reload Pattern Required a Fix

The plan's original pattern (`importlib.reload(airflow_settings)` + `importlib.reload(airflow_session)`) was insufficient in Airflow 3.x. In Airflow 3, `settings.Session` starts as `None` and is only populated when `configure_orm()` is called explicitly (via `initialize()` or directly). A plain module reload re-executes top-level code but does NOT call `configure_vars()` or `configure_orm()`, so `Session` remains `None`. The fix adds explicit calls after the reload.

## Round-Trip Test Coverage

| Test | Nyquist Dims | Requirements |
|------|-------------|--------------|
| `test_record_and_get_last_run_roundtrip` | — (DB correctness) | FRESH-04 |
| `test_multiple_records_return_max` | — (MAX semantics) | FRESH-04 |
| `test_other_unique_id_does_not_leak` | — (ID isolation) | FRESH-04 / T-3-01 |
| `test_operator_skip_when_fresh_end_to_end` | 1 (fresh → skip) | FRESH-03, FRESH-04 |
| `test_operator_runs_and_records_when_stale_end_to_end` | 2 (stale → run), 4 (success records) | FRESH-03, FRESH-04 |

## Final Test Counts

| Suite | Count |
|-------|-------|
| `tests/unit/` | 88 |
| `tests/integration/test_migrations.py` | 4 |
| `tests/integration/test_freshness_roundtrip.py` | 5 |
| `tests/integration/test_dag_wiring.py` | 2 |
| `tests/integration/test_run_results.py` | 2 |
| **Full suite** | **101** |

## Nyquist Dimension Coverage (All 8)

| Dim | Behavior | Unit Test | Integration Test |
|-----|----------|-----------|-----------------|
| 1 | Fresh → skip | `test_operator.py::test_skip_when_fresh` | `test_freshness_roundtrip.py::test_operator_skip_when_fresh_end_to_end` |
| 2 | Stale → run | `test_operator.py::test_no_skip_when_stale` | `test_freshness_roundtrip.py::test_operator_runs_and_records_when_stale_end_to_end` |
| 3 | No TTL → always run | `test_operator.py::test_no_skip_when_no_ttl` | — |
| 4 | Success records run | `test_operator.py::test_record_run_called_on_success` | `test_freshness_roundtrip.py::test_operator_runs_and_records_when_stale_end_to_end` |
| 5 | Failure does not record | `test_operator.py::test_record_run_not_called_on_failure` | — |
| 6 | Skip does not record | `test_operator.py::test_record_run_not_called_on_skip` | — |
| 7 | none_failed topology | `test_factory.py::test_none_failed_permits_skipped_upstream_topology` | — |
| 8 | Fail-open on missing table | `test_operator.py::test_fail_open_on_missing_table` | — |

All 8 dimensions have at least 1 automated test. Dims 1, 2, and 4 additionally have integration-level coverage.

## Phase 3 Requirements Completion

| Requirement | Description | Satisfied By |
|-------------|-------------|-------------|
| FRESH-01 | `freshness_hours` in manifest meta → factory passes to operator | `test_factory.py` freshness tests (Plan 03) |
| FRESH-03 | AirflowSkipException on fresh; dbt runs when stale | `test_operator.py` freshness tests (Plan 02) + integration roundtrip (Plan 04) |
| FRESH-04 | record_run writes completed_at; get_last_run reads MAX | `test_metadata_db_hook.py` (Plan 02) + integration roundtrip (Plan 04) |
| FRESH-06 | Alembic migration creates aircontroler_model_runs + custom version table | `test_migrations.py` (Plan 01) + integration roundtrip (Plan 04) |

## Task Commits

1. **Task 1: Create integration conftest** - `4636fa7` (feat)
2. **Task 2: Round-trip tests + conftest fix** - `c5dac47` (feat)
3. **Task 3: Full suite verification** - (no files modified; verification only)

## Files Created/Modified

- `/workspace/aircontroler/tests/integration/conftest.py` - sqlite_airflow_db fixture with configure_orm fix
- `/workspace/aircontroler/tests/integration/test_freshness_roundtrip.py` - 5 end-to-end integration tests

## Decisions Made

- Airflow 3.x requires explicit `configure_vars()` + `configure_orm()` calls after reload — importlib.reload() alone is insufficient to populate `settings.Session`
- Two env vars set defensively for Airflow 2/3 cross-version compatibility
- Stale-row seeding uses direct SQLAlchemy engine insertion (not mocked hook) to set `completed_at` to a past datetime — this is the correct approach since mocking the hook would defeat the purpose of integration testing

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Airflow 3.x create_session: importlib.reload() insufficient to wire Session**
- **Found during:** Task 2 (running test_freshness_roundtrip.py for the first time)
- **Issue:** All 5 integration tests failed with `RuntimeError: Session must be set before!`. The plan's reload pattern worked in Airflow 2.x where settings initialized at module load, but in Airflow 3.x `settings.Session = None` until `configure_orm()` is explicitly called.
- **Fix:** After `importlib.reload(airflow_settings)`, added `airflow_settings.configure_vars()` then `airflow_settings.configure_orm()` to populate the scoped_session factory.
- **Files modified:** `tests/integration/conftest.py`
- **Verification:** All 5 integration tests pass after fix
- **Committed in:** `c5dac47` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 - bug in plan's reload pattern for Airflow 3.x)
**Impact on plan:** Essential correctness fix. The configure_orm addition is the minimum required to make create_session functional in Airflow 3.x. No scope creep.

## Issues Encountered

- Airflow 3.x changed how `settings.Session` is initialized: it no longer auto-initializes on module load — it requires an explicit `configure_orm()` call. The plan's pattern assumed Airflow 2.x behavior. Fixed with Rule 1 auto-fix (2 lines added to conftest).

## Known Stubs

None — all tests exercise real code paths against a real SQLite DB. No placeholders or hardcoded returns.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes introduced. Test files only; no production surface added.

## Next Phase Readiness

- Phase 3 freshness machinery is fully tested end-to-end against a real SQLite DB
- All 8 Nyquist dimensions have automated coverage
- Full suite green at 101 tests
- Ready for `/gsd-verify-work` Phase 3 gate

## Self-Check: PASSED

- `tests/integration/conftest.py` exists: FOUND
- `tests/integration/test_freshness_roundtrip.py` exists: FOUND
- Commit `4636fa7` (Task 1): FOUND
- Commit `c5dac47` (Task 2 + conftest fix): FOUND
- All 101 tests pass: CONFIRMED

---
*Phase: 03-freshness-policies*
*Completed: 2026-04-19*
