---
phase: 02-execution-hardening
plan: "04"
subsystem: operator
tags: [operator, runtime, run-results, dbt-artifacts, integration-test, exec-02, security]

requires:
  - phase: 02-execution-hardening
    plan: "01"
    provides: "run_results_v6_data pytest fixture (used in unit tests)"
  - phase: 02-execution-hardening
    plan: "03"
    provides: "DbtRunModelOperator with target_path param (used by _run_results_path)"

provides:
  - "aircontroler/operator.py: _log_run_results() and _run_results_path() methods + execute() call site"
  - "tests/unit/test_operator.py: 7 new unit tests for run_results parsing (happy path, no-match, missing-file, skip-on-failure, path overrides, security regression)"
  - "tests/integration/test_run_results.py: 2 integration tests (real dbt build + missing-file degraded path)"

affects:
  - "Phase 3+: run_results parsing is the observability foundation for freshness TTL checks"

tech-stack:
  added: []
  patterns:
    - "Airflow 3 structlog capture in tests: mocker.patch.object(DbtRunModelOperator, 'log', new_callable=mocker.PropertyMock) — caplog does not capture structlog-based Airflow loggers"
    - "Security-constrained logging: only status/execution_time/adapter_response/rows_affected logged; compiled_code/relation_name explicitly excluded"
    - "Best-effort file parse: missing file or missing unique_id entry emits WARNING and returns, never raises"
    - "Integration test calls _log_run_results() directly with real dbt-written run_results.json to avoid dbt 1.11 selector vs unique_id mismatch"

key-files:
  created:
    - "tests/integration/test_run_results.py"
  modified:
    - "aircontroler/operator.py"
    - "tests/unit/test_operator.py"

key-decisions:
  - "Airflow 3 uses structlog (PrintLoggerFactory) not stdlib logging — pytest caplog cannot capture operator logs; tests patch DbtRunModelOperator.log property with PropertyMock instead"
  - "dbt 1.11 does not accept full unique_id format (model.fixture_project.stg_a) as a --select selector; integration test runs 'dbt build --select stg_a' then calls _log_run_results() directly with the correct unique_id from run_results"
  - "Security: operator.py contains compiled_code and relation_name only in the docstring (T-2-01 constraint) — no code path logs these fields"

metrics:
  duration: "~9 min"
  completed: "2026-04-19"
  tasks: 2
  files_modified: 3
---

# Phase 2 Plan 04: run_results.json Parsing (EXEC-02) Summary

**_log_run_results() and _run_results_path() added to DbtRunModelOperator — structured observability logs (status, execution_time, adapter_response, rows_affected) emitted after each successful dbt build, with security constraint T-2-01 enforced at source + unit + integration level.**

## Performance

- **Duration:** ~9 min
- **Started:** 2026-04-19T05:36:00Z
- **Completed:** 2026-04-19T05:45:24Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

### Task 1: _log_run_results, _run_results_path, execute() call site + 7 unit tests

Modified `aircontroler/operator.py` with two additions:

**Call site in execute() (after AirflowException raise block):**
```python
self._log_run_results()
```

**_run_results_path() method:**
```python
def _run_results_path(self):
    """Resolve the path to run_results.json based on target_path or project_dir."""
    from pathlib import Path
    if self.target_path:
        return Path(self.target_path) / "run_results.json"
    return Path(self.project_dir) / "target" / "run_results.json"
```

**_log_run_results() method:** Parses run_results.json, filters to the node matching `self.unique_id`, and emits:
- `dbt result: unique_id=%s status=%s execution_time=%.3fs`
- `dbt result: rows_affected=%s` (conditional — only when present in adapter_response)
- `dbt result: adapter_response=%s`

Missing file → WARNING + return. Missing entry → WARNING + return. JSON parse error → WARNING + return. Never raises.

**Security (T-2-01):** Only `status`, `execution_time`, `adapter_response` (full dict, but contains no SQL), and `rows_affected` are logged. `compiled_code` and `relation_name` are not referenced anywhere in the method body. The grep confirms:
```
grep -n "compiled_code\|relation_name" aircontroler/operator.py
# Only match: docstring comment (not a code path)
```

**D-10 enforcement:** `self._log_run_results()` is placed AFTER the `if result.returncode != 0: raise AirflowException(...)` block. On failure the raise exits `execute()` before the call is reached.

**7 new unit tests** added to `tests/unit/test_operator.py`:

| Test | Behavior |
|------|----------|
| `test_run_results_logging` | Happy path: matching entry found, structured log emitted |
| `test_run_results_no_match_warns` | unique_id not in results → WARNING "no entry for unique_id" |
| `test_run_results_missing_file_warns` | File absent → WARNING "run_results.json not found" |
| `test_run_results_skipped_on_failure` | returncode=1 → AirflowException raised, no WARNING from _log_run_results |
| `test_target_path_controls_run_results_path` | target_path='/explicit/target' → correct Path returned |
| `test_default_run_results_path` | No target_path → {project_dir}/target/run_results.json |
| `test_run_results_does_not_log_compiled_code_or_relation` | Security regression: no compiled_code/SQL/relation_name in any log call |

**Key deviation (Rule 1 — Bug):** Plan tests used `caplog` with `r.message`/`r.levelno`, but Airflow 3 uses structlog with `PrintLoggerFactory` — stdlib `logging` records are never emitted, so `caplog` always receives empty records. Fixed by patching `DbtRunModelOperator.log` as a `PropertyMock` and asserting on `mock_log.info.call_args_list` / `mock_log.warning.call_args_list` instead.

### Task 2: Integration tests

Created `tests/integration/test_run_results.py` with 2 tests:

**test_run_results_logged_after_real_build:**
- Copies fixture DuckDB project to tmp_path (no fixture dir pollution)
- Runs `dbt build --select stg_a` (real subprocess, exits 0)
- Reads real run_results.json, discovers actual unique_id written by dbt
- Calls `op._log_run_results()` with matching unique_id
- Asserts `dbt result:` and `unique_id=model.fixture_project.stg_a` in info logs
- Asserts `compiled_code` and `relation_name` absent from all logs (T-2-01)

**test_run_results_missing_warns_in_real_run:**
- Stubs subprocess (exit 0, no file written)
- Asserts `run_results.json not found` WARNING emitted gracefully

**Key deviation (Rule 1 — Bug):** Plan's integration test used `op.execute({})` with `unique_id=stg_a_uid` (full format `model.fixture_project.stg_a`). In dbt 1.11, `--select model.fixture_project.stg_a` is not a valid selector — dbt exits 0 with "Nothing to do" (empty run_results.json). Fixed by running `dbt build --select stg_a` directly (subprocess call) and calling `op._log_run_results()` with the correct unique_id discovered from the real run_results output.

## Log Format Emitted

```
dbt result: unique_id=model.fixture_project.stg_a status=success execution_time=0.028s
dbt result: adapter_response={'_message': 'OK'}
# rows_affected line only emitted when present in adapter_response:
dbt result: rows_affected=<N>
```

## Path Resolution Rule (D-07)

- `target_path` set: `{target_path}/run_results.json`
- `target_path` unset: `{project_dir}/target/run_results.json`

## Task Commits

1. **Task 1 RED: Failing unit tests** — `4f07b4d` (test)
2. **Task 1 GREEN: Implementation + updated tests** — `8afcefb` (feat)
3. **Task 2: Integration tests** — `32856b3` (feat)

## Files Created/Modified

- `/workspace/aircontroler/aircontroler/operator.py` — `_log_run_results()`, `_run_results_path()`, `self._log_run_results()` call site in `execute()`
- `/workspace/aircontroler/tests/unit/test_operator.py` — 7 new unit tests; helper functions `_make_mock_log()` and `_format_log_calls()` for structlog-aware log capture
- `/workspace/aircontroler/tests/integration/test_run_results.py` — 2 integration tests (new file)

## Total Test Count (Phase 2 Complete)

| Suite | Count |
|-------|-------|
| Phase 1 unit tests | 21 |
| PLAN 02 factory tests (cardinality, max_active_tasks) | 8 |
| PLAN 03 operator tests (per-model overrides) | 7 |
| PLAN 03 factory tests (meta forwarding, pool precedence) | 8 |
| PLAN 04 operator unit tests (run_results parsing) | 7 |
| PLAN 01+02 integration tests (dag wiring, model execution) | 2 |
| PLAN 04 integration tests (run_results parsing) | 2 |
| **Total** | **55** |

All 55 tests pass. Full suite command: `uv run --extra test pytest tests/ -x -q` exits 0.

## Security (T-2-01) Enforcement

Enforced at three levels:
1. **Source**: `grep "compiled_code\|relation_name" aircontroler/operator.py` → only docstring match (no code paths)
2. **Unit**: `test_run_results_does_not_log_compiled_code_or_relation` — asserts no SQL string, no `compiled_code` key, no `relation_name` key in any mock_log call
3. **Integration**: `test_run_results_logged_after_real_build` — asserts same constraint against real dbt-written run_results.json

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Airflow 3 structlog incompatibility with pytest caplog**
- **Found during:** Task 1 (RED → GREEN)
- **Issue:** Plan tests used `caplog.at_level(logging.INFO)` and `r.message`/`r.levelno` on `caplog.records`. Airflow 3 uses `structlog.PrintLoggerFactory` — log calls go to stdout, never to the stdlib `logging` system. `caplog.records` always empty.
- **Fix:** Replaced all `caplog`-based assertions with `mocker.patch.object(DbtRunModelOperator, 'log', new_callable=mocker.PropertyMock, return_value=mock_log)`. Added `_make_mock_log()` and `_format_log_calls()` helpers to reconstruct %-formatted log message strings from `mock_log.info.call_args_list`.
- **Files modified:** `tests/unit/test_operator.py`
- **Commit:** `8afcefb`

**2. [Rule 1 - Bug] dbt 1.11 selector incompatibility with full unique_id format**
- **Found during:** Task 2 (integration test execution)
- **Issue:** Plan's integration test called `op.execute({})` with `unique_id="model.fixture_project.stg_a"`. dbt 1.11 does not accept this format as a `--select` selector — exits 0 with "Nothing to do" (empty run_results.json). Operator then emits "no entry for unique_id" WARNING, never the happy-path `dbt result:` log.
- **Fix:** Integration test now runs `dbt build --select stg_a` directly (valid selector), reads the actual unique_id from the written run_results.json, then calls `op._log_run_results()` directly with the matching unique_id. Tests the parsing path with real dbt output without subprocess-selector mismatch.
- **Files modified:** `tests/integration/test_run_results.py`
- **Commit:** `32856b3`

## Known Stubs

None — `_log_run_results()` and `_run_results_path()` are fully implemented. The pre-existing `service_account` stub in factory.py is from Phase 1 (out of scope).

## Threat Flags

None — no new network endpoints, auth paths, or schema changes. File I/O (reading run_results.json) was the planned threat surface; T-2-01 mitigations are enforced.

## Self-Check: PASSED
