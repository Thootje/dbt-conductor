---
phase: 01-walking-skeleton
plan: "02"
subsystem: core-modules
tags: [manifest, selector, hooks, provider-registration, tdd]
dependency_graph:
  requires:
    - "01-01"   # pyproject.toml, test fixtures, test stubs
  provides:
    - aircontroler.__init__ (get_provider_info, __version__)
    - aircontroler.manifest (ManifestNode, parse_manifest, ManifestLoader)
    - aircontroler.selector (SelectorResolver)
    - aircontroler.hooks.metadata_db (MetadataDBHook stub)
  affects:
    - "01-03"   # DbtTaskFactory imports ManifestNode, parse_manifest
    - "01-04"   # DbtModelOperator — no direct dependency but shares contract
    - "01-05"   # DAG integration test imports the full chain
tech_stack:
  added:
    - dataclasses (ManifestNode)
    - subprocess.run as list[str] (dbt parse, dbt ls)
    - re.compile (selector flag extraction)
    - airflow.providers.common.compat.sdk.BaseHook (hooks stub)
  patterns:
    - version-dispatch pattern (v11/v12 manifest schema URL)
    - NDJSON parsing with silent skip of non-JSON log lines
    - hook-layer guard for Airflow 3 worker isolation
key_files:
  created:
    - aircontroler/__init__.py (21 lines)
    - aircontroler/manifest.py (148 lines)
    - aircontroler/selector.py (111 lines)
    - aircontroler/hooks/__init__.py (0 lines — package marker)
    - aircontroler/hooks/metadata_db.py (29 lines)
  modified:
    - tests/unit/test_manifest.py (stubs replaced with 5 real tests)
    - tests/unit/test_init.py (stubs replaced with 2 real tests)
    - tests/unit/test_selector.py (stubs replaced with 5 real tests)
decisions:
  - "v11/v12 dispatch: both versions use identical _parse_nodes() — shared implementation, branching only on version URL string"
  - "comment text avoids 'shell=True' substring to pass grep-based security checks without ambiguity"
  - "MetadataDBHook.get_conn() raises NotImplementedError as Phase 1 placeholder; Phase 3 implements"
metrics:
  duration: "~15 minutes"
  completed: "2026-04-18T08:00:28Z"
  tasks_completed: 2
  tasks_total: 2
  files_created: 5
  files_modified: 3
  tests_passed: 12
  tests_total: 12
---

# Phase 01 Plan 02: Core Modules (manifest, selector, hooks) Summary

**One-liner:** Manifest v11/v12 version-dispatch parser, dbt-ls-delegating selector resolver, and BaseHook-extending MetadataDBHook stub — all 12 unit tests green.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Implement `__init__.py` and `manifest.py` | `28d97b0` | `aircontroler/__init__.py`, `aircontroler/manifest.py`, `tests/unit/test_manifest.py`, `tests/unit/test_init.py` |
| 2 | Implement `selector.py` and hooks package stub | `803d636` | `aircontroler/selector.py`, `aircontroler/hooks/__init__.py`, `aircontroler/hooks/metadata_db.py`, `tests/unit/test_selector.py` |

## Modules Implemented

| File | Lines | Exports |
|------|-------|---------|
| `aircontroler/__init__.py` | 21 | `__version__ = "0.1.0"`, `get_provider_info()` |
| `aircontroler/manifest.py` | 148 | `ManifestNode`, `parse_manifest()`, `ManifestLoader` |
| `aircontroler/selector.py` | 111 | `SelectorResolver` |
| `aircontroler/hooks/__init__.py` | 0 | (package marker) |
| `aircontroler/hooks/metadata_db.py` | 29 | `MetadataDBHook` |

## Test Results

```
tests/unit/test_manifest.py::test_parse_v11 PASSED
tests/unit/test_manifest.py::test_parse_v12 PASSED
tests/unit/test_manifest.py::test_unknown_version_raises PASSED
tests/unit/test_manifest.py::test_manifest_path_validated_at_parse_time PASSED
tests/unit/test_manifest.py::test_compile_on_parse PASSED
tests/unit/test_selector.py::test_extract_select PASSED
tests/unit/test_selector.py::test_extract_exclude PASSED
tests/unit/test_selector.py::test_extract_selector PASSED
tests/unit/test_selector.py::test_parse_ls_output PASSED
tests/unit/test_selector.py::test_ls_failure_raises PASSED
tests/unit/test_init.py::test_provider_info PASSED
tests/unit/test_init.py::test_version_exported PASSED

12 passed in 0.04s
```

## Security Properties Confirmed

```
grep -n 'shell=True' aircontroler/manifest.py aircontroler/selector.py
→ no output (zero matches)

grep -n 'from airflow.providers.common.compat.sdk' aircontroler/hooks/metadata_db.py
→ 9:from airflow.providers.common.compat.sdk import BaseHook
```

All STRIDE threat mitigations applied:
- **T-01-02-01**: `parse_manifest()` opens path directly — `FileNotFoundError` raised immediately on missing path; resolved absolute path logged at INFO level
- **T-01-02-02**: All subprocess calls use `list[str]`; zero occurrences of `shell=True`; only specific flags extracted from dbt_command (not verbatim passthrough)
- **T-01-02-03**: Only executable path and flag names logged; `profiles_dir` path logged (not contents); env secrets never captured
- **T-01-02-04**: `MetadataDBHook` extends `BaseHook` — architectural guard preventing direct SQLAlchemy from workers established in Phase 1

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Docstring contained literal 'shell=True' substring**
- **Found during:** Task 1 acceptance criteria verification
- **Issue:** `ManifestLoader._run_dbt_parse()` docstring read "never shell=True" — the grep-based security check `grep 'shell=True' manifest.py` matched the comment, producing a false positive
- **Fix:** Rephrased to "no shell passthrough" — functionally identical guidance, grep-clean
- **Files modified:** `aircontroler/manifest.py` line 136
- **Commit:** `28d97b0` (included in same commit after fix)

## TDD Gate Compliance

Both tasks followed RED/GREEN pattern:
- **Task 1 RED:** Tests were skipped stubs (7 skipped) before implementation
- **Task 1 GREEN:** 7 tests passed after implementing `__init__.py` and `manifest.py`
- **Task 2 RED:** `ModuleNotFoundError: No module named 'aircontroler.selector'` confirmed before implementation
- **Task 2 GREEN:** 5 tests passed after implementing `selector.py`

## Known Stubs

| File | Stub | Reason |
|------|------|--------|
| `aircontroler/hooks/metadata_db.py` | `get_conn()` raises `NotImplementedError` | Intentional Phase 1 placeholder; Phase 3 implements full DB access via advisory locks |

This stub does NOT prevent this plan's goal — the plan's objective is the Phase 1 stub, not a working DB connection.

## Self-Check: PASSED

Files exist:
- `aircontroler/__init__.py` — FOUND
- `aircontroler/manifest.py` — FOUND
- `aircontroler/selector.py` — FOUND
- `aircontroler/hooks/__init__.py` — FOUND
- `aircontroler/hooks/metadata_db.py` — FOUND

Commits exist:
- `28d97b0` — FOUND (feat(01-02): implement __init__.py and manifest.py)
- `803d636` — FOUND (feat(01-02): implement selector.py and hooks package stub)
