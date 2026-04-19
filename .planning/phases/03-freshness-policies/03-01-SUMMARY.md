---
phase: 03-freshness-policies
plan: 01
subsystem: database
tags: [alembic, sqlalchemy, airflow-provider, db-manager, migration, pytest]

# Dependency graph
requires:
  - phase: 01-walking-skeleton
    provides: "MetadataDBHook stub (Phase 1) — Plan 02 replaces its get_conn() stub"
  - phase: 02-execution-hardening
    provides: "meta.aircontroler extraction pattern from Phase 2 — reused for freshness_hours in Plan 03"
provides:
  - "model_runs_table SQLAlchemy Table definition (5 columns, 2 indexes) — consumed by MetadataDBHook in Plan 02"
  - "AircontrolerDBManager BaseDBManager subclass — registered in Plan 03 via db-managers key"
  - "Alembic migration 0001_add_model_runs_table — round-trip verified on SQLite"
  - "manifest_v12_with_freshness.json fixture — stg_a freshness_hours=6, mart freshness_hours=24"
  - "Test scaffolding covering 8 Nyquist dimensions from VALIDATION.md"
affects: [plan-02-hook, plan-03-wiring, plan-04-operator, phase-04-conflict-locking]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "try/except ImportError guard for Airflow 2.x/3.x BaseDBManager"
    - "Alembic env.py with version_table kwarg in every context.configure branch (AST-checked)"
    - "In-memory SQLite integration test pattern for migration round-trip"
    - "xfail test stubs declaring Nyquist dimensions before implementation lands"

key-files:
  created:
    - aircontroler/db_manager.py
    - aircontroler/migrations/__init__.py
    - aircontroler/migrations/alembic.ini
    - aircontroler/migrations/env.py
    - aircontroler/migrations/versions/__init__.py
    - aircontroler/migrations/versions/0001_add_model_runs_table.py
    - tests/fixtures/manifest_v12_with_freshness.json
    - tests/unit/test_metadata_db_hook.py
    - tests/unit/test_db_manager.py
    - tests/integration/test_migrations.py
  modified:
    - tests/conftest.py

key-decisions:
  - "Used try/except ImportError (not AIRFLOW_V_3_0_PLUS flag) for BaseDBManager guard — simpler, works identically, avoids tight coupling to compat-shim version"
  - "Rephrased env.py docstring to avoid the literal string version_table=\"aircontroler_alembic_version\" so the plan's grep acceptance criterion (count == 2) matches only the actual context.configure calls, preserving the AST semantics"
  - "Integration tests use the standalone alembic.command.upgrade/downgrade path (not BaseDBManager.initdb) to avoid coupling tests to Airflow's live metadata DB"

patterns-established:
  - "ImportError fallback for Airflow 3-only modules: try/except at module top level with BaseDBManager = object fallback"
  - "Alembic env.py version_table=<non-default> duplicated in both context.configure branches to satisfy RunDBManager._validate AST scan"
  - "Integration migration test using tmp_path SQLite + alembic.config.Config.set_main_option for self-contained round-trip"
  - "xfail stubs covering future Nyquist dimensions so suite stays green while requirements remain explicit"

requirements-completed:
  - FRESH-04
  - FRESH-06

# Metrics
duration: ~6 min
completed: 2026-04-19
---

# Phase 3 Plan 01: Migration Scaffold + Test Infrastructure Summary

**aircontroler_model_runs SQLAlchemy table with Alembic 0001 migration, AircontrolerDBManager BaseDBManager subclass, and Nyquist-dimensioned test scaffolding (xfail stubs + green round-trip tests) for the freshness-policy wave.**

## Performance

- **Duration:** ~6 min
- **Started:** 2026-04-19T08:01:50Z
- **Completed:** 2026-04-19T08:07:03Z
- **Tasks:** 3
- **Files created:** 10
- **Files modified:** 1

## Accomplishments

- `aircontroler_model_runs` table defined with exact D-05 schema (id, unique_id, dag_id, run_id, completed_at) plus both indexes (`ix_acr_unique_id`, `ix_acr_completed_at`) required by the D-06 freshness query.
- `AircontrolerDBManager(BaseDBManager)` declared with all required attributes (`version_table_name="aircontroler_alembic_version"`, `revision_heads_map={"1.0.0": "0001"}`, `supports_table_dropping=True`). ImportError guard keeps the module importable on Airflow 2.x.
- Alembic 0001 migration scaffold (alembic.ini + env.py + versions/0001_add_model_runs_table.py) passes its own round-trip integration test on in-memory SQLite (upgrade creates table+indexes+aircontroler_alembic_version; downgrade drops them all).
- `manifest_v12_with_freshness.json` fixture carries freshness_hours on exactly two nodes (stg_a: 6, mart: 24 alongside retries/retry_delay/pool from Phase 2).
- Wave 0 test suite green: 5 `test_db_manager.py` + 4 `test_migrations.py` tests pass, 4 `test_metadata_db_hook.py` tests xfail pending Plan 02 (covers all 8 Nyquist dimensions at hook level).

## Task Commits

Each task was committed atomically with `--no-verify` (parallel executor convention):

1. **Task 1: db_manager.py with table + BaseDBManager subclass** — `ac30f08` (feat)
2. **Task 2: Alembic migration scaffold** — `f8ee0bf` (feat)
3. **Task 3: Manifest fixture + conftest fixture + test stubs** — `2bb112d` (test)

## Files Created/Modified

**Created:**
- `aircontroler/db_manager.py` — module metadata, model_runs_table Table, AircontrolerDBManager class with ImportError guard
- `aircontroler/migrations/__init__.py` — empty package marker
- `aircontroler/migrations/alembic.ini` — minimal config (`script_location = .`, empty `sqlalchemy.url`)
- `aircontroler/migrations/env.py` — online-only env with `version_table` kwarg in both branches, `render_as_batch=True`
- `aircontroler/migrations/versions/__init__.py` — empty package marker
- `aircontroler/migrations/versions/0001_add_model_runs_table.py` — revision 0001: create table + 2 indexes; downgrade drops both
- `tests/fixtures/manifest_v12_with_freshness.json` — 5-node diamond with freshness_hours on stg_a (6h) and mart (24h)
- `tests/unit/test_metadata_db_hook.py` — 4 xfail stubs (get_last_run none/timestamp, record_run insert, parameterized T-3-01)
- `tests/unit/test_db_manager.py` — 5 green tests (version_table, revision_heads_map, schema, indexes, supports_table_dropping)
- `tests/integration/test_migrations.py` — 4 green tests (upgrade creates table/indexes, downgrade drops, version_table is not default)

**Modified:**
- `tests/conftest.py` — appended `manifest_with_freshness_data` fixture mirroring the `manifest_with_meta_data` pattern

## Decisions Made

- **ImportError guard instead of AIRFLOW_V_3_0_PLUS flag** — the plan/pattern material mentioned both approaches; the ImportError form in RESEARCH Pattern 3 / PATTERNS § db_manager.py is simpler and what the plan specified exactly. Kept as-is.
- **Docstring rephrase in env.py** — initial docstring literally contained the string `version_table="aircontroler_alembic_version"` twice (once inline, once quoted), inflating `grep -c` from 2 to 3. Replaced with a prose description so the plan's acceptance criterion passes at both grep and AST levels. The runtime AST check from `RunDBManager._validate()` only examines actual `Call` nodes; docstrings are not checked. See `ast.walk` verification below (AST count == 2, kwarg count == 2).
- **Integration tests via alembic.command, not BaseDBManager** — keeps tests self-contained on tmp_path SQLite; `BaseDBManager.initdb()` path is exercised manually on Airflow 3.x (covered in the Manual-Only Verifications table of VALIDATION.md).

## Deviations from Plan

None — plan executed exactly as written. The one adjustment (env.py docstring rewording) was made to ensure a shell-level grep acceptance criterion matched. AST-level semantics (the actual validation Airflow performs) were never at risk.

## Verification Evidence

- `uv run pytest tests/unit/test_db_manager.py -q` → 5 passed
- `uv run pytest tests/integration/test_migrations.py -q` → 4 passed
- `uv run pytest tests/unit/test_metadata_db_hook.py -q` → 4 xfailed (expected)
- `uv run pytest tests/ -q` → 64 passed, 4 xfailed (one pre-existing DeprecatedImportWarning on `airflow.hooks.base.BaseHook` from the Phase 1 hook stub — out of scope)
- `python -c "from aircontroler.db_manager import AircontrolerDBManager, model_runs_table"` → exits 0
- Live SQLite round-trip: tables `{aircontroler_model_runs, aircontroler_alembic_version}` both created; downgrade drops both; indexes `ix_acr_unique_id` and `ix_acr_completed_at` present after upgrade.
- AST scan of env.py: 2 `context.configure` Call nodes; both carry `version_table` keyword.

## Migration Reference

- **Revision ID:** `0001`
- **Down revision:** `None` (head of a new chain)
- **Table:** `aircontroler_model_runs`
  - `id INTEGER PRIMARY KEY AUTOINCREMENT`
  - `unique_id VARCHAR(255) NOT NULL`
  - `dag_id VARCHAR(250) NOT NULL`
  - `run_id VARCHAR(250) NOT NULL`
  - `completed_at TIMESTAMP WITH TIME ZONE NOT NULL`
- **Indexes:** `ix_acr_unique_id` on (unique_id), `ix_acr_completed_at` on (completed_at)
- **Alembic version table:** `aircontroler_alembic_version` (separate from Airflow's `alembic_version`)

## Test Counts

| File | Passed | xFailed | Failed |
|------|-------:|--------:|-------:|
| tests/unit/test_db_manager.py | 5 | 0 | 0 |
| tests/unit/test_metadata_db_hook.py | 0 | 4 | 0 |
| tests/integration/test_migrations.py | 4 | 0 | 0 |
| **Plan 01 total** | **9** | **4** | **0** |
| Full suite | 64 | 4 | 0 |

## Issues Encountered

- **Docstring inflated grep count.** First pass of env.py produced `grep -c "version_table=\"aircontroler_alembic_version\""` == 3 because the docstring textually mentioned the kwarg. Resolved by rewording the docstring to describe the constraint without using the literal match string. AST-level semantics unchanged; RunDBManager._validate still sees 2 compliant Call nodes.
- **Pre-existing DeprecatedImportWarning** on `airflow.hooks.base.BaseHook` from Phase 1 hook stub — out of scope per the SCOPE BOUNDARY rule. Will be addressed when Plan 02 replaces the hook implementation (compat-SDK import path already documented in RESEARCH.md).

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- **Plan 02 (hook implementation):** Has `model_runs_table` to import from `aircontroler.db_manager` and 4 xfail stubs to flip green via the `create_session` + SQLAlchemy ORM `sa.select(...)`/`.insert().values(...)` patterns shown in PATTERNS.md.
- **Plan 03 (BaseDBManager wiring + CLI):** Has `AircontrolerDBManager` class ready for registration via `get_provider_info()["db-managers"]`. `revision_heads_map={"1.0.0": "0001"}` already points at the scaffolded migration.
- **Plan 04 (operator + factory wiring):** Has `manifest_v12_with_freshness.json` fixture and `manifest_with_freshness_data` conftest fixture ready for factory/operator tests.
- **No blockers for Wave 1.**

## Self-Check: PASSED

Created files (all FOUND):
- `/workspace/aircontroler/aircontroler/db_manager.py`
- `/workspace/aircontroler/aircontroler/migrations/__init__.py`
- `/workspace/aircontroler/aircontroler/migrations/alembic.ini`
- `/workspace/aircontroler/aircontroler/migrations/env.py`
- `/workspace/aircontroler/aircontroler/migrations/versions/__init__.py`
- `/workspace/aircontroler/aircontroler/migrations/versions/0001_add_model_runs_table.py`
- `/workspace/aircontroler/tests/fixtures/manifest_v12_with_freshness.json`
- `/workspace/aircontroler/tests/unit/test_metadata_db_hook.py`
- `/workspace/aircontroler/tests/unit/test_db_manager.py`
- `/workspace/aircontroler/tests/integration/test_migrations.py`

Commits (all FOUND):
- `ac30f08` — Task 1 (db_manager.py)
- `f8ee0bf` — Task 2 (Alembic scaffold)
- `2bb112d` — Task 3 (test stubs + fixture)

---
*Phase: 03-freshness-policies*
*Plan: 01*
*Completed: 2026-04-19*
