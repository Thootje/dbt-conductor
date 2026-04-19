---
phase: 04-cross-dag-conflict-locking
plan: 03
subsystem: testing
tags: [wave-3, xfail-activation, chaos-test, locking, tdd]
dependency_graph:
  requires: ["04-02"]
  provides: ["LOCK-01", "LOCK-02", "LOCK-03", "LOCK-04", "LOCK-05"]
  affects: []
tech_stack:
  added: []
  patterns:
    - "threading.Barrier(2) for real concurrent contention in unit tests"
    - "module-level create_session import for correct mocker.patch target"
    - "postgresql dialect mock for single-execute upsert path testing"
key_files:
  created: []
  modified:
    - tests/unit/test_lock_chaos.py
    - tests/unit/test_metadata_db_hook.py
    - tests/unit/test_operator.py
    - tests/unit/test_factory.py
    - aircontroler/operator.py
decisions:
  - "Promote create_session to module-level import in operator.py to enable mocker.patch('aircontroler.operator.create_session') — the test stubs patched this target but it wasn't at module scope after Wave 1"
  - "Set postgresql dialect on mock_session in test_upsert_lock_holder_writes_row — SQLite fallback executes DELETE+INSERT (2 calls) but test asserted call_count==1; postgresql path uses single pg_insert ON CONFLICT DO UPDATE"
metrics:
  duration: "~8 minutes"
  completed: "2026-04-19"
  tasks_completed: 2
  files_modified: 5
---

# Phase 4 Plan 03: Wave 2 Validation — xfail Activation Summary

**One-liner:** Replaced all 20 Wave 0 xfail stubs with real passing tests; full suite
goes from 104 passed / 20 xfail-or-xpass to 124 passed / 0 xfail.

## What Was Done

Wave 3 activated every Phase 4 xfail test stub by removing `@pytest.mark.xfail` decorators
and verifying each test exercises real Wave 1 production code. Two additional fixes were
required (Rule 1 deviations) to make previously-failing stubs pass.

### Task 1: Chaos Test + Hook Method Tests

**test_lock_chaos.py** — Replaced the stub (which returned "ran"/"skipped" without calling
any production code) with a real `threading.Barrier(2)` implementation. Two workers race
through the barrier before either calls `acquire_lock`; the mock deterministically returns
True for the first caller and False for the second. 50 iterations confirm exactly 50 runs
and 50 skips. Passes in ~2.2 seconds.

**test_metadata_db_hook.py** — Removed 9 `@pytest.mark.xfail(strict=False, ...)` decorators.
One body fix required (see Deviations).

### Task 2: Operator + Factory + Migration Tests

**test_operator.py** — Removed 8 xfail decorators. All conflict path tests now pass with the
module-level `create_session` patch target (see Deviations).

**test_factory.py** — Removed 2 xfail decorators. Both conflict_policy forwarding tests pass.

**test_migrations.py** — Confirmed Phase 4 integration tests were already plain
`@pytest.mark.integration` (no xfail), already passing. No changes needed.

## Final Suite State

```
124 passed, 0 xfailed, 0 xpassed, 1 warning in ~18s
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Promoted `create_session` to module-level import in operator.py**
- **Found during:** Task 2 (test_operator.py xfail removal)
- **Issue:** Wave 0 stubs patched `aircontroler.operator.create_session` but Wave 1 placed
  the import inside `execute()` at local scope. When xfail was removed, all 6 operator
  conflict tests failed with `AttributeError: <module 'aircontroler.operator'> does not
  have the attribute 'create_session'`.
- **Fix:** Added `from airflow.utils.session import create_session` to the module-level
  imports in `aircontroler/operator.py` and removed the redundant local import inside the
  `if self.conflict_policy is not None:` block.
- **Files modified:** `aircontroler/operator.py`
- **Commit:** 6e499f2

**2. [Rule 1 - Bug] Set postgresql dialect in test_upsert_lock_holder_writes_row**
- **Found during:** Task 1 (test_metadata_db_hook.py xfail removal)
- **Issue:** `upsert_lock_holder` has two code paths: PostgreSQL (single `pg_insert ON
  CONFLICT DO UPDATE` = 1 execute call) and SQLite (DELETE + INSERT = 2 execute calls).
  The mock session's `get_bind().dialect.name` returned a MagicMock (not "postgresql"),
  causing the SQLite fallback with 2 execute calls. The test asserted `call_count == 1`.
- **Fix:** Added `mock_session.get_bind.return_value.dialect.name = "postgresql"` to the
  test body so the single-execute PostgreSQL path is taken. Added a docstring note
  explaining the SQLite fallback behavior.
- **Files modified:** `tests/unit/test_metadata_db_hook.py`
- **Commit:** 6e499f2

## Known Stubs

None — all Phase 4 test stubs have been replaced with real assertions against Wave 1
production code.

## Threat Flags

None — this plan modifies only test files and adds one module-level import to operator.py.
No new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED

Files verified present:
- `tests/unit/test_lock_chaos.py` — contains `threading.Barrier(2)`, no xfail
- `tests/unit/test_metadata_db_hook.py` — contains 0 xfail Wave 1 markers
- `tests/unit/test_operator.py` — contains 0 xfail Wave 1 markers
- `tests/unit/test_factory.py` — contains 0 xfail Wave 1 markers
- `aircontroler/operator.py` — contains module-level `create_session` import

Commits verified:
- `6e499f2` — Task 1 (chaos test + hook tests)
- `87c2c3a` — Task 2 (operator + factory tests)

Final suite: `124 passed, 0 xfailed, 0 xpassed` confirmed via `uv run pytest tests/unit/ tests/integration/ -q`.
