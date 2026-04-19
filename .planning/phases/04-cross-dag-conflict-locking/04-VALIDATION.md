---
phase: 4
slug: cross-dag-conflict-locking
status: validated
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-19
validated: 2026-04-19
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 8.x + pytest-mock 3.14+ |
| **Config file** | `pyproject.toml` [tool.pytest.ini_options] |
| **Quick run command** | `uv run pytest tests/unit/ -q` |
| **Full suite command** | `uv run pytest tests/ -q` |
| **Estimated runtime** | ~25 seconds |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/ -q`
- **After every plan wave:** Run `uv run pytest tests/ -q`
- **Before `/gsd-verify-work`:** Full suite must be green (124 tests)
- **Max feedback latency:** ~25 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 4-01-01 | 01 | 0 | LOCK-04 | T-4-01 | `lock_holders` table created via Alembic migration, schema correct | integration | `uv run pytest tests/integration/test_migrations.py -k "lock_holders" -x` | Yes | green |
| 4-01-02 | 01 | 0 | LOCK-01/04 | -- | `lock_holders_table` exported from db_manager; revision_heads_map updated | unit | `uv run pytest tests/unit/test_db_manager.py -k "revision_heads_map" -x` | Yes | green |
| 4-01-03 | 01 | 0 | LOCK-02/03/04 | T-4-01 | Unit tests for operator conflict paths (skip, fail, ordering, cleanup) | unit | `uv run pytest tests/unit/test_operator.py -k "conflict" -x` | Yes | green |
| 4-01-04 | 01 | 0 | LOCK-01/02 | -- | Unit tests for hook acquire_lock and lock_holder methods | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -k "lock" -x` | Yes | green |
| 4-01-05 | 01 | 0 | LOCK-05 | -- | LOCK-05 chaos test (50-iteration determinism) | unit | `uv run pytest tests/unit/test_lock_chaos.py -x` | Yes | green |
| 4-02-01 | 02 | 1 | LOCK-02 | T-4-01 | `acquire_lock()` executes parameterized SQL, returns bool | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -k "acquire_lock" -x` | Yes | green |
| 4-02-02 | 02 | 1 | LOCK-02 | -- | SQLite dialect: `acquire_lock()` returns True + logs WARNING | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -k "sqlite" -x` | Yes | green |
| 4-02-03 | 02 | 1 | LOCK-04 | -- | `upsert_lock_holder` writes row; `delete_lock_holder` removes it | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -k "lock_holder" -x` | Yes | green |
| 4-02-04 | 02 | 1 | LOCK-02 | -- | Lock acquired BEFORE freshness check in execute() | unit | `uv run pytest tests/unit/test_operator.py -k "lock_acquired_before_freshness" -x` | Yes | green |
| 4-02-05 | 02 | 1 | LOCK-03 | -- | `acquire_lock` -> False + conflict_policy=skip -> AirflowSkipException | unit | `uv run pytest tests/unit/test_operator.py -k "conflict_skip" -x` | Yes | green |
| 4-02-06 | 02 | 1 | LOCK-04 | -- | `acquire_lock` -> False + conflict_policy=fail -> AirflowException with holder info | unit | `uv run pytest tests/unit/test_operator.py -k "conflict_fail" -x` | Yes | green |
| 4-02-07 | 02 | 1 | LOCK-04 | -- | `try/finally` cleans up lock_holder on success, skip, and fail paths | unit | `uv run pytest tests/unit/test_operator.py -k "finally_cleanup" -x` | Yes | green |
| 4-02-08 | 02 | 1 | LOCK-01 | -- | Factory extracts `conflict_policy` via two-level override | unit | `uv run pytest tests/unit/test_factory.py -k "conflict_policy" -x` | Yes | green |
| 4-03-01 | 03 | 2 | LOCK-05 | -- | 50-iteration concurrent task pair test runs clean (mocked acquire_lock) | unit | `uv run pytest tests/unit/test_lock_chaos.py -x` | Yes | green |
| 4-03-02 | 03 | 2 | all | -- | Full suite green: no regressions on Phase 1-3 tests | integration | `uv run pytest tests/ -q` | Yes | green |

*Status: pending -- green -- red -- flaky*

---

## Wave 0 Requirements

- [x] `tests/unit/test_lock_chaos.py` -- real 50-iteration threading.Barrier chaos test
- [x] `tests/unit/test_metadata_db_hook.py` -- real tests for LOCK-02/04 hook methods (`acquire_lock`, `upsert_lock_holder`, `delete_lock_holder`)
- [x] `tests/unit/test_operator.py` -- real tests for LOCK-02/03/04 operator conflict paths
- [x] `tests/unit/test_factory.py` -- real tests for LOCK-01 factory `conflict_policy` param
- [x] `tests/integration/test_migrations.py` -- real tests for LOCK-04 `lock_holders` table migration

*All Wave 0 stubs replaced with real assertions in Wave 2 (Plan 04-03). Zero xfail markers remain.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Real Postgres advisory lock held during subprocess | LOCK-02 | No Postgres in dev/CI | Query `pg_locks` during a held lock on a Postgres instance |
| Two concurrent real DAG runs produce exactly one dbt execution | LOCK-03 | Requires live Airflow + Postgres | Start two DAG runs simultaneously on the same model; confirm one skips |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** validated by Nyquist auditor 2026-04-19
**Suite result:** 124 passed, 0 xfailed, 0 failures (25.31s)
