---
phase: 04-cross-dag-conflict-locking
verified: 2026-04-19T11:25:00Z
status: human_needed
score: 5/6 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Run two real concurrent Airflow DAG runs against a live Postgres instance, both targeting the same model with conflict_policy='skip', and confirm exactly one dbt subprocess executes"
    expected: "One task completes, one task shows AirflowSkipException in logs. Only one dbt build appears in Postgres query logs."
    why_human: "No Postgres in dev/CI. The advisory lock behavior (pg_try_advisory_xact_lock) cannot be exercised without a real Postgres connection — all unit tests mock acquire_lock. SC1 and SC3 both require live Postgres verification."
  - test: "Query pg_locks during an active task execution with conflict_policy set, confirming the advisory transaction lock is held for the full duration of the dbt subprocess"
    expected: "SELECT * FROM pg_locks WHERE locktype = 'advisory' shows a row during subprocess execution; row disappears after commit."
    why_human: "Requires live Postgres and timing coordination. Cannot inspect pg_locks programmatically in unit/integration tests."
---

# Phase 4: Cross-DAG Conflict Locking Verification Report

**Phase Goal:** Cross-DAG conflict locking — LOCK-01 through LOCK-05 requirements fully implemented and tested
**Verified:** 2026-04-19T11:25:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| #  | Truth | Status | Evidence |
|----|-------|--------|----------|
| 1  | LOCK-01: User can configure conflict_policy (skip/fail) per model — operator stores it, factory extracts two-level override | VERIFIED | `conflict_policy` in `DbtRunModelOperator.__init__`, `template_fields`; `task_conflict_policy = ac_meta.get("conflict_policy") or conflict_policy` in factory.py:151; 2 factory tests pass |
| 2  | LOCK-02: Postgres advisory locks acquired before freshness check, released at transaction end | VERIFIED | `acquire_lock()` calls `pg_try_advisory_xact_lock(:h)` with adler32 hash; `acquire_lock` called at line 132 in execute(), before `get_last_run` at line 148; `test_lock_acquired_before_freshness_check` passes |
| 3  | LOCK-03: conflict_policy=skip causes losing task to raise AirflowSkipException | VERIFIED | operator.py line 141: `raise AirflowSkipException(msg)`; `test_conflict_skip_raises_airflow_skip_exception` passes |
| 4  | LOCK-04: conflict_policy=fail causes losing task to raise AirflowException with holder info | VERIFIED | operator.py line 143: `raise AirflowException(msg)` with holder info from `get_lock_holder`; `test_conflict_fail_raises_airflow_exception` passes; `try/finally` deletes lock_holder on all exit paths (3 path tests pass) |
| 5  | LOCK-05: 50-iteration chaos test proves exactly one run and one skip per concurrent pair | VERIFIED | `test_lock_determinism_50_iterations` passes in ~2.2s using `threading.Barrier(2)` for real contention, no xfail markers |
| 6  | Real Postgres advisory lock held during subprocess and inspectable via pg_locks (SC3 from ROADMAP) | HUMAN NEEDED | All unit tests mock `acquire_lock`. Cannot verify pg_locks behavior without live Postgres. |

**Score:** 5/6 truths verified (6th requires human verification with live Postgres)

**Note on SC3/SC4 implementation divergence:** ROADMAP.md SC3 specifies `pg_advisory_xact_lock(hashtext(unique_id))` but the implementation uses `pg_try_advisory_xact_lock` (non-blocking) with `zlib.adler32`. ROADMAP.md SC4 specifies multi-lock sorted acquisition but D-14 in 04-CONTEXT.md explicitly resolves this: "In Phase 4, each DbtRunModelOperator acquires exactly one advisory lock — LOCK-05 is trivially satisfied." These are documented design decisions made during the planning phase, not implementation gaps.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `aircontroler/migrations/versions/0002_add_lock_holders_table.py` | DDL for lock_holders table | VERIFIED | `revision="0002"`, `down_revision="0001"`, `op.create_table("aircontroler_lock_holders", ...)` with 4 columns, `PrimaryKeyConstraint("unique_id")` |
| `aircontroler/db_manager.py` | lock_holders_table + revision_heads_map | VERIFIED | `lock_holders_table` Table with unique_id PK, dag_id, run_id, locked_at; `revision_heads_map = {"1.0.0": "0001", "1.1.0": "0002"}` |
| `aircontroler/hooks/metadata_db.py` | acquire_lock, upsert/delete/get lock_holder, session overloads | VERIFIED | All 6 methods present; `get_last_run(session=None)`, `record_run(session=None)`; `acquire_lock` returns bool; `upsert/delete_lock_holder` use `create_session(scoped=False)` |
| `aircontroler/operator.py` | conflict_policy param + single-session execute() | VERIFIED | `conflict_policy` in `__init__` and `template_fields`; two-phase protocol in `execute()`; `try/finally` cleanup on all paths |
| `aircontroler/factory.py` | build_dbt_dag conflict_policy param + two-level override | VERIFIED | `conflict_policy: str | None = None` in signature; `task_conflict_policy = ac_meta.get("conflict_policy") or conflict_policy`; forwarded to `DbtRunModelOperator` |
| `tests/unit/test_lock_chaos.py` | Real 50-iteration threading.Barrier test | VERIFIED | Contains `threading.Barrier(2)`, `concurrent.futures.ThreadPoolExecutor`, asserts `run_count==50` and `skip_count==50`; NO xfail markers |
| `tests/unit/test_metadata_db_hook.py` | 9 real hook method tests (no xfail) | VERIFIED | All 9 Phase 4 tests present with no `xfail` markers; grep confirmed 0 matches for "Wave 1 implementation pending" |
| `tests/unit/test_operator.py` | 8 real conflict operator tests (no xfail) | VERIFIED | All 8 Phase 4 conflict tests present with no `xfail` markers |
| `tests/unit/test_factory.py` | 2 real factory conflict_policy tests (no xfail) | VERIFIED | Both tests present with no `xfail` markers |
| `tests/integration/test_migrations.py` | 3 lock_holders migration tests | VERIFIED | `test_migration_creates_lock_holders_table`, `test_migration_downgrade_removes_lock_holders_table`, `test_migration_downgrade_to_0001_removes_only_lock_holders` all pass |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `db_manager.py` | `0002_add_lock_holders_table.py` | `revision_heads_map["1.1.0"] = "0002"` | WIRED | Confirmed at line 74: `revision_heads_map = {"1.0.0": "0001", "1.1.0": "0002"}` |
| `hooks/metadata_db.py` | `db_manager.py lock_holders_table` | `from aircontroler.db_manager import lock_holders_table` | WIRED | Confirmed in `upsert_lock_holder` (line 119), `delete_lock_holder` (line 163), `get_lock_holder` (line 178) |
| `operator.py execute()` | `MetadataDBHook.upsert_lock_holder` | micro-transaction before main session | WIRED | Lines 123-127: `hook.upsert_lock_holder(...)` called BEFORE `with create_session() as session:` at line 130 |
| `operator.py execute()` | `MetadataDBHook.acquire_lock` | first call inside main session | WIRED | Line 132: `acquired = hook.acquire_lock(self.unique_id, session)` |
| `operator.py execute()` | `MetadataDBHook.delete_lock_holder` | `finally` block | WIRED | Lines 211-215: `finally: ... hook.delete_lock_holder(self.unique_id)` |
| `factory.py build_dbt_dag()` | `operator.py DbtRunModelOperator` | `conflict_policy=task_conflict_policy` | WIRED | Line 166: `conflict_policy=task_conflict_policy` in constructor call |

### Data-Flow Trace (Level 4)

Phase 4 adds locking infrastructure (hooks, operator flags, factory params). No new data rendering components. The `lock_holders_table` reads flow through `get_lock_holder` to the conflict error message — data path is: DB row → `fetchone()` → `holder.dag_id/holder.run_id` → `AirflowException` message string. This is correctly wired and tested.

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|-------------------|--------|
| `operator.py execute()` | `holder` (lock holder info) | `hook.get_lock_holder(unique_id, session)` | Yes — reads from `aircontroler_lock_holders` table | FLOWING |
| `operator.py execute()` | `acquired` (bool) | `hook.acquire_lock(unique_id, session)` | Yes — calls `pg_try_advisory_xact_lock(:h)` | FLOWING |
| `hooks/metadata_db.py acquire_lock` | `lock_id` | `zlib.adler32(unique_id.encode()) & 0x7FFFFFFF` | Yes — deterministic hash | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All unit tests pass | `uv run pytest tests/unit/ -q` | 108 passed, 1 warning in 2.83s | PASS |
| All integration tests pass | `uv run pytest tests/integration/ -q -m integration` | 16 passed, 1 warning in 18.42s | PASS |
| db_manager exports lock_holders_table + correct revision_heads_map | `python -c "from aircontroler.db_manager import lock_holders_table, AircontrolerDBManager; assert lock_holders_table.name == 'aircontroler_lock_holders'; assert AircontrolerDBManager.revision_heads_map == {'1.0.0': '0001', '1.1.0': '0002'}; print('OK')"` | OK | PASS |
| MetadataDBHook has all 6 Phase 4 methods | `python -c "from aircontroler.hooks.metadata_db import MetadataDBHook; ..."` | ALL STRUCTURAL CHECKS PASSED | PASS |
| Zero xfail Wave 1 markers remain | `grep -c "xfail.*Wave 1 implementation pending" tests/unit/test_lock_chaos.py ...` | 0 in all 5 files | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| LOCK-01 | 04-01, 04-02, 04-03 | conflict_policy param per model, two-level override | SATISFIED | Factory two-level override at factory.py:151; operator stores at operator.py:96; 2 factory tests + 2 operator storage tests pass |
| LOCK-02 | 04-01, 04-02, 04-03 | Postgres advisory locks acquired before freshness check | SATISFIED | `acquire_lock()` uses `pg_try_advisory_xact_lock(:h)` with adler32 hash; ordering enforced and tested by `test_lock_acquired_before_freshness_check` |
| LOCK-03 | 04-01, 04-02, 04-03 | skip policy raises AirflowSkipException | SATISFIED | `raise AirflowSkipException(msg)` at operator.py:141; confirmed by `test_conflict_skip_raises_airflow_skip_exception` |
| LOCK-04 | 04-01, 04-02, 04-03 | fail policy raises AirflowException; lock_holders table; try/finally cleanup | SATISFIED | `raise AirflowException(msg)` at operator.py:143; 3 migration tests pass; 3 finally_cleanup path tests pass |
| LOCK-05 | 04-01, 04-02, 04-03 | Deterministic lock acquisition — deadlock prevention | SATISFIED | D-14: single lock per task trivially satisfies sorted-order requirement; `test_lock_determinism_50_iterations` proves 50-iteration determinism with real threading.Barrier |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `aircontroler/factory.py` | 87 | `"service_account is not yet implemented"` | Info | Pre-existing Phase 1 stub for CLOUD feature, not Phase 4 — no impact on LOCK requirements |
| `aircontroler/operator.py` | 171-185 & 252-265 | Duplicated dbt cmd construction | Info | Maintenance issue flagged in 04-REVIEW.md WR-01; no functional impact on Phase 4 |

No blockers. No stubs in Phase 4 production code paths.

### Human Verification Required

#### 1. Live Postgres advisory lock behavior (SC1 + SC3)

**Test:** Set up two concurrent Airflow task runs against a live Postgres-backed Airflow instance. Both target the same dbt model with `conflict_policy='skip'`. Trigger both tasks simultaneously.

**Expected:** Exactly one task completes with dbt subprocess execution. The second task raises `AirflowSkipException` containing "conflict: model.X already locked by dag_Y/run_Z". Only one `dbt build` subprocess appears in Postgres query logs (`pg_stat_activity`).

**Why human:** `acquire_lock` uses `pg_try_advisory_xact_lock` which requires a real Postgres connection. All unit/integration tests mock `acquire_lock` at the hook level. SQLite (used in CI) returns `True` unconditionally per D-13.

#### 2. Advisory lock held during subprocess duration (SC3)

**Test:** With conflict_policy set, add `SELECT * FROM pg_locks WHERE locktype = 'advisory'` query at the start and end of a dbt subprocess execution to confirm the lock is held for the full duration.

**Expected:** Advisory lock row present in `pg_locks` while subprocess runs; absent after session commits.

**Why human:** Requires live Postgres with timing coordination. Cannot be tested in unit/integration suite without a real database.

### Gaps Summary

No functional gaps. All LOCK-01 through LOCK-05 production code is implemented, wired, and tested by real (non-xfail) tests. The full suite passes at 124 tests (108 unit + 16 integration), zero failures, zero xfail markers remaining in Phase 4 test files.

The human verification items are about confirming behavior against a real Postgres database, which is expected at this stage — the VALIDATION.md for this phase explicitly lists "Real Postgres advisory lock held during subprocess" and "Two concurrent real DAG runs" as manual-only verifications.

**Implementation note on SC3/SC4 wording in ROADMAP.md:** The roadmap uses `pg_advisory_xact_lock(hashtext(unique_id))` but 04-CONTEXT.md D-03/D-04 explicitly decided `pg_try_advisory_xact_lock` (non-blocking) and `zlib.adler32`. The roadmap wording was written before the implementation decisions were finalized. The implementation choices are more appropriate (non-blocking avoids potential hang; adler32 avoids PYTHONHASHSEED dependency).

---

_Verified: 2026-04-19T11:25:00Z_
_Verifier: Claude (gsd-verifier)_
