# Phase 4: Cross-DAG Conflict Locking - Context

**Gathered:** 2026-04-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Add distributed conflict locking so that when multiple Airflow DAGs attempt to run the same dbt model concurrently, exactly one proceeds and the others skip or fail deterministically. Locking uses Postgres advisory locks (`pg_try_advisory_xact_lock`) held for the duration of the task execution. No `wait` policy in Phase 4 — v1 has `skip` and `fail` only (wait is LOCK-V2-01, explicitly deferred).

</domain>

<decisions>
## Implementation Decisions

### Conflict policy configuration (LOCK-01)
- **D-01:** Conflict policy follows the same **two-level override pattern** as `trigger_rule` and `pool`:
  - DAG-level default: `build_dbt_dag(conflict_policy='skip'|'fail'|None)`
  - Per-model override: `meta.aircontroler.conflict_policy: skip|fail` in schema.yml
  - Per-model overrides the DAG-level default (same `or` short-circuit precedence as pool)
- **D-02:** Default when neither level is set: **no locking** (conflict_policy=None). Tasks without a conflict policy behave exactly as before Phase 4 — zero backward-compatibility impact. This allows gradual opt-in per model or per DAG.

### Lock acquisition (LOCK-02)
- **D-03:** Use `pg_try_advisory_xact_lock(hash_id)` — the **non-blocking** variant. Returns `true` if lock acquired, `false` if already held by another session. Appropriate for `skip` and `fail` policies (both act immediately on conflict). The blocking `pg_advisory_xact_lock` is for v2 `wait` policy only.
- **D-04:** Lock hash is computed via **Python** (`abs(hash(unique_id)) % (2**31 - 1)` to stay within Postgres `int4` range). No extra DB round-trip to call `hashtext()`. The hash is deterministic within a Python version for a given unique_id string (note: `PYTHONHASHSEED` must be fixed or we use a stable hash like `mmh3` or `zlib.adler32`). Claude's discretion: use `zlib.adler32(unique_id.encode()) & 0x7FFFFFFF` for cross-process stability (not affected by PYTHONHASHSEED).
- **D-05:** Lock is acquired as the **first operation** inside `execute()` when conflict_policy is set — before the freshness check (satisfies LOCK-02). The advisory lock and the freshness check must be inside the **same DB session/transaction** so the lock is held until the transaction commits after `record_run()`.

### Transaction scope during execution
- **D-06:** When conflict_policy is set, `execute()` opens a **single `create_session()` block** spanning the entire task: lock acquisition → freshness check → dbt subprocess → record_run → lock_holder cleanup → commit. The DB connection is held for the full duration of the dbt run (seconds to minutes). This is acceptable on Cloud Run (one container per task) and LocalExecutor.
- **D-07:** When conflict_policy is **not set** (None), the existing two-separate-sessions pattern from Phase 3 is preserved (`get_last_run()` and `record_run()` each open their own `create_session()`). No regression for Phase 3 behavior.

### Hook API changes (MetadataDBHook)
- **D-08:** `get_last_run(unique_id, session=None)` and `record_run(..., session=None)` gain an optional `session` parameter. When `session` is provided, the method uses it directly (no new `create_session` opened). When `session=None` (default), the method opens its own `create_session` as today. Fully backward-compatible — no callers need updating unless they want the session-sharing behavior.
- **D-09:** A new `MetadataDBHook.acquire_lock(unique_id, session)` method executes `SELECT pg_try_advisory_xact_lock(:hash)` and returns `bool`. Called by `execute()` within the single open session.

### Lock-holder tracking (LOCK-04)
- **D-10:** Add an `aircontroler_lock_holders` table (separate Alembic migration): `unique_id VARCHAR(255) PRIMARY KEY`, `dag_id VARCHAR(250) NOT NULL`, `run_id VARCHAR(250) NOT NULL`, `locked_at TIMESTAMP NOT NULL`. Upsert on `unique_id` (last writer wins — handles stale rows from crashes).
- **D-11:** **Two-phase identity protocol** (solves Postgres READ COMMITTED visibility gap):
  1. **Micro-transaction** (quick commit): upsert identity into `lock_holders` with `dag_id`, `run_id`, `locked_at=now()`. Commit immediately. Row is now visible to all other tasks.
  2. **Main transaction**: acquire advisory lock → freshness check → subprocess → record_run → delete lock_holder row → commit.
  If the advisory lock is NOT acquired (step 2), query `lock_holders` for the holder's identity → raise error with "conflict: {uid} already locked by {holder.dag_id}/{holder.run_id}". Then delete own lock_holder row (micro-transaction cleanup).
- **D-12:** On task failure (dbt returns non-zero): the main transaction rolls back → advisory lock auto-releases. The lock_holder row was committed in a separate micro-transaction, so it persists. Phase 4 adds a `try/finally` in `execute()` to delete the lock_holder row on any exit path (success, skip, or failure). This prevents stale rows from accumulating after crashes.

### Non-Postgres / SQLite behavior
- **D-13:** At the start of `acquire_lock()`, detect the DB dialect: `session.bind.dialect.name` (or `session.get_bind().dialect.name`). If dialect is not `postgresql`, log a `WARNING` and return `True` (pretend lock was acquired — no conflict checking). This means: on SQLite dev setups, conflict_policy is silently no-op with a logged warning. No exception raised. Integration tests can mock `acquire_lock()` to return `True` or `False` to exercise skip/fail paths without Postgres.

### LOCK-05: sorted acquisition order
- **D-14:** In Phase 4, each `DbtRunModelOperator` acquires exactly **one** advisory lock (for its own `unique_id`). LOCK-05 (sorted order for multi-lock scenarios) is trivially satisfied. The `acquire_lock()` method documents the sorted-order requirement for future multi-lock callers (e.g., v2 batch mode).

### Claude's Discretion
- Exact Alembic migration revision ID and file name for `aircontroler_lock_holders`
- Whether `acquire_lock()` and `release_lock_holder()` are separate hook methods or part of a context manager
- Whether the lock_holder micro-transaction uses a separate `Session` object or a `sessionmaker` directly
- The `zlib.adler32` vs another stable hash for lock ID computation (as long as it's stable across processes)
- Whether the two Alembic migrations (model_runs from Phase 3 and lock_holders from Phase 4) share one `version` chain or are separate revision heads

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements
- `.planning/REQUIREMENTS.md` §Conflict Handling — LOCK-01 through LOCK-05 are the acceptance criteria
- `.planning/ROADMAP.md` §Phase 4 — Four success criteria define what must be TRUE at phase completion

### Phase 3 context (locked decisions that carry forward)
- `.planning/phases/03-freshness-policies/03-CONTEXT.md` — D-07 (MetadataDBHook uses create_session), D-08 (Airflow 2.x/3.x session pattern), D-09 (hook public API)
- `.planning/phases/02-execution-hardening/02-CONTEXT.md` — D-16 (two-level override precedence pattern, per-model > DAG-level)

### Architecture constraints
- `CLAUDE.md` §Critical Pitfalls — Pitfall 2: "Never use direct SQLAlchemy sessions from worker execute() methods" — hooks only. Pitfall 3: "Never use SELECT FOR UPDATE — use pg_advisory_xact_lock with sorted acquisition order."

### Existing integration points
- `aircontroler/hooks/metadata_db.py` — MetadataDBHook gains session param overloads and acquire_lock() method
- `aircontroler/operator.py` — execute() gains the single-session block when conflict_policy is set
- `aircontroler/factory.py` — build_dbt_dag() gains conflict_policy param; extracted from meta.aircontroler.conflict_policy
- `aircontroler/db_manager.py` — AircontrolerDBManager extended or a new migration added for lock_holders table

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `MetadataDBHook` in `aircontroler/hooks/metadata_db.py` — Phase 3 full implementation. Phase 4 adds `session=None` overloads and `acquire_lock()`. Zero regressions on existing callers.
- `ac_meta = node.config.get('meta', {}).get('aircontroler', {})` extraction in factory.py — already extracts pool, trigger_rule, freshness_hours. Add `ac_meta.get('conflict_policy') or conflict_policy` for zero-cost addition.
- `create_session()` from `airflow.utils.session` — already imported and tested; Phase 4 just holds it open longer.
- `AirflowSkipException` (compat shim) — already in operator for freshness skip; reuse for conflict skip.

### Established Patterns
- Conditional kwargs injection (Phase 2 / 3): only non-None values forwarded to BaseOperator. Same for conflict_policy.
- Two-level override pattern: `task_pool = ac_meta.get('pool') or pool or None` — mirror for `task_conflict_policy = ac_meta.get('conflict_policy') or conflict_policy`.
- `self.log.warning(...)` for non-fatal issues (e.g., SQLite dialect skip) — established in Phase 3 fail-open path.

### Integration Points
- `DbtRunModelOperator.__init__()` gains `conflict_policy: str | None = None`
- `DbtRunModelOperator.execute()` gains the single-session block (conditional on conflict_policy)
- `build_dbt_dag()` signature gains `conflict_policy: str | None = None`
- New Alembic migration for `aircontroler_lock_holders` table in `aircontroler/migrations/versions/`
- `aircontroler/db_manager.py` gains `lock_holders_table` SQLAlchemy Table definition

</code_context>

<specifics>
## Specific Ideas

- The two-phase lock-holder protocol (micro-tx commit, then main tx with advisory lock) solves the Postgres READ COMMITTED visibility problem. If Task B fails to acquire the lock, it can always see Task A's holder row because Task A committed it in a separate micro-transaction before acquiring the advisory lock.
- The `try/finally` cleanup pattern in `execute()` must handle all exit paths: success (normal), skip (AirflowSkipException on freshness), conflict skip (AirflowSkipException on lock), conflict fail (AirflowException), and dbt failure (AirflowException). In all cases, the lock_holder row must be deleted.
- "No conflict protection on SQLite" is the right dev-time trade-off. Integration tests that need to exercise skip/fail paths can mock `MetadataDBHook.acquire_lock` to return `False`.

</specifics>

<deferred>
## Deferred Ideas

- **LOCK-V2-01 (wait policy)** — `conflict_policy='wait'` using deferrable Trigger; requires Airflow deferrable operator infrastructure. Explicitly deferred to v2.
- **LOCK-V2-02 (wait timeout)** — Dependent on wait policy; v2.
- **OBS-V2-01 (conflict metrics counter)** — Count of conflict-triggered skips/fails per DAG run as an Airflow metric. Deferred to v2 observability work.

</deferred>

---

*Phase: 04-cross-dag-conflict-locking*
*Context gathered: 2026-04-19*
