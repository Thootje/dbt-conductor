# Phase 4: Cross-DAG Conflict Locking - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-19
**Phase:** 04-cross-dag-conflict-locking
**Areas discussed:** Config surface, Transaction scope, Hook API, SQLite fallback, Lock holder identification, LOCK-05

---

## Config surface

| Option | Description | Selected |
|--------|-------------|----------|
| Both levels (two-level pattern) | build_dbt_dag(conflict_policy=...) + meta.aircontroler.conflict_policy per-model | ✓ |
| Per-model only | schema.yml meta only, no DAG-level param | |

**User's choice:** Both levels — same two-level override pattern as trigger_rule and pool.

---

## Transaction scope

| Option | Description | Selected |
|--------|-------------|----------|
| Single session held open | One create_session() spanning lock + freshness + subprocess + record_run | ✓ |
| Two-phase: lock then release | Separate micro-tx for advisory lock, subprocess runs without DB connection | |

**User's choice:** Single session held open — lock held for full dbt duration.

---

## Hook API

| Option | Description | Selected |
|--------|-------------|----------|
| Session-aware overloads | Optional `session` param on get_last_run() and record_run() | ✓ |
| New run_with_lock() method | Encapsulate lock+freshness+record cycle in one hook method with fn() callback | |

**User's choice:** Session-aware overloads — backward-compatible, zero breaking changes.

---

## SQLite / non-Postgres fallback

| Option | Description | Selected |
|--------|-------------|----------|
| Warn and skip locking | Detect non-Postgres at runtime, log WARNING, proceed without conflict protection | ✓ |
| Raise at runtime | NotImplementedError if conflict_policy set on non-Postgres DB | |
| Python threading.Lock fallback | Process-local lock for LocalExecutor; no-op on distributed | |

**User's choice:** Warn and skip — dev stays functional, integration tests use mocks for skip/fail path coverage.

---

## Lock holder identification

| Option | Description | Selected |
|--------|-------------|----------|
| Lock-holders table | aircontroler_lock_holders table with dag_id, run_id; two-phase protocol for READ COMMITTED visibility | ✓ |
| Omit DAG identity | Simpler error message without holder attribution | |
| pg_stat_activity lookup | Complex, requires pg_monitor role | |

**User's choice:** Lock-holders table — precise attribution in fail error messages.

**Notes:** Two-phase protocol required: micro-transaction commits identity before main transaction acquires advisory lock, solving Postgres READ COMMITTED visibility gap.

---

## LOCK-05 (sorted multi-lock order)

| Option | Description | Selected |
|--------|-------------|----------|
| One lock per task in Phase 4 | Each operator acquires its own unique_id lock only; sorted order trivially satisfied | ✓ |
| Multi-lock scenario exists | (Not specified by user) | |

**User's choice:** One lock per task. LOCK-05 documented for future multi-lock callers.

---

## Claude's Discretion

- Stable hash function for lock ID (zlib.adler32 vs mmh3 — cross-process stability required)
- Exact two-phase micro-transaction implementation details
- Alembic migration chain structure for lock_holders table
- acquire_lock() as method vs context manager
