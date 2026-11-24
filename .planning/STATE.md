---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: "### Phase 1: Walking Skeleton"
status: in-progress
stopped_at: "Phase 4 Plan 03 complete. Wave 2 validation: all xfail stubs activated, 124 tests green."
last_updated: "2026-04-19T10:45:00Z"
progress:
  total_phases: 5
  completed_phases: 3
  total_plans: 16
  completed_plans: 16
  percent: 100
---

# Project State

## Current Status

**Phase:** Phase 4 — Cross-DAG Conflict Locking (planned, ready to execute)
**Active Phase:** Phase 4
**Last Updated:** 2026-04-19

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-17)
**Core value:** A dbt DAG where every model is a first-class Airflow task, with cross-DAG freshness policies and conflict resolution ensuring no model runs more times than necessary.
**Current focus:** Phase 3

## Phase Progress

| Phase | Status | Plans | Progress |
|-------|--------|-------|----------|
| 1. Walking Skeleton | ✓ Complete | 5/5 | 100% |
| 2. Execution Hardening | ✓ Complete | 4/4 | 100% |
| 3. Freshness Policies | ✓ Complete | 4/4 | 100% |
| 4. Cross-DAG Conflict Locking | ✓ Complete | 3/3 | 100% |
| 5. Rename project to dbt-conductor | Not started | 0/0 | 0% |
| 6. Add proper CI setup | Not started | 0/0 | 0% |
| 7. Cloud Run + Distribution | Not started | 0/0 | 0% |

## Notes

- Roadmap derived from 5-category requirement structure: CORE (9), EXEC (3), FRESH (6), LOCK (5), CLOUD (3). Each category maps 1:1 to a phase.
- Phases 3 and 4 deliver the two primary differentiators over Cosmos (cross-DAG freshness, cross-DAG conflict locking). Shipping the headline features requires reaching at least Phase 4.
- Research SUMMARY.md flags several cross-cutting architectural decisions that must land in Phase 1 (manifest-as-input default, static task loop vs `.expand()`, dedicated hook for metadata DB access) — these unblock Phases 3 and 4.
- v2 scope (wait mode, observability, additional adapters, Airflow connection credentials) is intentionally deferred; do not expand Phase 5 to absorb it.

## Decisions

- Wave 0 (02-01): Created manifest_v12_with_meta.json as a separate file (not modifying manifest_v12.json) to keep Phase 1 tests stable
- Wave 0 (02-01): run_results fixture uses execution_time=0.028, adapter_response={"_message":"OK"} matching real DuckDB output (no rows_affected)
- Wave 1 (02-02): EXEC-03 cardinality guard uses strict > (not >=) so exactly-at-limit does not raise
- Wave 1 (02-02): env var AIRCONTROLER_MAX_MODELS read via int() with no try/except — fail loud per spec
- Wave 1 (02-02): dag.max_active_tasks assigned only when explicitly set; None preserves Airflow's default of 16
- Wave 2 (02-03): Conditional kwargs injection pattern — only non-None values forwarded to BaseOperator; preserves retries=0/retry_delay=timedelta(300)/pool defaults when unset
- Wave 2 (02-03): retry_delay int (seconds) converted to timedelta inside operator __init__ before BaseOperator injection (Pitfall 4 fix)
- Wave 2 (02-03): pool_slots applied only when task_pool is truthy: pool_slots if task_pool else 1
- Wave 3 (02-04): Airflow 3 uses structlog (PrintLoggerFactory) not stdlib logging — test log capture requires PropertyMock patch on DbtRunModelOperator.log, not pytest caplog
- Wave 3 (02-04): dbt 1.11 does not accept full unique_id format (model.fixture_project.stg_a) as --select selector; integration test calls _log_run_results() directly with real dbt-written run_results.json
- Wave 3 (02-04): Security T-2-01 enforced at source (no code path logs compiled_code/relation_name), unit (security regression test), and integration (real dbt output) levels
- Wave 0 (04-01): lock_holders_table uses unique_id as direct VARCHAR(255) primary key (no surrogate id) — supports upsert semantics per D-10; no op.create_index needed (PK is already indexed)
- Wave 0 (04-01): Migration 0002 chains off 0001 (down_revision='0001') forming a single linear revision head; revision_heads_map extended to include 1.1.0 -> 0002
- Wave 0 (04-01): Rule 1 fix: test_db_manager.py revision_heads_map assertion updated from {"1.0.0": "0001"} to {"1.0.0": "0001", "1.1.0": "0002"} to match new correct value

- Wave 1 (04-02): Removed duplicate local AirflowException import inside conflict_policy if-block — Python scoping makes any assignment in a function create a local binding, causing UnboundLocalError in Phase 3 path; module-level import at line 15 is sufficient
- Wave 1 (04-02): acquire_lock uses session.get_bind().dialect.name (not deprecated session.bind) per SQLAlchemy 2.0 pattern
- Wave 1 (04-02): upsert_lock_holder SQLite fallback: DELETE + INSERT (SQLAlchemy Core lacks cross-dialect ON CONFLICT DO UPDATE for non-Postgres)
- Wave 2 (04-03): Promoted create_session to module-level import in operator.py — required for mocker.patch("aircontroler.operator.create_session") patch target to resolve correctly
- Wave 2 (04-03): test_upsert_lock_holder_writes_row sets postgresql dialect on mock_session — SQLite fallback executes DELETE+INSERT (2 calls) but pg_insert ON CONFLICT path executes once; test exercises the pg path

## Accumulated Context

### Roadmap Evolution

- Phase 6 added: Add proper CI setup
- Phase 7 added: Rename the project from aircontroler to dbt-conductor
- Phases 5-7 reordered: Rename (5) → CI (6) → Cloud Run (7) to avoid rework

## Session Continuity

Last session: 2026-04-19
Stopped at: Phase 4 Plan 03 complete. Wave 2 validation done. All 124 tests green, zero xfail markers. Phase 4 complete.

- Next action: Phase 5 (Cloud Run + Distribution) or milestone completion review.
