---
status: partial
phase: 04-cross-dag-conflict-locking
source: [04-VERIFICATION.md]
started: 2026-04-19T11:12:00Z
updated: 2026-04-19T11:12:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Live concurrent DAG behavior (Postgres)
expected: Two simultaneous task runs for the same model — exactly one dbt subprocess executes, the other raises AirflowSkipException (skip policy) or AirflowException (fail policy). lock_holders table shows one row during the window, zero after both complete.
result: [pending]

### 2. pg_locks advisory lock inspection (Postgres)
expected: During the dbt subprocess window, pg_locks contains a row with locktype='advisory' and classid/objid matching adler32(unique_id) & 0x7FFFFFFF. After the task completes (success or conflict), the advisory lock row is gone (xact-scoped auto-release).
result: [pending]

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
