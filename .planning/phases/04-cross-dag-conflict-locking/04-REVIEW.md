---
phase: 04-cross-dag-conflict-locking
reviewed: 2026-04-19T00:00:00Z
depth: standard
files_reviewed: 11
files_reviewed_list:
  - aircontroler/migrations/versions/0002_add_lock_holders_table.py
  - aircontroler/db_manager.py
  - aircontroler/hooks/metadata_db.py
  - aircontroler/operator.py
  - aircontroler/factory.py
  - tests/unit/test_lock_chaos.py
  - tests/unit/test_metadata_db_hook.py
  - tests/unit/test_operator.py
  - tests/unit/test_factory.py
  - tests/unit/test_db_manager.py
  - tests/integration/test_migrations.py
findings:
  critical: 0
  warning: 4
  info: 3
  total: 7
status: issues_found
---

# Phase 04: Code Review Report

**Reviewed:** 2026-04-19T00:00:00Z
**Depth:** standard
**Files Reviewed:** 11
**Status:** issues_found

## Summary

This phase adds cross-DAG conflict locking using Postgres advisory transactions, an `aircontroler_lock_holders` identity table, and wires both into `DbtRunModelOperator.execute()`. The design choices (advisory locks, micro-transactions for identity rows, scoped=False for cleanup) are sound and well-reasoned. Security posture is strong: parameterized queries throughout, adler32 hash for lock IDs, allowlisted adapter response keys.

Four warnings and three info items were found. No critical issues. The most actionable finding is a test reliability gap in `test_subprocess_command` where a `KeyError` is silently swallowed on every run, making the test's implicit assertion about `record_run` unreachable.

---

## Warnings

### WR-01: `test_subprocess_command` silently swallows a `KeyError` on every run

**File:** `tests/unit/test_operator.py:31`
**Issue:** `test_subprocess_command` calls `op.execute({})` with an empty context dict. The operator has no `freshness_hours` and no `conflict_policy`, so it takes the Phase 3 path. After subprocess success, the code at `operator.py:284-288` calls `hook.record_run(... dag_id=context["dag_run"].dag_id, run_id=context["run_id"])`. With `context = {}` this raises `KeyError: 'dag_run'`, which is caught by the bare `except Exception` at line 289 and silently logged as a warning. The test passes, but `record_run` never executes. Any future regression that breaks the `record_run` call signature or context access will not be caught by this test.

**Fix:** Provide a minimal context with `dag_run` and `run_id` (the same pattern used in `_make_fresh_context()`), and mock `record_run` so the test can assert it was called:

```python
def test_subprocess_command(mocker):
    from unittest.mock import MagicMock
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    mock_record = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    ctx = {}
    dag_run = MagicMock()
    dag_run.dag_id = "test_dag"
    ctx["dag_run"] = dag_run
    ctx["run_id"] = "test_run"
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        dbt_executable_path="dbt",
    )
    op.execute(ctx)
    # verify both the subprocess call and the record_run call
    assert mock_run.call_count == 1
    assert mock_record.call_count == 1
    ...
```

---

### WR-02: `or`-based override chain silently ignores falsy-but-explicit per-model values

**File:** `aircontroler/factory.py:147-151`
**Issue:** The per-model override logic uses `or` short-circuit for `task_pool`, `task_trigger_rule`, and `task_conflict_policy`:

```python
task_pool = ac_meta.get("pool") or pool or None
task_trigger_rule = ac_meta.get("trigger_rule") or trigger_rule
task_conflict_policy = ac_meta.get("conflict_policy") or conflict_policy
```

If a model's `meta.aircontroler` contains `trigger_rule: ""` or `conflict_policy: ""` (an empty string — possibly a user misconfiguration), the `or` chain falls through to the DAG-level value instead of forwarding the explicit (if invalid) per-model value. For `trigger_rule` this would silently use the DAG default instead of surfacing the misconfiguration. More importantly, if a future valid value is introduced that happens to be falsy, it would be silently suppressed.

The `conflict_policy` case is lower risk because the operator's `__init__` already validates and warns on unknown values; however the silent masking still applies.

**Fix:** Use explicit `None` checks to distinguish "not set" from "set to a falsy value":

```python
_per_pool = ac_meta.get("pool")
task_pool = _per_pool if _per_pool is not None else pool

_per_tr = ac_meta.get("trigger_rule")
task_trigger_rule = _per_tr if _per_tr is not None else trigger_rule

_per_cp = ac_meta.get("conflict_policy")
task_conflict_policy = _per_cp if _per_cp is not None else conflict_policy
```

---

### WR-03: `create_session(scoped=False)` relies on an undocumented internal parameter

**File:** `aircontroler/hooks/metadata_db.py:121` and `164`
**Issue:** `upsert_lock_holder` and `delete_lock_holder` both call `create_session(scoped=False)`. The `scoped` parameter of `airflow.utils.session.create_session` is an internal implementation detail, not part of the public Airflow API. While it is present in Airflow 2.7+ and 3.x today, there is no guarantee of forward compatibility. If this parameter is removed or renamed, both micro-transaction methods will silently revert to scoped session behavior (or raise a `TypeError`), breaking the independence guarantee that makes cleanup safe after a rolled-back main session.

**Fix:** Add a version-guarded comment and a try/except fallback, or track this dependency explicitly:

```python
# NOTE: scoped=False is an internal Airflow parameter used to obtain an
# independent session that commits independently of any active scoped session
# on the current thread. This is required so delete_lock_holder commits even
# after the main session has been rolled back. Tested on Airflow 2.7-3.x.
# If this parameter is removed in a future Airflow release, update this call.
with create_session(scoped=False) as session:
    ...
```

At minimum, add a unit test that `delete_lock_holder` commits independently of a rolled-back outer session, so a breakage is caught on upgrade.

---

### WR-04: Integration test does not verify `aircontroler_lock_holders` schema constraints

**File:** `tests/integration/test_migrations.py:89-100`
**Issue:** `test_migration_creates_lock_holders_table` verifies the table exists and has the correct column names, but does not verify:
- `unique_id` is the primary key (i.e., that duplicate inserts are rejected)
- `locked_at` is `NOT NULL`
- `dag_id` and `run_id` are `NOT NULL`

The migration at `0002_add_lock_holders_table.py` defines `nullable=False` on all four columns and `PrimaryKeyConstraint("unique_id")`. If these constraints were accidentally dropped in a future schema change, the integration test would not catch it, and the upsert-on-conflict logic in `upsert_lock_holder` would silently allow duplicate rows.

By contrast, the `aircontroler_model_runs` table has the same gap (no constraint verification in integration tests), so this is consistent — but worth fixing for both.

**Fix:**

```python
def test_migration_creates_lock_holders_table(tmp_path):
    from sqlalchemy import create_engine, inspect
    from alembic import command
    db_path = tmp_path / "test_lock.db"
    engine = create_engine(f"sqlite:///{db_path}")
    cfg = _alembic_config(_migrations_dir(), f"sqlite:///{db_path}")
    command.upgrade(cfg, "head")
    insp = inspect(engine)
    assert "aircontroler_lock_holders" in insp.get_table_names()
    cols = {c["name"]: c for c in insp.get_columns("aircontroler_lock_holders")}
    assert set(cols.keys()) == {"unique_id", "dag_id", "run_id", "locked_at"}
    # Verify NOT NULL constraints
    assert cols["dag_id"]["nullable"] is False
    assert cols["run_id"]["nullable"] is False
    assert cols["locked_at"]["nullable"] is False
    # Verify primary key
    pk = insp.get_pk_constraint("aircontroler_lock_holders")
    assert pk["constrained_columns"] == ["unique_id"]
```

---

## Info

### IN-01: Duplicate dbt command construction in `execute()` (two identical code paths)

**File:** `aircontroler/operator.py:172-185` and `252-265`
**Issue:** The list construction for the `dbt build` subprocess command is duplicated verbatim between the lock-enabled path (lines 172-185) and the Phase 3 no-lock path (lines 252-265). If a new flag needs to be added (e.g., `--target`, `--vars`), it must be applied in two places. The comment at line 171 acknowledges this ("same logic as Phase 3 path below") but the duplication remains a maintenance risk.

**Fix:** Extract to a helper method:

```python
def _build_dbt_cmd(self) -> list[str]:
    executable = self.dbt_executable_path or "dbt"
    return [
        executable, "build",
        "--select", self.unique_id,
        "--project-dir", self.project_dir,
        "--profiles-dir", self.profiles_path,
    ]
```

Then both paths call `cmd = self._build_dbt_cmd()`.

---

### IN-02: Adler32 hash has weak collision resistance for the lock key space

**File:** `aircontroler/hooks/metadata_db.py:98`
**Issue:** `lock_id = zlib.adler32(unique_id.encode()) & 0x7FFFFFFF` produces a 31-bit integer. While the cardinality guard caps resolved models at 800 per DAG, two models from *different* DAGs or different projects can have the same hash, causing a spurious advisory lock contention between otherwise unrelated models. Adler32 was designed for error detection, not hash distribution. For 800 models the birthday-paradox collision probability is approximately 1.5×10⁻⁴ per pair of models sharing a hash space, which is low but non-zero.

This is an advisory lock (non-blocking), so a collision causes a model to be skipped or failed incorrectly rather than a deadlock. This would be a silent correctness failure that is hard to diagnose.

**Fix:** Use a stronger hash function with better distribution, or widen the hash space:

```python
import hashlib
# 64-bit hash, stable (not PYTHONHASHSEED-dependent), good distribution
digest = int.from_bytes(hashlib.blake2b(unique_id.encode(), digest_size=8).digest(), "little")
lock_id = digest & 0x7FFFFFFFFFFFFFFF  # Stay within Postgres int8 range
# Use pg_try_advisory_xact_lock(bigint) variant
result = session.execute(
    sa.text("SELECT pg_try_advisory_xact_lock(:h)"),
    {"h": lock_id},
).scalar()
```

Alternatively, use `mmh3` (murmurhash3) which is already common in data-engineering stacks. If staying with 32-bit (Postgres int4), CRC32 (`binascii.crc32`) has better distribution than Adler32.

---

### IN-03: `test_lock_chaos.py` workers each recreate patches inside the loop — patch teardown risk

**File:** `tests/unit/test_lock_chaos.py:64-101`
**Issue:** Inside the `worker()` function, `patch()` context managers are opened but the `with` block's cleanup depends on proper `__exit__` being called. Each call to `worker()` creates a new `DbtRunModelOperator` instance inside the patched context. Because patches are applied on every call to `worker()` (which runs in a thread), and the outer `run_pair()` loop runs 50 times, this is 100 patch/unpatch cycles nested inside threads.

The current implementation is correct — the `with` statement ensures cleanup even on exception — but `patch()` is not thread-safe in all versions of `unittest.mock`. If two threads are simultaneously entering or exiting `patch()` on the same target attribute, there is a potential for a race on `sys.modules` or the patched object's original value. In practice the `acquire_call_lock` barrier ensures the critical section is serialized, but the patch `__enter__`/`__exit__` calls are not protected.

This is low risk in practice (each thread patches the same methods on the same class, which is idempotent for `MagicMock` replacements), but worth a note if this test becomes flaky.

**Fix:** Move the `patch()` context managers to the `run_pair()` level (outside the thread workers) since the mocks are shared across both threads in a pair:

```python
def run_pair():
    barrier = threading.Barrier(2)
    acquire_call_lock = threading.Lock()
    acquire_calls = [0]

    def mock_acquire(unique_id, session):
        barrier.wait()
        with acquire_call_lock:
            idx = acquire_calls[0]
            acquire_calls[0] += 1
        return idx == 0

    mock_run_result = MagicMock()
    mock_run_result.returncode = 0
    mock_run_result.stdout = ""
    mock_run_result.stderr = ""

    with (
        patch("aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock", side_effect=mock_acquire),
        patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder"),
        # ... rest of patches ...
    ):
        def worker():
            op = DbtRunModelOperator(...)
            try:
                op.execute(_make_execute_context())
                return "ran"
            except AirflowSkipException:
                return "skipped"

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker) for _ in range(2)]
            outcomes = [f.result() for f in futures]
```

---

_Reviewed: 2026-04-19T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
