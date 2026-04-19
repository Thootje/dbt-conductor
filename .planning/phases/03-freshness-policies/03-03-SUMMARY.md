---
phase: 03-freshness-policies
plan: 03
subsystem: factory-wiring, provider-info, cli
tags:
  - airflow-provider
  - click-cli
  - factory
  - manifest-parsing
dependency_graph:
  requires:
    - 03-01 (AircontrolerDBManager, model_runs_table)
    - 03-02 (MetadataDBHook, DbtRunModelOperator freshness_hours + trigger_rule)
  provides:
    - build_dbt_dag() with trigger_rule param and per-node freshness extraction
    - get_provider_info() with db-managers key for Airflow 3.x DB migration discovery
    - aircontroler CLI (aircontroler db init) for Airflow 2.x table creation
    - pyproject.toml console_scripts entry point
  affects:
    - 03-04 (freshness integration / end-to-end validation)
tech_stack:
  added:
    - click (CLI framework, transitive via apache-airflow)
  patterns:
    - Two-level click group (cli > db > init) mirrors airflow db migrate layout
    - Per-model trigger_rule `or` short-circuit — same pattern as pool override (D-16)
    - Deferred Airflow imports inside db_init() body
key_files:
  created:
    - aircontroler/cli.py
    - tests/unit/test_cli.py
  modified:
    - aircontroler/factory.py
    - aircontroler/__init__.py
    - aircontroler/operator.py
    - pyproject.toml
    - tests/unit/test_factory.py
    - tests/unit/test_operator.py
    - tests/unit/test_init.py
decisions:
  - "trigger_rule default is none_failed (D-02): all tasks in a DAG run even when upstreams are skipped (fresh TTL path)"
  - "Per-model meta.aircontroler.trigger_rule overrides DAG-level via or short-circuit (D-03), same idiom as pool override"
  - "db-managers key uses hyphen not underscore — enforced by Airflow provider_info.schema.json"
  - "CLI uses standalone click group (not Airflow native cli provider extension) so --help works without Airflow context"
  - "All Airflow imports deferred inside db_init() body — import aircontroler.cli is safe in build/dist tooling"
metrics:
  duration: "~16 minutes"
  completed: "2026-04-19"
  tasks_completed: 2
  tasks_total: 2
  files_created: 2
  files_modified: 7
---

# Phase 3 Plan 03: Factory Wiring + Provider Info + CLI Summary

**One-liner:** Factory wires trigger_rule + freshness_hours per model via manifest meta; provider_info registers db-managers for Airflow 3.x; click CLI adds aircontroler db init for Airflow 2.x.

## What Was Built

### Task 1: Wire trigger_rule + freshness_hours through factory.py

**Signature change** — added `trigger_rule: str = "none_failed"` as last kwarg to `build_dbt_dag()`:

```python
def build_dbt_dag(
    dag: "DAG",
    manifest_path: str,
    ...
    pool_slots: int = 1,
    target_path: str | None = None,
    trigger_rule: str = "none_failed",   # NEW
) -> dict:
```

**Per-node loop extension** — two additions after `task_pool = ac_meta.get("pool") or pool or None`:

```python
# D-03: per-model trigger_rule overrides DAG-level (same `or` short-circuit as pool).
task_trigger_rule = ac_meta.get("trigger_rule") or trigger_rule
tasks[uid] = DbtRunModelOperator(
    ...
    pool_slots=pool_slots if task_pool else 1,
    freshness_hours=ac_meta.get("freshness_hours"),   # D-10 / FRESH-01
    trigger_rule=task_trigger_rule,                    # D-02 / D-03
)
```

No other changes — cardinality guard, max_active_tasks handling, edge wiring, and entry/exit classification all untouched. Factory.py grew from 201 to 205 lines.

**Also committed as part of Task 1:** The `operator.py` Phase 3 additions were present in the working tree but not yet committed (Plan 03-02 only committed the MetadataDBHook changes). These were staged together:
- `freshness_hours: float | None = None` and `trigger_rule: str = "none_failed"` kwargs
- `kwargs["trigger_rule"] = trigger_rule` always forwarded to BaseOperator
- `self.freshness_hours = freshness_hours` instance attribute with <= 0 validation
- `execute()` freshness TTL check (AirflowSkipException on fresh path)
- `execute()` `record_run()` call on success path

### Task 2: Register db-managers, add CLI, declare entry point

**`aircontroler/__init__.py`** — `get_provider_info()` extended with:
```python
"db-managers": ["aircontroler.db_manager.AircontrolerDBManager"],
```
Key uses hyphen (per `provider_info.schema.json`). On Airflow 3.x `airflow db migrate` discovers this list and calls `AircontrolerDBManager(session).initdb()` automatically.

**`aircontroler/cli.py`** — new module:
```
cli (group)
  db (subgroup)
    init (command) — calls AircontrolerDBManager(session).initdb()
```
All Airflow imports deferred inside `db_init()` body. `aircontroler --help` works without any Airflow context initialized.

**`pyproject.toml`** — added `[project.scripts]` section between `[project.entry-points.]` and `[tool.hatch.build.targets.wheel]`:
```toml
[project.scripts]
aircontroler = "aircontroler.cli:cli"
```

## Test Counts

| File | Before | After | New Tests |
|------|--------|-------|-----------|
| `tests/unit/test_factory.py` | 20 | 26 | +6 (FRESH-01, D-02, D-03, Nyquist dim 7, regression) |
| `tests/unit/test_operator.py` | 16 | 34 | +18 (Phase 3 freshness/skip/record tests from 03-02) |
| `tests/unit/test_init.py` | 2 | 4 | +2 (db-managers key, hyphen enforcement) |
| `tests/unit/test_cli.py` | 0 | 3 | +3 (help, db group, db init dispatch) |

**Full unit suite: 88 passed, 0 failed**
**Integration suite (test_migrations.py): 4 passed, 0 failed**

## Verification Results

```
uv run pytest tests/unit/ -q --tb=short          → 88 passed
uv run pytest tests/integration/test_migrations.py -q  → 4 passed
python -c "from aircontroler import get_provider_info; assert get_provider_info()['db-managers'] == ['aircontroler.db_manager.AircontrolerDBManager']"  → OK
python -c "from aircontroler.cli import cli; cli.main(['--help'], standalone_mode=False)"  → exit 0
```

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `1f81548` | feat | Wire trigger_rule + freshness_hours through factory.py (+ operator.py Phase 3 completion) |
| `8753e78` | feat | Register db-managers in provider_info, add CLI module and console entry point |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] operator.py Phase 3 kwargs missing from committed state**

- **Found during:** Task 1 start — `task.freshness_hours` and `task.trigger_rule` used in new factory tests would fail with `AttributeError` if operator was not updated
- **Issue:** Plan 03-02 committed only `MetadataDBHook` changes (commit `aecad2a`). The `operator.py` additions (`freshness_hours`, `trigger_rule` kwargs, freshness TTL check in `execute()`, `record_run()` on success path) and corresponding `test_operator.py` Phase 3 tests were present in the working tree but not committed
- **Fix:** Staged and committed `operator.py` + `test_operator.py` together with Task 1 changes (factory.py + test_factory.py). All 34 operator tests pass
- **Files modified:** `aircontroler/operator.py`, `tests/unit/test_operator.py`
- **Commit:** `1f81548`

## Known Stubs

None — all implemented functionality is fully wired. No placeholder values, TODO comments, or hardcoded empty returns in created/modified files.

## Threat Flags

No new security-relevant surface beyond what is documented in the plan's threat model (T-3-08, T-3-09, T-3-10 — all `accept` disposition). The CLI `db init` command, `db-managers` registration, and factory `trigger_rule` forwarding were all modeled in the threat register before implementation.

## Self-Check

### Files Exist

- `aircontroler/factory.py` — contains `trigger_rule: str = "none_failed"`, `task_trigger_rule`, `freshness_hours=ac_meta.get("freshness_hours")`, `trigger_rule=task_trigger_rule`
- `aircontroler/__init__.py` — contains `"db-managers": ["aircontroler.db_manager.AircontrolerDBManager"]`
- `aircontroler/cli.py` — contains `@click.group()`, `@cli.group("db")`, `@db_group.command("init")`, `manager.initdb()`
- `pyproject.toml` — contains `[project.scripts]` and `aircontroler = "aircontroler.cli:cli"`
- `tests/unit/test_factory.py` — 26 tests (20 Phase 1/2 + 6 Phase 3)
- `tests/unit/test_init.py` — 4 tests (2 Phase 1/2 + 2 Phase 3)
- `tests/unit/test_cli.py` — 3 tests (new)

### Commits Exist

- `1f81548` — Task 1: factory + operator wiring
- `8753e78` — Task 2: provider_info + CLI + entry point

## Self-Check: PASSED

All files present, both commits confirmed in git log, 88 unit + 4 integration tests green.
