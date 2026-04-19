---
phase: 3
slug: freshness-policies
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-19
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.3 |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` |
| **Quick run command** | `uv run pytest tests/unit/ -q --tb=short` |
| **Full suite command** | `uv run pytest tests/ -q --tb=short` |
| **Estimated runtime** | ~5 seconds (55 existing + Phase 3 new tests) |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/ -q --tb=short`
- **After every plan wave:** Run `uv run pytest tests/ -q --tb=short`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 3-01-01 | 01 | 0 | FRESH-04, FRESH-06 | T-3-01 | No raw SQL — parameterized `sa.select()` in `get_last_run()` | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -q --tb=short` | ❌ W0 | ⬜ pending |
| 3-01-02 | 01 | 0 | FRESH-06 | — | N/A | unit | `uv run pytest tests/unit/test_db_manager.py -q --tb=short` | ❌ W0 | ⬜ pending |
| 3-01-03 | 01 | 0 | FRESH-06 | — | N/A | integration | `uv run pytest tests/integration/test_migrations.py -q --tb=short` | ❌ W0 | ⬜ pending |
| 3-02-01 | 02 | 1 | FRESH-03, FRESH-04 | T-3-02 | freshness_hours validated > 0 in __init__ | unit | `uv run pytest tests/unit/test_operator.py -k freshness -q --tb=short` | ❌ W0 | ⬜ pending |
| 3-03-01 | 03 | 1 | FRESH-01, FRESH-03 | — | N/A | unit | `uv run pytest tests/unit/test_factory.py -k freshness -q --tb=short` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/unit/test_metadata_db_hook.py` — unit tests for `get_last_run()` and `record_run()` (FRESH-04)
- [ ] `tests/unit/test_db_manager.py` — tests for `AircontrolerDBManager` attributes and `version_table_name` (FRESH-06)
- [ ] `tests/integration/test_migrations.py` — Alembic migration round-trip: upgrade creates table + indexes, downgrade drops them (FRESH-06)
- [ ] `tests/fixtures/manifest_v12_with_freshness.json` — manifest fixture with `freshness_hours` set on a mart node
- [ ] `aircontroler/migrations/alembic.ini` + `env.py` + `versions/0001_add_model_runs_table.py` — migration scaffold
- [ ] `tests/unit/test_init.py -k db_managers` — `get_provider_info()` includes `db-managers` key

*Existing `tests/unit/test_operator.py` and `tests/unit/test_factory.py` extend in Wave 1 — stubs not required in Wave 0 but can be added.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Yellow "skipped" state in Airflow UI | FRESH-03 | Requires live Airflow scheduler + UI | Trigger a DAG with a fresh model; verify task shows yellow/skipped status in Graph view |
| `aircontroler db init` applies migration on Airflow 2.x | FRESH-06 | Requires Airflow 2.x environment | Install package, run `aircontroler db init`, verify `aircontroler_model_runs` table exists in metadata DB |
| `airflow db migrate` creates table on Airflow 3.x | FRESH-06 | Requires Airflow 3.x environment | Register `AircontrolerDBManager`, run `airflow db migrate`, verify table and `aircontroler_alembic_version` entry |
| Cross-DAG freshness: model fresh in DAG_A skips in DAG_B | FRESH-04 | Requires two running DAGs + shared metadata DB | Run DAG_A with model M; within TTL, trigger DAG_B with same model M; verify DAG_B task is skipped |

---

## Critical Nyquist Sampling Dimensions

The following 8 behaviors are the minimum required to trust the freshness machinery in production:

1. **Fresh → skip:** Hook returns timestamp within TTL → `AirflowSkipException` raised (not silent return, not success)
2. **Stale → run:** Hook returns timestamp outside TTL → dbt subprocess invoked
3. **No TTL → always run:** `freshness_hours=None` → no hook call, dbt always invoked
4. **Success records run:** Successful dbt → `record_run()` called with correct `unique_id`, `dag_id`, `run_id`
5. **Failure does not record run:** Failed dbt (non-zero returncode) → `record_run()` NOT called
6. **Skip does not record run:** Skip path → `record_run()` NOT called (exception exits before recording)
7. **none_failed passes skipped upstream:** DAG topology — task with skipped upstream still runs under `none_failed`
8. **Fail-open on missing table:** `get_last_run()` catches DB error, returns `None`, dbt runs

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
