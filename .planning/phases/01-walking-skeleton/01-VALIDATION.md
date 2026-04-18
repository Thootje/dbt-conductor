---
phase: 1
slug: walking-skeleton
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-18
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 8.x |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` — Wave 0 installs |
| **Quick run command** | `pytest tests/unit/ -x -q` |
| **Full suite command** | `pytest tests/ -x --cov=aircontroler --cov-report=term-missing` |
| **Estimated runtime** | ~30 seconds (unit), ~120 seconds (full with integration) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/unit/ -x -q`
- **After every plan wave:** Run `pytest tests/ -x --cov=aircontroler --cov-report=term-missing`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds (unit), 120 seconds (full)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| manifest-parse-v11 | manifest | 1 | CORE-01 | — | N/A | unit | `pytest tests/unit/test_manifest.py::test_parse_v11 -x` | ✅ | ✅ green |
| manifest-parse-v12 | manifest | 1 | CORE-01 | — | N/A | unit | `pytest tests/unit/test_manifest.py::test_parse_v12 -x` | ✅ | ✅ green |
| manifest-unknown-version | manifest | 1 | CORE-01 | — | raises ValueError | unit | `pytest tests/unit/test_manifest.py::test_unknown_version_raises -x` | ✅ | ✅ green |
| manifest-compile-on-parse | manifest | 1 | CORE-02 | subprocess-injection | dbt parse uses list args, no shell=True | unit | `pytest tests/unit/test_manifest.py::test_compile_on_parse -x` | ✅ | ✅ green |
| selector-extract-select | selector | 1 | CORE-03 | — | N/A | unit | `pytest tests/unit/test_selector.py::test_extract_select -x` | ✅ | ✅ green |
| selector-extract-exclude | selector | 1 | CORE-03 | — | N/A | unit | `pytest tests/unit/test_selector.py::test_extract_exclude -x` | ✅ | ✅ green |
| selector-extract-selector | selector | 1 | CORE-03 | — | N/A | unit | `pytest tests/unit/test_selector.py::test_extract_selector -x` | ✅ | ✅ green |
| selector-parse-ndjson | selector | 1 | CORE-03 | subprocess-injection | dbt ls uses list args, no shell=True | unit | `pytest tests/unit/test_selector.py::test_parse_ls_output -x` | ✅ | ✅ green |
| selector-ls-failure | selector | 1 | CORE-03 | — | non-zero exit raises RuntimeError | unit | `pytest tests/unit/test_selector.py::test_ls_failure_raises -x` | ✅ | ✅ green |
| factory-task-per-model | factory | 2 | CORE-04 | — | N/A | unit | `pytest tests/unit/test_factory.py::test_task_per_model -x` | ✅ | ✅ green |
| factory-edge-wiring | factory | 2 | CORE-05 | — | N/A | unit | `pytest tests/unit/test_factory.py::test_edge_wiring -x` | ✅ | ✅ green |
| factory-entry-exit | factory | 2 | CORE-05 | — | N/A | unit | `pytest tests/unit/test_factory.py::test_entry_exit_classification -x` | ✅ | ✅ green |
| operator-subprocess | operator | 2 | CORE-06 | subprocess-injection | command built as list[str], no shell=True | unit | `pytest tests/unit/test_operator.py::test_subprocess_command -x` | ✅ | ✅ green |
| operator-custom-executable | operator | 2 | CORE-06 | — | N/A | unit | `pytest tests/unit/test_operator.py::test_custom_executable -x` | ✅ | ✅ green |
| operator-failure-raises | operator | 2 | CORE-06 | — | non-zero exit raises AirflowException | unit | `pytest tests/unit/test_operator.py::test_failure_raises -x` | ✅ | ✅ green |
| operator-profiles-path | operator | 2 | CORE-07 | credential-leak | profiles_path passed as --profiles-dir arg, contents never logged | unit | `pytest tests/unit/test_operator.py::test_profiles_path -x` | ✅ | ✅ green |
| operator-compat-import | operator | 2 | CORE-08 | — | N/A | unit | `pytest tests/unit/test_operator.py::test_import_compat -x` | ✅ | ✅ green |
| provider-info | init | 1 | CORE-09 | — | N/A | unit | `pytest tests/unit/test_init.py::test_provider_info -x` | ✅ | ✅ green |
| integration-full-pipeline | integration | 3 | CORE-01–09 | — | full subprocess + DAG run | integration | `pytest tests/integration/ -x` | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/conftest.py` — shared fixtures: `manifest_v11_data`, `manifest_v12_data`, `mock_dag`, `mock_context`
- [ ] `tests/unit/test_manifest.py` — stubs covering CORE-01, CORE-02
- [ ] `tests/unit/test_selector.py` — stubs covering CORE-03
- [ ] `tests/unit/test_operator.py` — stubs covering CORE-06, CORE-07, CORE-08
- [ ] `tests/unit/test_factory.py` — stubs covering CORE-04, CORE-05
- [ ] `tests/unit/test_init.py` — stubs covering CORE-09
- [ ] `tests/integration/test_dag_wiring.py` — end-to-end coverage CORE-01–09
- [ ] `tests/fixtures/manifest_v11.json` — hand-crafted 5-node v11 fixture
- [ ] `tests/fixtures/manifest_v12.json` — same topology, v12 schema (adds `unit_tests: {}`)
- [ ] `tests/fixtures/dbt_project/` — minimal dbt-duckdb project (5 models, profiles.yml)
- [ ] `pyproject.toml` `[tool.pytest.ini_options]` — testpaths, addopts, markers
- [ ] Framework install: `pip install -e ".[test]"` — installs pytest + dbt-duckdb

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Provider appears in Airflow Providers list UI | CORE-09 | Requires running Airflow webserver | `pip install -e . && airflow providers list \| grep aircontroler` |
| Airflow Graph view shows dbt lineage correctly | CORE-05 | Requires Airflow UI | Trigger sample DAG on local Airflow, open Graph view, verify edges match manifest |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s (unit), < 120s (full)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** COMPLETE — 23 tests pass (21 unit + 2 integration), 90% coverage, 2026-04-18
