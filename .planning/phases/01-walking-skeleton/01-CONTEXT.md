# Phase 1: Walking Skeleton - Context

**Gathered:** 2026-04-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Render a dbt project as an Airflow DAG where every model is a separate Airflow task with accurate upstream/downstream wiring. The user pip-installs `airflow-provider-aircontroler`, passes a pre-built `manifest.json` and a dbt command string, and gets a fully wired DAG they can trigger on Airflow 2.7+ or 3.x. Execution hardening, freshness policies, conflict locking, and Cloud Run support are separate phases.

</domain>

<decisions>
## Implementation Decisions

### User-facing DAG API
- **D-01:** Factory function pattern: `build_dbt_dag(dag, ...)` mutates a user-owned DAG and adds all model tasks to it. The user creates the DAG, calls the factory, and wires other operators around it.
- **D-02:** Return value is a dict with three keys: `{'entry_tasks': [tasks with no dbt upstream], 'exit_tasks': [tasks with no dbt downstream], 'tasks': {unique_id: AirflowTask}}`. Enables `start >> result['entry_tasks']` and `result['exit_tasks'] >> notify` patterns.
- **D-03:** Factory signature (Phase 1): `build_dbt_dag(dag, manifest_path, dbt_command, project_dir, profiles_path, dbt_executable_path=None, service_account=None)`.

### Select command interface
- **D-04:** User passes a full dbt command string: `dbt_command='dbt build --select tag:daily+ --exclude raw_* --profile prod --target dev'`. The factory parses `--select`, `--exclude`, and `--selector` flags out of this string to pass to `dbt ls` for graph resolution. Each model task then runs `dbt build --select <unique_id>` (with profile/target/project-dir injected from separate params).
- **D-05:** `--project-dir` and `--profiles-dir` are NOT embedded in `dbt_command` — they are supplied as explicit `project_dir` and `profiles_path` factory params, validated at parse time.

### Package layout
- **D-06:** Flat module structure:
  - `aircontroler/__init__.py` — `get_provider_info()`
  - `aircontroler/operator.py` — `DbtRunModelOperator`
  - `aircontroler/factory.py` — `build_dbt_dag()`
  - `aircontroler/manifest.py` — `ManifestLoader`, v11/v12 dispatch
  - `aircontroler/selector.py` — `SelectorResolver` (delegates to `dbt ls`)
  - `aircontroler/hooks/__init__.py`
  - `aircontroler/hooks/metadata_db.py` — `MetadataDBHook` (stub for Phase 3)

### Provider identity
- **D-07:** PyPI name: `airflow-provider-aircontroler`. Provider name in Airflow UI and `get_provider_info()`: `aircontroler` (lowercase, matching the package slug).

### Manifest parser internals
- **D-08:** Internal model representation uses `@dataclass` — `ManifestNode` with typed fields (`unique_id`, `depends_on`, `fqn`, `config`, `tags`, `resource_type`). Both v11 and v12 parsers return the same `ManifestNode` shape.
- **D-09:** `parse_manifest(data)` reads `manifest_version` first, dispatches to `_parse_v11(data)` / `_parse_v12(data)`. Never hardcode field paths without checking version first.

### Test infrastructure
- **D-10:** Phase 1 ships unit tests AND full integration tests.
  - Unit tests: mock subprocess calls, fixture `manifest.json` files for v11 and v12, test graph wiring and return dict structure.
  - Integration tests: a tiny sample dbt project using `dbt-duckdb` (zero infra, no cloud credentials). Tests instantiate a real Airflow DAG, parse it, and trigger a full run against DuckDB. Sample dbt project lives at `tests/fixtures/dbt_project/`.

### service_account stub
- **D-11:** `service_account` parameter exists on `build_dbt_dag()` from Phase 1 (adding later is a breaking API change). In Phase 1 it accepts any value and logs a `WARNING`: `"service_account is not yet implemented — will be supported in a future release"`. No crash, no silent discard.

### Claude's Discretion
- Internal naming of the graph-resolution step (e.g., class name for the selector resolver)
- Exact fields exposed on `ManifestNode` beyond the minimum needed for graph wiring
- CI matrix specifics (which exact Airflow 2.7/3.x minor versions to test against)
- Whether `dbt_executable_path` defaults to `"dbt"` (PATH lookup) or requires explicit setting

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements
- `.planning/REQUIREMENTS.md` — CORE-01 through CORE-09 are the acceptance criteria for this phase. Read before planning tasks.
- `.planning/ROADMAP.md` §Phase 1 — Success criteria (4 items) define what must be TRUE at phase completion.

### Architecture decisions
- `.planning/research/SUMMARY.md` — Critical Decisions section (especially decisions 1–3) and "Watch Out For" pitfalls. These are pre-researched constraints the planner and researcher must not re-open.
- `.planning/research/PITFALLS.md` — C1 (compile-at-parse stall), C2 (direct DB access from workers), C3 (XCom payload), C4 (manifest schema drift), C6 (SELECT FOR UPDATE deadlocks). Phase 1 must not introduce any of these.
- `.planning/research/ARCHITECTURE.md` — Parse-time vs runtime split, manifest loading, static task loop pattern.
- `.planning/research/STACK.md` — Confirmed stack: hatchling, common-compat shim, dbt-core version range, orjson optional extra.

### Airflow provider conventions
- `.planning/research/FEATURES.md` — Table stakes features (Cosmos parity bar); Phase 1 delivers items 1–4.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- None — greenfield. No existing Python source files.

### Established Patterns
- Stack is fully decided: `hatchling` build backend, `pyproject.toml`, `apache-airflow-providers-common-compat>=1.7.0` shim for 2.x/3.x compat. All imports must go through the shim (e.g., `from airflow.providers.common.compat.sdk import BaseOperator`) — no `if AIRFLOW_V_3_0_PLUS` branches.
- CLI subprocess only: `subprocess.run(["dbt", ...])` via configurable `dbt_executable_path`. Never `import dbtRunner`.

### Integration Points
- `pyproject.toml` `project.entry-points."apache_airflow_provider"` registers `get_provider_info` — this is how the provider appears in Airflow's Providers list.
- `aircontroler/hooks/metadata_db.py` is a Phase 1 stub that Phase 3 will fill in. The hook must extend `BaseHook` so Airflow 3 worker isolation is respected from day one.

</code_context>

<specifics>
## Specific Ideas

- Factory return dict key names confirmed: `'entry_tasks'`, `'exit_tasks'`, `'tasks'` (keyed by `unique_id` string).
- The dbt command string is parsed to extract selector args for `dbt ls` — the string is not passed verbatim to the worker. Each worker task independently assembles `dbt build --select <unique_id> --project-dir ... --profiles-dir ...`.
- Integration test fixture at `tests/fixtures/dbt_project/` uses `dbt-duckdb`; this adapter must be added as a `[test]` extra in `pyproject.toml`, not a runtime dependency.

</specifics>

<deferred>
## Deferred Ideas

- None raised during discussion — discussion stayed within phase scope.

</deferred>

---

*Phase: 01-walking-skeleton*
*Context gathered: 2026-04-18*
