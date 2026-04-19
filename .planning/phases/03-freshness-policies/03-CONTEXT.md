# Phase 3: Freshness Policies - Context

**Gathered:** 2026-04-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Add time-based freshness TTL checks to the operator. Before invoking dbt, each task checks whether the model ran successfully within its configured freshness window. If fresh, the task raises `AirflowSkipException` (visually distinct in Airflow UI). State is persisted in the `aircontroler_model_runs` table in the Airflow metadata DB via `MetadataDBHook`. No per-DAG Python overrides in this phase — freshness window is configured in `schema.yml` only.

</domain>

<decisions>
## Implementation Decisions

### Skip signal (FRESH-03)
- **D-01:** When a model is within its TTL, the operator raises `AirflowSkipException` (not a silent early return). Task appears yellow ("skipped") in Airflow UI, giving operators clear visibility into which models skipped vs actually executed dbt.
- **D-02:** Default `trigger_rule` for all generated `DbtRunModelOperator` tasks is `'none_failed'` (not Airflow's default `'all_success'`). This means a downstream model runs as long as none of its upstreams failed — even if some upstreams were skipped (fresh). Example: if model B depends on models A (fresh, skipped) and C (stale, ran), model B still runs because new data arrived via C.
- **D-03:** `trigger_rule` is configurable at two levels: `build_dbt_dag(trigger_rule='none_failed')` sets the default for all tasks in that DAG; `meta.aircontroler.trigger_rule` in `schema.yml` overrides per-model. Same pattern as `pool` and `retries` from Phase 2.

### State table schema (FRESH-04, FRESH-06)
- **D-04:** `aircontroler_model_runs` is an **append-only, success-only** table. One row is written per successful dbt execution. Rows are never updated in-place — full history is preserved.
- **D-05:** Table columns: `id` (integer PK, auto-increment), `unique_id` (varchar — dbt model unique_id), `dag_id` (varchar), `run_id` (varchar — Airflow run_id), `completed_at` (timestamp with timezone). No `status` column needed since only successes are written.
- **D-06:** Freshness check query: `SELECT MAX(completed_at) FROM aircontroler_model_runs WHERE unique_id = :uid`. If `now() - MAX(completed_at) < freshness_hours`, the model is fresh → raise `AirflowSkipException`.

### Hook DB access (FRESH-04)
- **D-07:** `MetadataDBHook` uses Airflow's internal `create_session` context manager for runtime reads and writes (`get_last_run()`, `record_run()`). No user-configured connection required — zero-config for the deploying user. The compat shim handles the import path difference between Airflow 2.x and 3.x.
- **D-08:** On Airflow 3.x, table creation uses `BaseDBManager` (official provider pattern, required by FRESH-06). On Airflow 2.x, table creation is via the `aircontroler db init` CLI command. The hook itself (runtime reads/writes) uses `create_session` on both versions.
- **D-09:** `MetadataDBHook` exposes two public methods:
  - `get_last_run(unique_id: str) -> datetime | None` — returns the `MAX(completed_at)` for a model, or `None` if never run
  - `record_run(unique_id: str, dag_id: str, run_id: str) -> None` — inserts a success row with `completed_at = now()`

### Freshness TTL source (FRESH-01, FRESH-02, FRESH-05)
- **D-10:** Freshness window (`freshness_hours`) is read exclusively from `meta.aircontroler.freshness_hours` in `schema.yml` (surfaced via `ManifestNode.config`). Same extraction path as Phase 2 meta overrides: `node.config.get('meta', {}).get('aircontroler', {}).get('freshness_hours')`.
- **D-11:** Per-DAG Python override (`freshness_overrides` dict on `build_dbt_dag()`) is **deferred** from Phase 3. See Deferred Ideas. As a result, FRESH-05 ("longest TTL wins") is trivially satisfied in Phase 3 — there is one TTL per model from schema.yml, no cross-DAG TTL conflict is possible.
- **D-12:** If `freshness_hours` is not set for a model (no meta key), freshness check is skipped entirely for that model — it always runs dbt. This preserves backward compatibility with models that have no freshness configuration.

### Claude's Discretion
- Exact Alembic migration file structure and revision ID
- Whether `aircontroler db init` is implemented as a `click` CLI or Airflow's native CLI extension
- Index strategy for `aircontroler_model_runs` (suggested: index on `unique_id` and `completed_at` for the MAX query)
- Whether `get_last_run()` and `record_run()` are called in the same DB session (single transaction) or separate sessions

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements and roadmap
- `.planning/REQUIREMENTS.md` §Freshness Policies — FRESH-01 through FRESH-06 are the acceptance criteria; each must be covered by a plan
- `.planning/ROADMAP.md` §Phase 3 — Four success criteria define what must be TRUE at phase completion

### Phase 2 context (locked decisions that carry forward)
- `.planning/phases/02-execution-hardening/02-CONTEXT.md` — D-04: `freshness` key in `meta.aircontroler` reserved for Phase 3; D-03: operator constructor pattern for new kwargs

### Existing integration points
- `aircontroler/hooks/metadata_db.py` — Phase 1 stub; Phase 3 fully implements this hook
- `aircontroler/operator.py` — `execute()` method is the integration point for the freshness check (before subprocess call) and the run recording (after subprocess success)
- `aircontroler/factory.py` — `build_dbt_dag()` gains `trigger_rule` param; the per-model meta extraction loop (already present for Phase 2 overrides) is extended to extract `freshness_hours` and `trigger_rule`

### Architecture constraints
- `CLAUDE.md` §Critical Pitfalls — "Never use direct SQLAlchemy sessions from worker execute() methods" is the primary constraint driving the hook design

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `MetadataDBHook(BaseHook)` in `aircontroler/hooks/metadata_db.py` — stub with correct `conn_name_attr`, `default_conn_name`, `conn_type`, `hook_name`. Phase 3 replaces the `get_conn()` stub with `get_last_run()` and `record_run()` implementations.
- `ac_meta = node.config.get('meta', {}).get('aircontroler', {})` — already in factory.py loop (Phase 2). `ac_meta.get('freshness_hours')` and `ac_meta.get('trigger_rule')` are zero-cost additions.
- `DbtRunModelOperator.execute()` — freshness check slots in at the top of `execute()`, before the `cmd` construction. Run recording slots in after `self._log_run_results()` on the success path.

### Established Patterns
- Conditional kwargs injection (Phase 2): only non-None values forwarded. Apply same pattern for `freshness_hours` — if not set, skip check entirely (D-12).
- Compat shim: `from airflow.providers.common.compat.sdk import BaseOperator`. Same shim handles `create_session` import path differences.
- `self.log.info(...)` / `self.log.warning(...)` in operator — established logging pattern.

### Integration Points
- `DbtRunModelOperator.__init__()` gains `freshness_hours: float | None = None` and `trigger_rule: str = 'none_failed'`.
- `build_dbt_dag()` gains `trigger_rule: str = 'none_failed'` param. The task creation loop passes per-model `trigger_rule` (from meta, falling back to DAG-level param).
- Alembic migration lives in `aircontroler/migrations/` (new directory). `aircontroler db init` CLI applies it on Airflow 2.x. Airflow 3 `BaseDBManager` subclass registered via provider entry point.

</code_context>

<specifics>
## Specific Ideas

- The "mixed freshness" behavior (one upstream fresh/skipped, another stale/running → downstream still runs) is the primary user scenario that drove `trigger_rule='none_failed'` as the default. Plans and tests should explicitly cover this case.
- Freshness check and run recording must go through the hook (never raw SQLAlchemy) because Airflow 3 workers have no direct DB access — this is a hard constraint, not a style choice.

</specifics>

<deferred>
## Deferred Ideas

- **FRESH-02 (per-DAG Python TTL override)** — `build_dbt_dag(freshness_overrides={'model.x': 6})` dict param deferred from Phase 3. Will be implemented in a follow-up once the core freshness machinery is proven.
- **FRESH-05 (cross-DAG longest-TTL-wins registry)** — Dependent on FRESH-02; trivially satisfied in Phase 3 since there is one TTL per model. Full cross-DAG TTL governance deferred with FRESH-02.
- **OBS-V2-02 (`aircontroler db cleanup`)** — Pruning stale rows from `aircontroler_model_runs` is a v2 requirement. The full-history table design chosen in D-04 enables this cleanup without schema changes.

</deferred>

---

*Phase: 03-freshness-policies*
*Context gathered: 2026-04-19*
