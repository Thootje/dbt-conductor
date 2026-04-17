# Pitfalls Research: Airflow dbt Provider

**Project:** aircontroler — Airflow provider for dbt with cross-DAG conflict handling
**Researched:** 2026-04-17
**Overall confidence:** HIGH (verified against official docs, Cosmos issue tracker, dbt-core issue tracker, Airflow upgrade guides)

This document catalogues domain-specific failure modes for an Airflow provider that maps dbt models to Airflow tasks via dynamic task mapping. The mistakes here are drawn from Cosmos's public issue tracker, dbt-core's manifest schema history, Airflow 3 migration guides, and Cloud Composer / Cloud Run operational reports. Generic "test your code" advice is excluded.

---

## Critical Pitfalls

These cause project-level failure: rewrites, broken installs, or unrecoverable production incidents.

### C1. Calling `dbt compile` at DAG parse time (the Cosmos trap)

**What goes wrong:** The DAG file calls `dbt compile` (or even `dbt parse`) on every scheduler heartbeat to produce a manifest. With a 2,300-node project, parsing takes 50-100 seconds per import; with multiple DAGs sharing the same manifest, every DAG redundantly re-parses. Workers (Cloud Run jobs especially) re-import the DAG and re-run the compile step before they can execute their assigned task. This produces DagBag import timeouts and "executor/DB mismatch" errors where Airflow marks the task failed because the worker exceeded the dagbag_import_timeout.

**Warning signs:**
- Scheduler `dag_processing.last_duration` rising above 30s for the dbt DAGs
- `DagFileProcessor` logs showing repeated manifest reads
- Cold-start failures on Cloud Run executor (worker times out before task starts)
- "Unable to do partial parsing because profile has changed" in logs (Cosmos Issue #2294 pattern)
- DAG run `queued` → `failed` transitions with no logs from the task itself

**Prevention strategy:**
1. **Manifest-as-input pattern:** Require users to ship a pre-built `manifest.json` (built in CI/CD) alongside their DAG. Provider reads JSON from disk, never invokes `dbt compile` at parse time.
2. Provide a `BuildManifestOperator` for users who genuinely want runtime compilation, but document loudly that this is for development only.
3. Use `orjson` (Cosmos's experimental fix in PR #2320 yields ~40% improvement) instead of stdlib `json` for manifest deserialization — manifests are 50-200MB at scale.
4. Cache parsed manifest at the module level (one read per Python process, not per DAG).
5. For dynamic task mapping, the upstream task that produces the model list MUST NOT trigger compilation — it should read from cached manifest only.

**Build phase to address:** Phase 1 (foundational architecture). Get this wrong in Phase 1 and every subsequent phase compounds the problem. Bake "manifest-as-input" into the operator's primary API, not as an opt-in.

---

### C2. Direct metadata DB access in operator code (Airflow 3 incompatibility)

**What goes wrong:** Operators call `airflow.models.XCom.get_many(...)` or open a SQLAlchemy session to read/write a custom freshness/locking table directly from inside `execute()`. This works in Airflow 2.x. In Airflow 3.x it breaks completely: workers no longer have direct database access — all interaction goes through the Task Execution API. The operator silently runs against a cached connection that no longer exists, or raises "no module named airflow.models" because the worker runs the Task SDK, not the full Airflow framework.

**Warning signs:**
- Code imports `from airflow import settings` or `from airflow.utils.session import provide_session` inside an operator
- `db.session.execute(...)` or `Session()` calls inside `execute()`
- `XCom.set()` called directly instead of via `context["ti"].xcom_push()`
- Tests pass on Airflow 2.x dev install, fail on Airflow 3.x

**Prevention strategy:**
1. **Treat workers as having zero DB access from day one.** Any state read/write must go through the Task SDK's public interface (XCom via context, custom hooks via API).
2. For cross-DAG freshness state and distributed locking — which require persistent shared state — implement as a **custom hook that talks to a dedicated backend**, not direct metadata DB writes from workers. Options:
   - **Recommended:** Use a separate logical database (or dedicated schema) in the same Postgres instance as the Airflow metadata DB, accessed via a `BaseHook` subclass with its own connection. This isolates failure modes and avoids polluting Airflow's tables.
   - Alternative: Use Airflow Connections + a lightweight HTTP service for state, but this adds infrastructure that violates the project's "no extra infra" goal.
3. Use Airflow's `AIR301` and `AIR302` Ruff rules in CI to catch direct DB access patterns automatically.
4. For Airflow 2.x compatibility shim, use the `apache-airflow-task-sdk` package which exists for both 2.x and 3.x.

**Build phase to address:** Phase 1 (architecture decision). Decision must be locked before any state-management code is written. Adding a separate DB client later requires rewriting the locking layer.

---

### C3. Dynamic task mapping cardinality exceeding `max_map_length`

**What goes wrong:** A `dbt build --select +my_mart` resolves to 1500 models. Dynamic task mapping has a default `max_map_length = 1024`. The expand task fails with `AirflowException: max_map_length exceeded`. Worse, raising `max_map_length` above ~4000 makes the scheduler unresponsive (documented Airflow community finding) and triggers Postgres `stack depth limit exceeded` errors on the metadata DB.

**Warning signs:**
- Selectors that resolve to >800 models (approach the soft ceiling)
- Tests that only use small dbt projects (<100 models)
- Users reporting "expand task failed immediately"
- Postgres logs showing `stack depth limit exceeded` errors

**Prevention strategy:**
1. **Validate selector cardinality at DAG parse time** — count resolved models and fail loud with an actionable error: "Selector resolved to 1500 models, exceeds Airflow's max_map_length of 1024. Split into multiple selectors or raise max_map_length (with caution above 4000)."
2. Document the 800/1024/4000 thresholds prominently in user-facing docs.
3. Provide a "splitter" pattern that auto-shards large selectors into multiple mapped task groups (e.g., by dbt tag, schema, or hash bucket).
4. Use `max_active_tis_per_dag` to throttle concurrent execution even when map size is large — prevents the runtime explosion, even if scheduling succeeds.

**Build phase to address:** Phase 2 (dynamic task mapping integration). Cardinality guard must ship with the first version that supports task mapping — adding it later is a breaking change to user error messages.

---

### C4. dbt manifest schema version drift breaking the parser

**What goes wrong:** The provider hardcodes parsing logic for manifest schema v11. A user upgrades dbt-core from 1.7 → 1.8 and the manifest jumps to v12. The provider crashes with KeyError on a renamed/removed field, or silently mis-parses a renamed field and produces a wrong DAG. The dbt project itself hasn't changed — only the dbt version.

Manifest schema versions advanced rapidly: v5 (dbt 1.1), v6 (dbt 1.2), v10 (dbt 1.6), v11 (dbt 1.7), v12 (dbt 1.8+). dbt-labs's own `upgrade_schema_version` tool was historically delayed in supporting newer versions (Issues #9438, #9487). Even within a single version, the published JSON schema doesn't always match the actual emitted manifest (Issue #8763: dbt 1.6.5 emits fields not in the v10 schema).

**Warning signs:**
- Provider code references manifest fields by raw string keys without version checks
- No CI matrix testing across dbt-core minor versions
- User reports "AttributeError: 'dict' object has no attribute 'X'" after dbt upgrade
- Provider silently accepts a manifest version it has never seen

**Prevention strategy:**
1. **Read the `manifest_version` field FIRST** and dispatch to a version-specific parser. Never assume schema.
2. Maintain a compatibility matrix: which provider versions support which manifest versions. Publish in README.
3. Use **dbt's own `WritableManifest.read_and_check_versions(...)`** if available — let dbt-core do the upgrade work where possible.
4. CI matrix must run against all supported dbt-core minor versions (e.g., 1.7, 1.8, 1.9, latest) on every PR.
5. Provide a clear error when the manifest version is unsupported: "Manifest version v13 not supported by aircontroler 0.4.0. Upgrade aircontroler or downgrade dbt-core."
6. Don't trust the published JSON schema — test against real manifests emitted by real dbt versions.

**Build phase to address:** Phase 1 (manifest parsing layer). The version-dispatch architecture must exist from the first parser commit — retrofitting "version awareness" onto a single-version parser is a rewrite.

---

### C5. `dbt` binary not found on Cloud Run executor

**What goes wrong:** Provider calls `subprocess.run(["dbt", "run", ...])`. On the developer's laptop, `dbt` is on PATH. On Cloud Composer with LocalExecutor it works because dbt was installed in the worker's venv. On Cloud Run executor, the task runs in a fresh container that may not have dbt installed at all, or has it at a non-standard path, or has a venv that isn't activated. Result: `FileNotFoundError: [Errno 2] No such file or directory: 'dbt'`. Same root cause as the well-documented Airflow Issue #23652.

**Warning signs:**
- Tasks fail immediately with FileNotFoundError on first deploy to Cloud Run
- Provider hard-codes `"dbt"` as the executable name
- No `dbt_executable_path` configuration option exposed
- Tests only run with dbt on PATH

**Prevention strategy:**
1. **Always allow `dbt_executable_path` configuration**, defaulting to shutil.which("dbt") with a clear error if not found. Never assume the bare string `"dbt"` works.
2. For Cloud Run executor: document the **container image requirement** explicitly — users must build a container that includes `dbt-core` + `dbt-bigquery` + the provider. Provide a reference Dockerfile.
3. Validate at DAG parse time: log a warning if the configured `dbt_executable_path` doesn't exist on the scheduler (likely also missing on workers).
4. Include `subprocess.run(..., env=...)` with explicit PATH/PYTHONPATH/HOME — don't rely on inherited environment, especially on Cloud Run where the env may be sparse.
5. First task in any user's DAG should be a "preflight" check: run `dbt --version` and fail loud with a useful message if missing.

**Build phase to address:** Phase 3 (Cloud Run executor support). Phase 1/2 can defer this if LocalExecutor is the only target, but the abstraction (configurable executable path) must be in place from the operator's first version.

---

### C6. Distributed locking deadlock with `SELECT FOR UPDATE`

**What goes wrong:** To handle cross-DAG conflict resolution, provider uses `SELECT ... FOR UPDATE` on a custom freshness table to lock a model row. DAG A locks model `X`, then tries to lock model `Y`. DAG B (running concurrently) locks model `Y`, then tries to lock model `X`. Postgres detects the deadlock and aborts one transaction — but if locks are acquired on different rows in different orders across DAGs, this happens randomly under load. Worse: if the provider catches and retries the deadlock, retry storms can pin the metadata DB at 100% CPU.

Row locks on freshness tables have an additional problem: if a row doesn't exist yet (first run of a model), there's nothing to lock — concurrent transactions both proceed thinking they're the only one.

**Warning signs:**
- Postgres logs showing `deadlock detected` errors
- Random task failures with `psycopg2.errors.DeadlockDetected`
- Lock acquisition order varies between code paths
- No deterministic ordering when locking multiple resources
- "Lost update" symptoms: model runs twice despite freshness check

**Prevention strategy:**
1. **Use Postgres advisory locks (`pg_advisory_xact_lock`)**, not row locks. Advisory locks work on integer keys (hash of model unique_id), don't require a row to exist, and are released automatically at transaction end. Pattern is well-established for distributed coordination (Subskribe, Richard Clayton references).
2. **If you must use row locks**, always acquire them in a deterministic order (e.g., sorted by model unique_id) to make deadlocks impossible by construction.
3. For "first run, no row exists" case: use `INSERT ... ON CONFLICT DO NOTHING` to seed the row, then lock.
4. Use `SELECT FOR UPDATE NOWAIT` or `SKIP LOCKED` to fail fast instead of waiting — the `wait/skip/fail` conflict policies in PROJECT.md map naturally onto these primitives.
5. Set a transaction-level statement timeout on the locking transaction (e.g., 30s) so a hung lock doesn't pin a worker forever.
6. Test with **chaos**: spin up 50 concurrent DAG runs targeting overlapping model sets in CI.

**Build phase to address:** Phase 4 (cross-DAG conflict handling / freshness state). This is the hardest pitfall to detect before production. Locking design must be reviewed before any code lands. Strongly consider a small spike/prototype before committing to a locking strategy.

---

## Performance Pitfalls

Will degrade as users scale up — broken at 1000+ models or 50+ DAGs.

### P1. Scheduler overload from large XCom payloads in mapped tasks

**What goes wrong:** The "list of models" passed into `.expand()` is stored in XCom. If each item is a rich dict (full model config, dependencies, dbt args), and there are 1000 models, the XCom row is megabytes. The scheduler reads this every time it expands the mapped task — Airflow's documented behavior is that the scheduler must materialize the full XCom value to count map entries. This pins scheduler CPU and slows every other DAG.

**Warning signs:**
- XCom values >100KB for the upstream task
- Scheduler CPU climbs as more model selectors are added
- `xcom` table size growing rapidly
- Users with custom XCom backends reporting slow expand-phase

**Prevention strategy:**
1. The upstream task that feeds `.expand()` should produce a **list of model unique_ids only** (strings). Never pass rich dicts.
2. Each mapped task fetches its own model details from the cached manifest at runtime — cheap, local, no XCom roundtrip.
3. Document this pattern in the user-facing docs: "We pass strings, not configs."
4. If users need to override per-model config, allow them to register overrides at DAG construction time (Python dict in the DAG file), not via XCom.

**Build phase to address:** Phase 2 (dynamic task mapping design). Setting the data model wrong here forces a breaking API change later.

---

### P2. Custom freshness/lock table without indexes or VACUUM

**What goes wrong:** Provider creates a `aircontroler_model_state` table for freshness tracking. Every task writes one row on completion. After 6 months of running 200 DAGs/day with 500 models each, the table has 18M rows. Queries against it (used at every conflict check) slow from milliseconds to seconds. Postgres autovacuum can't keep up because of the constant write load. Index bloat compounds the problem.

**Warning signs:**
- Custom table grows >1M rows without an archival strategy
- Freshness checks slow at scale
- Postgres `pg_stat_user_tables` showing high `n_dead_tup` for the custom table
- Lock contention on the custom table observed in `pg_locks`

**Prevention strategy:**
1. **Design the schema with retention in mind from day 1.** Required columns: `model_unique_id`, `dag_id`, `run_id`, `completed_at`, `freshness_window_seconds`. Index on `(model_unique_id, completed_at DESC)` for the freshness query.
2. Provide a built-in `aircontroler db cleanup --older-than 90d` CLI command (analogous to Airflow's own `airflow db clean`).
3. Document recommended VACUUM/ANALYZE settings (autovacuum_vacuum_scale_factor=0.05 for this table).
4. Surface table size in the provider's observability output (e.g., a "health check" Airflow operator users can add to a maintenance DAG).
5. **Avoid storing full execution logs in the metadata DB.** Only store coordination state. Execution logs go to Airflow's normal task log infrastructure.

**Build phase to address:** Phase 4 (state persistence). Schema design + retention strategy must ship together.

---

### P3. Repeated manifest re-parsing across worker processes

**What goes wrong:** Each worker process loads the manifest independently. With Cloud Run executor, every task = new container = full manifest re-parse. A 100MB manifest takes 5-10 seconds to JSON-parse. With 1000 mapped tasks running in parallel, this is 1000 cold-start parses — and the user pays for that compute time.

**Warning signs:**
- Long task startup times on Cloud Run (>30s before first dbt log line)
- High Cloud Run cost per task with low actual runtime
- Workers logging "loading manifest" every task

**Prevention strategy:**
1. **Don't re-parse the full manifest per task.** When mapping over models, each task receives only the unique_id and fetches its parsed model dict from a per-process cache. The scheduler can pre-extract per-model dicts at expand time and pass them via XCom (small per-item payloads, see P1).
2. For Cloud Run specifically: ship the manifest as a baked artifact in the container image, not pulled at runtime. Use a build-time `COPY manifest.json /opt/dbt/`.
3. Use `orjson` for parsing — meaningful even at the per-worker level.
4. If using GCS for manifest distribution, prefer streaming download + parse over file-then-parse.

**Build phase to address:** Phase 3 (Cloud Run executor). Phase 1 LocalExecutor implementation can defer; revisit when Cloud Run support is added.

---

### P4. dbt subprocess overhead per model (the "death by a thousand dbts" problem)

**What goes wrong:** Each model = `dbt run --select model_x` = full dbt cold start (~3-15 seconds depending on project size, even with partial parsing). For a 500-model selector, this is 25-125 minutes of pure dbt startup overhead, dwarfing actual SQL execution time. Cosmos hit this exact problem and added the `ExecutionMode.VIRTUALENV` mode (cached venv) and the still-experimental dbt Fusion mode (Rust engine, 10-100x faster startup).

**Warning signs:**
- Per-task wallclock time dominated by "Running with dbt=X.Y.Z" log line delay
- Total DAG duration much longer than expected from BigQuery query times alone
- High Cloud Run cost for short-running models

**Prevention strategy:**
1. **Document the tradeoff explicitly:** native lineage visibility (per-model task) vs. dbt startup overhead. This is the fundamental cost of the project's design.
2. Pre-build the partial parsing artifact (`partial_parse.msgpack`) in the container image so dbt's own startup is faster.
3. Pass the manifest path via `--state` to skip parsing entirely on each invocation — dbt can use a pre-computed manifest as ground truth.
4. Consider an opt-in "batched" mode: group N adjacent models into a single dbt subprocess invocation. Sacrifices per-model granularity for throughput. Make this an explicit user decision, not the default.
5. Track and expose dbt startup time as a metric so users can see the overhead vs. actual SQL time.

**Build phase to address:** Phase 2 (subprocess execution layer). Document the tradeoff publicly in the README before users hit it.

---

## Compatibility Pitfalls

Version/environment issues that surface only on specific user setups.

### CO1. Pinning Airflow version too tightly in `pyproject.toml`

**What goes wrong:** Provider declares `apache-airflow>=2.10,<3.0` because that's what was tested. Airflow 3.x users can't install. Or the provider declares no upper bound and a future Airflow 3.5 breaking change silently breaks the provider. Provider declares `apache-airflow-providers-google>=10.0` and conflicts with Cloud Composer's bundled version.

The Airflow team's own guidance: providers should not include dependencies that conflict with the underlying Airflow version's deps. Real-world example from Airflow docs: google-provider <10.1.0 pinned `protobuf<=3.20.0` while google-ads required `protobuf>=4`, breaking installs.

**Warning signs:**
- `pip check` reports conflicts after installing the provider
- Provider tested only against one Airflow version
- Hard upper bounds on Airflow dependencies (e.g., `apache-airflow<2.11`)
- Provider transitively pins libraries that Airflow also constrains

**Prevention strategy:**
1. **Test against Airflow's official constraints files** in CI. Airflow publishes per-version constraint files at `https://raw.githubusercontent.com/apache/airflow/constraints-X.Y.Z/constraints-3.11.txt` — use these in install tests.
2. CI matrix must include: Airflow 2.10, 2.11 (last 2.x), 3.0, 3.x latest, with Python 3.10, 3.11, 3.12.
3. Use **soft lower bounds** (`>=`) for Airflow itself; avoid upper bounds unless a known-incompatible version exists.
4. **Don't redeclare Airflow's transitive dependencies.** If Airflow already pins `pendulum`, don't add a different pin in your provider.
5. Use the `apache-airflow-providers-{name}` PyPI naming convention (e.g., `apache-airflow-providers-aircontroler`) and follow Astronomer's `airflow-provider-sample` repo as a template.
6. Run `pip check` as a CI step on every supported matrix combination.

**Build phase to address:** Phase 1 (project scaffolding). The packaging structure is the single biggest determinant of "does this install at all" — get it right early.

---

### CO2. Airflow 2.x vs 3.x API divergence in operator base classes

**What goes wrong:** Provider's operator inherits from `airflow.models.BaseOperator`. In Airflow 3.x, the recommended path is `airflow.sdk.BaseOperator` (Task SDK). Some context fields (`execution_date`, `tomorrow_ds`, `prev_ds`) were removed in 3.x. Provider code uses `context["execution_date"]` and crashes silently when `None` is returned in Airflow 3.x.

**Warning signs:**
- Code uses removed context variables (full list: `execution_date`, `tomorrow_ds`, `yesterday_ds`, `prev_ds`, etc.)
- Subclasses `airflow.models.BaseOperator` directly
- Uses SubDAGs (removed in 3.x)
- Uses SequentialExecutor (removed in 3.x)
- Imports from `airflow.operators.python` instead of `airflow.providers.standard.operators.python`

**Prevention strategy:**
1. **Use Ruff with the AIR301 and AIR302 rules** (Airflow 2→3 migration linter) on the provider's own codebase, not just user DAGs.
2. Inherit from `airflow.sdk.BaseOperator` if available; fall back to `airflow.models.BaseOperator` for 2.x. Use a small compatibility shim module (`_compat.py`).
3. Use `context["logical_date"]` (3.x name) with a fallback to `execution_date` (2.x), or use the Task SDK's `get_current_context()` which abstracts this.
4. Avoid removed features (SubDAGs, SequentialExecutor) entirely — they're not in scope per PROJECT.md anyway.
5. Run the full test suite against both Airflow 2.x and 3.x in CI on every PR.

**Build phase to address:** Phase 1 (operator scaffolding). The compatibility shim pattern must be set up before writing the first operator.

---

### CO3. dbt-bigquery adapter version not aligned with dbt-core

**What goes wrong:** Provider depends on `dbt-core>=1.7`. User installs `dbt-core 1.9` but their environment also has an old `dbt-bigquery 1.6` that's incompatible. Or the provider doesn't depend on `dbt-bigquery` at all (correctly leaving it to the user) but doesn't validate the user has it. dbt fails at runtime with cryptic adapter errors.

**Warning signs:**
- Provider lists `dbt-core` as a hard dependency without `dbt-bigquery`
- No preflight check that dbt + adapter combination is functional
- User reports "Could not load adapter type 'bigquery'" errors

**Prevention strategy:**
1. Make `dbt-core` a **soft / optional dependency** (PEP 621 `[project.optional-dependencies] bigquery = ["dbt-core>=1.7", "dbt-bigquery>=1.7"]`). Users install via `pip install aircontroler[bigquery]`.
2. At provider startup, run `dbt --version` and parse the output to verify the adapter is loaded and the versions are within tested ranges. Log a warning, don't crash, if outside tested range.
3. Test matrix: dbt-core 1.7, 1.8, 1.9 × dbt-bigquery matching minor versions.
4. Document the supported version matrix in README, updated per release.
5. Be explicit in error messages: "dbt-bigquery 1.6 detected, requires >=1.7. Run `pip install --upgrade dbt-bigquery`."

**Build phase to address:** Phase 1 (dependency declarations). Phase 2 (preflight checks).

---

### CO4. Manifest-schema-bumping on `dbt-fusion`

**What goes wrong:** dbt Labs is shipping `dbt-fusion` (Rust-based) which generates manifests but, per Cosmos Issue #2263, currently does NOT generate Airflow Assets and may emit subtly different manifest structures. If the provider claims dbt Fusion support without actually testing against Fusion-generated manifests, users hit silent failures.

**Warning signs:**
- Provider docs claim Fusion support without CI evidence
- Asset-scheduled DAGs fail to trigger when manifest is from Fusion
- Users running `dbt-fusion` report missing or wrong tasks

**Prevention strategy:**
1. **Be explicit about what's tested.** If Fusion is not in CI, document "dbt-fusion not yet supported" rather than being silent.
2. When adding Fusion support, treat it as a separate compatibility track (different version dispatch, separate test matrix).
3. Watch the dbt-fusion release pipeline — manifest format changes are likely as it stabilizes.

**Build phase to address:** Out of scope for v1; revisit when Fusion is GA and BigQuery adapter is supported by Fusion.

---

## Operational Pitfalls

Will cause production incidents but are recoverable.

### O1. Cloud Run executor cold start exceeding task timeout

**What goes wrong:** Cloud Run jobs have a configurable task timeout (default short, max 24 hours per Cloud Run docs but Airflow's task heartbeat windows are smaller). A cold start (5-30 seconds for container pull + initialization) plus dbt cold start (5-15 seconds) plus manifest re-parse (5-10 seconds) plus actual work eats budget. Users hit `TaskTimeout` failures that look like dbt failures but are actually orchestration timeouts.

**Warning signs:**
- Tasks failing exactly at the timeout boundary, not on actual SQL issues
- Cloud Run cold starts visible in GCP metrics
- First task per DAG run failing more often than subsequent tasks
- Failures cluster after periods of low activity (cold container pool)

**Prevention strategy:**
1. **Document the cold start budget explicitly:** users must set Airflow task timeouts to (cold_start + dbt_startup + expected_query_time + buffer). Provide a recommended formula in docs.
2. For Cloud Run executor specifically: recommend `min-instances=1` for the dbt task pool to keep at least one warm container (cost vs. latency tradeoff).
3. Use lazy imports inside the operator's `execute()` to minimize container init time. Don't import dbt-core at module scope — let the subprocess do it.
4. Bake the manifest into the container image (see P3) to eliminate one variable.
5. Provide a "warm-up" hook / sentinel task pattern in docs for users who need predictable latency.

**Build phase to address:** Phase 3 (Cloud Run executor support).

---

### O2. profiles.yml leaking credentials in logs

**What goes wrong:** Provider's subprocess invocation logs the working directory, env vars, and command. If `profiles.yml` is rendered with secrets inline (rather than env-var references), or if `DBT_PROFILES_DIR` and a service account JSON path are logged, credentials end up in Airflow task logs visible to anyone with UI access.

**Warning signs:**
- `subprocess.run(... capture_output=True)` with stdout returned to Airflow logs unfiltered
- profiles.yml committed to version control (developer footgun, not the provider's bug, but worth flagging)
- env var dumps in operator logging
- Service account key files referenced by absolute path in logs

**Prevention strategy:**
1. **Never log the full env dict.** If logging env for debugging, allowlist non-sensitive keys (PATH, HOME, PYTHONPATH) and redact everything else.
2. Document the recommended pattern: `profiles.yml` references env vars (`{{ env_var('GCP_KEYFILE_PATH') }}`) which are populated by Airflow Connections or secret backends in production.
3. Use Airflow's secret masking: register sensitive strings via `airflow.utils.log.secrets_masker.mask_secret()` so they get `***` in logs.
4. For Cloud Run + workload identity: prefer ADC (no key file) over JSON keys. Document this prominently.
5. Audit dbt's own log output — dbt 1.7+ generally redacts credentials, but earlier versions don't.

**Build phase to address:** Phase 2 (subprocess execution). Phase 5 (production-readiness review).

---

### O3. Silent re-runs because freshness check happens after model starts

**What goes wrong:** Order of operations: task starts → spawn dbt subprocess → check freshness table → discover model already fresh → kill subprocess. By the time freshness was checked, dbt has already done its compile + connection setup work, wasting time and cost. Worse: race condition where two DAGs both see "not fresh" and both run the model, defeating the entire freshness mechanism.

**Warning signs:**
- Freshness check inside `execute()` after subprocess starts
- No transactional read-then-write on the freshness table
- Multiple DAGs running the same model within seconds of each other in production logs
- Metrics show "freshness saved" count is low

**Prevention strategy:**
1. **Freshness check must be the first thing in `execute()`, before any dbt subprocess.** Pattern:
   ```python
   def execute(self, context):
       with self.lock_model(model_id):
           if self.is_fresh(model_id, freshness_window):
               self.log.info("Skipping; fresh")
               return
           self._run_dbt(model_id)
           self.record_completion(model_id)
   ```
2. Lock acquisition + freshness check + completion recording must all happen in one transaction (or one advisory-lock-protected window). See C6 for locking strategy.
3. Test under load: 10 concurrent DAG runs of the same model must result in exactly 1 dbt invocation, not 10.
4. Surface "freshness skipped" as a structured metric so users can verify it's working.

**Build phase to address:** Phase 4 (freshness + conflict handling). This is the core value prop per PROJECT.md — get it wrong and there's no point in shipping.

---

### O4. Per-model service account selection without GCP IAM groundwork

**What goes wrong:** Provider lets users specify `service_account: foo@project.iam.gserviceaccount.com` per model. On Cloud Run executor with workload identity, this requires the executing service account to have `roles/iam.serviceAccountTokenCreator` on the target SA. Users hit a wall of IAM errors that look like dbt/BigQuery permission errors but are actually impersonation failures.

**Warning signs:**
- `403 Permission denied` errors on `iam.serviceAccounts.getAccessToken`
- User reports "works in one model, fails in another"
- IAM policies not documented as a prerequisite
- No `gcloud iam service-accounts add-iam-policy-binding` in setup docs

**Prevention strategy:**
1. **Implement via SA impersonation** (preferred over swapping keys). Use the `google-auth` `impersonated_credentials.Credentials` flow.
2. Document the IAM prerequisites loudly: the Composer / Cloud Run runtime SA needs `serviceAccountTokenCreator` on every per-model SA.
3. At configuration time (DAG parse), validate that referenced SAs exist and are reachable (lightweight `getIamPolicy` call). Fail loud with actionable message.
4. Provide a "dry run" CLI: `aircontroler validate-iam --manifest manifest.json` that audits all referenced SAs.
5. Consider deferring per-model SA support past v1 (PROJECT.md says "design TBD") — this is a heavyweight feature with significant edge cases.

**Build phase to address:** Deferred — likely Phase 5 or later, after the core flow is solid. Per PROJECT.md, design is TBD.

---

### O5. Manifest-on-disk distribution to workers

**What goes wrong:** Provider expects `manifest.json` at a specific path. Scheduler reads it from one location (e.g., DAGs folder); workers read from another location that may not exist (Cloud Run containers, Composer worker pods). Manifest is stale on workers because it wasn't synced. Or manifest is GBs and the GCS sync takes forever.

**Warning signs:**
- "manifest.json not found" errors only on workers, not scheduler
- Different manifest versions on scheduler vs. worker (timestamp mismatch in logs)
- Slow worker startup correlated with manifest size
- DAG runs succeed in dev (single host) but fail in Cloud Composer (distributed)

**Prevention strategy:**
1. **Define ONE distribution mechanism explicitly.** Recommendation:
   - Scheduler: read manifest from `${dbt_project_dir}/target/manifest.json` (synced via DAG bucket).
   - Workers (LocalExecutor): same path, same bucket, no problem.
   - Workers (Cloud Run): manifest baked into container image at build time. Path must match.
2. Pass the manifest path explicitly as an operator parameter, never assume conventions.
3. Validate at scheduler startup: hash the manifest, store hash, compare on workers. Fail if mismatch.
4. For very large manifests (>50MB), document GCS-backed distribution as the recommended pattern.

**Build phase to address:** Phase 3 (Cloud Run executor). Phase 1 LocalExecutor can defer.

---

## Phase-Specific Warnings Summary

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Phase 1: Manifest parsing | C4 (schema version drift), CO1 (packaging) | Version-dispatch parser from day 1; test against Airflow constraints files |
| Phase 1: Operator scaffolding | C2 (DB access), CO2 (Airflow 2/3 API) | Use Task SDK base class with compat shim; no direct DB sessions ever |
| Phase 2: Dynamic task mapping | C3 (cardinality), P1 (XCom payload size), P4 (subprocess overhead) | Cardinality guard in expand task; pass strings only via XCom; document subprocess cost |
| Phase 2: Subprocess execution | C5 (binary not found), O2 (credential leaks) | `dbt_executable_path` configurable; secrets_masker integration |
| Phase 3: Cloud Run executor | O1 (cold start timeouts), P3 (manifest re-parse), O5 (manifest distribution) | Bake manifest into image; document timeout formula; lazy imports |
| Phase 4: Freshness + locking | C6 (deadlocks), P2 (table bloat), O3 (race conditions) | Advisory locks not row locks; transactional freshness check; archival CLI |
| Phase 5: Per-model SA | O4 (IAM prerequisites) | Document IAM upfront; preflight validation; consider deferring entirely |
| All phases | Manifest-on-disk vs. dbt compile at parse | C1 is the existential risk — never compile at parse time |

---

## Sources

### dbt manifest schema versioning
- [dbt-core Issue #9438 — Support manifest v11 in WritableManifest upgrade_schema_version](https://github.com/dbt-labs/dbt-core/issues/9438) (HIGH)
- [dbt-core Issue #9487 — Manifest upgrade_schema_version needs v11 support](https://github.com/dbt-labs/dbt-core/issues/9487) (HIGH)
- [dbt-core Issue #8328 / #8763 — Published schema for manifest v10 cannot be used for validation](https://github.com/dbt-labs/dbt-core/issues/8328) (HIGH)
- [dbt-core Issue #5033 — Bumping manifest schema to v5](https://github.com/dbt-labs/dbt-core/issues/5033) (HIGH)
- [dbt docs — Project Parsing](https://docs.getdbt.com/reference/parsing) (HIGH)

### Cosmos pitfalls (the project we're outcompeting)
- [astronomer-cosmos Issue #2294 — Optimize manifest loading for DAGs with multiple DbtTaskGroups](https://github.com/astronomer/astronomer-cosmos/issues/2294) (HIGH)
- [astronomer-cosmos Issue #2263 — Airflow 3 Assets not generated using DBT Fusion](https://github.com/astronomer/astronomer-cosmos/issues/2263) (HIGH)
- [astronomer-cosmos Issue #520 — Evaluate alternatives to rendering dbt projects other than during DAG parsing time](https://github.com/astronomer/astronomer-cosmos/issues/520) (HIGH)
- [astronomer-cosmos Issue #1145 / #1185 — Airflow can't find dbt_project.yml](https://github.com/astronomer/astronomer-cosmos/issues/1185) (HIGH)
- [astronomer-cosmos PR #2320 — orjson parser for whole project](https://github.com/astronomer/astronomer-cosmos/pull/2320) (HIGH)
- [Cosmos Caching docs](https://astronomer.github.io/astronomer-cosmos/configuration/caching.html) (HIGH)
- [Cosmos Partial parsing docs](https://astronomer.github.io/astronomer-cosmos/configuration/partial-parsing.html) (HIGH)
- [GetYourGuide — Airflow & dbt at scale with Cosmos](https://www.getyourguide.careers/posts/airflow-dbt-leveraging-astronomer-cosmos-at-getyourguide) (MEDIUM)

### Airflow 2.x → 3.x migration
- [Airflow docs — Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html) (HIGH)
- [Astronomer — Upgrading Airflow 2 to Airflow 3 Checklist for 2026](https://www.astronomer.io/blog/upgrading-airflow-2-to-airflow-3-a-checklist-for-2026/) (HIGH)
- [Airflow Issue #41641 — Auto migration rules (AIR301/AIR302)](https://github.com/apache/airflow/issues/41641) (HIGH)
- [Airflow Task SDK docs](https://airflow.apache.org/docs/task-sdk/stable/) (HIGH)
- [Airflow — Public Interface for Airflow 3.0+](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html) (HIGH)
- [AWS — MWAA migration best practices Airflow 2 to 3](https://aws.amazon.com/blogs/big-data/best-practices-for-migrating-from-apache-airflow-2-x-to-apache-airflow-3-x-on-amazon-mwaa/) (MEDIUM)

### Dynamic task mapping
- [Airflow 3.2 Dynamic Task Mapping docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) (HIGH)
- [AIP-42 Dynamic Task Mapping](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42+Dynamic+Task+Mapping) (HIGH)
- [Airflow Discussion #26172 — Processing millions of files via Dynamic Task Mapping](https://github.com/apache/airflow/discussions/26172) (HIGH)
- [Airflow Issue #29959 — Expand dynamic mapped tasks in batches](https://github.com/apache/airflow/issues/29959) (HIGH)

### Subprocess execution & Airflow integration
- [Airflow Issue #23652 — Dbt operators no longer working after migrating to Airflow 2.3.0](https://github.com/apache/airflow/issues/23652) (HIGH)
- [Astronomer — Orchestrate dbt Core projects with Airflow and Cosmos](https://www.astronomer.io/docs/learn/airflow-dbt) (HIGH)
- [Datafold — Running dbt with Airflow](https://www.datafold.com/blog/running-dbt-with-airflow/) (MEDIUM)

### Distributed locking
- [PostgreSQL docs — Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html) (HIGH)
- [Subskribe — Leveraging Postgres Advisory Locks for Distributed Consensus](https://www.subskribe.com/blog/leveraging-postgres-advisory-locks-for-distributed-consensus) (MEDIUM)
- [Richard Clayton — Distributed Locking with Postgres Advisory Locks](https://rclayton.silvrback.com/distributed-locking-with-postgres-advisory-locks) (MEDIUM)

### Metadata DB performance
- [Astronomer — Understanding the Airflow metadata database](https://www.astronomer.io/docs/learn/airflow-database) (HIGH)
- [Airflow docs — Set up a Database Backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) (HIGH)
- [Airflow Summit 2024 — Optimising Airflow Performance](https://airflowsummit.org/slides/2024/32-Optimizing-Airflow-Performance.pdf) (HIGH)

### Cloud Composer / Cloud Run / GCP
- [Google Cloud — Cloud Composer Access Control with IAM](https://cloud.google.com/composer/docs/composer-2/access-control) (HIGH)
- [Google Cloud — Cloud Composer Known Issues](https://cloud.google.com/composer/docs/known-issues) (HIGH)
- [Google Cloud — Cloud Run task timeout configuration](https://docs.cloud.google.com/run/docs/configuring/task-timeout) (HIGH)
- [Google Cloud — Troubleshoot Cloud Run issues](https://docs.cloud.google.com/run/docs/troubleshooting) (HIGH)

### PyPI packaging
- [Airflow docs — Installation from PyPI (constraints files)](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) (HIGH)
- [Astronomer airflow-provider-sample template](https://github.com/astronomer/airflow-provider-sample) (HIGH)
- [AWS — MWAA best practices for managing Python dependencies](https://aws.amazon.com/blogs/big-data/amazon-mwaa-best-practices-for-managing-python-dependencies/) (MEDIUM)

### XCom backends and custom storage
- [Astronomer — Strategies for custom XCom backends](https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies) (HIGH)
- [Airflow Object Storage XCom Backend docs](https://airflow.apache.org/docs/apache-airflow-providers-common-io/stable/xcom_backend.html) (HIGH)

### Confidence note
All Critical pitfalls (C1-C6) are HIGH confidence — verified against official sources, Cosmos issue tracker, dbt-core issue tracker, or Airflow upgrade documentation. Performance pitfalls (P1-P4) are HIGH confidence — most have direct Cosmos issue references showing they hit these in production. Operational pitfalls (O1-O5) include some MEDIUM confidence items where the failure pattern is well-documented in adjacent contexts (Cloud Run cold starts, IAM impersonation) but specific to this provider's behavior, the exact symptoms will only be observable once built.
