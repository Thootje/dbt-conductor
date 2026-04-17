# Features Research: Airflow dbt Provider

**Researched:** 2026-04-17
**Domain:** Airflow provider for dbt orchestration
**Primary competitor:** Astronomer Cosmos (200M+ downloads in 2025)
**Overall confidence:** HIGH for Cosmos features (official docs), MEDIUM for community pain points (GitHub issues sample)

---

## Table Stakes (must have or users won't switch from Cosmos)

These are features Cosmos has had for 2+ years. Missing any of them means a user evaluating aircontroler will close the tab and stay on Cosmos.

### 1. Model-as-task rendering (already in scope)
**What:** Each dbt model becomes a first-class Airflow task with retries, alerts, logs visible in Graph view.
**Why expected:** This is the entire reason Cosmos exists; the BashOperator approach (whole project as one task) is what users are explicitly fleeing.
**Complexity (us):** MEDIUM — already in PROJECT.md scope via dynamic task mapping. The wiring of upstream/downstream from the manifest is the meat of the work.
**Cosmos parity:** Yes (default behavior).
**Source:** [Cosmos product page](https://www.astronomer.io/cosmos/), [Astronomer docs: airflow-dbt](https://www.astronomer.io/docs/learn/airflow-dbt)

### 2. Manifest-based parsing (skip `dbt compile` at parse time)
**What:** Allow users to point at a pre-generated `manifest.json` instead of running `dbt compile` during DAG parsing.
**Why expected:** `dbt compile` at parse time is the #1 documented Cosmos performance complaint — scheduler bottlenecks of 50–100s per parse cycle on large projects. Best-practice users generate manifest in CI and ship it with the DAG.
**Complexity (us):** LOW — just an optional `manifest_path` arg on the operator that bypasses the subprocess call. Critical caveat: PROJECT.md currently says "compiles dbt manifest via `dbt compile` at DAG parse time" as the only path. **This requirement should be widened** to accept either a pre-built manifest or compile-on-parse, otherwise we ship with the worst-known Cosmos pain point built in.
**Cosmos parity:** Yes (`LoadMode.DBT_MANIFEST`, plus remote manifest from S3/GCS since 1.6).
**Source:** [Cosmos parsing methods](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html), [Issue #2294: optimize manifest loading](https://github.com/astronomer/astronomer-cosmos/issues/2294), [Issue #520: alternatives to parse-time rendering](https://github.com/astronomer/astronomer-cosmos/issues/520)

### 3. dbt selector syntax in DAG definition (already in scope)
**What:** Accept full `dbt build --select tag:daily+ --exclude config.materialized:ephemeral` strings; resolve via dbt's own selector engine.
**Why expected:** Users already know dbt selector syntax; making them learn a Python-only DSL is a non-starter. Cosmos supports the `selector` arg (named YAML selectors) since 1.3 and inline selectors throughout.
**Complexity (us):** MEDIUM — selector resolution against the manifest is non-trivial; either reuse dbt's selector library directly or shell out to `dbt ls --select ...` and parse output. The latter is simpler but slower. Per PROJECT.md this is in scope.
**Cosmos parity:** Yes.
**Source:** [Cosmos render config](https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html)

### 4. Tests and seeds as tasks
**What:** dbt tests should appear as Airflow tasks (default: a test task after each model that owns the test). Seeds should become tasks too.
**Why expected:** Cosmos defaults to `test_behavior=after_each` (per-model test task group) and added `test_behavior=build` in 1.8 (single `dbt build` task per model). Users orchestrating dbt expect tests to be visible failures, not buried in a model's logs. PROJECT.md mentions `dbt build/run` as input which implies tests, but this needs to be explicit.
**Complexity (us):** MEDIUM — we have to walk the manifest to discover tests attached to each model and emit a downstream test task. `dbt build --select <model>` per task neatly handles this without separate test invocations.
**Cosmos parity:** Yes (multiple test_behavior modes).
**Source:** [Cosmos testing behavior](https://astronomer.github.io/astronomer-cosmos/configuration/testing-behavior.html)

### 5. `on_warning_callback` for dbt warnings (especially source freshness)
**What:** Hook that fires when dbt emits a warning (test warns, source freshness warns), with `test_names` and `test_results` in context.
**Why expected:** Cosmos added this and it's heavily used for routing warnings to Slack/PagerDuty without failing the DAG. Source freshness warnings are particularly important — users want to know stale data was used without halting the pipeline.
**Complexity (us):** LOW–MEDIUM — parse `run_results.json` after each task and invoke the callback with structured context. Just plumbing.
**Cosmos parity:** Yes (added across 1.x; freshness-warning callback was a notable gap addressed).
**Source:** [Cosmos operator args docs](https://astronomer.github.io/astronomer-cosmos/configuration/operator-args.html)

### 6. Multiple execution modes (or a clear story why we don't need them)
**What:** Cosmos exposes 4 in-process modes (local, virtualenv, watcher, airflow_async) and 7+ container modes (docker, k8s, ECS, EKS, ACI, **GCP Cloud Run**, etc.). Users in different deployments need different isolation levels.
**Why expected:** "How do I avoid dbt dependency conflicts with Airflow?" is the most-asked deployment question. Local mode = fastest but conflicts with Airflow's deps. Virtualenv = isolated but slow startup. Container = isolated and consistent but cold-start latency.
**Complexity (us):** PROJECT.md targets LocalExecutor + Cloud Run executor. That covers two of Cosmos's modes naturally (Airflow's executor handles the isolation). **We do not need to ship our own execution-mode abstraction** — the Airflow executor handles that. This is a simplification win, not a parity gap, as long as the README is explicit: "isolation = your Airflow executor's responsibility."
**Cosmos parity:** Different model, not a feature gap. **Differentiator opportunity:** the Cosmos execution-mode abstraction is widely cited as confusing; not having it is a feature.
**Source:** [Cosmos execution modes](https://astronomer.github.io/astronomer-cosmos/getting_started/execution-modes.html)

### 7. Per-task retries, alerts, callbacks (free with native tasks)
**What:** Standard Airflow per-task `retries`, `retry_delay`, `on_failure_callback`, `sla`, etc. configurable per model or by tag.
**Why expected:** Free if we render real Airflow tasks. The expectation is that users can override per-model: "retry the `fct_orders` model 5 times because the source flakes."
**Complexity (us):** LOW — pass args through to the underlying operator. Per-node operator arg customization is a common Cosmos config pattern (`operator_args`, `node_converters`).
**Cosmos parity:** Yes.
**Source:** [Cosmos operator args](https://astronomer.github.io/astronomer-cosmos/configuration/operator-args.html)

### 8. Profiles management with at least one ergonomic mode
**What:** Either bring-your-own `profiles.yml` (PROJECT.md choice) or generate from Airflow Connections. Cosmos supports both; this is a major Cosmos selling point.
**Why expected:** Users hate maintaining `profiles.yml` in source-controlled-but-secret-ridden form. Connection-based profile generation is the headline reason teams pick Cosmos over hand-rolled solutions.
**Complexity (us):** PROJECT.md scopes us to profiles.yml-on-disk for v1 and explicitly defers Connection abstraction. **This is a real risk** — for many teams it's a deal-breaker. Document loudly that v1 = profiles.yml only and have a clear "v2: Airflow Connections" plan, otherwise reviewers will dismiss us as "Cosmos minus the connection thing."
**Cosmos parity:** No (we are deliberately weaker here in v1). Mitigation: ship a documented recipe (Secret Manager → mount profiles.yml → done) so the friction is concrete and small.
**Source:** [Cosmos: dbt-airflow concepts mapping](https://astronomer.github.io/astronomer-cosmos/getting_started/dbt-airflow-concepts.html)

### 9. Generated dbt docs hosting (likely table stakes; verify)
**What:** Run `dbt docs generate` and serve the output (S3/GCS/Azure or via Airflow plugin).
**Why expected:** Cosmos ships `DbtDocsGCSOperator`, `DbtDocsS3Operator`, `DbtDocsAzureStorageOperator`, and an Airflow plugin to serve docs in the Airflow UI.
**Complexity (us):** LOW for the GCS variant only — `dbt docs generate` then upload static assets. The Airflow plugin to serve them in the UI is more work (MEDIUM).
**Cosmos parity:** Yes. **Defer to v2 with no shame** — most teams host docs separately (CI artifact). LOW priority for differentiating, but ship at least a `DbtDocsGCSOperator` equivalent at v1 to avoid "Cosmos has it, you don't" objections.
**Source:** [Cosmos generating docs](https://astronomer.github.io/astronomer-cosmos/configuration/generating-docs.html)

### 10. Airflow 2.x AND 3.x compatibility (already in scope)
**What:** Single package works on both, ideally with feature gating for Airflow 3 features (Assets, the `@asset` decorator).
**Why expected:** Airflow 3 is the future; Airflow 2 is the install base. Cosmos 1.11 added explicit Airflow 3 support including `@asset`. There is a documented Cosmos bug where dbt Fusion + Airflow 3 Assets do not interoperate (#2263) — we can avoid by validating the asset path early.
**Complexity (us):** MEDIUM — abstracting over breaking provider/operator API changes between 2.x and 3.x. Use feature detection rather than version sniffing.
**Cosmos parity:** Yes.
**Source:** [Cosmos 1.11 announcement](https://www.astronomer.io/blog/supercharge-dbt-orchestration-with-astronomer-cosmos-and-apache-airflow/), [Issue #2263: Fusion+Airflow 3 Assets bug](https://github.com/astronomer/astronomer-cosmos/issues/2263)

---

## Differentiators (what makes this worth using over Cosmos)

These are the bets. Each one needs to be the reason a user clicks "install" instead of staying on Cosmos.

### D1. Time-based freshness policies with cross-DAG state (PRIMARY DIFFERENTIATOR)
**What:** Per-model "do not rerun if last successful run was within N hours" check, with state stored in the Airflow metadata DB. Default from `dbt model meta`, overridable per-DAG. When a task starts, it queries the freshness table; if a successful run for this `(project, model, target)` happened within the freshness window, the task short-circuits with a SKIPPED state and a clear log message.
**Value proposition:** This is dbt Cloud's `build_after` feature (released to Enterprise tier in 2025) reimplemented for self-hosted Airflow. Today the only way to get this is to pay for dbt Cloud Enterprise + Fusion. Open-source self-hosters running on BigQuery have no equivalent.
**Why this matters specifically for the target user:** The PROJECT.md target is Composer + BigQuery. BigQuery slot/on-demand costs are the dominant pain. Skipping a model that ran 30 minutes ago saves real money on every triggered DAG run.
**Complexity (us):** MEDIUM — schema design for the state table is the only real decision. We need: (model_unique_id, target_database, target_schema, target_name, last_success_ts, last_run_id, last_dag_id). Suggested approach: a single Airflow metadata-DB table created via Airflow's standard migration path (`airflow db migrate` extension via the provider). Use `func.now()` defaults; index on `(unique_id, target)`. The freshness check is a single SELECT on task start. Storing run metadata is a single INSERT/UPDATE on task success.
**Cosmos parity:** No. Cosmos has zero equivalent. Closest analog in the ecosystem is Dagster freshness policies (per-asset, but no skip-if-fresh execution semantics — Dagster's policies are alerting, not gating). dbt Cloud `build_after` is the only real prior art and it's behind an Enterprise paywall.
**Risk:** Users may try to use this as a "scheduler" (set freshness to 1 hour, schedule DAG every 5 minutes). Document that this is an idempotency layer, not a schedule.
**Source:** [dbt build_after / state-aware orchestration](https://docs.getdbt.com/reference/resource-configs/freshness), [dbt Labs blog on state-aware orchestration](https://www.getdbt.com/blog/announcing-state-aware-orchestration), [Dagster freshness policy issue #15488](https://github.com/dagster-io/dagster/issues/15488)

### D2. Cross-DAG conflict handling: wait / skip / fail (PRIMARY DIFFERENTIATOR)
**What:** When DAG A is currently running model `fct_orders` and DAG B starts a task for the same model, B can be configured to:
- **wait** — block until A's run completes, then evaluate freshness (likely skip),
- **skip** — short-circuit immediately with SKIPPED state,
- **fail** — raise an error (use case: detect bad scheduling).
Backed by row-level lock on a "currently_running" table in the Airflow metadata DB.
**Value proposition:** This is the documented gap in Cosmos. The Cosmos team has not addressed it; the community has filed [Dagster issue #15488](https://github.com/dagster-io/dagster/issues/15488) ("Asset-level concurrency: skip materialization when another materialization is ongoing") which is still open — meaning this gap exists across the orchestrator-dbt landscape, not just Cosmos.
**Why this matters specifically:** Real-world setups have one "main" daily DAG plus several event-driven sub-DAGs (when a Fivetran sync finishes, when a user clicks "refresh," etc.). Without conflict handling these DAGs race, double-run models, and produce inconsistent intermediate states.
**Complexity (us):** MEDIUM-HIGH — distributed locking via Postgres `SELECT ... FOR UPDATE` is well-trodden (Airflow's own scheduler uses this pattern for pool slots), but the deferred/wait variant requires either an Airflow Sensor + reschedule pattern (cheap, polling) or a true deferrable Trigger (better, but more code). Recommendation: ship **skip** and **fail** modes in v1 (synchronous, single SQL roundtrip). Ship **wait** as deferrable Trigger in v2 once the basic mechanics are proven.
**Edge cases to design for:**
- Crashed task that never released its lock → need a TTL/heartbeat or a "max lock age" stored alongside the row.
- Task that legitimately retries → its retry should be allowed to re-acquire the lock it lost.
- DAG that selects 100 models, half conflict → don't fail the whole DAG; per-task decisions only.
**Cosmos parity:** No. Strict differentiator.
**Pool-based mitigation comparison:** Users today work around this with Airflow Pools (one slot per model = serialized access). Pools queue but don't skip; they don't deduplicate; they require manual setup per model. We are strictly more powerful and zero-config.
**Source:** [Airflow scheduler row-level locking](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html), [Dagster issue #15488](https://github.com/dagster-io/dagster/issues/15488), [Airflow Pools docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)

### D3. Zero-infra coordination (sub-differentiator of D1+D2)
**What:** All freshness state and locking lives in the Airflow metadata DB. No Redis, no separate Postgres, no DynamoDB.
**Value proposition:** Composer comes with a managed Postgres. We use it. Users don't provision anything new.
**Complexity (us):** Already low if D1+D2 use the metadata DB.
**Cosmos parity:** N/A — Cosmos doesn't do coordination at all, so there's no infra comparison.

### D4. "What would have run?" dry-run mode (NICE-TO-HAVE)
**What:** Render the DAG, evaluate freshness for every model, and show in logs which models WOULD run vs. WOULD skip — without actually running anything.
**Value proposition:** Operators love this for capacity planning and debugging "why did this take so long?" or "why was this skipped?"
**Complexity (us):** LOW once D1 exists — just a `--dry-run` flag on the operator that exits before subprocess invocation.
**Cosmos parity:** No.

### D5. Conflict telemetry / "deduplication savings" report
**What:** A simple table or Airflow metric (`aircontroler.skipped_due_to_freshness_total`, `aircontroler.skipped_due_to_lock_total`) that lets users see how much they're saving.
**Value proposition:** Hard ROI numbers for the team that pushed for this provider.
**Complexity (us):** LOW — counters incremented on every short-circuit. Defer to v2.

---

## Anti-Features (deliberately NOT building)

### A1. Watcher / single-process execution mode
**What Cosmos does:** Watcher mode (`EXECUTION_MODE.WATCHER`, GA in 1.11) runs `dbt build` once and uses Airflow tasks to track state of each model — claims "up to 80% faster" by avoiding per-model subprocess spawn.
**Why we should NOT build this:** Watcher fundamentally breaks the "each model is an isolated CLI call" model. It defeats per-task retry semantics, per-task service accounts (which PROJECT.md explicitly wants for BigQuery), and per-task isolation in Cloud Run. It's a performance optimization that tradeoffs the very things our differentiators (D1, D2) depend on.
**What to do instead:** Document plainly: "Watcher mode is faster on small/medium projects but breaks per-task isolation. If you don't need cross-DAG dedup or per-model identities, Cosmos Watcher mode is faster than us; use that. If you need our differentiators, the per-task overhead is the price."
**Source:** [Cosmos 1.11 GA blog: watcher mode](https://www.astronomer.io/blog/cosmos-1-11-ga/)

### A2. dbt Cloud / dbt Platform integration
**What it would be:** Operators that hit the dbt Cloud REST API (`DbtCloudRunJobOperator` etc.).
**Why we should NOT build this:** This space is fully owned by Apache's official `apache-airflow-providers-dbt-cloud`. Reproducing it adds maintenance, fragments the ecosystem, and doesn't serve our target user (who is running dbt-core to *avoid* paying dbt Cloud).
**What to do instead:** Make it explicit in the README: "If you use dbt Cloud, use the apache-airflow-providers-dbt-cloud provider. aircontroler is for dbt-core users."
**Source:** [apache-airflow-providers-dbt-cloud](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/index.html)

### A3. Adapter abstraction beyond BigQuery
**What it would be:** Plugin system for Snowflake / Redshift / Postgres / Databricks adapters.
**Why we should NOT build this in v1:** PROJECT.md is explicit. The freshness/lock features (D1, D2) are warehouse-agnostic, but the per-model service-account selection is BigQuery-specific. Trying to abstract too early balloons the surface.
**What to do instead:** Build BigQuery-only with adapter-agnostic seams (no `if bigquery:` checks scattered through the code; isolate adapter code to one module). Add Snowflake in a later milestone if there's demand.
**Source:** PROJECT.md constraints.

### A4. Custom DSL / non-dbt syntax for selecting models
**What it would be:** A Python-only API like `dbt_dag(include=["fct_*", "dim_*"], exclude=["staging.*"])`.
**Why we should NOT build this:** dbt's selector syntax exists, is documented, and users know it. A custom DSL is a recurring complaint about other tools (the Cosmos ebook explicitly highlights "we let you pass dbt's own selectors" as a feature).
**What to do instead:** Accept selector strings verbatim. Allow users to pass full `dbt build --select ...` arglists.

### A5. Asset-graph rendering (Airflow 3 Assets)
**What it would be:** Auto-emit Airflow 3 `@asset` definitions from the dbt manifest so downstream DAGs can be triggered by dbt-asset materialization.
**Why we should NOT build this in v1:** Cosmos has tried and shipped a known-broken implementation (Issue #2263: Fusion + Airflow 3 Assets generates no assets). The asset model and dbt's lineage model don't align cleanly (asset URIs vs. dbt unique_ids, partitioning semantics, etc.). Build this in v2 once Cosmos has done the design exploration for us.
**What to do instead:** Mention in README as "planned for v2." Ship v1 with classic task-based DAGs only.
**Source:** [Issue #2263: Fusion+Airflow 3 Assets bug](https://github.com/astronomer/astronomer-cosmos/issues/2263)

### A6. dbt-core Python API execution
**What it would be:** Use `dbtRunner.invoke(["run", "--select", model])` instead of subprocess.
**Why we should NOT build this:** PROJECT.md is correct. The Python API has unstable signatures across minor versions and pulls dbt's deps into the Airflow process (dependency-conflict hell). Cosmos's recent move toward `dbtRunner` for `dbt ls` is a performance optimization that works for them only because they own the version pinning. We don't.

### A7. Source freshness as a separate task type
**What Cosmos does:** Renders `dbt source freshness` as Airflow tasks attached to source nodes.
**Why deprioritize for v1:** Useful but not differentiating. Source freshness is conceptually orthogonal to model freshness (D1) and the implementation is straightforward but adds surface area. Defer to v2.

---

## Competitive Landscape

| Tool | What it is | Strengths | Weaknesses (vs. our differentiators) |
|------|-----------|-----------|---------------------------------------|
| **Astronomer Cosmos** | OSS Airflow library, `DbtDag` / `DbtTaskGroup` | Mature; 200M+ downloads in 2025; Airflow 3 + Fusion + 11 execution modes; rich docs/test/seed support | No cross-DAG conflict handling. No freshness-based skip. Manifest-parsing performance issues on large projects (issue #2294, #520, #722). Connection abstraction is great but execution-mode taxonomy is widely cited as confusing. |
| **apache-airflow-providers-dbt-cloud** | Official Apache provider, dbt Cloud API only | Maintained by Apache; deferrable triggers; standard provider patterns | Requires dbt Cloud (paid). Whole job runs as one Airflow task — no per-model lineage. Not a competitor for the dbt-core user. |
| **prefect-dbt + prefect-dbt-flow** | Prefect's dbt integration | Per-node tasks; freshness-check trigger pattern; clean Python API | Not Airflow. Different orchestrator entirely. Useful only as a feature reference. |
| **dagster-dbt** | Dagster's dbt integration | First-class assets; freshness policies (alerting); rich lineage UI; data-versioning that *can* skip materialization | Not Airflow. Freshness policies are alerting/observation, not gating execution. Issue #15488 (skip-when-running) still open since 2023, confirming our gap is a real industry gap. |
| **dbt Cloud (jobs UI)** | dbt Labs' SaaS | State-aware orchestration via `build_after` (the *exact* feature we're cloning); managed; supports Fusion | Enterprise tier only. Vendor lock-in. Doesn't integrate with the rest of an Airflow data platform. |
| **Hand-rolled BashOperator** | `BashOperator("dbt run")` | Trivial; works | One opaque task. No lineage. No retry granularity. Universally derided. |

**Key competitive insight:** dbt Cloud's `build_after` (Enterprise, Fusion-only, 2025) validates D1's value to the market. The only places to get it today are dbt Cloud Enterprise (paid) or self-built. We are the OSS, dbt-core-compatible alternative.

---

## User Pain Points (documented from issues, blog posts, forum threads)

These are pain points users have publicly complained about. Bold ones map to our differentiators or anti-features.

1. **DAG parsing is slow on large dbt projects.** 50–100s scheduler stalls per parse cycle when manifest is 30+ MB. Workers also re-parse on cold start. (Cosmos issues [#2294](https://github.com/astronomer/astronomer-cosmos/issues/2294), [#722](https://github.com/astronomer/astronomer-cosmos/issues/722), [#520](https://github.com/astronomer/astronomer-cosmos/issues/520)) → **Mitigation:** support pre-built manifest path (Table Stakes #2). Cache manifest in module-level state.
2. **Workers run out of disk** copying the dbt project to a tmp dir per task; SIGKILL cascades. ([Issue #362](https://github.com/astronomer/astronomer-cosmos/issues/362)) → **Mitigation:** PROJECT.md target is Cloud Run executor where each task is a fresh container. Document this anti-pattern; on shared LocalExecutor workers, document recommended tmp dir hygiene.
3. **dbt deps conflict with Airflow deps.** Causes deployment headaches; drives users toward virtualenv/container modes. ([Cosmos #1751](https://github.com/astronomer/astronomer-cosmos/issues/1751), general dbt-Airflow forum complaint) → **Mitigation:** PROJECT.md's CLI-subprocess approach sidesteps this — dbt is installed alongside Airflow but never imported into the Airflow process.
4. **Documentation maturity / lack of concrete examples.** Cited in multiple blog posts ("requires significant time investment to establish a workable DAG"). → **Mitigation:** ship 3–5 fully-working example DAGs (BigQuery + Composer + Cloud Run) at v1.
5. **Source nodes default to empty operators**, breaking inter-DAG dependency expectations. → **Mitigation:** out of scope for v1 (defer source rendering); document the limitation.
6. **No way to skip a model that ran recently (D1).** Users requesting `build_after`-equivalent in dbt-core are pointed to dbt Cloud Enterprise. (Forum threads, dbt-core issue #12719) → **Our D1.**
7. **No way to prevent the same model running concurrently from two DAGs (D2).** Users today work around with Airflow Pools (one-slot pool per model = forced serialization, no dedup). → **Our D2.**
8. **Cosmos's execution-mode taxonomy is confusing.** Users routinely ask which of 11 modes they should pick. → **Our simplification:** "Pick your Airflow executor; we don't have an opinion."

---

## Implications for Roadmap

If grouping into phases:

1. **Phase 1 (Walking skeleton):** Manifest parse → dynamic task mapping → CLI subprocess → BigQuery profile. Table Stakes #1, #3, #6 (no execution-mode abstraction), #8 (profiles.yml only). Get a real dbt project running on a real Composer + BigQuery deployment end-to-end. **No differentiators yet** — prove the boring thing works.
2. **Phase 2 (Differentiator: freshness):** D1 + D3. Ship the freshness table, the check-on-start logic, the per-model meta config, the per-DAG override API. This is the headline feature; it needs to be excellent.
3. **Phase 3 (Differentiator: locking):** D2, skip + fail modes only (not wait). Distributed lock via metadata DB. Telemetry for skipped runs.
4. **Phase 4 (Polish for adoption):** Table Stakes #2 (pre-built manifest path), #4 (tests as tasks via `dbt build`), #5 (`on_warning_callback`), #7 (per-task arg overrides), #9 (docs hosting), #10 (Airflow 3 explicit support). Plus example DAGs and migration guide from Cosmos.
5. **Phase 5 (Deferred-execution differentiator):** D2 wait-mode via deferrable Triggers. D4 dry-run. D5 telemetry dashboard.

**Critical roadmap-level recommendation:** Re-open the Active requirement in PROJECT.md that says "Operator compiles dbt manifest via `dbt compile` at DAG parse time." This is the #1 documented Cosmos pain point. The requirement should read: "Operator accepts either a pre-built manifest path OR compiles via `dbt compile` at DAG parse time, with the pre-built path as the recommended production pattern."

---

## Sources

### Cosmos (primary competitor)
- [Cosmos product page](https://www.astronomer.io/cosmos/)
- [Cosmos documentation home](https://astronomer.github.io/astronomer-cosmos/)
- [GitHub: astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos/)
- [Cosmos 1.11 GA / Watcher mode](https://www.astronomer.io/blog/cosmos-1-11-ga/)
- [Cosmos 1.11 alpha / dbt Fusion](https://www.astronomer.io/blog/supercharge-dbt-orchestration-with-astronomer-cosmos-and-apache-airflow/)
- [Astronomer: Orchestrate dbt Core with Airflow and Cosmos](https://www.astronomer.io/docs/learn/airflow-dbt)
- [Cosmos parsing methods](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html)
- [Cosmos execution modes](https://astronomer.github.io/astronomer-cosmos/getting_started/execution-modes.html)
- [Cosmos render config](https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html)
- [Cosmos testing behavior](https://astronomer.github.io/astronomer-cosmos/configuration/testing-behavior.html)
- [Cosmos operator args](https://astronomer.github.io/astronomer-cosmos/configuration/operator-args.html)
- [Cosmos generating docs](https://astronomer.github.io/astronomer-cosmos/configuration/generating-docs.html)
- [Cosmos lineage / OpenLineage config](https://astronomer.github.io/astronomer-cosmos/configuration/lineage.html)

### Cosmos issues (pain points)
- [#520: Evaluate alternatives to rendering dbt projects other than during DAG parsing time](https://github.com/astronomer/astronomer-cosmos/issues/520)
- [#722: Tasks are running full parse each time](https://github.com/astronomer/astronomer-cosmos/issues/722)
- [#2294: Optimize manifest loading for DAGs with multiple DbtTaskGroups](https://github.com/astronomer/astronomer-cosmos/issues/2294)
- [#362: Slimming down tmp_dir to just the --target-dir](https://github.com/astronomer/astronomer-cosmos/issues/362)
- [#1323: Support adding concurrency argument to DbtTaskGroup](https://github.com/astronomer/astronomer-cosmos/issues/1323)
- [#1751: Task execution hanging on Airflow 2 when using dbt runner in DAG processing](https://github.com/astronomer/astronomer-cosmos/issues/1751)
- [#2263: Airflow 3 Assets not generated using DBT Fusion](https://github.com/astronomer/astronomer-cosmos/issues/2263)
- [#2239: Optionally pass Airflow variables through to dbt via --vars](https://github.com/astronomer/astronomer-cosmos/issues/2239)

### Other orchestrator-dbt integrations
- [apache-airflow-providers-dbt-cloud docs](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/index.html)
- [dbt Cloud operator details](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/operators.html)
- [Dagster dbt integration docs](https://docs.dagster.io/integrations/libraries/dbt/dagster-dbt)
- [Dagster freshness policies](https://docs.dagster.io/guides/observe/asset-freshness-policies)
- [Dagster issue #15488: Asset-level concurrency, skip when materialization ongoing](https://github.com/dagster-io/dagster/issues/15488)
- [Dagster issue #10924: Freshness policies for dbt assets](https://github.com/dagster-io/dagster/issues/10924)
- [prefect-dbt-flow](https://github.com/datarootsio/prefect-dbt-flow)
- [Prefect dbt integration docs](https://prefecthq.github.io/prefect-dbt/)

### dbt freshness / state-aware orchestration
- [dbt freshness reference](https://docs.getdbt.com/reference/resource-configs/freshness)
- [dbt: About state-aware orchestration](https://docs.getdbt.com/docs/deploy/state-aware-about)
- [dbt Labs: Announcing state-aware orchestration](https://www.getdbt.com/blog/announcing-state-aware-orchestration)
- [dbt Labs: Using state-aware orchestration to slash data costs](https://www.getdbt.com/blog/using-state-aware-orchestration-to-slash-your-data-costs)
- [dbt-core issue #12719: Model freshness checks EPIC](https://github.com/dbt-labs/dbt-core/issues/12719)

### Airflow infrastructure / locking
- [Airflow scheduler docs (row-level locking)](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)
- [Airflow metadata database](https://www.astronomer.io/docs/learn/airflow-database)
- [Airflow Pools (cross-DAG resource limiting)](https://www.sparkcodehub.com/airflow/variables-connections/pools)
- [Airflow + OpenLineage integration](https://www.astronomer.io/docs/learn/airflow-openlineage)

### BigQuery service-account specifics
- [dbt: BigQuery service account impersonation setup](https://docs.getdbt.com/faqs/Warehouse/bq-impersonate-service-account-setup)
- [dbt: Why impersonate a service account](https://docs.getdbt.com/faqs/Warehouse/bq-impersonate-service-account-why)
- [dbt-bigquery issue #769: impersonate_service_account + long timeouts](https://github.com/dbt-labs/dbt-bigquery/issues/769)
