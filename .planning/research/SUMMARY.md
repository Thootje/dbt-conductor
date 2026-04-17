# Research Summary: aircontroler

**Synthesized:** 2026-04-17
**Inputs:** STACK.md, FEATURES.md, ARCHITECTURE.md, PITFALLS.md, PROJECT.md

---

## Recommended Stack

- **`apache-airflow>=2.7,<4.0`** + **`apache-airflow-providers-common-compat>=1.7.0`** — single-import cross-version shim (`from airflow.providers.common.compat.sdk import BaseOperator`) is the canonical 2.x/3.x pattern. No `if AIRFLOW_V_3_0_PLUS` branches anywhere in our code.
- **`dbt-core>=1.7,<1.12`** + **`dbt-bigquery`** under a `[bigquery]` extra. Parser targets manifest schema v11 + v12 explicitly with version dispatch on `manifest_version`.
- **CLI subprocess only** — `subprocess.run(["dbt", ...])` via configurable `dbt_executable_path`. Never import `dbtRunner` into the Airflow process (unsafe for parallel use; dependency-conflict hell). One exception: a single-shot `dbt ls --select X --output json` at parse time to resolve selector syntax.
- **`hatchling` build backend** + `pyproject.toml` with `project.entry-points."apache_airflow_provider"` registering `get_provider_info`. PyPI name: `airflow-provider-aircontroler`.
- **Postgres advisory locks** (`pg_advisory_xact_lock`) for cross-DAG mutex; SQLAlchemy ORM with a separate declarative base. Airflow 3 `BaseDBManager` for migrations, with a manual `aircontroler db init` fallback for 2.x.
- **Optional:** `orjson` behind `[fast-parse]` extra (~40% manifest parse speedup at 50MB+).

---

## Table Stakes

The bar Cosmos has set. Missing any of these and users close the tab.

1. **Model-as-task rendering** with real upstream/downstream wiring in Graph view.
2. **Pre-built manifest support** — accept `manifest_path` as input (avoids 50–100s scheduler stalls from compile-at-parse).
3. **dbt selector syntax verbatim** — accept full `--select tag:daily+ --exclude ...` strings; resolve via `dbt ls`.
4. **Tests and seeds as tasks** — use `dbt build --select <model>` per task so tests become visible Airflow failures.
5. **`on_warning_callback`** hook fed by parsed `run_results.json`.
6. **Per-task overrides** — `retries`, `retry_delay`, `on_failure_callback`, `pool` configurable per model or by tag via `meta.aircontroler.*`.
7. **profiles.yml on disk** with a documented Secret-Manager-to-mounted-file recipe.
8. **Airflow 2.x + 3.x** single package, no separate wheels.

---

## Key Differentiators

- **D1. Time-based freshness policies with cross-DAG state.** Per-model "skip if last successful run < N hours ago" backed by the Airflow metadata DB. This is dbt Cloud's `build_after` (Enterprise-only, paid, 2025) reimplemented for self-hosted Airflow. Nothing in the OSS ecosystem does this for dbt-core.
- **D2. Cross-DAG conflict handling (wait / skip / fail).** When two DAGs target the same model, a distributed lock prevents double-runs. Cosmos has zero equivalent; Dagster issue #15488 has been open since 2023.
- **D3. Zero-infra coordination.** D1 + D2 both live in the Airflow metadata DB. No Redis, no side Postgres, no DynamoDB.
- **Simplicity.** Cosmos's 11 execution modes are widely cited as confusing. Our answer: "Isolation is your Airflow executor's responsibility."

---

## Critical Decisions to Make

### 1. Compile-at-parse vs manifest-as-input ⚠️ CONTRADICTION

- **PROJECT.md** Active requirement: *"Operator compiles dbt manifest via `dbt compile` at DAG parse time."*
- **PITFALLS.md C1 (Critical):** This is the documented #1 Cosmos performance killer (50–100s scheduler stalls).
- **FEATURES.md** Table Stake #2: pre-built manifest support is the #1 thing users want.

**Recommendation:** Default to `manifest_path` (pre-built); offer compile-on-parse as a dev-only opt-in. Update PROJECT.md requirement accordingly.

### 2. "Dynamic task mapping" means static task loop at parse time

PROJECT.md says "dynamic task mapping." Airflow's `.expand()` collapses all model tasks into a single Graph node — destroying the lineage value prop. The correct approach is a Python `for node in graph.nodes: DbtRunModelOperator(task_id=node.name, ...)` loop at parse time. This is what Cosmos does in `build_airflow_graph`.

### 3. Workers cannot access metadata DB directly in Airflow 3

ARCHITECTURE.md proposes direct SQLAlchemy sessions from `execute()`. PITFALLS C2: Airflow 3 workers have no direct DB access. State/locking must go through a dedicated `BaseHook` subclass from day one.

### 4. Freshness policy conflict resolution

If DAG_A says 1h and DAG_B says 6h for the same model — most permissive wins (longest TTL).

### 5. Per-model service account (decide the seam now)

`service_account` parameter must exist on the operator from day one even if unimplemented. Converge on Workload Identity Federation + impersonation. Adding it later is a breaking change.

---

## Watch Out For

1. **Compile-at-parse performance death (C1).** Default to manifest-file mode. Cache parsed manifest at module level.
2. **Direct metadata-DB access from workers (C2).** Route all state I/O through a dedicated hook. Enable Ruff AIR301/AIR302 in CI.
3. **`SELECT FOR UPDATE` deadlocks (C6).** Use `pg_advisory_xact_lock(hashtext(unique_id))` instead of row locks. Always acquire in sorted order.
4. **Manifest schema version drift (C4).** Read `manifest_version` first; dispatch to a version-specific parser. CI matrix covers dbt-core 1.7–1.11.
5. **Task cardinality + XCom payload size (C3 + P1).** Validate selector cardinality at parse time; fail above 800 models. XCom carries `unique_id` strings only — no rich dicts.

---

## Suggested Phase Structure

### Phase 1 — Walking Skeleton
Foundation. Real dbt project running on Composer + BigQuery with per-model Airflow tasks.
Delivers: `DbtRunModelOperator`, `ManifestLoader` (v11+v12), `SelectorResolver` via `dbt ls`, `TaskFactory.build_airflow_graph()`, `DbtBuildOperator`, pyproject.toml with entry points, 2.x/3.x compat shim.

### Phase 2 — Execution Hardening
Productionize subprocess + task structure.
Delivers: Cardinality guard, structured `run_results.json` parsing, `on_warning_callback`, per-task overrides, configurable `dbt_executable_path`, secrets masking.

### Phase 3 — Freshness Policies (Primary Differentiator)
D1 headline feature.
Delivers: `aircontroler_model_runs` table, Alembic migrations, `FreshnessPolicy` dataclass, `meta.aircontroler.freshness` parsing, per-DAG Python override API, skip-before-execute pattern.

### Phase 4 — Cross-DAG Conflict Locking (Primary Differentiator)
D2 skip + fail modes.
Delivers: `pg_advisory_xact_lock` helpers, `ConflictPolicy` (skip / fail), lock-acquire-before-freshness-check flow, chaos test suite. (Spike advisory locks before committing to schema.)

### Phase 5 — Cloud Run + Distribution Polish
Make the target deployment story real.
Delivers: Reference Dockerfile, WIF + impersonation for per-model SA, `service_account` operator parameter, 3–5 working example DAGs.

### Phase 6 — Wait Mode + Observability
Complete D2 with deferrable triggers; ship telemetry.
Delivers: `wait` conflict policy via deferrable Trigger, skipped-count counters, dry-run mode, `aircontroler db cleanup`, Cosmos migration guide.

---

## Confidence Assessment

| Area | Confidence |
|------|------------|
| Stack (Airflow tooling) | HIGH |
| Stack (cross-version edge cases) | MEDIUM |
| Features (Cosmos parity) | HIGH |
| Features (differentiator demand) | MEDIUM-HIGH |
| Architecture (parse/runtime split) | HIGH |
| Architecture (DBManager + advisory locks) | MEDIUM |
| Architecture (Cloud Run specifics) | MEDIUM |
| Pitfalls (C1–C6) | HIGH |

---
*Synthesized: 2026-04-17*
