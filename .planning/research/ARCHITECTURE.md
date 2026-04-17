# Architecture Research: Airflow dbt Provider

**Project:** aircontroler
**Researched:** 2026-04-17
**Confidence:** MEDIUM-HIGH (Cosmos source code + Airflow docs verified; some Cloud Run specifics inferred)

---

## Component Map

The provider should be organized into seven distinct layers. Data flows **downward** (from user-facing API to subprocess execution) and **upward** (from execution results to metadata DB to scheduling decisions). Cross-cutting concerns (config, logging) sit beside the flow.

```
+-----------------------------------------------------------------------+
| User-Facing API Layer                                                 |
|   DbtBuildOperator (the public entry point — accepts a full           |
|   `dbt build/run --select ...` string + project config)               |
+---------------------------------------+-------------------------------+
                                        |
                                        v
+-----------------------------------------------------------------------+
| Orchestration Layer (DAG parse time)                                  |
|   GraphBuilder — invokes Manifest Loader, applies selector,           |
|   wires task dependencies into the surrounding TaskGroup/DAG          |
+----------------+------------------------------------+-----------------+
                 |                                    |
                 v                                    v
+----------------------------+      +----------------------------------+
| Manifest Loader            |      | Selector Resolver                |
|   - Runs `dbt compile` or  |      |   - Parses `--select` syntax     |
|     reads cached manifest  |      |   - Filters DbtNode set          |
|   - Produces DbtNode list  |      |   - Reuses dbt selector logic    |
+--------------+-------------+      +----------------+-----------------+
               |                                     |
               +------------------+------------------+
                                  v
+-----------------------------------------------------------------------+
| Task Factory                                                          |
|   - Per-node: instantiates DbtRunModelOperator (one per dbt model)    |
|   - Wires upstream/downstream from DbtNode.depends_on                 |
|   - Applies freshness policy + conflict policy from `meta`/Python     |
+---------------------------------------+-------------------------------+
                                        |
                                        v
+-----------------------------------------------------------------------+
| Execution Layer (runtime, per task)                                   |
|   DbtRunModelOperator.execute()                                       |
|     1. Acquire freshness lock (StateStore.try_lock)                   |
|     2. Check freshness (StateStore.is_fresh)                          |
|     3. If stale: shell out to `dbt run --select model_name`           |
|     4. Parse run_results.json                                         |
|     5. Update StateStore with result                                  |
|     6. Release lock                                                   |
+----------------+------------------------+-----------------------------+
                 |                        |
                 v                        v
+----------------------------+   +----------------------------------+
| Subprocess Runner          |   | StateStore (DB Layer)            |
|   - Wraps subprocess.run   |   |   - SQLAlchemy ORM models        |
|   - Streams stdout to log  |   |   - Freshness queries            |
|   - Surfaces exit code     |   |   - Lock acquire/release         |
|   - Honors profiles.yml    |   |   - Migration registration       |
|     path + project path    |   |     (Alembic via DBManager)      |
+----------------------------+   +----------------+-----------------+
                                                  |
                                                  v
                                  +----------------------------------+
                                  | Airflow Metadata DB (Postgres)   |
                                  |   - aircontroler_model_runs      |
                                  |   - aircontroler_model_locks     |
                                  +----------------------------------+
```

### Module Layout (suggested package structure)

```
aircontroler/
  __init__.py               # get_provider_info() entry point
  provider_info.py          # provider metadata (name, versions, hooks)
  config/
    __init__.py
    project_config.py       # ProjectConfig (dbt project path, profiles path)
    execution_config.py     # ExecutionConfig (LOCAL vs CLOUD_RUN)
    policy_config.py        # FreshnessPolicy, ConflictPolicy dataclasses
  dbt/
    __init__.py
    manifest.py             # ManifestLoader — runs `dbt compile`, parses JSON
    graph.py                # DbtNode dataclass, DbtGraph container
    selector.py             # SelectorResolver — wraps dbt's selector logic
  airflow/
    __init__.py
    operators/
      base.py               # BaseDbtRunOperator (shared logic)
      local.py              # DbtRunLocalOperator (subprocess on Airflow worker)
      cloud_run.py          # DbtRunCloudRunOperator (delegates to CloudRun job)
    task_factory.py         # build_airflow_graph(nodes, dag, ...) -> tasks_map
    dag_builder.py          # DbtBuildOperator — public entry; orchestrates above
  state/
    __init__.py
    models.py               # SQLAlchemy ORM: ModelRun, ModelLock
    db_manager.py           # AircontrolerDBManager(BaseDBManager)
    migrations/
      env.py
      versions/
        0001_initial.py     # Alembic migration creating tables
    store.py                # StateStore — high-level API (is_fresh, try_lock)
    locks.py                # advisory lock helpers (pg_advisory_xact_lock)
  subprocess/
    __init__.py
    runner.py               # DbtCliRunner — wraps subprocess + log streaming
    parser.py               # parses run_results.json
  exceptions.py
  constants.py
tests/
  unit/
  integration/
pyproject.toml              # entry point: apache_airflow_provider
```

### Component Interaction Rules

| Component | May Depend On | Must NOT Depend On |
|-----------|---------------|--------------------|
| `dbt/*` | stdlib, `dbt-core` (parser only), pydantic | Airflow, SQLAlchemy, state |
| `state/*` | SQLAlchemy, Alembic, Airflow Session | dbt, subprocess, operators |
| `subprocess/*` | stdlib, dbt CLI | Airflow, state, dbt parser |
| `airflow/operators/*` | base operator, state, subprocess, dbt | dag_builder (avoid cycle) |
| `airflow/task_factory.py` | operators, dbt graph | dag_builder |
| `airflow/dag_builder.py` | task_factory, manifest loader | — (top of chain) |
| `config/*` | stdlib, pydantic | everything else |

The strict dependency direction makes each layer **independently testable**. `dbt/*` can be unit-tested with manifest fixtures. `state/*` can be tested with an in-memory SQLite. `subprocess/*` can be tested by mocking `subprocess.run`. Operators can be tested with Airflow's `BaseOperator.test()` plus mocked subprocess + state.

---

## DAG Parse vs Runtime

The boundary here is the single most important architectural decision in this project. Get it wrong and the scheduler will grind to a halt or the lineage view will be empty.

### What MUST Happen at Parse Time

DAG parse time is when Airflow's scheduler imports the Python file to discover the task graph. **The set of tasks and their dependencies must be known at this point** — this is non-negotiable for the "first-class task per model" requirement and for native Graph view rendering.

| Activity | Where | Why |
|----------|-------|-----|
| `dbt compile` invocation (or read cached manifest) | Parse time | Need DbtNode list to know task count |
| Selector resolution (`--select tag:daily`) | Parse time | Need filtered node set |
| Task instantiation (one operator per node) | Parse time | Required for static graph in Airflow UI |
| Dependency wiring (`upstream >> downstream`) | Parse time | Lineage rendering needs explicit edges |

### What MUST Happen at Runtime

| Activity | Where | Why |
|----------|-------|-----|
| Freshness check against StateStore | Task runtime | State changes between parses |
| Lock acquisition for cross-DAG conflict resolution | Task runtime | Concurrency live |
| `dbt run --select <one_model>` subprocess | Task runtime | Actual execution |
| Updating ModelRun row | Task runtime | Records execution outcome |

### Why NOT Use Dynamic Task Mapping (.expand()) for Models

This is a common confusion. Airflow's `dynamic task mapping` (`.expand()`) defers the task **count** to runtime, based on an upstream task's XCom output. The OperatorPartial / MappedOperator machinery creates `n` copies right before execution.

**This is the wrong tool for dbt models.** Reasons:

1. **Lineage breakage:** Mapped tasks render as a single collapsed node in the Graph view ("model[0]", "model[1]" ...) — not as named, individually-clickable model tasks. This destroys the core value proposition.
2. **No per-model dependencies:** `.expand()` produces a "fan-out, fan-in" pattern. Inter-model dependencies (model_a -> model_b -> model_c) cannot be expressed across the expanded set.
3. **Per-task config impossible:** Different models need different freshness policies, retries, pools. With mapping, all expanded instances share the partial config.

**Use static expansion at parse time instead.** The PROJECT.md mentions "dynamic task mapping" but the architectural intent is "dynamically generate static tasks at parse time" — a Python `for node in graph.nodes: DbtRunModelOperator(task_id=node.name, ...)` loop. This is exactly Cosmos' approach (see `build_airflow_graph` in `cosmos/converter.py`).

### Parse-Time Performance Hazard

Cosmos has a documented issue where dynamic task generation slows DAG file processing to the point of scheduler timeouts. Mitigations to design in from day one:

1. **Manifest caching by content hash.** Hash the dbt project files; if unchanged, skip `dbt compile` and load cached manifest from disk. Cosmos uses `LoadMode.DBT_LS_CACHE` storing in Airflow Variables — we can do the same with a content-addressed file cache.
2. **Lazy compile via "manifest mode."** Allow users to provide a pre-compiled `manifest.json` path (CI artifact) so parse never shells out. This is the single biggest perf win.
3. **Parse-time budget telemetry.** Log parse duration with model count. Helps users diagnose "why is my DAG slow?" without code-diving.

### Recommended LoadModes (mirror Cosmos)

| Mode | When to Use | Speed |
|------|-------------|-------|
| `DBT_COMPILE` (default) | Local dev, small projects (<200 models) | Slow (10s+) |
| `MANIFEST_FILE` | Production, CI-built manifests | Fast (<1s) |
| `MANIFEST_FILE_CACHED` | Same as above, with content-hash cache | Fastest |

---

## Metadata DB Extension

This is the trickiest part of the architecture and the place where most homegrown providers go wrong. There are three valid approaches and one tempting-but-wrong approach.

### The Wrong Approach: Side-Channel Database

Tempting because it's simple: spin up a dedicated Postgres for state/locks, separate from Airflow's metadata DB. **Don't.** It violates the PROJECT.md constraint ("no extra infrastructure") and introduces a second DB to back up, monitor, and migrate.

### Approach 1 (RECOMMENDED): Airflow 3 DBManager + SQLAlchemy on Airflow's Engine

Airflow 3.0+ introduced a first-class extension mechanism for this exact use case: `BaseDBManager`. The FAB provider (`apache-airflow-providers-fab`) uses this pattern for its session/permission tables. See `airflow.providers.fab.auth_manager.models.db.FABDBManager`.

**How it works:**

1. Define SQLAlchemy ORM models inheriting from a **separate declarative base** (do NOT extend Airflow's internal `Base`):
   ```python
   from sqlalchemy.orm import declarative_base
   AircontrolerBase = declarative_base()

   class ModelRun(AircontrolerBase):
       __tablename__ = "aircontroler_model_runs"
       # columns...
   ```
2. Create an Alembic migration directory under `aircontroler/state/migrations/` with its own `env.py` pointing at `AircontrolerBase.metadata`.
3. Implement `AircontrolerDBManager(BaseDBManager)` pointing at the migrations directory and metadata.
4. Register it in the user's `airflow.cfg`:
   ```ini
   [core]
   external_db_managers = airflow.providers.fab...,aircontroler.state.db_manager.AircontrolerDBManager
   ```
5. User runs `airflow db migrate` and our tables are created in the same Airflow metadata DB.

**Confidence:** MEDIUM. This is the mechanism Airflow 3 explicitly supports (commit `93c5caa` adds the CLI). But: documentation is thin and there's an Airflow 2.x compatibility gap (DBManager doesn't exist in 2.x).

### Approach 2 (FALLBACK for Airflow 2.x): Idempotent Schema Bootstrap

Airflow 2.x does not have `BaseDBManager`. For 2.x compatibility:

1. Define ORM models with `extend_existing=True` and `if_not_exists=True` table args.
2. Provide a CLI command (`aircontroler db init`) that runs `Base.metadata.create_all(airflow_engine)`.
3. Document this as a one-time setup step.
4. On every DAG parse, do a cheap existence check (`inspect(engine).has_table(...)`) and raise a friendly error if missing.

This is uglier than approach 1 but works. **Recommendation:** support both — DBManager for 3.x, manual bootstrap for 2.x. Detect Airflow version at import time.

### Tables to Create

```sql
-- aircontroler_model_runs: one row per (dag_id, task_id, model_unique_id, execution_date)
CREATE TABLE aircontroler_model_runs (
  id              BIGSERIAL PRIMARY KEY,
  model_unique_id VARCHAR(512) NOT NULL,    -- dbt unique_id e.g. "model.proj.foo"
  dag_id          VARCHAR(250) NOT NULL,
  task_id         VARCHAR(250) NOT NULL,
  run_id          VARCHAR(250) NOT NULL,
  started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
  finished_at     TIMESTAMP WITH TIME ZONE,
  status          VARCHAR(32) NOT NULL,     -- success/failed/skipped/running
  rows_affected   BIGINT,
  bytes_processed BIGINT,
  error_message   TEXT,
  INDEX idx_model_unique_id_finished (model_unique_id, finished_at DESC),
  INDEX idx_dag_run (dag_id, run_id)
);

-- aircontroler_model_locks: distributed mutex per model
CREATE TABLE aircontroler_model_locks (
  model_unique_id VARCHAR(512) PRIMARY KEY,
  holder_dag_id   VARCHAR(250) NOT NULL,
  holder_task_id  VARCHAR(250) NOT NULL,
  holder_run_id   VARCHAR(250) NOT NULL,
  acquired_at     TIMESTAMP WITH TIME ZONE NOT NULL,
  expires_at      TIMESTAMP WITH TIME ZONE NOT NULL  -- TTL for stale lock recovery
);
```

### Distributed Locking: PostgreSQL Advisory Locks (RECOMMENDED)

For the conflict-resolution use case ("two DAGs want to run model_x at the same time, only one should run"), there are three options:

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| `SELECT FOR UPDATE` on `aircontroler_model_locks` row | Portable (works on Postgres + MySQL) | Requires a row to exist; blocks readers; deadlock risk if access order varies | OK for MySQL fallback |
| **`pg_advisory_xact_lock(hashtext(model_id))`** | Fastest; no row writes; auto-released at txn end; no table bloat | Postgres-only | **Recommended for Postgres** |
| Custom poll loop on a "lock" table with TTL | Portable; visible to operators | Race conditions; needs heartbeat for liveness | Avoid |

**Architecture decision:** Use **transaction-scoped advisory locks** (`pg_advisory_xact_lock`) on Postgres, fall back to `SELECT FOR UPDATE` on MySQL. Hide the dialect difference behind `state/locks.py::acquire_model_lock(session, model_unique_id, timeout_s)`.

The `aircontroler_model_locks` table is for **observability** (which DAG is currently running which model — visible to operators), not for the actual mutex. Insert/update rows inside the advisory lock-protected transaction.

```python
# state/locks.py — sketch
import hashlib
from sqlalchemy import text

def _model_lock_key(model_unique_id: str) -> int:
    # Stable 64-bit signed int from string
    h = hashlib.blake2b(model_unique_id.encode(), digest_size=8).digest()
    return int.from_bytes(h, byteorder="big", signed=True)

def try_acquire_model_lock(session, model_unique_id: str) -> bool:
    key = _model_lock_key(model_unique_id)
    result = session.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": key})
    return result.scalar()
```

Lock is automatically released when the surrounding transaction commits/rolls back. No cleanup code needed. No stale-lock recovery needed. This is the cleanest pattern that exists.

### Freshness Check Pattern

Inside the same transaction that holds the advisory lock:

```python
def execute(self, context):
    with state_store.session_scope() as session:
        if not try_acquire_model_lock(session, self.model_unique_id):
            return self._handle_conflict(context)  # wait/skip/fail per policy

        last_run = state_store.last_successful_run(session, self.model_unique_id)
        if self._is_fresh(last_run):
            self.log.info("Skipping %s (fresh)", self.model_unique_id)
            return

        run_id = state_store.start_run(session, self.model_unique_id, context)
        session.commit()  # release advisory lock so others can wait, not block

    # Long-running subprocess OUTSIDE the lock
    result = subprocess_runner.run_dbt(self.model_unique_id)

    with state_store.session_scope() as session:
        state_store.finish_run(session, run_id, result)
```

Critical: **release the lock before the subprocess runs**. Holding a DB lock for the duration of a multi-minute dbt run is an anti-pattern that will cause Postgres connection pressure. The `aircontroler_model_runs.status='running'` row + heartbeat is the way other DAGs detect "someone is already running this" without holding a DB lock.

---

## Cloud Run Considerations

The Google Cloud Run executor is fundamentally different from LocalExecutor in three ways that affect this provider's design.

### 1. Filesystem is Ephemeral and Unshared

Each task runs in a fresh container. There is no shared `/opt/airflow/dags` mount. The dbt project directory and `profiles.yml` must reach the container by one of:

| Mechanism | Pros | Cons | Recommendation |
|-----------|------|------|----------------|
| **Bake into Docker image** | Fast cold start; immutable | Rebuild image on dbt project change | **Recommended for v1** |
| GCS Fuse mount on container | Live updates without rebuild | Cloud Run GCS fuse needs second-gen execution env; permission model fiddly | Document as advanced option |
| Download from GCS on startup | Simple to script | Adds startup latency per task | Fallback if image bake too slow |

**Design implication:** the operator must accept a `dbt_project_path` that **exists in the executor's filesystem at runtime**. Validate at parse time with a "this path may not exist on the worker — provide it relative to the container image" warning if Cloud Run executor is detected.

### 2. The Manifest Compilation Asymmetry

`dbt compile` happens at **DAG parse time on the scheduler/worker filesystem**. The compiled `manifest.json` is consumed at parse time only. The Cloud Run task does NOT need the manifest — it just needs the dbt project files + profiles.yml + the model name to run.

This means:
- Scheduler container needs: dbt project files (for parse-time compile) + dbt-bigquery + dbt-core
- Cloud Run task container needs: dbt project files + dbt-bigquery + dbt-core (NOT this provider package)

The provider should generate **a self-contained CLI invocation** that the Cloud Run executor wraps. We're not running our Python operator code inside the Cloud Run container — we're running raw `dbt run --select foo`.

### 3. Service Account per Task = Cloud Run Job per Task

The PROJECT.md mentions "per-model service account selection (design TBD)." Cloud Run binds service accounts at the **job** level. So per-model SA selection means either:

(a) Pre-provision N Cloud Run jobs (one per SA), select the right job per task. Static, requires deploy-time knowledge of all SAs.

(b) Dynamically create a Cloud Run job per task with the desired SA. Slow (job creation latency), but flexible.

(c) Single Cloud Run job, use Workload Identity Federation + impersonation inside the container, parameterized via env var. Fast and flexible but adds an IAM hop.

**Recommendation:** Pursue (c) for v1 — simplest deploy story. Document (a) as scale-out option. (b) is a non-starter due to latency.

### 4. Connection Pooling to Metadata DB

Cloud Run task workers connect to Postgres for state/lock operations. Postgres connection limits are real — Cloud Composer's default Postgres has limited connections. With many concurrent Cloud Run tasks, connection exhaustion is a risk.

**Design:** Use **short-lived connections** (no SQLAlchemy connection pool inside Cloud Run tasks) — open, do work, close. Each operator method call gets one connection. Pair with PgBouncer (Cloud SQL Auth Proxy can run as sidecar in the Cloud Run job).

---

## Suggested Build Order

Build order matters here because some components are upstream of others in the dependency graph. Build the inner components first; they have the simplest mocks for testing the outer components.

### Phase 1: Standalone Building Blocks (No Airflow needed)

1. **`dbt/manifest.py` + `dbt/graph.py`** — given a manifest.json file, produce `list[DbtNode]`. Zero Airflow dependencies. Test with manifest fixtures.
2. **`dbt/selector.py`** — given DbtNode list + selector string, return filtered list. Wraps dbt's selector logic (or reimplements minimal subset). Test with selector strings.
3. **`subprocess/runner.py`** — given a dbt command, run it, stream logs, return exit code + parsed `run_results.json`. Test with a fake `dbt` shell script.
4. **`config/*` dataclasses** — pure data, validated. Test with bad inputs.

**Why first:** Each is independently useful, has no Airflow surface area, and is trivially unit-testable. They de-risk the most "domain-y" parts.

### Phase 2: State Layer (Requires Airflow's DB but not its DAG machinery)

5. **`state/models.py`** — SQLAlchemy ORM. Test with in-memory SQLite (Postgres-specific behavior tested in integration).
6. **`state/migrations/`** — Alembic setup with first revision creating the tables. Test by running `alembic upgrade head` against ephemeral Postgres.
7. **`state/locks.py`** — advisory lock helpers + dialect detection. Test with real Postgres (concurrency tests with two threads).
8. **`state/store.py`** — high-level API used by operators. Test with real Postgres.
9. **`state/db_manager.py`** — `BaseDBManager` subclass for Airflow 3. Test by registering and running `airflow db migrate`.

**Why second:** Provides the dependencies for the runtime path. Can be developed against a docker-compose Postgres with no Airflow at all. Surfaces the trickiest design problems early (locking semantics, migration registration).

### Phase 3: Operators (Requires Airflow but not the parser)

10. **`airflow/operators/base.py`** — `BaseDbtRunOperator` with the freshness/lock/execute/finalize flow. Composes `state/store.py` + `subprocess/runner.py`. Test with `BaseOperator.test()` + mocked subprocess.
11. **`airflow/operators/local.py`** — concrete subclass for LocalExecutor. Mostly inherits.
12. **`airflow/operators/cloud_run.py`** — concrete subclass that delegates to `CloudRunExecuteJobOperator`. Test with mocked GCP client.

**Why third:** Operators are where most "Airflow gotchas" surface. Building them after state means execution logic is well-tested before the Airflow layer is added on top.

### Phase 4: DAG Generation (Top of the Stack)

13. **`airflow/task_factory.py`** — `build_airflow_graph(nodes, dag, task_group, ...)`. Given DbtNode list + Airflow context, wire up operator instances and dependencies. Test by inspecting the resulting DAG structure.
14. **`airflow/dag_builder.py`** — `DbtBuildOperator`-like top-level class users instantiate. Composes manifest loader + selector + factory.

**Why last:** This is the user-facing API. Everything else is in service of this. Building it last lets us compose proven pieces rather than discover integration issues in the public API.

### Phase 5: Distribution + Polish

15. **`provider_info.py` + `pyproject.toml`** — entry points, version metadata. Test by `pip install -e .` and verifying Airflow discovers the provider.
16. **Documentation, examples, integration tests** — end-to-end DAG with real BigQuery against tiny dbt project.

### Critical Path Minimization

If you want a "thin slice" demo working ASAP, build: 1, 3, 10 (minus state), 13, 14 — skip the state layer entirely. This gets you "Cosmos-but-simpler" and proves the lineage story. Then add 5-9 (state) and update the operator. **This bisects the project into two ship-able milestones.**

---

## Open Questions

These are architectural decisions where the research did not yield a clear answer; they require either prototyping or a project-level decision.

### 1. Airflow 2.x DBManager Story

`BaseDBManager` only exists in Airflow 3. For 2.x compatibility, we need a fallback. Options:
- (a) Drop 2.x support entirely. Cleaner code, but PROJECT.md mandates 2.x compat.
- (b) Manual bootstrap CLI (`aircontroler db init`) for 2.x, DBManager for 3.x. Two code paths.
- (c) Always use manual bootstrap, ignore DBManager. Simpler but doesn't integrate with `airflow db migrate`.

**Recommendation needed:** is the DBManager integration worth the version-detection complexity, or is a documented `aircontroler db init` step acceptable?

### 2. Selector Library: dbt-core vs Reimplementation

dbt's selector syntax (`tag:daily +model_a my_model+2`) is non-trivial. Options:
- (a) Import `dbt.graph.selector` directly. Couples us to dbt-core internals (unstable).
- (b) Use `dbt ls --select <expr>` as a subprocess at parse time. Slow but stable.
- (c) Reimplement minimal subset in pure Python. Risk of behavioral drift from dbt.

Cosmos chose (b) (`DBT_LS` mode). Likely correct, but adds parse-time latency.

**Recommendation needed:** confirm we accept the parse-time `dbt ls` call, or invest in a maintained subset reimpl.

### 3. Freshness Policy Resolution Order

`PROJECT.md` says "dbt `meta` as default, overridable per-DAG in Python." Need to spec:
- Is per-task override allowed (different tasks in the same DAG with different policies for the same model)?
- What happens when DAG_A says "freshness 1h" and DAG_B says "freshness 6h" for the same model? Whose policy applies during a check?

**Recommendation:** the policy is **a property of the model, not the DAG**. The "most permissive freshness wins" rule (longest TTL) avoids redundant runs, which is the stated goal. But this needs explicit confirmation.

### 4. Conflict Wait Semantics

"Conflict handling configurable per policy (wait / skip / fail)." For `wait`:
- Poll the lock with backoff? (Wastes worker slots while waiting.)
- Use Airflow's deferrable operator + trigger? (Frees the slot but adds complexity.)
- Defer to a sensor pattern? (Composable but slower.)

**Recommendation:** Build it as a deferrable operator (Airflow 2.5+ feature). Free the worker slot during wait. But this requires the trigger to poll the StateStore, which adds another DB connection pattern.

### 5. Cross-DAG Dependency Visibility

If model_a is in DAG_A and model_b (which depends on model_a) is in DAG_B, the Airflow Graph view in DAG_B does not show the dependency. Cosmos has this same limitation.

Options:
- Show the upstream as an `ExternalTaskSensor` to a virtual "model_a" task in some sentinel DAG.
- Render an `EmptyOperator` placeholder labeled "external: model_a" in DAG_B.
- Accept the limitation; surface dependencies only in dbt docs / a separate UI.

**Recommendation needed:** is cross-DAG lineage in-scope for v1, or v2?

### 6. Manifest Caching Key

If we cache the manifest by content hash, what gets hashed?
- All `.sql` and `.yml` files under the dbt project? (Slow but accurate.)
- Just `dbt_project.yml` mtime? (Fast but misses model edits.)
- Hash of `dbt parse` output? (Self-defeating — already runs dbt.)

**Recommendation:** hash the concatenated SHA of all `models/**/*.sql`, `models/**/*.yml`, `dbt_project.yml`, `packages.yml`. Linear in project size but parallelizable. Worth benchmarking on a 500-model project before committing.

### 7. Per-Model Service Account Mechanism

Three options listed in Cloud Run section above. Decision deferred to whoever builds the Cloud Run path — but the abstraction in the operator (`service_account` parameter) needs to exist from day one or it's a breaking change later.

---

## Sources

- [Astronomer Cosmos - GitHub repository](https://github.com/astronomer/astronomer-cosmos/) — primary architectural reference
- [Cosmos: DbtDag and DbtTaskGroup - DeepWiki](https://deepwiki.com/astronomer/astronomer-cosmos/2.1-dbtdag-and-dbttaskgroup) — high-level component docs
- [Cosmos dbt/graph.py source](https://github.com/astronomer/astronomer-cosmos/blob/main/cosmos/dbt/graph.py) — graph parsing implementation
- [Cosmos parsing methods documentation](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html) — LoadMode design
- [Cosmos execution modes documentation](https://astronomer.github.io/astronomer-cosmos/getting_started/execution-modes.html) — execution mode patterns
- [Airflow Dynamic Task Mapping documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) — confirms .expand() semantics
- [Airflow Plugins documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html) — plugin extension points (no DB tables)
- [Airflow PR #50657 - db-manager CLI](https://github.com/apache/airflow/commit/93c5caa2fe808bbd89738ed8c9fc52a257712e3f) — BaseDBManager mechanism
- [FAB Provider db.py source](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/_modules/airflow/providers/fab/auth_manager/models/db.html) — reference DBManager implementation
- [Airflow custom provider creation guide](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html) — provider entry point pattern
- [airflow-provider-sample template](https://github.com/astronomer/airflow-provider-sample) — reference repo structure
- [airflow-provider-alembic](https://github.com/astronomer/apache-airflow-providers-alembic) — Alembic-as-Airflow-operator pattern
- [PostgreSQL Explicit Locking documentation](https://www.postgresql.org/docs/current/explicit-locking.html) — advisory locks reference
- [PALs - PostgreSQL Advisory Locks library](https://github.com/level12/pals) — production patterns for advisory locks in SQLAlchemy
- [dbt-artifacts-parser](https://pypi.org/project/dbt-artifacts-parser/) — Python library for parsing manifest.json
- [Google Cloud Run Operators - Airflow Provider](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/cloud_run.html) — CloudRunExecuteJobOperator reference
- [Cosmos GCP Cloud Run Job execution mode source](https://github.com/astronomer/astronomer-cosmos/blob/main/cosmos/operators/gcp_cloud_run_job.py) — reference implementation
