# Stack Research: Airflow dbt Provider

**Researched:** 2026-04-17
**Target:** Airflow 2.x + 3.x compatible PyPI provider for dbt on BigQuery
**Overall confidence:** HIGH on Airflow tooling, HIGH on dbt artifacts, MEDIUM on cross-version compat edge cases

---

## Recommended Stack

### Core Runtime Dependencies

| Library | Version Constraint | Purpose | Rationale |
|---------|--------------------|---------|-----------|
| `apache-airflow` | `>=2.7,<4.0` | Runtime host | 2.7 is the lowest practical floor (`@task.virtualenv` + dynamic-task-mapping maturity); upper bound open through 3.x |
| `apache-airflow-providers-common-compat` | `>=1.7.0` | Cross-version SDK shim | Provides `airflow.providers.common.compat.sdk` so a single import works on 2.x and 3.x without `if AIRFLOW_V_3_0_PLUS` branches |
| `dbt-core` | `>=1.7,<1.12` | Manifest schema + (optional) `dbtRunner` | Pinning to <1.12 keeps `manifest.json` schema in the v10–v12 range we'll parse explicitly |
| `dbt-bigquery` | `>=1.7,<1.12` | Adapter for target warehouse | Required so users get a working `dbt build` env when they install our provider with `[bigquery]` extra |
| `pyyaml` | `>=6.0` | Read `profiles.yml`, `selectors.yml`, dbt `_meta` | Already an Airflow transitive; explicit dep is cheap |
| `packaging` | `>=23.0` | Compare dbt + airflow versions at runtime | Stable, stdlib-adjacent, used by both Airflow and dbt internally |

### Optional Runtime Dependencies (extras)

| Library | Extra | Purpose |
|---------|-------|---------|
| `apache-airflow-providers-google` | `[gcp]` | Re-export `CloudRunExecuteJobOperator` patterns; provides `GoogleBaseHook` if we later swap profiles for connections |
| `orjson` | `[fast-parse]` | 40% faster manifest parsing (Cosmos uses this — issue [#2320](https://github.com/astronomer/astronomer-cosmos/pull/2320)) |

### Build / Dev Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| `hatchling` | `>=1.27` | Build backend — what Airflow itself uses; lighter than setuptools and well-aligned with Airflow's monorepo conventions |
| `hatch` | `>=1.14` | Optional CLI for building/publishing |
| `pytest` | `>=8.0` | Test runner |
| `pytest-mock` | `>=3.14` | Mock subprocess calls and Airflow context |
| `pytest-cov` | `>=5.0` | Coverage reporting |
| `freezegun` | `>=1.5` | Time travel for freshness-policy tests (future milestone) |
| `ruff` | `>=0.7` | Lint + format (replaces black + flake8 + isort) |
| `mypy` | `>=1.13` | Static typing |
| `apache-airflow[devel-tests]` | matches runtime pin | Brings DAG-test utilities, `dag.test()`, fake context fixtures |

### Why NOT these alternatives

| Rejected | Reason |
|----------|--------|
| `dbt-node-selector` (Wasmer/WASM) | Listed as "in development" since 2023 with no recent releases; not safe for production. Use the dbt CLI itself (`dbt ls --select X --output json`) for selector resolution instead. |
| `setuptools` / `setup.py` | Airflow itself moved to Hatchling; sample provider template still uses setup.cfg but new providers should align with upstream choice. |
| `dbt-core`'s `dbtRunner` (programmatic) | Officially **not safe for parallel use in the same process** — and Airflow workers are happy to run multiple tasks per process. Subprocess is the correct boundary, matching the project's Key Decision. We can use `dbtRunner` in a *single-shot* path during DAG parse for `dbt ls` (Cosmos does this since 1.9) but NOT for execution. |
| `apache-airflow-providers-dbt-cloud` | Targets dbt Cloud SaaS, not dbt Core CLI — different surface area; not a useful reference. |

---

## Airflow Provider Conventions

### Package Layout

```
aircontroler/                                # repo root
├── LICENSE                                  # Apache-2.0 (matches Airflow)
├── README.md
├── pyproject.toml                           # hatchling build, entry-point declared here
├── src/
│   └── aircontroler/                        # importable package
│       ├── __init__.py                      # exports get_provider_info()
│       ├── provider.yaml                    # canonical provider metadata
│       ├── operators/
│       │   ├── __init__.py
│       │   └── dbt.py                       # DbtBuildOperator (the dynamic-mapped one)
│       ├── hooks/
│       │   ├── __init__.py
│       │   └── dbt.py                       # DbtCliHook — wraps subprocess
│       ├── parsers/
│       │   ├── __init__.py
│       │   ├── manifest.py                  # manifest.json → DbtNode
│       │   └── selector.py                  # invoke `dbt ls --select X --output json`
│       └── utils/
│           ├── __init__.py
│           └── compat.py                    # re-export from common.compat.sdk
├── tests/
│   ├── unit/
│   ├── integration/                         # spins up dbt + sqlite/bigquery emulator
│   └── conftest.py
└── docs/
```

### Provider Registration (pyproject.toml)

```toml
[build-system]
requires = ["hatchling>=1.27"]
build-backend = "hatchling.build"

[project]
name = "airflow-provider-aircontroler"        # PyPI naming convention: airflow-provider-<name>
version = "0.1.0"
requires-python = ">=3.9,<3.13"               # match Airflow 2.7+/3.x supported range
dependencies = [
    "apache-airflow>=2.7,<4.0",
    "apache-airflow-providers-common-compat>=1.7.0",
    "dbt-core>=1.7,<1.12",
    "pyyaml>=6.0",
    "packaging>=23.0",
]

[project.optional-dependencies]
bigquery = ["dbt-bigquery>=1.7,<1.12"]
gcp = ["apache-airflow-providers-google>=10.0"]
fast-parse = ["orjson>=3.10"]
dev = ["pytest>=8", "pytest-mock>=3.14", "pytest-cov>=5", "freezegun>=1.5",
       "ruff>=0.7", "mypy>=1.13"]

[project.entry-points."apache_airflow_provider"]
provider_info = "aircontroler.__init__:get_provider_info"
```

### `__init__.py` Entry Function

```python
__version__ = "0.1.0"

def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aircontroler",
        "name": "aircontroler",
        "description": "Map dbt models to native Airflow tasks via dynamic task mapping.",
        "versions": [__version__],
        # No connection-types in v1 — we use profiles.yml on disk per the project decision.
    }
```

### `provider.yaml` (Canonical Metadata)

```yaml
package-name: airflow-provider-aircontroler
name: aircontroler
description: |
    Aircontroler maps each dbt model to a first-class Airflow task using
    dynamic task mapping, with cross-DAG freshness policies and conflict
    resolution backed by the Airflow metadata DB.
versions:
  - 0.1.0
dependencies:
  - apache-airflow>=2.7,<4.0
  - apache-airflow-providers-common-compat>=1.7.0
  - dbt-core>=1.7,<1.12
integrations:
  - integration-name: dbt
    external-doc-url: https://docs.getdbt.com/
    tags: [software]
operators:
  - integration-name: dbt
    python-modules:
      - aircontroler.operators.dbt
hooks:
  - integration-name: dbt
    python-modules:
      - aircontroler.hooks.dbt
```

`provider.yaml` must be kept in sync with `get_provider_info()` — Airflow validates against `provider_info.schema.json` when discovering the entry point.

Source: [How to create your own provider — Airflow docs](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html), [astronomer/airflow-provider-sample](https://github.com/astronomer/airflow-provider-sample)

---

## Dynamic Task Mapping

### The 2.x vs 3.x Reality

Airflow 3.0 moved DAG-author-facing surfaces (`BaseOperator`, `task`, `Context`, `BaseHook`, exceptions) into the new **Task SDK** namespace `airflow.sdk`. The legacy paths (`airflow.models.BaseOperator`, `airflow.decorators.task`, etc.) still work in 3.x but emit `DeprecatedImportWarning` and are slated for removal.

For a provider that must support both, the *only* sane pattern is:

```python
# aircontroler/utils/compat.py — single source of truth
from airflow.providers.common.compat.sdk import (
    BaseOperator,
    BaseHook,
    Context,
    get_current_context,
)
```

Everything else in our codebase imports from `aircontroler.utils.compat`. This means we ship one operator class, not two, and we don't pollute imports with `try/except ImportError` dances.

### `expand()` API — Same On Both Versions

The `expand()` / `partial()` / `expand_kwargs()` API surface is **stable across 2.3 → 3.2**. The tricky bits aren't the call sites; they're:

1. **`expand_kwargs()` vs `expand()`** — `expand_kwargs()` lets each mapped instance receive a *different set* of keyword arguments (perfect for "model A gets `--select model_a`, model B gets `--select model_b`, AND each can have its own service account / threads / vars"). This is what our operator should use. Plain `expand()` only varies a single argument.
2. **Lazy XCom proxies in 3.x** — Output from mapped tasks is a `LazyXComAccess` (2.x) / lazy proxy (3.x); the API is compatible but performance characteristics differ. Don't materialize unnecessarily.
3. **Mapped task dependency wiring** — Each mapped instance gets `map_index >= 0`. Inter-mapped-task dependencies (model B depends on model A within the same `.expand_kwargs()` call) are **not** representable directly. Cosmos sidesteps this by generating a full TaskGroup of *non-mapped* tasks; we will use mapping but must wire intra-DAG deps via `set_upstream` on the mapped operator references that Airflow returns.
4. **DAG parse vs render** — Mapping decisions made at parse time (compile manifest, resolve `--select`, build the kwargs list) cost real wall-clock time. Cosmos caches this; we should plan for caching from day 1 (Airflow Variables or an XCom-backend-backed cache).

### Concrete Pattern

```python
from aircontroler.utils.compat import BaseOperator
from aircontroler.parsers.manifest import resolve_models_for_select

class DbtBuildOperator(BaseOperator):
    def __init__(self, dbt_command: str, project_dir: str, profiles_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.dbt_command = dbt_command
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir

    def execute(self, context):
        # Single-model execution branch (when used as mapped instance)
        ...

    @classmethod
    def for_select(cls, *, dag, dbt_command: str, project_dir: str,
                   profiles_dir: str, task_id: str = "dbt_build") -> "DbtBuildOperator":
        # Called once at DAG parse time; returns the *mapped* operator
        models = resolve_models_for_select(project_dir, profiles_dir, dbt_command)
        kwargs_list = [{"model_unique_id": m.unique_id, "task_id": f"{task_id}.{m.name}"}
                       for m in models]
        return cls.partial(task_id=task_id, project_dir=project_dir,
                           profiles_dir=profiles_dir, dbt_command=dbt_command) \
                  .expand_kwargs(kwargs_list)
```

Sources: [Dynamic Task Mapping — Airflow 3.2 docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html), [Task SDK docs](https://airflow.apache.org/docs/task-sdk/stable/), [Issue #52378](https://github.com/apache/airflow/issues/52378)

---

## dbt Manifest Parsing

### Manifest Schema Versions In Scope

| dbt-core version | manifest schema | Status |
|------------------|-----------------|--------|
| 1.7.x | v11 | Still supported |
| 1.8.x – 1.10.x | v12 | Most installs |
| 1.11.x (current GA, March 2026) | v12 | Latest stable |
| 1.12.x (TBD) | likely v13 | Out of v1 scope |
| Fusion v2.0 (preview) | v20 (identical to v12 for our fields) | Out of v1 scope |

We will target **v11 + v12** explicitly. Schema URL: `https://schemas.getdbt.com/dbt/manifest/v12.json`.

### Top-Level Keys We Care About

```json
{
  "metadata": { "dbt_schema_version": "...", "project_name": "...", "adapter_type": "bigquery" },
  "nodes": { "model.proj.name": { ... } },     // models, seeds, snapshots, tests, analyses
  "sources": { "source.proj.src.tbl": { ... } },
  "exposures": { ... },
  "metrics": { ... },
  "macros": { ... },
  "parent_map": { "model.proj.x": ["model.proj.y", "source.proj.s.t"] },
  "child_map":  { "model.proj.y": ["model.proj.x"] },
  "selectors": { ... },                         // user-defined selectors.yml
  "disabled": { ... }
}
```

### Per-Node Fields We Actually Use

| Field | Why |
|-------|-----|
| `unique_id` | Stable identifier; format `<resource_type>.<package>.<name>`; we use this as the dynamic-task-mapping key |
| `name` | Display name for the Airflow task |
| `resource_type` | Filter to `model`, `seed`, `snapshot` for execution; `test` is handled separately by `dbt build` |
| `depends_on.nodes` | Build the upstream/downstream DAG mirror |
| `fqn` | Useful for grouping (could become Airflow task group) |
| `config.materialized` | Affects retry semantics — `incremental` vs `table` matters for conflict handling |
| `config.meta.aircontroler.*` | Where users will declare freshness policies and per-model SA (future milestone) |
| `tags` | Pass-through for users who want to filter |
| `original_file_path` | Surface in operator UI for debuggability |

### Selector Resolution Strategy

**Recommendation: shell out to `dbt ls --select <user_input> --output json --resource-type model`** rather than reimplementing dbt's selector parser.

Rationale:
- dbt's selector grammar is non-trivial: graph operators (`+`, `@`, `*`), set operators (intersection via `,`, union via space), methods (`tag:`, `path:`, `config:`, `selector:`, `package:`, `source:`, `state:`), exclusion via `--exclude`, and YAML-defined named selectors. Reimplementing this is a perpetual catch-up game.
- `dbt-node-selector` (Wasmer/WASM, dbt-labs) is *in development* with no recent releases — not production-grade.
- Cosmos took the same path: their `LoadMode.DBT_LS` is documented as "the most accurate way of loading dbt projects and filtering them out, since it uses the dbt command line for both parsing and filtering the nodes."
- `dbt ls --output json` returns one JSON object per line — easy to parse and includes `unique_id`, `depends_on`, `tags`, `config`. (Note: Fusion's output is sparser; explicitly target dbt-core 1.7–1.11 for v1.)

Trade-off: we incur one `dbt ls` invocation at DAG parse time (typically 5–30 seconds depending on project size). Cache it. See **Caching** below.

### Manifest Generation

We need a manifest to wire dependencies even after `dbt ls` resolves the selection. Two paths:

1. **Run `dbt parse` (recommended)** — Faster than `dbt compile`, only generates `manifest.json` without compiling SQL. Available since dbt 1.5.
2. **Accept user-supplied `target/manifest.json`** — Fastest at DAG parse, but requires CI to refresh it. This is what Cosmos calls `LoadMode.DBT_MANIFEST` and what most production users land on.

Support both. `dbt parse` is the default, manifest path is a config option.

### Caching

Cosmos's caching design (see [Issue #2294](https://github.com/astronomer/astronomer-cosmos/issues/2294)) is informative — they store base64+zlib-compressed `dbt ls` output in Airflow Variables to avoid re-parsing on every DAG file evaluation. We should:

- Hash the inputs: `(project_dir mtime, profiles.yml mtime, dbt_command string, dbt version)`.
- Cache key: `aircontroler::dbt_ls::<hash>`.
- Storage: Airflow Variables for v1; pluggable later (file system, GCS).
- Invalidation: TTL + content-hash. Default TTL 1 hour.

### orjson Optional Acceleration

For large monorepo manifests (>50MB), `orjson` halves parse time vs stdlib `json`. Add behind `[fast-parse]` extra; detect at runtime and fall back gracefully.

Sources: [Manifest JSON file — dbt docs](https://docs.getdbt.com/reference/artifacts/manifest-json), [v12 schema](https://github.com/dbt-labs/dbt-core/blob/main/schemas/dbt/manifest/v12.json), [Cosmos parsing methods](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html), [How to parse dbt artifacts](https://guitton.co/posts/dbt-artifacts)

---

## Cosmos Reference

Cosmos (`astronomer-cosmos`) is the incumbent. Studying it pays off — they've shipped the obvious solutions and documented the painful corners.

### What Cosmos Does Well — Replicate

| Pattern | Where in Cosmos | Why we want it |
|---------|-----------------|----------------|
| `LoadMode` enum (AUTOMATIC, DBT_LS, DBT_LS_FILE, DBT_LS_CACHE, DBT_MANIFEST) | `cosmos/dbt/graph.py` | A clean abstraction over "where do I get the dbt graph from?" — exactly the surface our v1 needs |
| `DbtNode` data class | `cosmos/dbt/graph.py` | Internal representation independent of dbt version; lets us upgrade dbt schema versions without ripple |
| `parse_dbt_ls_output()` line-delimited JSON parser | `cosmos/dbt/graph.py` | Handles edge cases like versioned models and external nodes; reuse the algorithm, not the code |
| `RenderConfig` separate from `ExecutionConfig` | Cosmos public API | Splitting *what to build* from *how to run it* is the right axis |
| Multi-tier cache with content hashing | Recent Cosmos versions | Solves the DAG-parse latency problem cleanly |
| dbt deps via symlink not copy | `cosmos/dbt/project.py` | Cuts container startup time substantially |
| `ProfileConfig` abstraction | Cosmos public API | We *won't* use Airflow connections in v1 (per project decision) but we need an analogous on-disk profile pointer |

### What Cosmos Does Badly — Improve Upon

This is the project's whole reason for existing. Document the gaps:

| Cosmos limitation | Source | Our fix |
|-------------------|--------|---------|
| **No cross-DAG model deduplication.** Two DAGs both running `model_x` will run it twice. | [GetYourGuide post](https://www.getyourguide.careers/posts/airflow-dbt-leveraging-astronomer-cosmos-at-getyourguide), [phData blog](https://www.phdata.io/blog/how-to-achieve-source-freshness-and-stateful-selection-with-cosmos-api-dbt-core/) | Future milestone: freshness state + distributed lock in metadata DB |
| **No freshness policies.** Cosmos has no concept of "this model was built 10 minutes ago, skip it." | Cosmos docs (no such feature) | `meta.aircontroler.freshness` per model, overridable in DAG |
| **TaskGroup mode hides tasks from `dag.tasks`** | [Comment in issue #2294](https://github.com/astronomer/astronomer-cosmos/issues/2294) | We use mapped tasks at the DAG level — naturally visible in `dag.tasks` and Graph view |
| **Manifest reloaded on every DAG evaluation** in some load modes | [Issue #2294](https://github.com/astronomer/astronomer-cosmos/issues/2294) | Cache-first design, content-hash invalidation |
| **`LoadMode.CUSTOM` doesn't support YAML selectors** | Cosmos parsing-methods docs | We delegate selector parsing to `dbt ls` itself — full grammar support for free |
| **Source-freshness orchestration is a workaround** | [phData blog](https://www.phdata.io/blog/how-to-achieve-source-freshness-and-stateful-selection-with-cosmos-api-dbt-core/) | Out of scope for v1; called out in PITFALLS for future milestone |

### What Cosmos Did That We Should NOT Do

- **Don't** ship a "custom" home-grown SQL parser as a fallback. It's a maintenance black hole and doesn't support YAML selectors. Just require dbt to be installed.
- **Don't** offer multiple execution modes (local, virtualenv, docker, kubernetes, aws_eks, gcp_cloud_run_job, dbt_runner) in v1. Per the project's Out of Scope, we run via CLI subprocess. Period. Add modes later if validated.
- **Don't** support both `DbtDag` (whole DAG) and `DbtTaskGroup` (subset) in v1. One operator (`DbtBuildOperator.for_select`) returning a mapped task is enough surface to validate.

Sources: [astronomer-cosmos GitHub](https://github.com/astronomer/astronomer-cosmos/), [Cosmos parsing methods](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html), [Render Config docs](https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html), [Cosmos 1.6 announcement](https://www.astronomer.io/blog/introducing-cosmos-1-6/), [Cosmos 1.11 announcement](https://www.astronomer.io/blog/cosmos-1-11-ga/)

---

## Testing Approach

### Layers

1. **Pure unit tests** — Manifest parser, selector-output parser, freshness policy logic. Fast, no Airflow imports needed. Use `pytest` + `pytest-mock`.
2. **Operator unit tests** — Instantiate `DbtBuildOperator`, mock the subprocess, call `execute()` with a fake context. Use the `airflow.providers.common.compat.sdk` import path so tests pass on both 2.x and 3.x.
3. **DAG-test integration** — Use `dag.test()` (Airflow 2.5+, fully supported in 3.x) to run a small DAG against a fixture dbt project + SQLite or BigQuery emulator. This catches the dynamic-mapping wiring.
4. **Provider compatibility matrix** — Run the unit suite against a matrix of `(airflow==2.7, 2.10, 3.0, 3.2) × (dbt-core==1.7, 1.10, 1.11)`. GitHub Actions matrix job. Mirrors Airflow's own provider compatibility testing approach.

### Recommended Test Tooling

- `pytest` + `pytest-mock` + `pytest-cov` — table stakes.
- `freezegun` — for freshness-policy time travel (future milestone).
- **Avoid** `pytest-airflow` — abandoned (last release 2017). Use `dag.test()` instead.
- Fixture dbt project: a tiny 5-model graph in `tests/fixtures/dbt_project/` with a SQLite profile.
- BigQuery integration tests: gated behind `BIGQUERY_PROJECT` env var so OSS contributors can run unit tests without GCP credentials.

Sources: [Airflow unit test docs](https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst), [pytest-airflow PyPI](https://pypi.org/project/pytest-airflow/) (note: stale), [Issue #58893 — testing operators in 3.1.3+](https://github.com/apache/airflow/issues/58893)

---

## Cloud Run Executor Compatibility (Project-Specific)

The project targets the Google Cloud Run Executor on Cloud Composer. Two implications for our stack choices:

1. **Each task runs in an isolated Cloud Run job.** That job needs the dbt project files and `profiles.yml` available. The provider can't assume a shared filesystem between tasks. Solution: project files baked into the container image OR mounted from GCS at task start. Out of scope for v1 implementation but **not** out of scope for documentation — we should ship a recipe.
2. **Subprocess model is unchanged.** Cloud Run executor still runs Python tasks; our operator still shells out to `dbt`. No special API needed.
3. **Per-model service-account selection** (a project requirement) maps cleanly onto Cloud Run's per-job SA configuration. We should expose a `service_account` field in the dbt model `meta.aircontroler` block and propagate it to the executor's Cloud Run job spec via Airflow's `executor_config` mechanism.

Source: [Cloud Run operators docs](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/cloud_run.html)

---

## Confidence Levels

| Recommendation | Confidence | Basis |
|----------------|------------|-------|
| `apache-airflow-providers-common-compat` shim is the right cross-version pattern | **HIGH** | Documented in Airflow upstream issues #52378, #57018; provider docs confirm `from airflow.providers.common.compat.sdk import BaseOperator` is the canonical pattern |
| Hatchling as build backend | **HIGH** | Airflow itself uses it; reduces friction with upstream tooling |
| `dbt ls --output json` for selector resolution (vs reimplementing) | **HIGH** | Cosmos took this exact path and documents it as their most-robust mode; `dbt-node-selector` library is not production-ready |
| Manifest schema v11 + v12 coverage | **HIGH** | Confirmed via dbt JSON schemas site; v12 maps to dbt 1.8–1.11; v11 covers 1.7 |
| `expand_kwargs()` over `expand()` for per-model task config | **HIGH** | Documented in Airflow 3.2 dynamic-task-mapping docs; works identically on 2.x |
| Subprocess execution (not `dbtRunner`) | **HIGH** | dbt's own docs explicitly warn against parallel `dbtRunner` use in same process; matches project's stated Key Decision |
| Cosmos limitations on cross-DAG dedup / freshness | **HIGH** | Multiple independent reports (GetYourGuide, phData) and absence of any such feature in Cosmos docs |
| Airflow version floor of 2.7 | **MEDIUM** | 2.7 has a sufficient mapping API but 2.5 is technically supported by `common.compat`. 2.7 chosen because it's the lowest version with stable `@task.virtualenv` and most installed in the wild as of 2026; can drop to 2.5 if a target user demands it |
| dbt-core upper bound of <1.12 | **MEDIUM** | 1.11 is current GA (March 2026); 1.12 release timing/schema changes unknown. Conservative pin gives us release-time visibility before users hit edge cases |
| Cache key strategy (project mtime + profiles mtime + dbt version + command) | **MEDIUM** | Inferred from Cosmos's caching approach + general invalidation principles; needs validation against real monorepo project structures |
| `pytest-airflow` is abandoned, use `dag.test()` | **MEDIUM** | PyPI shows old release dates; `dag.test()` is documented as the modern approach but exact 3.x semantics differ slightly per Issue #58893 |
| orjson gives ~40% manifest parse speedup | **MEDIUM** | Single source: Cosmos PR #2320. Benchmark before promoting to default |
| Airflow 3.2 GA on April 7, 2026 | **HIGH** | Confirmed via Airflow blog and PyPI |

---

## Sources

- [How to create your own provider — apache-airflow-providers Documentation](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html)
- [airflow-provider-sample template (Astronomer)](https://github.com/astronomer/airflow-provider-sample)
- [airflow.providers.common.compat.sdk module source](https://airflow.apache.org/docs/apache-airflow-providers-common-compat/stable/_modules/airflow/providers/common/compat/sdk.html)
- [Update BaseOperator imports for Airflow 3.0 compatibility — Issue #52378](https://github.com/apache/airflow/issues/52378)
- [Migrate remaining providers to common.compat — Issue #57018](https://github.com/apache/airflow/issues/57018)
- [Dynamic Task Mapping — Airflow 3.2 docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Apache Airflow Task SDK docs](https://airflow.apache.org/docs/task-sdk/stable/)
- [Apache Airflow 3.2.0 release blog](https://airflow.apache.org/blog/airflow-3.2.0/)
- [Manifest JSON file — dbt Developer Hub](https://docs.getdbt.com/reference/artifacts/manifest-json)
- [dbt manifest v12 JSON schema](https://github.com/dbt-labs/dbt-core/blob/main/schemas/dbt/manifest/v12.json)
- [dbt JSON Schemas index](https://schemas.getdbt.com/)
- [dbt programmatic invocations docs](https://docs.getdbt.com/reference/programmatic-invocations)
- [dbt node selection syntax overview](https://docs.getdbt.com/reference/node-selection/syntax)
- [dbt ls command reference](https://docs.getdbt.com/reference/commands/list)
- [dbt-node-selector (Wasmer)](https://wasmer.io/dbt-labs/dbt-node-selector) — note: in development, not production-ready
- [dbt 1.11 release info / dbt versions](https://docs.getdbt.com/docs/dbt-versions)
- [astronomer-cosmos GitHub](https://github.com/astronomer/astronomer-cosmos/)
- [cosmos/dbt/graph.py source](https://github.com/astronomer/astronomer-cosmos/blob/main/cosmos/dbt/graph.py)
- [Cosmos parsing methods documentation](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html)
- [Cosmos issue #2294 — manifest loading optimization](https://github.com/astronomer/astronomer-cosmos/issues/2294)
- [Cosmos PR #2320 — orjson parser](https://github.com/astronomer/astronomer-cosmos/pull/2320)
- [Cosmos 1.6 announcement](https://www.astronomer.io/blog/introducing-cosmos-1-6/)
- [Cosmos 1.11 GA announcement](https://www.astronomer.io/blog/cosmos-1-11-ga/)
- [Cosmos at GetYourGuide — cross-DAG limitations](https://www.getyourguide.careers/posts/airflow-dbt-leveraging-astronomer-cosmos-at-getyourguide)
- [phData on source freshness with Cosmos](https://www.phdata.io/blog/how-to-achieve-source-freshness-and-stateful-selection-with-cosmos-api-dbt-core/)
- [How to parse dbt artifacts (Olivier Guitton)](https://guitton.co/posts/dbt-artifacts)
- [Google Cloud Run operators (apache-airflow-providers-google)](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/cloud_run.html)
- [Airflow unit testing guide](https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst)
- [Provider Compatibility Testing — DeepWiki](https://deepwiki.com/apache/airflow/4.2-provider-compatibility-testing)
- [Issue #58893 — operator unit testing in Airflow 3.1.3+](https://github.com/apache/airflow/issues/58893)
- [Apache Airflow PyPI page](https://pypi.org/project/apache-airflow/)
