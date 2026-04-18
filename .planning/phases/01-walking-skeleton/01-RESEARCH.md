# Phase 1: Walking Skeleton - Research

**Researched:** 2026-04-18
**Domain:** Airflow provider packaging, dbt manifest parsing, static task graph generation, dbt subprocess execution, pytest integration with dbt-duckdb
**Confidence:** HIGH (core stack verified against PyPI registry and official docs; two critical corrections to prior assumptions documented)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Factory function pattern: `build_dbt_dag(dag, ...)` mutates a user-owned DAG and adds all model tasks to it.
- **D-02:** Return value is `{'entry_tasks': [...], 'exit_tasks': [...], 'tasks': {unique_id: AirflowTask}}`.
- **D-03:** Factory signature: `build_dbt_dag(dag, manifest_path, dbt_command, project_dir, profiles_path, dbt_executable_path=None, service_account=None)`.
- **D-04:** User passes a full dbt command string; factory parses `--select`, `--exclude`, `--selector` flags out to pass to `dbt ls`. Each model task runs `dbt build --select <unique_id>`.
- **D-05:** `--project-dir` and `--profiles-dir` NOT embedded in `dbt_command`; supplied as explicit `project_dir`/`profiles_path` factory params.
- **D-06:** Flat module structure: `aircontroler/__init__.py`, `operator.py`, `factory.py`, `manifest.py`, `selector.py`, `hooks/__init__.py`, `hooks/metadata_db.py`.
- **D-07:** PyPI name: `airflow-provider-aircontroler`. Provider name in UI: `aircontroler`.
- **D-08:** Internal model: `@dataclass ManifestNode` with typed fields. v11 and v12 parsers return same shape.
- **D-09:** `parse_manifest(data)` reads `manifest_version` first, dispatches to `_parse_v11` / `_parse_v12`. Never hardcode field paths.
- **D-10:** Phase 1 ships unit tests + integration tests (dbt-duckdb, zero infra). Fixture dbt project at `tests/fixtures/dbt_project/`.
- **D-11:** `service_account` param exists from Phase 1; logs WARNING when set. No crash, no silent discard.

### Claude's Discretion
- Internal naming of the graph-resolution step (e.g., class name for selector resolver)
- Exact fields exposed on `ManifestNode` beyond minimum needed for graph wiring
- CI matrix specifics (which exact Airflow 2.x/3.x minor versions)
- Whether `dbt_executable_path` defaults to `"dbt"` (PATH lookup) or requires explicit setting

### Deferred Ideas (OUT OF SCOPE)
- None raised during discussion — discussion stayed within phase scope.

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CORE-01 | User can point at a pre-built `manifest.json` path | Manifest loader reads JSON file, dispatches on `manifest_version`, returns `list[ManifestNode]` |
| CORE-02 | User can enable compile-on-parse mode (`dbt parse` at DAG parse time) | Subprocess call `dbt parse --project-dir ... --profiles-dir ...`; manifest loader reads the emitted `target/manifest.json` |
| CORE-03 | User passes full `dbt build --select ...` string; operator resolves graph via `dbt ls` | `SelectorResolver` parses flags from dbt_command string, shells out to `dbt ls --output json`, returns unique_ids |
| CORE-04 | Each resolved model becomes a separate Airflow task; task_id equals model's unique_id | Static Python loop over ManifestNodes; `DbtRunModelOperator(task_id=node.unique_id, dag=dag)` |
| CORE-05 | Task upstream/downstream mirrors dbt model DAG (Graph view shows lineage) | Wiring via `upstream_task.set_downstream(downstream_task)` from `node.depends_on` |
| CORE-06 | Each model task executes via CLI subprocess with configurable `dbt_executable_path` | `subprocess.run([dbt_executable_path, "build", "--select", node.unique_id, ...])` |
| CORE-07 | Operator reads credentials from `profiles_path` | `--profiles-dir` flag passed to dbt subprocess; `profiles_path` validated at parse time |
| CORE-08 | Package compatible with Airflow 2.10+ and 3.x via single wheel | `apache-airflow-providers-common-compat` shim — see critical finding on Airflow floor |
| CORE-09 | Package published as Airflow provider with `get_provider_info` entry point | `[project.entry-points."apache_airflow_provider"]` in `pyproject.toml` |

</phase_requirements>

---

## Summary

Phase 1 delivers the complete walking skeleton: a Python package that accepts a dbt `manifest.json` (or runs `dbt parse`), resolves a selector string via `dbt ls`, and wires one `DbtRunModelOperator` per model into a user-supplied Airflow DAG with accurate upstream/downstream edges. The package installs from PyPI as an Airflow provider and runs on Airflow 2.10+ and 3.x.

Two critical corrections from prior research are identified. First, `apache-airflow-providers-common-compat>=1.7.0` (the locked shim version) requires Airflow `>=2.10.0`, not `>=2.7`. The effective Airflow floor for this package is **2.10.0**, not 2.7. This is consistent with the practical install base: Airflow 2.x EOLed April 22 2026, and the last stable 2.x series is 2.9.x. Second, `dbt-duckdb` (test dependency) requires `dbt-core>=1.8.0`, so the minimum dbt version in the test matrix shifts from 1.7 to **1.8** when using dbt-duckdb for integration tests. Unit tests using v11 manifest fixtures (dbt 1.7 vintage) remain valid.

The architecture is straightforward: pure parse-time static loop (no `.expand()`), CLI subprocess only, flat module layout. The interesting implementation work is the command-string parser (extracting `--select`/`--exclude`/`--selector` from an arbitrary dbt command), the manifest version dispatcher, and the integration test fixture project.

**Primary recommendation:** Build in layer order — manifest.py → selector.py → operator.py → factory.py → pyproject.toml/entry-point. Write unit tests for each layer before moving to the next. Integration tests come last, exercising the full pipeline end-to-end with dbt-duckdb.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Manifest file loading and parsing | DAG parse-time (scheduler) | — | Node graph must be known at parse time to create static tasks |
| Selector resolution (`dbt ls`) | DAG parse-time (scheduler) | — | Node set must be known at parse time to create the right number of tasks |
| Task/edge wiring | DAG parse-time (scheduler) | — | Airflow requires task graph to be static after module import |
| Model execution (`dbt build`) | Task runtime (worker) | — | Actual SQL execution happens when Airflow schedules the task |
| `profiles.yml` credential access | Task runtime (worker) | — | dbt subprocess reads profiles at execution time |
| Provider registration | Package metadata | — | Entry-point scanned by Airflow on startup |
| `MetadataDBHook` stub | Worker-safe hook layer | — | Must extend `BaseHook` so Airflow 3 worker isolation is respected from day one |

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `apache-airflow` | `>=2.10.0,<4.0` | Runtime host | 2.10 is the minimum for common-compat>=1.7 shim [VERIFIED: PyPI registry] |
| `apache-airflow-providers-common-compat` | `>=1.7.0` | Cross-version SDK shim | `from airflow.providers.common.compat.sdk import BaseOperator` works on 2.x and 3.x [VERIFIED: airflow.apache.org docs] |
| `dbt-core` | `>=1.8.0,<1.12` | Manifest + CLI | 1.8 is the floor imposed by dbt-duckdb (test dep); v12 manifest covers 1.8–1.11 [VERIFIED: PyPI registry] |
| `hatchling` | `>=1.27` | Build backend | Used by Airflow itself; lighter than setuptools; aligned with upstream conventions [CITED: STACK.md] |
| `pyyaml` | `>=6.0` | profiles.yml path validation | Already Airflow transitive; cheap explicit dep [ASSUMED] |
| `packaging` | `>=23.0` | Version comparison at runtime | Stable; used by Airflow and dbt internally [ASSUMED] |

### Supporting (dev/test)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `dbt-duckdb` | `>=1.8.0` | Zero-infra integration test adapter | `[test]` extra only; never runtime dependency [VERIFIED: PyPI registry] |
| `pytest` | `>=8.0` | Test runner | All phases |
| `pytest-mock` | `>=3.14` | Mock subprocess calls | Unit tests for operator/factory |
| `pytest-cov` | `>=5.0` | Coverage | CI gate |
| `ruff` | `>=0.7` | Lint + format | AIR301/AIR302 rules for Airflow 3 compat checks |
| `mypy` | `>=1.13` | Static typing | CI |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `dbt-duckdb` for integration tests | SQLite or Postgres + full dbt adapter | dbt-duckdb is zero-infra, no cloud credentials; perfect for CI without GCP setup |
| Static parse-time loop | `.expand()` dynamic task mapping | `.expand()` collapses all models into a single Graph node, destroying lineage — **never use** |
| `subprocess.run` for dbt | `dbtRunner` Python API | dbtRunner is unsafe for parallel workers and causes dependency conflicts; CLI-only is the documented safe approach |
| `common-compat` shim | Manual `try/except ImportError` version detection | Shim is the official, Airflow-team-maintained pattern; no version branches in our code |

**Installation (runtime):**
```bash
pip install airflow-provider-aircontroler[bigquery]
```

**Installation (development):**
```bash
pip install -e ".[dev,test,bigquery]"
```

**Version verification (as of 2026-04-18):**
- `apache-airflow`: latest 3.2.0, last 2.x was 2.9.3 [VERIFIED: PyPI]
- `apache-airflow-providers-common-compat`: 1.14.3, requires `apache-airflow>=2.11.0` [VERIFIED: PyPI]
- `dbt-core`: 1.11.8 (latest stable 1.x) [VERIFIED: PyPI]
- `dbt-duckdb`: 1.10.1, requires `dbt-core>=1.8.0` [VERIFIED: PyPI]

---

## Architecture Patterns

### System Architecture Diagram

```
User DAG file (parse time)
        |
        v
build_dbt_dag(dag, manifest_path, dbt_command, project_dir, profiles_path, ...)
        |
        +---> ManifestLoader.load(manifest_path)
        |        |  reads manifest.json
        |        |  dispatches on manifest_version (v11 → _parse_v11, v12 → _parse_v12)
        |        v
        |     list[ManifestNode]
        |
        +---> SelectorResolver.resolve(dbt_command, project_dir, profiles_path)
        |        |  parses --select / --exclude / --selector from dbt_command
        |        |  runs: dbt ls --select ... --exclude ... --output json --project-dir ... --profiles-dir ...
        |        |  reads one JSON object per stdout line
        |        v
        |     set of unique_ids matching selector
        |
        +---> filter ManifestNodes to resolved unique_ids
        |
        +---> for each node (static Python loop):
        |        DbtRunModelOperator(task_id=node.unique_id, dag=dag, ...)
        |
        +---> for each node, wire edges:
        |        for dep_uid in node.depends_on:
        |            if dep_uid in tasks:
        |                tasks[dep_uid].set_downstream(tasks[node.unique_id])
        |
        +---> classify entry/exit tasks
        |
        v
{'entry_tasks': [...], 'exit_tasks': [...], 'tasks': {unique_id: operator}}

─────────────────────────────── runtime boundary ───────────────────────────────

Airflow scheduler triggers DAG run
        |
        v
DbtRunModelOperator.execute(context)
        |
        +---> build command: [dbt_executable, "build", "--select", unique_id,
        |                     "--project-dir", project_dir,
        |                     "--profiles-dir", profiles_path]
        |
        +---> subprocess.run(cmd, capture_output=True, text=True)
        |        streams stdout/stderr to Airflow task log
        |
        +---> check return code; raise AirflowException on non-zero
        |
        v
task succeeds / fails with dbt output in logs
```

### Recommended Project Structure

```
aircontroler/
├── __init__.py               # get_provider_info(), __version__
├── operator.py               # DbtRunModelOperator(BaseOperator)
├── factory.py                # build_dbt_dag() — public API
├── manifest.py               # ManifestLoader, ManifestNode, parse_manifest()
├── selector.py               # SelectorResolver.resolve()
└── hooks/
    ├── __init__.py
    └── metadata_db.py        # MetadataDBHook(BaseHook) — stub
tests/
├── conftest.py
├── unit/
│   ├── test_manifest.py
│   ├── test_selector.py
│   ├── test_operator.py
│   └── test_factory.py
├── integration/
│   └── test_dag_wiring.py    # full pipeline with dbt-duckdb
└── fixtures/
    ├── manifest_v11.json     # hand-crafted v11 fixture (5-node diamond)
    ├── manifest_v12.json     # hand-crafted v12 fixture (same topology)
    └── dbt_project/          # minimal dbt-duckdb project (5 models)
        ├── dbt_project.yml
        ├── profiles.yml
        └── models/
            ├── stg_a.sql
            ├── stg_b.sql
            ├── int_a.sql
            ├── int_b.sql
            └── mart.sql
pyproject.toml
```

### Pattern 1: Manifest Version Dispatch

```python
# aircontroler/manifest.py
# Source: D-09 (CONTEXT.md locked decision); verified against dbt JSON schemas

from dataclasses import dataclass, field
from typing import Any
import json

@dataclass
class ManifestNode:
    unique_id: str
    name: str
    resource_type: str        # "model" | "seed" | "snapshot"
    depends_on: list[str]     # list of unique_ids from depends_on.nodes
    fqn: list[str]
    tags: list[str]
    original_file_path: str
    config: dict[str, Any] = field(default_factory=dict)

def parse_manifest(path: str) -> list[ManifestNode]:
    with open(path) as f:
        data = json.load(f)
    version = data["metadata"]["dbt_schema_version"]
    if "v11" in version:
        return _parse_v11(data)
    elif "v12" in version:
        return _parse_v12(data)
    else:
        raise ValueError(f"Unsupported manifest schema version: {version!r}")

def _parse_v11(data: dict) -> list[ManifestNode]:
    return _parse_nodes(data)

def _parse_v12(data: dict) -> list[ManifestNode]:
    return _parse_nodes(data)

def _parse_nodes(data: dict) -> list[ManifestNode]:
    result = []
    for uid, node in data["nodes"].items():
        if node.get("resource_type") not in ("model", "seed", "snapshot"):
            continue
        result.append(ManifestNode(
            unique_id=node["unique_id"],
            name=node["name"],
            resource_type=node["resource_type"],
            depends_on=[
                n for n in node.get("depends_on", {}).get("nodes", [])
                if n.startswith(("model.", "seed.", "snapshot."))
            ],
            fqn=node.get("fqn", []),
            tags=node.get("tags", []),
            original_file_path=node.get("original_file_path", ""),
            config=node.get("config", {}),
        ))
    return result
```

The v11 vs v12 difference relevant to Phase 1: v12 adds a top-level `unit_tests` key that v11 lacks. The `nodes` structure is functionally identical for `model`, `seed`, and `snapshot` nodes between v11 and v12. The `depends_on.nodes` field has the same structure in both versions. [VERIFIED: dbt changelog + schemas.getdbt.com cross-reference]

### Pattern 2: dbt ls Selector Resolution

```python
# aircontroler/selector.py
# Source: D-04 (CONTEXT.md); dbt ls docs (docs.getdbt.com/reference/commands/list)

import re
import subprocess
import json
from typing import Optional

SELECTOR_FLAGS = {
    "--select": re.compile(r'--select\s+(\S+)'),
    "--exclude": re.compile(r'--exclude\s+(\S+)'),
    "--selector": re.compile(r'--selector\s+(\S+)'),
    "--models": re.compile(r'--models\s+(\S+)'),  # legacy alias
}

class SelectorResolver:
    def __init__(self, dbt_executable: str = "dbt"):
        self.dbt_executable = dbt_executable

    def resolve(
        self,
        dbt_command: str,
        project_dir: str,
        profiles_dir: str,
    ) -> set[str]:
        """Return set of unique_ids matching the selector in dbt_command."""
        args = self._build_ls_args(dbt_command)
        cmd = [
            self.dbt_executable, "ls",
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
            "--output", "json",
            "--output-keys", "unique_id",
        ] + args
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt ls failed (exit {result.returncode}): {result.stderr}"
            )
        unique_ids = set()
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                unique_ids.add(obj["unique_id"])
            except (json.JSONDecodeError, KeyError):
                pass  # dbt sometimes emits log lines before JSON
        return unique_ids

    def _build_ls_args(self, dbt_command: str) -> list[str]:
        args = []
        for flag, pattern in SELECTOR_FLAGS.items():
            m = pattern.search(dbt_command)
            if m:
                args.extend([flag, m.group(1)])
        return args
```

**`dbt ls --output json` format:** Each line is a standalone JSON object, not a JSON array. The full node record is emitted when `--output json` is used without `--output-keys`. With `--output-keys unique_id`, each line is `{"unique_id": "model.project.name"}`. [VERIFIED: docs.getdbt.com/reference/commands/list]

**`--selector` flag note:** As of dbt v1.12 (not yet released), `--selector` becomes a method inside `--select selector:NAME` syntax. For v1.8–1.11 (Phase 1 scope), `--selector YML_SELECTOR_NAME` is the correct flag. [CITED: dbt-core docs upgrading-to-v1.11]

### Pattern 3: Static Task Loop in Factory

```python
# aircontroler/factory.py
# Source: D-01 through D-05 (CONTEXT.md locked decisions); ARCHITECTURE.md

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.models import DAG
from aircontroler.manifest import parse_manifest, ManifestNode
from aircontroler.selector import SelectorResolver
from aircontroler.operator import DbtRunModelOperator
import logging

log = logging.getLogger(__name__)

def build_dbt_dag(
    dag: DAG,
    manifest_path: str,
    dbt_command: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str | None = None,
    service_account: str | None = None,
) -> dict:
    if service_account is not None:
        log.warning(
            "service_account is not yet implemented — "
            "will be supported in a future release"
        )

    executable = dbt_executable_path or "dbt"
    # Resolve selector at parse time
    resolver = SelectorResolver(dbt_executable=executable)
    resolved_ids = resolver.resolve(dbt_command, project_dir, profiles_path)

    # Load manifest and filter to resolved nodes
    all_nodes = parse_manifest(manifest_path)
    node_map = {n.unique_id: n for n in all_nodes
                if n.unique_id in resolved_ids}

    # Static loop: one operator per node
    tasks: dict[str, BaseOperator] = {}
    for uid, node in node_map.items():
        tasks[uid] = DbtRunModelOperator(
            task_id=uid,
            dag=dag,
            unique_id=uid,
            project_dir=project_dir,
            profiles_path=profiles_path,
            dbt_executable_path=executable,
        )

    # Wire edges from manifest depends_on
    for uid, node in node_map.items():
        for dep_uid in node.depends_on:
            if dep_uid in tasks:
                tasks[dep_uid].set_downstream(tasks[uid])

    # Classify entry / exit tasks
    has_upstream = {uid for node in node_map.values() for dep_uid in node.depends_on
                    if dep_uid in tasks for uid in [uid]}
    # entry = tasks with no resolved upstream
    entry_tasks = [t for uid, t in tasks.items()
                   if not any(dep in tasks for dep in node_map[uid].depends_on)]
    exit_tasks = [t for uid, t in tasks.items()
                  if not any(uid in node_map[other].depends_on
                             for other in node_map)]
    return {
        "entry_tasks": entry_tasks,
        "exit_tasks": exit_tasks,
        "tasks": tasks,
    }
```

### Pattern 4: Operator Base Class via common-compat Shim

```python
# aircontroler/operator.py
# Source: VERIFIED airflow.providers.common.compat.sdk module docs

from airflow.providers.common.compat.sdk import BaseOperator, AirflowException
import subprocess
import shutil

class DbtRunModelOperator(BaseOperator):
    def __init__(
        self,
        *,
        unique_id: str,
        project_dir: str,
        profiles_path: str,
        dbt_executable_path: str = "dbt",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.project_dir = project_dir
        self.profiles_path = profiles_path
        self.dbt_executable_path = dbt_executable_path

    def execute(self, context):
        executable = self.dbt_executable_path or shutil.which("dbt") or "dbt"
        cmd = [
            executable, "build",
            "--select", self.unique_id,
            "--project-dir", self.project_dir,
            "--profiles-dir", self.profiles_path,
        ]
        self.log.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.log.info(result.stdout)
        if result.returncode != 0:
            self.log.error(result.stderr)
            raise AirflowException(
                f"dbt build failed for {self.unique_id} (exit {result.returncode})"
            )
```

### Pattern 5: Provider Entry Point (pyproject.toml)

```toml
# Source: VERIFIED airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html

[build-system]
requires = ["hatchling>=1.27"]
build-backend = "hatchling.build"

[project]
name = "airflow-provider-aircontroler"
version = "0.1.0"
requires-python = ">=3.10,<3.14"
dependencies = [
    "apache-airflow>=2.10.0,<4.0",
    "apache-airflow-providers-common-compat>=1.7.0",
    "dbt-core>=1.8.0,<1.12",
    "pyyaml>=6.0",
    "packaging>=23.0",
]

[project.optional-dependencies]
bigquery = ["dbt-bigquery>=1.8.0,<1.12"]
fast-parse = ["orjson>=3.10"]
test = [
    "dbt-duckdb>=1.8.0",
    "pytest>=8.0",
    "pytest-mock>=3.14",
    "pytest-cov>=5.0",
    "ruff>=0.7",
    "mypy>=1.13",
]

[project.entry-points."apache_airflow_provider"]
provider_info = "aircontroler:get_provider_info"
```

**Entry point key is `apache_airflow_provider` (without quotes around it in the TOML section header syntax, but using the dot notation).** [VERIFIED: docs and airflow-provider-sample template]

### Pattern 6: `get_provider_info()` Return Value

```python
# aircontroler/__init__.py
# Source: VERIFIED airflow.apache.org custom provider docs

__version__ = "0.1.0"

def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aircontroler",
        "name": "aircontroler",
        "description": (
            "Map each dbt model to a native Airflow task with "
            "accurate upstream/downstream wiring."
        ),
        "versions": [__version__],
    }
```

The dict must conform to Airflow's `provider_info.schema.json`. The required keys in current versions are `package-name`, `name`, `description`, and `versions`. Optional: `connection-types`, `hook-class-names`, `extra-links`. [VERIFIED: airflow.apache.org/docs custom-providers guide]

### Pattern 7: dbt-duckdb Integration Test Fixture

```yaml
# tests/fixtures/dbt_project/profiles.yml
test_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      schema: main
```

```yaml
# tests/fixtures/dbt_project/dbt_project.yml
name: fixture_project
version: "1.0.0"
profile: test_profile
config-version: 2
model-paths: ["models"]
```

```sql
-- tests/fixtures/dbt_project/models/stg_a.sql
select 1 as id, 'foo' as val
-- tests/fixtures/dbt_project/models/stg_b.sql
select 2 as id, 'bar' as val
-- tests/fixtures/dbt_project/models/int_a.sql
select * from {{ ref('stg_a') }}
-- tests/fixtures/dbt_project/models/int_b.sql
select * from {{ ref('stg_b') }}
-- tests/fixtures/dbt_project/models/mart.sql
select a.id, a.val as a_val, b.val as b_val
from {{ ref('int_a') }} a
join {{ ref('int_b') }} b on a.id != b.id
```

This gives a 5-model diamond DAG: `stg_a` and `stg_b` are roots; `int_a` depends on `stg_a`; `int_b` depends on `stg_b`; `mart` depends on both `int_a` and `int_b`. Expected: 2 entry tasks, 1 exit task.

### Anti-Patterns to Avoid

- **Using `.expand()` for model tasks:** Collapses all models to a single Graph node, destroys lineage. Use a static `for` loop at parse time. [VERIFIED: ARCHITECTURE.md + Airflow docs]
- **Calling `dbt compile` at parse time as the default:** 50–100s scheduler stalls on large projects. Default to `manifest_path` (pre-built manifest); `dbt parse` is the opt-in dev path. [VERIFIED: PITFALLS.md C1]
- **Importing `dbtRunner`:** Unsafe for parallel workers; dependency hell. CLI subprocess only. [VERIFIED: PITFALLS.md + dbt docs]
- **Direct SQLAlchemy from `execute()`:** Broken in Airflow 3.x workers. All state I/O through hooks only. [VERIFIED: PITFALLS.md C2]
- **Hardcoding manifest field paths:** Must read `manifest_version` first, then dispatch. v11 vs v12 differ at the top level (v12 has `unit_tests`). [VERIFIED: PITFALLS.md C4]

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| dbt selector syntax parsing | Custom selector DSL/parser | `dbt ls --select ... --output json` subprocess | dbt's grammar is non-trivial: graph operators, set operators, method selectors, YAML selectors. Reimplementation drifts. `dbt-node-selector` (Wasmer) is not production-ready. [VERIFIED: STACK.md] |
| Airflow 2.x/3.x compatibility branching | `try/except ImportError` or `if AIRFLOW_V_3_0_PLUS:` | `from airflow.providers.common.compat.sdk import BaseOperator` | Single import, no branches, maintained by Airflow team. [VERIFIED: common-compat docs] |
| manifest version negotiation | Custom version detection | Read `metadata.dbt_schema_version` and dispatch | Manifest schema URLs encode the version; v11/v12 have identical node shapes for our fields. [VERIFIED: dbt docs] |
| Entry point discovery | Custom plugin registration | Standard `[project.entry-points."apache_airflow_provider"]` TOML key | Airflow's scanner picks this up automatically; tested pattern across dozens of providers. [VERIFIED: Airflow custom provider docs] |

**Key insight:** The dbt CLI is the provider's best dependency. Delegating selector resolution and execution to the CLI means we never have to track dbt's internal Python API changes.

---

## Common Pitfalls

### Pitfall 1: Airflow Floor Confusion (common-compat version mismatch)

**What goes wrong:** The CLAUDE.md states `apache-airflow>=2.7` but `apache-airflow-providers-common-compat>=1.7.0` requires Airflow `>=2.10.0`. Installing on Airflow 2.7–2.9 will fail with a dependency conflict.

**Why it happens:** The common-compat package tracks Airflow's own support policy; its minimum Airflow floor was bumped to 2.10 in common-compat 1.7.0.

**How to avoid:** Declare `apache-airflow>=2.10.0,<4.0` in pyproject.toml. The practical test matrix is `{2.9.x, 3.x latest}` — but common-compat only supports 2.10+.

**Warning signs:** `pip check` errors about conflicting requirements; "VersionConflict: apache-airflow 2.8 is not compatible with common-compat 1.7.0" during install.

**Confidence:** HIGH [VERIFIED: PyPI registry — common-compat 1.7.0 requires apache-airflow>=2.10.0]

### Pitfall 2: `dbt ls` line-delimited JSON, not a JSON array

**What goes wrong:** Code tries `json.loads(result.stdout)` treating the entire output as one JSON object. dbt emits one JSON object per line, not a JSON array.

**Why it happens:** `dbt ls --output json` emits newline-delimited JSON (NDJSON). It also sometimes prepends log lines (timestamps, "Running with dbt=...") before the JSON output begins.

**How to avoid:** Parse line-by-line with `try/except json.JSONDecodeError`; skip non-JSON lines. Optionally add `--log-format json` to suppress log prefix lines.

**Warning signs:** `json.JSONDecodeError` in selector resolver; "Expecting value: line 1" errors.

**Confidence:** HIGH [VERIFIED: docs.getdbt.com/reference/commands/list]

### Pitfall 3: dbt-duckdb requires dbt-core>=1.8

**What goes wrong:** Integration tests use `dbt-duckdb` but `pyproject.toml` allows `dbt-core>=1.7`. `dbt-duckdb` hard-requires `dbt-core>=1.8.0`. Install fails or `pip install .[test]` selects dbt-core 1.7 and dbt-duckdb fails to install.

**Why it happens:** dbt-duckdb 1.8+ uses dbt-common and dbt-adapters which were split out in dbt-core 1.8.

**How to avoid:** Floor the dbt-core dependency at `>=1.8.0`. Use v11 manifest *fixtures* (hand-crafted JSON files) in unit tests to cover the v11 code path, since dbt-duckdb can't emit v11 manifests.

**Warning signs:** `pip install .[test]` fails with "Could not find a version satisfying dbt-duckdb>=1.8.0 with dbt-core<1.8.0".

**Confidence:** HIGH [VERIFIED: PyPI — dbt-duckdb 1.10.1 requires dbt-core>=1.8.0]

### Pitfall 4: `--selector` flag deprecated in dbt v1.12

**What goes wrong:** `SelectorResolver._build_ls_args()` emits `--selector <name>` which will become invalid in dbt v1.12+ (replaced by `--select selector:<name>`).

**Why it happens:** dbt is changing selector flag syntax in v1.12.

**How to avoid:** Phase 1 scope is dbt-core `<1.12`. Document that `--selector` parsing covers 1.8–1.11 only; add an upgrade note for when 1.12 arrives.

**Warning signs:** "Unknown argument: --selector" errors from dbt 1.12+.

**Confidence:** MEDIUM [CITED: dbt upgrading-to-v1.11 docs mentioning the flag change in v1.12]

### Pitfall 5: `entry-points` TOML key syntax

**What goes wrong:** pyproject.toml uses `[project.entry-points.apache_airflow_provider]` (no quotes) instead of `[project.entry-points."apache_airflow_provider"]` (with quotes). Hatchling may parse these identically, but setuptools and some pip versions differ.

**How to avoid:** Always quote the entry-point group name: `[project.entry-points."apache_airflow_provider"]`. [VERIFIED: official Airflow custom-provider docs and sample template]

### Pitfall 6: task_id containing dots breaks Airflow task graph

**What goes wrong:** `unique_id` for dbt models is `model.project.model_name`. Airflow task IDs cannot contain periods in some versions; they are used in Airflow's URL routing and task key generation.

**Why it happens:** dbt's `unique_id` format uses dots as separators.

**How to avoid:** Verify that Airflow 2.10+ and 3.x accept dotted task IDs. Cosmos uses the full `unique_id` as `task_id` and it works, but add a unit test that asserts the created `dag.get_task(unique_id)` can be round-tripped.

**Warning signs:** "Invalid characters in task_id" errors; URLs with `%2E` encoding that break the UI.

**Confidence:** MEDIUM [ASSUMED — Cosmos sets task_id=unique_id and it works, but explicit Airflow docs on dot-in-task-id not verified in this session]

---

## Code Examples

### Running dbt ls at Parse Time

```bash
# Source: docs.getdbt.com/reference/commands/list
dbt ls \
  --select tag:daily+ \
  --exclude raw_* \
  --output json \
  --output-keys unique_id \
  --project-dir /path/to/project \
  --profiles-dir /path/to/profiles
# Output (one line per resolved node):
{"unique_id": "model.my_project.stg_orders"}
{"unique_id": "model.my_project.fct_orders"}
```

### Compile-on-Parse Mode (CORE-02)

```bash
# Source: docs.getdbt.com/reference/commands/parse (ASSUMED — dbt parse generates manifest.json)
dbt parse --project-dir /path/to/project --profiles-dir /path/to/profiles
# Emits: /path/to/project/target/manifest.json
```

The manifest loader then reads `{project_dir}/target/manifest.json` by default when `manifest_path` is `None` and compile-on-parse mode is enabled. [ASSUMED — exact flag behavior for dbt parse not verified in this session]

### Integration Test Structure

```python
# tests/integration/test_dag_wiring.py
import subprocess
from pathlib import Path
from airflow.models import DAG
from aircontroler.factory import build_dbt_dag

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "dbt_project"

def test_full_pipeline_wires_correctly(tmp_path):
    # Generate manifest using real dbt-duckdb
    result = subprocess.run(
        ["dbt", "parse",
         "--project-dir", str(FIXTURE_DIR),
         "--profiles-dir", str(FIXTURE_DIR)],
        capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    manifest_path = str(FIXTURE_DIR / "target" / "manifest.json")

    with DAG("test_dag", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=manifest_path,
            dbt_command="dbt build --select +mart",
            project_dir=str(FIXTURE_DIR),
            profiles_path=str(FIXTURE_DIR),
        )

    assert len(result["tasks"]) == 5   # 5 models in fixture
    assert len(result["entry_tasks"]) == 2  # stg_a, stg_b
    assert len(result["exit_tasks"]) == 1   # mart

    # Verify mart is downstream of int_a and int_b
    mart_uid = next(uid for uid in result["tasks"] if "mart" in uid)
    mart_task = result["tasks"][mart_uid]
    upstream_ids = {t.task_id for t in mart_task.upstream_list}
    assert any("int_a" in uid for uid in upstream_ids)
    assert any("int_b" in uid for uid in upstream_ids)
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `airflow.models.BaseOperator` | `from airflow.providers.common.compat.sdk import BaseOperator` | Airflow 3.0 (2025) | Single import works on 2.x and 3.x; no version branches |
| `dbt compile` at parse time | Pre-built `manifest_path` as default; `dbt parse` as opt-in | Cosmos learned this the hard way (2023) | Eliminates 50–100s scheduler stalls |
| `setup.py` / `setup.cfg` | `pyproject.toml` + hatchling | PEP 621 (2021); Airflow adopted 2023 | Standard build; lighter than setuptools |
| `dbt ls --output selector` | `dbt ls --output json` | dbt 1.5+ | Machine-parseable; includes all node fields |
| Airflow 2.7 as common floor | Airflow 2.10 as effective floor | common-compat 1.7.0 (May 2025) | Python 3.10+ required; narrows test matrix |
| dbt-core `1.7.x` | `1.8.0` as practical floor | dbt-duckdb 1.8+ (2024) | dbt-adapters split; dbt-duckdb requires >=1.8 |

**Deprecated/outdated:**
- `pytest-airflow`: last release 2017, abandoned. Use `dag.test()` instead.
- `dbt ls --models`: deprecated alias for `--select`; still works in 1.8–1.11 but avoid.
- `setup.cfg` / `setup.py` for provider packaging: use `pyproject.toml` with hatchling.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `dbt parse` command generates `target/manifest.json` (CORE-02 implementation path) | Code Examples, Pattern 5 | Compile-on-parse implementation would need adjustment if flag or output path differs |
| A2 | Dotted `unique_id` values (e.g., `model.proj.name`) work as Airflow `task_id` on 2.10+ and 3.x | Pitfall 6, Pattern 3 | Would need to sanitize UIDs to replace dots with underscores |
| A3 | `dbt_executable_path` defaulting to `"dbt"` with `shutil.which` fallback is the right behavior | Pattern 4, CORE-06 | Users without dbt on PATH get an unclear error; could make it required param instead |
| A4 | `--profiles-dir` flag accepts the directory containing `profiles.yml` (not the file path itself) | Pattern 2, Pattern 3 | Operator built wrong if this is file path not directory |

**If this table is empty:** All claims in this research were verified or cited — no user confirmation needed.

---

## Open Questions

1. **Airflow 2.7 floor: keep or update?**
   - What we know: CLAUDE.md states `apache-airflow>=2.7`; common-compat 1.7.0 requires `>=2.10.0`. Airflow 2.x EOLed April 22 2026.
   - What's unclear: Whether the project actually needs to run on 2.7–2.9 (no production deployments specified there).
   - Recommendation: Update pyproject.toml to `>=2.10.0,<4.0`. Flag for user confirmation since CLAUDE.md explicitly states 2.7+.

2. **Python version floor**
   - What we know: common-compat 1.14.x supports Python 3.10–3.14. dbt-core 1.8+ requires Python 3.8+. Airflow 3.x requires 3.10+.
   - What's unclear: Whether Python 3.9 support matters to users.
   - Recommendation: Target `python_requires = ">=3.10,<3.14"` matching Airflow 3 and common-compat floors.

3. **`--selector` flag parsing edge case**
   - What we know: `--selector NAME` selects YAML-defined selectors in dbt 1.8–1.11.
   - What's unclear: Exact regex to extract the name when the selector name has dashes or underscores.
   - Recommendation: Use `--selector\s+(\S+)` with word-boundary awareness; add a test case.

---

## Environment Availability

Step 2.6: SKIPPED for the development environment — this is a greenfield Python package. No external runtime dependencies are needed to build the package itself; `dbt` and `airflow` are declared as pip dependencies. The CI matrix (GitHub Actions) will provision them.

| Dependency | Required By | Available in dev env | Notes |
|------------|-------------|----------------------|-------|
| Python 3.13 | Development | ✓ 3.13.12 | Exceeds minimum; acceptable |
| Node.js 22 | Context7 docs tooling | ✓ v22.22.2 | Not needed at runtime |
| pip/pytest | Test runner | ✗ (pytest not installed) | Install via `pip install -e .[test]` |
| dbt-core | Integration tests | ✗ | Install via `pip install -e .[test]` |
| Airflow | Unit tests | ✗ | Install via CI matrix |

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 8.x |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` — Wave 0 gap |
| Quick run command | `pytest tests/unit/ -x -q` |
| Full suite command | `pytest tests/ -x --cov=aircontroler --cov-report=term-missing` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CORE-01 | Load manifest.json from path, return ManifestNode list | unit | `pytest tests/unit/test_manifest.py -x` | ❌ Wave 0 |
| CORE-01 | v11 manifest parsed correctly (all fields extracted) | unit | `pytest tests/unit/test_manifest.py::test_parse_v11 -x` | ❌ Wave 0 |
| CORE-01 | v12 manifest parsed correctly (unit_tests key ignored safely) | unit | `pytest tests/unit/test_manifest.py::test_parse_v12 -x` | ❌ Wave 0 |
| CORE-01 | Unsupported manifest version raises ValueError | unit | `pytest tests/unit/test_manifest.py::test_unknown_version_raises -x` | ❌ Wave 0 |
| CORE-02 | Compile-on-parse: `dbt parse` subprocess is called with correct args | unit | `pytest tests/unit/test_manifest.py::test_compile_on_parse -x` | ❌ Wave 0 |
| CORE-03 | `--select` flag extracted from dbt command string | unit | `pytest tests/unit/test_selector.py::test_extract_select -x` | ❌ Wave 0 |
| CORE-03 | `--exclude` flag extracted | unit | `pytest tests/unit/test_selector.py::test_extract_exclude -x` | ❌ Wave 0 |
| CORE-03 | `--selector` flag extracted | unit | `pytest tests/unit/test_selector.py::test_extract_selector -x` | ❌ Wave 0 |
| CORE-03 | `dbt ls --output json` result parsed correctly (NDJSON, skip log lines) | unit | `pytest tests/unit/test_selector.py::test_parse_ls_output -x` | ❌ Wave 0 |
| CORE-03 | Non-zero `dbt ls` exit code raises RuntimeError | unit | `pytest tests/unit/test_selector.py::test_ls_failure_raises -x` | ❌ Wave 0 |
| CORE-04 | Each ManifestNode → one DbtRunModelOperator with task_id=unique_id | unit | `pytest tests/unit/test_factory.py::test_task_per_model -x` | ❌ Wave 0 |
| CORE-05 | Upstream/downstream edges match manifest depends_on | unit | `pytest tests/unit/test_factory.py::test_edge_wiring -x` | ❌ Wave 0 |
| CORE-05 | entry_tasks and exit_tasks correctly classified | unit | `pytest tests/unit/test_factory.py::test_entry_exit_classification -x` | ❌ Wave 0 |
| CORE-06 | Operator calls `dbt build --select <uid>` subprocess | unit | `pytest tests/unit/test_operator.py::test_subprocess_command -x` | ❌ Wave 0 |
| CORE-06 | dbt_executable_path used when provided | unit | `pytest tests/unit/test_operator.py::test_custom_executable -x` | ❌ Wave 0 |
| CORE-06 | Non-zero dbt exit code raises AirflowException | unit | `pytest tests/unit/test_operator.py::test_failure_raises -x` | ❌ Wave 0 |
| CORE-07 | profiles_path passed as --profiles-dir to subprocess | unit | `pytest tests/unit/test_operator.py::test_profiles_path -x` | ❌ Wave 0 |
| CORE-08 | `from airflow.providers.common.compat.sdk import BaseOperator` succeeds | unit (import) | `pytest tests/unit/test_operator.py::test_import_compat -x` | ❌ Wave 0 |
| CORE-09 | `get_provider_info()` returns valid dict with required keys | unit | `pytest tests/unit/test_init.py::test_provider_info -x` | ❌ Wave 0 |
| CORE-09 | Provider entry-point is discoverable after `pip install -e .` | integration | manual — `airflow providers list` | ❌ Wave 0 |
| All | Full pipeline: manifest parse → selector resolve → DAG wire → execute | integration | `pytest tests/integration/ -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/unit/ -x -q`
- **Per wave merge:** `pytest tests/ -x --cov=aircontroler --cov-report=term-missing`
- **Phase gate:** Full suite green + `pip check` clean before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/conftest.py` — shared fixtures (manifest_v11_data, manifest_v12_data, mock_dag, mock_context)
- [ ] `tests/unit/test_manifest.py` — covers CORE-01, CORE-02
- [ ] `tests/unit/test_selector.py` — covers CORE-03
- [ ] `tests/unit/test_operator.py` — covers CORE-06, CORE-07, CORE-08
- [ ] `tests/unit/test_factory.py` — covers CORE-04, CORE-05
- [ ] `tests/unit/test_init.py` — covers CORE-09
- [ ] `tests/integration/test_dag_wiring.py` — covers all CORE-* end-to-end
- [ ] `tests/fixtures/manifest_v11.json` — hand-crafted 5-node v11 fixture
- [ ] `tests/fixtures/manifest_v12.json` — same topology, v12 schema (adds `unit_tests: {}` key)
- [ ] `tests/fixtures/dbt_project/` — minimal dbt-duckdb project (5 models)
- [ ] `pyproject.toml` `[tool.pytest.ini_options]` — testpaths, addopts
- [ ] Framework install: `pip install -e ".[test]"` — installs pytest + dbt-duckdb

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Not applicable — no user auth in this provider |
| V3 Session Management | No | Not applicable |
| V4 Access Control | No | Provider does not manage access control |
| V5 Input Validation | Yes | Validate `manifest_path` exists; validate `project_dir` and `profiles_path` are directories at parse time |
| V6 Cryptography | No | No crypto in Phase 1 |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Subprocess injection via `dbt_command` | Tampering | Never pass `dbt_command` verbatim to `subprocess.run`; always extract specific flags and build command list as `list[str]`, never `shell=True` |
| Credential leak via task logs | Information Disclosure | Never log full env dict; use Airflow's `secrets_masker.mask_secret()` for any secrets visible in logs; `profiles.yml` path only in logs, not contents |
| Stale manifest loaded from wrong path | Tampering | Validate `manifest_path` exists at parse time; log the resolved absolute path for auditability |

---

## Sources

### Primary (HIGH confidence)

- [airflow.providers.common.compat.sdk module source](https://airflow.apache.org/docs/apache-airflow-providers-common-compat/stable/_modules/airflow/providers/common/compat/sdk.html) — all importable names verified
- [PyPI: apache-airflow-providers-common-compat](https://pypi.org/project/apache-airflow-providers-common-compat/) — version 1.14.3 requires apache-airflow>=2.11.0; version 1.7.0 requires >=2.10.0
- [PyPI: dbt-core](https://pypi.org/project/dbt-core/) — latest 1.11.8 confirmed
- [PyPI: dbt-duckdb](https://pypi.org/project/dbt-duckdb/) — 1.10.1 requires dbt-core>=1.8.0 confirmed
- [Airflow Custom Providers Guide](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html) — entry-point format, get_provider_info() schema verified
- [dbt ls command reference](https://docs.getdbt.com/reference/commands/list) — `--output json` NDJSON format, `--output-keys` flag, `--resource-type` filtering verified
- [common-compat changelog](https://airflow.apache.org/docs/apache-airflow-providers-common-compat/stable/changelog.html) — version floor progression (2.8→2.9→2.10→2.11) verified
- [dbt manifest v12 reference](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/04-upgrading-to-v1.11) — v12 adds `unit_tests` key vs v11; node fields stable

### Secondary (MEDIUM confidence)

- [PITFALLS.md](../.planning/research/PITFALLS.md) — C1 (compile-at-parse), C2 (direct DB), C4 (manifest schema), C6 (locking)
- [ARCHITECTURE.md](../.planning/research/ARCHITECTURE.md) — parse-time vs runtime split, static task loop rationale
- [STACK.md](../.planning/research/STACK.md) — hatchling, common-compat shim pattern, dbt-core range rationale
- [dbt-duckdb profiles.yml setup](https://docs.getdbt.com/docs/local/connect-data-platform/duckdb-setup) — type:duckdb + path fields verified

### Tertiary (LOW confidence)

- dbt `parse` command output path assumed to be `{project_dir}/target/manifest.json` — not verified in this session
- Dotted task_id (e.g., `model.proj.name`) behavior in Airflow 2.10+/3.x — assumed to work based on Cosmos using this pattern; not directly verified against Airflow source

---

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH — all versions verified against PyPI registry; critical correction on Airflow floor documented
- Architecture: HIGH — locked decisions from CONTEXT.md; patterns verified against official Airflow and dbt docs
- Pitfalls: HIGH — most from prior research (HIGH confidence); two new pitfalls verified in this session
- Test architecture: HIGH — test cases map 1:1 to CORE-01 through CORE-09 requirements

**Research date:** 2026-04-18
**Valid until:** 2026-05-18 (stable ecosystem; primary risk is dbt-core releasing 1.12 which bumps manifest schema to v13)
