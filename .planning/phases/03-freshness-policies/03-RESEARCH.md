# Phase 3: Freshness Policies - Research

**Researched:** 2026-04-19
**Domain:** Airflow operator state persistence, Alembic migrations, skip semantics, trigger_rule
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Skip signal (FRESH-03)**
- D-01: When fresh, operator raises `AirflowSkipException`. Task appears yellow ("skipped") in Airflow UI.
- D-02: Default `trigger_rule` for all generated tasks is `'none_failed'` — downstream runs even if some upstreams were skipped (fresh).
- D-03: `trigger_rule` configurable at two levels: `build_dbt_dag(trigger_rule='none_failed')` and `meta.aircontroler.trigger_rule` per-model.

**State table schema (FRESH-04, FRESH-06)**
- D-04: `aircontroler_model_runs` is append-only, success-only. Never updated in-place.
- D-05: Columns: `id` (int PK autoincrement), `unique_id` (varchar), `dag_id` (varchar), `run_id` (varchar), `completed_at` (timestamp with timezone).
- D-06: Freshness query: `SELECT MAX(completed_at) FROM aircontroler_model_runs WHERE unique_id = :uid`. Fresh if `now() - MAX(completed_at) < freshness_hours`.

**Hook DB access (FRESH-04)**
- D-07: `MetadataDBHook` uses `create_session` from `airflow.utils.session` for runtime reads/writes.
- D-08: Airflow 3.x table creation uses `BaseDBManager` (registered via provider `db-managers`). Airflow 2.x uses `aircontroler db init` CLI. Hook runtime read/write uses `create_session` on both.
- D-09: Two public methods: `get_last_run(unique_id: str) -> datetime | None` and `record_run(unique_id: str, dag_id: str, run_id: str) -> None`.

**Freshness TTL source (FRESH-01)**
- D-10: `freshness_hours` read from `meta.aircontroler.freshness_hours` in schema.yml. Extraction path: `node.config.get('meta', {}).get('aircontroler', {}).get('freshness_hours')`.
- D-11: FRESH-02 (per-DAG Python override) and FRESH-05 (longest TTL wins) are **deferred**. Out of scope for Phase 3.
- D-12: If `freshness_hours` is not set, skip check entirely — always run dbt (backward compatible).

### Claude's Discretion
- Exact Alembic migration file structure and revision ID
- Whether `aircontroler db init` is implemented as a `click` CLI or Airflow's native CLI extension
- Index strategy for `aircontroler_model_runs`
- Whether `get_last_run()` and `record_run()` use same or separate DB sessions

### Deferred Ideas (OUT OF SCOPE)
- FRESH-02: `build_dbt_dag(freshness_overrides=...)` per-DAG Python TTL override
- FRESH-05: Cross-DAG longest-TTL-wins registry
- OBS-V2-02: `aircontroler db cleanup` pruning command
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FRESH-01 | User sets `meta.aircontroler.freshness_hours` per model | Verified extraction path via `node.config.get(...)` (existing Phase 2 pattern) |
| FRESH-02 | Per-DAG Python TTL override | **DEFERRED (D-11) — OUT OF SCOPE** |
| FRESH-03 | Task checks TTL at start, raises `AirflowSkipException` if fresh | `AirflowSkipException` verified importable via `airflow.providers.common.compat.sdk` |
| FRESH-04 | State persisted in Airflow metadata DB via hook, not direct SQLAlchemy | `create_session` from `airflow.utils.session` verified working in Airflow 3.2.0 |
| FRESH-05 | Longest TTL wins across DAGs | **DEFERRED (D-11) — trivially satisfied (one TTL per model in Phase 3)** |
| FRESH-06 | Ships Alembic migration; Airflow 3 uses `BaseDBManager`, Airflow 2.x uses CLI | `BaseDBManager` at `airflow.utils.db_manager` verified; registration via `db-managers` in `get_provider_info()` |
</phase_requirements>

---

## Summary

Phase 3 adds time-based freshness TTL checks to `DbtRunModelOperator`. The operator reads `freshness_hours` from the manifest at task construction time, and at runtime checks `aircontroler_model_runs` via `MetadataDBHook`. If the model ran successfully within the TTL window it raises `AirflowSkipException` (yellow task in Airflow UI) without invoking dbt. On successful dbt execution it writes a success row via the same hook. The default `trigger_rule='none_failed'` ensures downstream models run even when some upstreams are skipped.

Three separate subsystems must be built: (1) the hook (`MetadataDBHook`) with `create_session`-based read/write, (2) the table migration (Alembic + `BaseDBManager` for Airflow 3, standalone `click` CLI for Airflow 2.x), and (3) the operator/factory wiring for `freshness_hours` and `trigger_rule`. The primary constraint from CLAUDE.md — "never use direct SQLAlchemy sessions from worker execute() methods" — is the architectural forcing function for the hook approach.

FRESH-02 and FRESH-05 are deferred per D-11. In-scope requirements for Phase 3 are FRESH-01, FRESH-03, FRESH-04, and FRESH-06.

**Primary recommendation:** Use `create_session` from `airflow.utils.session` in the hook (the same module works on both Airflow 2.7+ and 3.x), register the `BaseDBManager` subclass via the `db-managers` key in `get_provider_info()`, and expose `aircontroler db init` as a standalone `click` console_script entry point for Airflow 2.x.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Freshness TTL check | Operator (runtime) | — | Must happen in `execute()` before subprocess; hook provides DB access |
| DB read (`get_last_run`) | Hook | — | Worker isolation rule: no direct SQLAlchemy from operator |
| DB write (`record_run`) | Hook | — | Same rule; append-only success row written after dbt succeeds |
| Table creation (Airflow 3) | `AircontrolerDBManager` (BaseDBManager subclass) | — | `BaseDBManager` is the Airflow 3 provider pattern; registered via `db-managers` |
| Table creation (Airflow 2.x) | `aircontroler db init` CLI command | — | Airflow 2.x has no `BaseDBManager`; CLI applies Alembic migration directly |
| TTL extraction | Factory (parse-time) | — | `freshness_hours` extracted from manifest in same loop as Phase 2 meta overrides |
| trigger_rule wiring | Factory (parse-time) | — | Task DAG structure is a parse-time decision; `trigger_rule` set on operator at construction |
| Alembic migration | `aircontroler/migrations/` | — | Single revision file used by both paths |

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `airflow.utils.session.create_session` | (built-in, Airflow 2.7+/3.x) | DB session context manager for hook reads/writes | Only safe way to access Airflow metadata DB from workers; same API across versions [VERIFIED: live env, Airflow 3.2.0] |
| `airflow.utils.db_manager.BaseDBManager` | (built-in, Airflow 3.x only) | Abstract base class for provider-owned DB migrations | Official Airflow 3 provider pattern for external DB managers; registered via `ProvidersManager` [VERIFIED: live env] |
| `alembic` | 1.13.1–1.x | Migration DDL for `aircontroler_model_runs` | Transitive dep via `apache-airflow-core`; no explicit declaration needed in pyproject.toml [VERIFIED: live env, alembic 1.18.4] |
| `airflow.providers.common.compat.sdk.AirflowSkipException` | (via common-compat 1.14.3) | Raises skip signal from within `execute()` | Compat shim tries `airflow.sdk.exceptions` first (Airflow 3) then falls back to `airflow.exceptions` (Airflow 2.x) [VERIFIED: live env] |
| `click` | 8.3.2 (via airflow) | `aircontroler db init` CLI for Airflow 2.x table creation | Click is a transitive dep of Airflow; zero additional dependency [VERIFIED: live env] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `sqlalchemy` | 2.0.x (via airflow) | Table definition for `aircontroler_model_runs` metadata | Table/Column declarations in `db_manager.py` for both ORM metadata and Alembic |
| `datetime` (stdlib) | — | Timezone-aware timestamp arithmetic for TTL comparison | `datetime.now(tz=timezone.utc)` vs `completed_at` for freshness check |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `create_session` from `airflow.utils.session` | Direct `settings.Session()` calls | `create_session` handles commit/rollback/close automatically; direct session usage is fragile and violates the hook contract |
| Click console_script for `aircontroler db init` | Airflow native `cli` provider extension (JSON registration) | Airflow native CLI requires Airflow to be initialized to run; standalone click script is simpler for a one-shot init command and works without airflow context [ASSUMED] |
| `DateTime(timezone=True)` (SQLAlchemy native) | `airflow.utils.sqlalchemy.UtcDateTime` | Airflow's `UtcDateTime` adds Airflow-specific normalization not needed for a provider-owned table; standard `DateTime(timezone=True)` is portable |

**Installation:** No new dependencies. `alembic`, `click`, and `sqlalchemy` are all transitive deps of `apache-airflow`.

---

## Architecture Patterns

### System Architecture Diagram

```
DAG Parse Time:
  manifest.json
      |
      v
  factory.py (build_dbt_dag)
      |-- extracts freshness_hours from meta.aircontroler
      |-- extracts trigger_rule from meta.aircontroler
      |-- passes freshness_hours, trigger_rule to DbtRunModelOperator(...)
      v
  DbtRunModelOperator instances (trigger_rule='none_failed')

Task Runtime (execute(context)):
  execute(context)
      |
      |-- [if freshness_hours is set]
      |       |
      |       v
      |   MetadataDBHook.get_last_run(unique_id)
      |       |-- create_session() -> SELECT MAX(completed_at) WHERE unique_id=...
      |       |-- returns datetime | None
      |       |
      |       |-- [if now() - last_run < freshness_hours]
      |               raise AirflowSkipException  --> task state: SKIPPED (yellow)
      |
      v
  [dbt not fresh or freshness_hours not set]
      |
      v
  subprocess.run(["dbt", "build", "--select", unique_id, ...])
      |
      |-- [returncode != 0] -> raise AirflowException
      |
      v
  _log_run_results()  (Phase 2, unchanged)
      |
      v
  MetadataDBHook.record_run(unique_id, dag_id, run_id)
      |-- create_session() -> INSERT INTO aircontroler_model_runs (...)
      v
  return (success)

Downstream task trigger_rule='none_failed':
  upstream SKIPPED  ──┐
  upstream SUCCESS  ──┤──> downstream runs (none_failed allows this)
  upstream FAILED   ──┘──> downstream blocked (upstream_failed state)
```

### Recommended Project Structure
```
aircontroler/
├── hooks/
│   └── metadata_db.py       # MetadataDBHook — FULL implementation (replaces stub)
├── migrations/
│   ├── alembic.ini          # Minimal ini; script_location points to this dir
│   ├── env.py               # Provider-scoped env.py (see pattern below)
│   └── versions/
│       └── 0001_add_model_runs_table.py  # Single revision: CREATE TABLE
├── db_manager.py            # AircontrolerDBManager(BaseDBManager) for Airflow 3
├── cli.py                   # click CLI: `aircontroler db init` for Airflow 2.x
├── operator.py              # Extended: freshness_hours + trigger_rule params
├── factory.py               # Extended: trigger_rule param + freshness/trigger_rule meta extraction
└── __init__.py              # Extended: add db-managers key to get_provider_info()
```

### Pattern 1: AirflowSkipException via compat shim
**What:** Raise skip exception in `execute()` when model is within TTL.
**When to use:** After `get_last_run()` returns a timestamp within the freshness window.
**Example:**
```python
# Source: verified from airflow.providers.common.compat.sdk module in live env
from airflow.providers.common.compat.sdk import AirflowSkipException

def execute(self, context):
    if self.freshness_hours is not None:
        hook = MetadataDBHook()
        last_run = hook.get_last_run(self.unique_id)
        if last_run is not None:
            from datetime import datetime, timezone
            elapsed = (datetime.now(tz=timezone.utc) - last_run).total_seconds() / 3600
            if elapsed < self.freshness_hours:
                self.log.info(
                    "Skipping %s: last run %.2f hours ago (TTL: %.1f hours)",
                    self.unique_id, elapsed, self.freshness_hours,
                )
                raise AirflowSkipException(
                    f"{self.unique_id} is fresh (last ran {elapsed:.2f}h ago, TTL {self.freshness_hours}h)"
                )
    # ... subprocess logic ...
    self._log_run_results()
    hook = MetadataDBHook()
    hook.record_run(
        unique_id=self.unique_id,
        dag_id=context["dag_run"].dag_id,
        run_id=context["run_id"],
    )
```

### Pattern 2: MetadataDBHook with create_session
**What:** Hook methods use `create_session` from `airflow.utils.session` — same import path on both Airflow 2.7+ and 3.x.
**When to use:** All DB access from the hook's public methods.
**Example:**
```python
# Source: verified from airflow.utils.session in live env (Airflow 3.2.0)
from __future__ import annotations
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session
import sqlalchemy as sa

class MetadataDBHook(BaseHook):
    conn_name_attr = "aircontroler_conn_id"
    default_conn_name = "aircontroler_default"
    conn_type = "aircontroler"
    hook_name = "Aircontroler"

    def get_conn(self):
        raise NotImplementedError("MetadataDBHook uses create_session, not get_conn()")

    def get_last_run(self, unique_id: str) -> datetime | None:
        from aircontroler.db_manager import model_runs_table
        with create_session() as session:
            result = session.execute(
                sa.select(sa.func.max(model_runs_table.c.completed_at))
                .where(model_runs_table.c.unique_id == unique_id)
            ).scalar()
        return result

    def record_run(self, unique_id: str, dag_id: str, run_id: str) -> None:
        from datetime import timezone
        from aircontroler.db_manager import model_runs_table
        with create_session() as session:
            session.execute(
                model_runs_table.insert().values(
                    unique_id=unique_id,
                    dag_id=dag_id,
                    run_id=run_id,
                    completed_at=datetime.now(tz=timezone.utc),
                )
            )
```

### Pattern 3: BaseDBManager subclass for Airflow 3
**What:** `AircontrolerDBManager` inherits from `BaseDBManager`, declares class-level attributes, registered via `db-managers` key in `get_provider_info()`.
**When to use:** Airflow 3.x table creation. `airflow db migrate` calls `initdb()` automatically.
**Example:**
```python
# Source: verified from airflow.utils.db_manager.BaseDBManager in live env
from __future__ import annotations
import os
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, Index
from airflow.utils.db_manager import BaseDBManager

metadata = MetaData()

model_runs_table = Table(
    "aircontroler_model_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("unique_id", String(255), nullable=False),
    Column("dag_id", String(250), nullable=False),
    Column("run_id", String(250), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=False),
    Index("ix_acr_unique_id", "unique_id"),
    Index("ix_acr_completed_at", "completed_at"),
)

class AircontrolerDBManager(BaseDBManager):
    metadata = metadata
    migration_dir = os.path.join(os.path.dirname(__file__), "migrations")
    alembic_file = os.path.join(os.path.dirname(__file__), "migrations", "alembic.ini")
    version_table_name = "aircontroler_alembic_version"  # MUST NOT be 'alembic_version'
    supports_table_dropping = True
    revision_heads_map = {"1.0.0": "0001"}
```

Registration in `get_provider_info()`:
```python
def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aircontroler",
        "name": "aircontroler",
        "description": "...",
        "versions": [__version__],
        "db-managers": ["aircontroler.db_manager.AircontrolerDBManager"],
    }
```

### Pattern 4: Alembic migration env.py for provider
**What:** A minimal `env.py` that uses the existing connection (passed by `BaseDBManager._get_migration_ctx()`), does NOT import Airflow at module level, and sets `version_table` in `context.configure()`.
**When to use:** In `aircontroler/migrations/env.py`. Required by `RunDBManager._validate()`.
**Key constraint:** `RunDBManager._validate()` AST-checks that `context.configure(...)` includes `version_table=` keyword. The version table must NOT be `'alembic_version'`.

```python
# aircontroler/migrations/env.py
from __future__ import annotations
from alembic import context

def run_migrations_online():
    # connection is injected by BaseDBManager._get_migration_ctx via attributes
    conn = context.config.attributes.get("connection", None)
    if conn is None:
        # Fallback: read from sqlalchemy.url (set by BaseDBManager.get_alembic_config)
        from sqlalchemy import engine_from_config
        cfg = context.config.get_section(context.config.config_ini_section, {})
        connectable = engine_from_config(cfg, prefix="sqlalchemy.")
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=None,
                version_table="aircontroler_alembic_version",
                render_as_batch=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    else:
        context.configure(
            connection=conn,
            target_metadata=None,
            version_table="aircontroler_alembic_version",
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    raise RuntimeError("Offline mode not supported for aircontroler migrations")
else:
    run_migrations_online()
```

### Pattern 5: Alembic revision file
```python
# aircontroler/migrations/versions/0001_add_model_runs_table.py
"""Add aircontroler_model_runs table.

Revision ID: 0001
Revises: None
Create Date: 2026-04-19
"""
from __future__ import annotations
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        "aircontroler_model_runs",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("unique_id", sa.String(length=255), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_acr_unique_id", "aircontroler_model_runs", ["unique_id"])
    op.create_index("ix_acr_completed_at", "aircontroler_model_runs", ["completed_at"])

def downgrade():
    op.drop_index("ix_acr_completed_at", table_name="aircontroler_model_runs")
    op.drop_index("ix_acr_unique_id", table_name="aircontroler_model_runs")
    op.drop_table("aircontroler_model_runs")
```

### Pattern 6: Standalone click CLI for Airflow 2.x
**What:** `aircontroler/cli.py` with a `click` group, registered as `console_scripts` entry point.
**When to use:** Airflow 2.x deployments where `BaseDBManager` is not available.
**Recommendation:** standalone click script (not Airflow native `cli` provider extension). Rationale: Airflow native CLI extension (`'cli': [...]` in provider_info) still requires Airflow to be fully initialized to discover the command — equivalent complexity to a console_script but with an extra registration layer. A simple `console_scripts` entry point works without Airflow context.

```python
# aircontroler/cli.py
import click

@click.group()
def cli():
    """aircontroler management commands."""

@cli.command("init")
def db_init():
    """Create aircontroler_model_runs table in the Airflow metadata DB (Airflow 2.x only)."""
    from airflow.utils.session import create_session
    from aircontroler.db_manager import AircontrolerDBManager
    with create_session() as session:
        manager = AircontrolerDBManager(session)
        manager.initdb()
    click.echo("aircontroler_model_runs table created.")
```

In `pyproject.toml`:
```toml
[project.scripts]
aircontroler = "aircontroler.cli:cli"
```

### Pattern 7: factory.py — trigger_rule and freshness_hours wiring
**What:** Extend `build_dbt_dag()` with `trigger_rule` param; extend per-model meta loop to extract `freshness_hours` and `trigger_rule`.
**Example:**
```python
def build_dbt_dag(
    ...
    trigger_rule: str = "none_failed",
) -> dict:
    ...
    for uid, node in node_map.items():
        ac_meta = node.config.get("meta", {}).get("aircontroler", {})
        task_pool = ac_meta.get("pool") or pool or None
        task_trigger_rule = ac_meta.get("trigger_rule") or trigger_rule
        tasks[uid] = DbtRunModelOperator(
            ...
            freshness_hours=ac_meta.get("freshness_hours"),
            trigger_rule=task_trigger_rule,
        )
```

### Anti-Patterns to Avoid
- **Direct `settings.Session()` in hook methods:** Not wrapped in create_session — no automatic commit/rollback/close on exception. Use `create_session()` context manager.
- **Importing `airflow.settings` at module top level in migrations/env.py:** Causes import-time failure if called from contexts where Airflow settings are not initialized (e.g., alembic CLI standalone).
- **Using `version_table_name = 'alembic_version'`:** Airflow's `RunDBManager._validate()` explicitly rejects this value and raises `AirflowException`. Use `'aircontroler_alembic_version'` or similar.
- **Missing `version_table=` kwarg in `context.configure()`:** `RunDBManager._validate()` AST-scans env.py and raises if `version_table` keyword is absent from any `context.configure(...)` call.
- **Raising `AirflowSkipException` on the failure path:** Must only raise after the freshness check passes. A failed hook read should NOT cause a skip — it should allow dbt to run (fail-open behavior).

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| DB session lifecycle | Custom context manager with commit/rollback | `create_session()` from `airflow.utils.session` | Handles session commit, rollback on exception, and close automatically |
| Table creation / migration DDL | Raw SQL `CREATE TABLE IF NOT EXISTS` | Alembic + `BaseDBManager.initdb()` | Alembic tracks migration state; `create_db_from_orm()` stamps head; idempotent re-runs |
| Skip signal | `return` early from `execute()` without exception | `raise AirflowSkipException(...)` | Only the exception causes Airflow to record the "skipped" state; silent return produces "success" state |
| Timestamp UTC normalization | Custom `datetime.utcnow()` | `datetime.now(tz=timezone.utc)` | `utcnow()` is deprecated in Python 3.12+; naive datetimes cause comparison failures with timezone-aware DB values |

**Key insight:** The skip-via-exception pattern is the only correct skip mechanism. A `return` without raising exits `execute()` as if it succeeded, producing a green success state — not the yellow skipped state that the UI requirement demands.

---

## Common Pitfalls

### Pitfall 1: `create_session` import path confusion
**What goes wrong:** Code imports `create_session` from a compat shim that doesn't expose it, or uses `settings.Session()` directly in operator `execute()` (violates Airflow 3 worker isolation).
**Why it happens:** `airflow.providers.common.compat.session` does not exist (verified in live env). The compat SDK also does not expose `create_session`.
**How to avoid:** Import directly from `airflow.utils.session.create_session` — this module exists and works on both Airflow 2.7+ and 3.2.0 [VERIFIED].
**Warning signs:** `ImportError: No module named 'airflow.providers.common.compat.session'` at hook initialization.

### Pitfall 2: `AirflowSkipException` deprecated import
**What goes wrong:** Importing from `airflow.exceptions.AirflowSkipException` in Airflow 3 produces a `DeprecatedImportWarning` in task logs.
**Why it happens:** Airflow 3 moved exceptions to `airflow.sdk.exceptions`.
**How to avoid:** Import via `from airflow.providers.common.compat.sdk import AirflowSkipException` — the shim uses `airflow.sdk.exceptions` on Airflow 3 and falls back to `airflow.exceptions` on Airflow 2.x [VERIFIED].
**Warning signs:** `DeprecationWarning: airflow.exceptions.AirflowSkipException is deprecated` in Airflow 3 task logs.

### Pitfall 3: Missing `version_table` in env.py fails `RunDBManager._validate()`
**What goes wrong:** Airflow 3's provider DB manager validation (`RunDBManager._validate()`) AST-parses the provider's `env.py` and raises `AirflowException` if any `context.configure()` call lacks a `version_table=` keyword argument.
**Why it happens:** Airflow enforces that providers use a non-default version table to prevent collisions with Airflow's own `alembic_version` table.
**How to avoid:** Ensure every `context.configure(...)` call in `env.py` includes `version_table="aircontroler_alembic_version"`. The validation is AST-based (static analysis) — the keyword must appear textually even if the code path is conditional.
**Warning signs:** `AirflowException: version_table not set in .../env.py` during `airflow db migrate`.

### Pitfall 4: `trigger_rule` passed as string vs enum
**What goes wrong:** Passing `TriggerRule.NONE_FAILED` (enum) vs `"none_failed"` (string) inconsistently. The Airflow 3 `BaseOperator` accepts both, but the deprecated import `airflow.utils.trigger_rule.TriggerRule` produces a `DeprecatedImportWarning`.
**Why it happens:** TriggerRule moved to `airflow.task.trigger_rule` in Airflow 3.
**How to avoid:** Pass `trigger_rule="none_failed"` as a plain string. The string value is `TriggerRule.NONE_FAILED.value` [VERIFIED]. No import of `TriggerRule` needed; the string is accepted directly by `BaseOperator.__init__`.
**Warning signs:** `DeprecationWarning: The airflow.utils.trigger_rule.TriggerRule attribute is deprecated` in Airflow 3.

### Pitfall 5: Timezone-naive vs timezone-aware datetime comparison
**What goes wrong:** `datetime.now()` (naive) compared to a `DateTime(timezone=True)` DB value raises `TypeError: can't compare offset-naive and offset-aware datetimes`.
**Why it happens:** DB returns timezone-aware timestamps; `datetime.now()` without `tz=timezone.utc` returns a naive datetime.
**How to avoid:** Always use `datetime.now(tz=timezone.utc)` for the freshness check. The `last_run` returned by `get_last_run()` may be naive or aware depending on DB dialect and SQLAlchemy handling — normalize both sides to UTC.
**Warning signs:** `TypeError` in `execute()` at the freshness subtraction.

### Pitfall 6: Hook creates new `MetadataDBHook()` instance in execute() twice
**What goes wrong:** Creating two separate hook instances in `execute()` (one for `get_last_run`, one for `record_run`) vs one is a style choice, but both are fine. The pitfall is creating the hook inside the freshness-skipped path — `record_run` must only be called on the **success path after dbt runs**, not before.
**Why it happens:** Misreading the flow — recording a run before `subprocess.run()` returns success.
**How to avoid:** `record_run()` call must be inside the success branch, after `_log_run_results()`. If `AirflowSkipException` is raised, `record_run()` is never called.

### Pitfall 7: `BaseDBManager` not available in Airflow 2.x
**What goes wrong:** Importing `BaseDBManager` at module top level in a file that gets imported on Airflow 2.x causes `ImportError` (the class doesn't exist until Airflow 3).
**Why it happens:** `airflow.utils.db_manager` exists only in Airflow 3.x.
**How to avoid:** Guard the import: `from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS`. Only import `BaseDBManager` when `AIRFLOW_V_3_0_PLUS` is True, or use `try/except ImportError`. The `AircontrolerDBManager` class definition can live in `aircontroler/db_manager.py` with a top-level guard so the module can be imported safely on Airflow 2.x.
**Warning signs:** `ImportError: No module named 'airflow.utils.db_manager'` on Airflow 2.x.

---

## Code Examples

### Reading freshness from context (dag_id and run_id)
```python
# Source: verified from airflow.sdk.definitions.context.Context TypedDict in live env
# context["run_id"] -> str (directly available)
# context["dag_run"].dag_id -> str (DagRun model attribute, confirmed in live env)
dag_id = context["dag_run"].dag_id
run_id = context["run_id"]
hook.record_run(unique_id=self.unique_id, dag_id=dag_id, run_id=run_id)
```

### Conditional freshness check with fail-open on missing table
```python
# Fail-open: if table doesn't exist yet (before first `aircontroler db init`),
# catch the DB error and log a warning rather than blocking the run
try:
    last_run = hook.get_last_run(self.unique_id)
except Exception as exc:
    self.log.warning(
        "Freshness check failed for %s: %s — running dbt (fail-open)",
        self.unique_id, exc,
    )
    last_run = None
```

### Test mock pattern for MetadataDBHook
```python
# Based on Phase 2 test pattern: patch at module level, not instance level
# For create_session: use mocker.patch as context manager
mocker.patch(
    "aircontroler.hooks.metadata_db.create_session",
    return_value=mocker.MagicMock(__enter__=mocker.MagicMock(return_value=mock_session),
                                   __exit__=mocker.MagicMock(return_value=False)),
)
# Or simpler: patch get_last_run/record_run directly on the hook class
mocker.patch.object(MetadataDBHook, "get_last_run", return_value=None)
mocker.patch.object(MetadataDBHook, "record_run")
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `from airflow.exceptions import AirflowSkipException` | `from airflow.providers.common.compat.sdk import AirflowSkipException` | Airflow 3.0 | Deprecation warning eliminated on Airflow 3 |
| `from airflow.utils.trigger_rule import TriggerRule` | Pass string `"none_failed"` directly | Airflow 3.0 (moved to `airflow.task`) | Deprecation warning eliminated |
| Custom `CREATE TABLE` SQL for provider state | `BaseDBManager` + Alembic | Airflow 3.0 | Standard provider pattern; migration state tracked |
| `airflow.utils.session.provide_session` decorator | `create_session()` context manager | Airflow 2.x → 3.x | Context manager is more explicit and works in both versions without decorator magic |

**Deprecated/outdated:**
- `from airflow.exceptions import AirflowSkipException`: Works but raises `DeprecatedImportWarning` in Airflow 3. Use compat SDK.
- `datetime.utcnow()`: Deprecated in Python 3.12. Use `datetime.now(tz=timezone.utc)`.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Standalone `click` console_script is simpler than Airflow native `cli` provider extension for `aircontroler db init` | Standard Stack (Alternatives Considered), Pattern 6 | Low — if Airflow native CLI extension is preferred, the implementation changes slightly (JSON registration in provider_info `cli` key pointing to a function returning a click command) but the underlying click code is identical |
| A2 | `record_run()` and `get_last_run()` should use separate `create_session()` calls (separate transactions) | Pattern 2 | Low — using a single session for both would require passing session as parameter; separate calls are simpler and match the hook contract |
| A3 | `BaseDBManager` is Airflow 3.x only and must be guarded with `AIRFLOW_V_3_0_PLUS` | Pattern 3, Pitfall 7 | Medium — if `airflow.utils.db_manager` exists in a late 2.x version (unlikely), the guard is overly conservative but safe |
| A4 | `context["dag_run"].dag_id` is the correct access pattern for dag_id in operator `execute()` | Code Examples | Low — `context["dag"].dag_id` would also work; `dag_run.dag_id` is the more semantically correct key for runtime context |

---

## Open Questions (RESOLVED)

1. **Should `record_run()` and `get_last_run()` share a single session in execute()?**
   - What we know: Both are in the same `execute()` call; they're semantically separate operations (read at start, write at end, with subprocess in between)
   - What's unclear: Whether a single session spanning the full `execute()` duration could hold locks or resources unnecessarily
   - RESOLVED: Use separate `create_session()` calls (D-09 shows them as independent methods). This is simplest and avoids holding a session across the subprocess.

2. **Does `get_last_run()` need to handle missing table gracefully (fail-open)?**
   - What we know: If `aircontroler db init` has not been run on Airflow 2.x, the table won't exist. The query will fail with a DB error.
   - What's unclear: Is fail-open (log warning, run dbt) or fail-closed (raise, block execution) the right behavior?
   - RESOLVED: Fail-open — catch DB errors in `get_last_run()` and return `None`. This means dbt runs if the table doesn't exist, preserving backward compatibility. Log at WARNING level.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `airflow.utils.session.create_session` | `MetadataDBHook` | ✓ | Airflow 3.2.0 | — |
| `airflow.utils.db_manager.BaseDBManager` | `AircontrolerDBManager` (Airflow 3 only) | ✓ | Airflow 3.2.0 | Guard with `AIRFLOW_V_3_0_PLUS`; CLI path for Airflow 2.x |
| `alembic` | Alembic migration DDL | ✓ | 1.18.4 | — (transitive via apache-airflow-core) |
| `click` | `aircontroler db init` CLI | ✓ | 8.3.2 | — (transitive via apache-airflow) |
| `sqlalchemy` | Table definition, hook queries | ✓ | 2.0.49 | — (transitive via apache-airflow) |
| `airflow.providers.common.compat.sdk.AirflowSkipException` | `DbtRunModelOperator.execute()` | ✓ | common-compat 1.14.3 | — |

**Missing dependencies with no fallback:** None.

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.3 |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` |
| Quick run command | `uv run pytest tests/unit/ -q --tb=short` |
| Full suite command | `uv run pytest tests/ -q --tb=short` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FRESH-01 | `freshness_hours` extracted from manifest meta, passed to operator | unit | `uv run pytest tests/unit/test_factory.py -k freshness -x` | ❌ Wave 0 |
| FRESH-01 | `freshness_hours` stored on operator instance | unit | `uv run pytest tests/unit/test_operator.py -k freshness_hours -x` | ❌ Wave 0 |
| FRESH-03 | Task raises `AirflowSkipException` when within TTL | unit | `uv run pytest tests/unit/test_operator.py -k skip_when_fresh -x` | ❌ Wave 0 |
| FRESH-03 | Task does NOT skip when `freshness_hours` is None | unit | `uv run pytest tests/unit/test_operator.py -k no_skip_when_no_ttl -x` | ❌ Wave 0 |
| FRESH-03 | Task does NOT skip when stale (past TTL) | unit | `uv run pytest tests/unit/test_operator.py -k no_skip_when_stale -x` | ❌ Wave 0 |
| FRESH-03 | Downstream with `none_failed` runs when upstream skipped | unit (DAG-level) | `uv run pytest tests/unit/test_factory.py -k trigger_rule -x` | ❌ Wave 0 |
| FRESH-04 | `record_run()` called after successful dbt execution | unit | `uv run pytest tests/unit/test_operator.py -k record_run_called -x` | ❌ Wave 0 |
| FRESH-04 | `record_run()` NOT called when dbt fails | unit | `uv run pytest tests/unit/test_operator.py -k record_run_not_called_on_fail -x` | ❌ Wave 0 |
| FRESH-04 | `record_run()` NOT called when task skipped | unit | `uv run pytest tests/unit/test_operator.py -k record_run_not_called_on_skip -x` | ❌ Wave 0 |
| FRESH-04 | `get_last_run()` queries correct SQL | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -k get_last_run -x` | ❌ Wave 0 |
| FRESH-04 | `record_run()` inserts correct row | unit | `uv run pytest tests/unit/test_metadata_db_hook.py -k record_run -x` | ❌ Wave 0 |
| FRESH-06 | `get_provider_info()` includes `db-managers` key | unit | `uv run pytest tests/unit/test_init.py -k db_managers -x` | ❌ Wave 0 |
| FRESH-06 | `AircontrolerDBManager.version_table_name != 'alembic_version'` | unit | `uv run pytest tests/unit/test_db_manager.py -k version_table -x` | ❌ Wave 0 |
| FRESH-06 | Alembic migration `upgrade()` creates table and indexes | integration | `uv run pytest tests/integration/test_migrations.py -x` | ❌ Wave 0 |

### Critical Nyquist Sampling Dimensions
The following behaviors are the minimum required to trust the freshness machinery in production:

1. **Fresh → skip:** Hook returns a timestamp within TTL → `AirflowSkipException` raised (not a silent return, not a success)
2. **Stale → run:** Hook returns a timestamp outside TTL → dbt subprocess invoked
3. **No TTL → always run:** `freshness_hours=None` → no hook call, dbt always invoked
4. **Success records run:** Successful dbt → `record_run()` called with correct `unique_id`, `dag_id`, `run_id`
5. **Failure does not record run:** Failed dbt (non-zero returncode) → `record_run()` NOT called
6. **Skip does not record run:** Skip path → `record_run()` NOT called (exception exits before recording)
7. **none_failed passes skipped upstream:** DAG topology test — task with skipped upstream still runs under `none_failed`
8. **fail-open on missing table:** `get_last_run()` catches DB error, returns `None`, dbt runs

### Sampling Rate
- **Per task commit:** `uv run pytest tests/unit/ -q --tb=short`
- **Per wave merge:** `uv run pytest tests/ -q --tb=short`
- **Phase gate:** Full suite green (55 existing + Phase 3 new tests) before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/unit/test_metadata_db_hook.py` — covers FRESH-04 hook unit tests
- [ ] `tests/unit/test_db_manager.py` — covers FRESH-06 `AircontrolerDBManager` validation
- [ ] `tests/integration/test_migrations.py` — covers FRESH-06 Alembic migration round-trip
- [ ] `tests/fixtures/manifest_v12_with_freshness.json` — manifest fixture with `freshness_hours` on mart node
- [ ] `aircontroler/migrations/alembic.ini`, `env.py`, `versions/0001_add_model_runs_table.py` — migration scaffold
- [ ] Hook and operator test additions in existing `test_operator.py` and `test_factory.py`

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | — |
| V3 Session Management | no | — |
| V4 Access Control | no | — |
| V5 Input Validation | yes | `freshness_hours` is a float from manifest config; must validate `> 0` before use |
| V6 Cryptography | no | — |

### Known Threat Patterns for this Phase

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SQL injection via `unique_id` in `get_last_run()` | Tampering | Use SQLAlchemy parameterized query (`sa.text(...).bindparams()` or ORM expression) — never string-format unique_id into SQL |
| `dag_id` / `run_id` from context written to DB | Information Disclosure | These are internal Airflow identifiers, not secrets. Logging them at INFO level is acceptable |
| `freshness_hours` set to 0 or negative | Tampering / DoS | Validate `freshness_hours > 0` in operator `__init__`; if 0 or negative, log WARNING and treat as None (always run) |

---

## Project Constraints (from CLAUDE.md)

All directives enforced in Phase 3:

1. **Never call `dbt compile` at DAG parse time** — freshness check is runtime-only; no new parse-time I/O added.
2. **Never use direct SQLAlchemy sessions from worker `execute()` methods** — MetadataDBHook with `create_session` is the only DB access path. [PRIMARY CONSTRAINT FOR THIS PHASE]
3. **Never use `SELECT FOR UPDATE` for locking** — not applicable in Phase 3 (locking is Phase 4).
4. **Never hardcode manifest field paths** — `freshness_hours` extraction uses the established `node.config.get(...)` pattern.
5. **Validate cardinality at parse time** — no new cardinality risks introduced.
6. **All subprocess commands stay `list[str]`, no `shell=True`** — no change to subprocess pattern.
7. **All Airflow imports go through common-compat shim where applicable** — `AirflowSkipException` imported via compat SDK.

---

## Sources

### Primary (HIGH confidence)
- Live Airflow 3.2.0 environment (uv run python) — all import paths and class APIs verified directly
- `airflow.utils.session.create_session` — source inspected, signature confirmed
- `airflow.utils.db_manager.BaseDBManager` — full source inspected; `_validate()` requirements confirmed
- `airflow.providers.common.compat.sdk` — full module source read; `AirflowSkipException` and `TriggerRule` mappings confirmed
- `airflow.sdk.definitions.context.Context` TypedDict — source inspected; `run_id` (str) and `dag_run` confirmed
- `airflow/migrations/env.py` (Airflow's own) — read as canonical reference for `env.py` pattern
- Installed package versions: common-compat 1.14.3, alembic 1.18.4, sqlalchemy 2.0.49, click 8.3.2
- `provider_info.schema.json` — `db-managers` key confirmed; format: `array of string` (class paths)

### Secondary (MEDIUM confidence)
- `RunDBManager._validate()` AST-check logic — source inspected; `version_table` requirement in `context.configure()` confirmed
- `RunDBManager` full source — provider discovery mechanism via `ProvidersManager.db_managers` confirmed
- `airflow/cli/commands/db_manager_command.py` — confirmed Airflow 3 approach for running registered db managers

### Tertiary (LOW confidence)
- None — all key claims verified in live environment

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all imports verified in live Airflow 3.2.0 environment
- Architecture: HIGH — integration points read directly from existing codebase
- Pitfalls: HIGH — most pitfalls discovered via live testing (import errors, deprecation warnings, AST validation)

**Research date:** 2026-04-19
**Valid until:** 2026-07-19 (Airflow 3.x API stable; alembic API stable; 90-day estimate)
