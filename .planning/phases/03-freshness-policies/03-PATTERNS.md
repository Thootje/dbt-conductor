# Phase 3: Freshness Policies - Pattern Map

**Mapped:** 2026-04-19
**Files analyzed:** 13
**Analogs found:** 10 / 13

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `aircontroler/hooks/metadata_db.py` | hook | request-response | `aircontroler/hooks/metadata_db.py` (stub) + `airflow/utils/session.py` | exact (stub → full) |
| `aircontroler/db_manager.py` | utility / config | CRUD | `airflow/utils/db_manager.py` (BaseDBManager) | role-match (subclass target) |
| `aircontroler/cli.py` | utility | request-response | `aircontroler/__init__.py` (entry-point pattern) | partial |
| `aircontroler/migrations/alembic.ini` | config | — | none in codebase | no analog |
| `aircontroler/migrations/env.py` | config | — | none in codebase | no analog |
| `aircontroler/migrations/versions/0001_add_model_runs_table.py` | migration | CRUD | none in codebase | no analog |
| `aircontroler/operator.py` | operator | request-response | `aircontroler/operator.py` (existing) | exact (modify) |
| `aircontroler/factory.py` | factory | request-response | `aircontroler/factory.py` (existing) | exact (modify) |
| `aircontroler/__init__.py` | config | — | `aircontroler/__init__.py` (existing) | exact (modify) |
| `tests/unit/test_metadata_db_hook.py` | test | — | `tests/unit/test_operator.py` | role-match |
| `tests/unit/test_db_manager.py` | test | — | `tests/unit/test_init.py` | role-match |
| `tests/integration/test_migrations.py` | test | — | `tests/integration/test_dag_wiring.py` | role-match |
| `tests/fixtures/manifest_v12_with_freshness.json` | fixture | — | `tests/fixtures/manifest_v12_with_meta.json` | exact |

---

## Pattern Assignments

### `aircontroler/hooks/metadata_db.py` (hook, request-response)

**Analog:** `aircontroler/hooks/metadata_db.py` (existing Phase 1 stub)

**Imports pattern** (existing stub, lines 1-9):
```python
"""MetadataDBHook — Phase 1 stub for Airflow metadata DB access."""
from __future__ import annotations

from airflow.hooks.base import BaseHook
```

**Full implementation imports** (from RESEARCH.md Pattern 2, verified in live env):
```python
from __future__ import annotations
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session   # NOT from compat shim — direct import
import sqlalchemy as sa
```

**Class header pattern** (existing stub, lines 12-22 — PRESERVE these four class attributes unchanged):
```python
class MetadataDBHook(BaseHook):
    conn_name_attr = "aircontroler_conn_id"
    default_conn_name = "aircontroler_default"
    conn_type = "aircontroler"
    hook_name = "Aircontroler"
```

**get_conn replacement** — replace the existing `get_conn()` stub body with:
```python
    def get_conn(self):
        raise NotImplementedError("MetadataDBHook uses create_session, not get_conn()")
```

**Core pattern — get_last_run** (from RESEARCH.md Pattern 2):
```python
    def get_last_run(self, unique_id: str) -> datetime | None:
        from aircontroler.db_manager import model_runs_table
        with create_session() as session:
            result = session.execute(
                sa.select(sa.func.max(model_runs_table.c.completed_at))
                .where(model_runs_table.c.unique_id == unique_id)
            ).scalar()
        return result
```

**Core pattern — record_run** (from RESEARCH.md Pattern 2):
```python
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

**Fail-open error handling** (from RESEARCH.md Code Examples — applied in operator, not hook):
```python
# In operator.execute(), wrap get_last_run() call:
try:
    last_run = hook.get_last_run(self.unique_id)
except Exception as exc:
    self.log.warning(
        "Freshness check failed for %s: %s — running dbt (fail-open)",
        self.unique_id, exc,
    )
    last_run = None
```

**Critical import constraint:** `create_session` comes from `airflow.utils.session` directly — NOT from any compat shim. `airflow.providers.common.compat.session` does not exist (verified in live env).

---

### `aircontroler/db_manager.py` (utility, CRUD)

**Analog:** `airflow/utils/db_manager.py` (BaseDBManager in live env at `.venv/lib/python3.13/site-packages/airflow/utils/db_manager.py`)

**Imports pattern** (from RESEARCH.md Pattern 3, verified against live BaseDBManager source):
```python
from __future__ import annotations
import os
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, Index

try:
    from airflow.utils.db_manager import BaseDBManager
    _AIRFLOW3 = True
except ImportError:
    BaseDBManager = object   # Airflow 2.x fallback — class defined but not registered
    _AIRFLOW3 = False
```

**Table definition pattern** (from RESEARCH.md Pattern 3):
```python
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
```

**BaseDBManager subclass pattern** (from RESEARCH.md Pattern 3, verified against live source lines 53-63):
```python
class AircontrolerDBManager(BaseDBManager):
    metadata = metadata
    migration_dir = os.path.join(os.path.dirname(__file__), "migrations")
    alembic_file = os.path.join(os.path.dirname(__file__), "migrations", "alembic.ini")
    version_table_name = "aircontroler_alembic_version"   # MUST NOT be 'alembic_version'
    supports_table_dropping = True
    revision_heads_map = {"1.0.0": "0001"}
```

**Critical constraints from live BaseDBManager source (lines 246-258):**
- `RunDBManager._validate()` AST-walks `env.py` checking for `context.configure(...)` calls — each one must include a `version_table=` keyword.
- `version_table_name` must NOT equal `"alembic_version"` (line 258 raises `AirflowException`).
- The `AircontrolerDBManager` class is instantiated with `(session)` — constructor matches `BaseDBManager.__init__(self, session)` (line 64).

---

### `aircontroler/cli.py` (utility, request-response)

**Analog:** `aircontroler/__init__.py` (entry-point pattern) + pyproject.toml `[project.entry-points]` section

**Existing entry-points pattern** (`pyproject.toml` lines 41-42):
```toml
[project.entry-points."apache_airflow_provider"]
provider_info = "aircontroler:get_provider_info"
```

**New console_scripts entry point to add** (`pyproject.toml`):
```toml
[project.scripts]
aircontroler = "aircontroler.cli:cli"
```

**CLI implementation pattern** (from RESEARCH.md Pattern 6):
```python
from __future__ import annotations
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

**Module-level import avoidance:** All Airflow imports are deferred inside the command function body — this allows `import aircontroler.cli` to succeed even without a full Airflow context (e.g., `aircontroler --help`).

---

### `aircontroler/migrations/alembic.ini` (config)

**No analog in codebase.** Use minimal Alembic config pointing script_location to the migrations directory.

**Minimal required content** (from RESEARCH.md Pattern 4):
```ini
[alembic]
script_location = .
sqlalchemy.url =
```

The `sqlalchemy.url` is intentionally empty — it is set programmatically by `BaseDBManager.get_alembic_config()` (live source line 91: `config.set_main_option("sqlalchemy.url", settings.SQL_ALCHEMY_CONN)`). Do not hardcode a DB URL.

---

### `aircontroler/migrations/env.py` (config)

**No analog in codebase.** Pattern comes entirely from RESEARCH.md Pattern 4 and from the AST validation constraints in the live `RunDBManager._validate()` source (`.venv/lib/python3.13/site-packages/airflow/utils/db_manager.py` lines 244-258).

**Critical constraint:** Every `context.configure(...)` call must include `version_table="aircontroler_alembic_version"` as a keyword argument — this is AST-checked statically, not at runtime.

**Core pattern** (from RESEARCH.md Pattern 4):
```python
from __future__ import annotations
from alembic import context

def run_migrations_online():
    conn = context.config.attributes.get("connection", None)
    if conn is None:
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

**Anti-pattern:** Do NOT `import airflow.settings` at module top level — causes import-time failure when env.py is loaded by the alembic CLI without Airflow initialized.

---

### `aircontroler/migrations/versions/0001_add_model_runs_table.py` (migration)

**No analog in codebase.**

**Core pattern** (from RESEARCH.md Pattern 5):
```python
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

---

### `aircontroler/operator.py` (operator, request-response) — MODIFY

**Analog:** `aircontroler/operator.py` (existing, lines 1-164)

**`__init__` signature extension pattern** (copy from existing lines 42-72 and add two params):

The existing conditional-injection pattern (lines 57-67) for `retries`, `retry_delay`, `pool` is the direct model. Apply it to `trigger_rule` via a different mechanism: `trigger_rule` should be passed directly to `kwargs` because it is a standard `BaseOperator` param (unlike custom `freshness_hours`). `freshness_hours` is stored as instance attribute.

```python
def __init__(
    self,
    *,
    unique_id: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str = "dbt",
    target_path: str | None = None,
    retries: int | None = None,
    retry_delay: int | None = None,
    pool: str | None = None,
    freshness_hours: float | None = None,   # NEW Phase 3
    trigger_rule: str = "none_failed",      # NEW Phase 3
    **kwargs,
) -> None:
    from datetime import timedelta
    if retries is not None:
        kwargs["retries"] = retries
    if retry_delay is not None:
        kwargs["retry_delay"] = timedelta(seconds=retry_delay)
    if pool is not None:
        kwargs["pool"] = pool
    kwargs["trigger_rule"] = trigger_rule   # Always pass — has a non-None default
    super().__init__(**kwargs)
    self.unique_id = unique_id
    self.project_dir = project_dir
    self.profiles_path = profiles_path
    self.dbt_executable_path = dbt_executable_path
    self.target_path = target_path
    # Phase 3: validate freshness_hours > 0 (security: reject 0/negative)
    if freshness_hours is not None and freshness_hours <= 0:
        import logging
        logging.getLogger(__name__).warning(
            "freshness_hours=%s is not positive for %s — treating as None (always run)",
            freshness_hours, unique_id,
        )
        freshness_hours = None
    self.freshness_hours = freshness_hours
```

**`execute()` freshness check insertion** (from RESEARCH.md Pattern 1 — insert at TOP of execute(), before `cmd` construction):
```python
def execute(self, context: dict) -> None:
    # Phase 3: freshness TTL check (before subprocess)
    if self.freshness_hours is not None:
        from aircontroler.hooks.metadata_db import MetadataDBHook
        from airflow.providers.common.compat.sdk import AirflowSkipException
        hook = MetadataDBHook()
        try:
            last_run = hook.get_last_run(self.unique_id)
        except Exception as exc:
            self.log.warning(
                "Freshness check failed for %s: %s — running dbt (fail-open)",
                self.unique_id, exc,
            )
            last_run = None
        if last_run is not None:
            from datetime import datetime, timezone
            # Normalize both sides to UTC to avoid TypeError (RESEARCH.md Pitfall 5)
            now = datetime.now(tz=timezone.utc)
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=timezone.utc)
            elapsed_hours = (now - last_run).total_seconds() / 3600
            if elapsed_hours < self.freshness_hours:
                self.log.info(
                    "Skipping %s: last run %.2f hours ago (TTL: %.1f hours)",
                    self.unique_id, elapsed_hours, self.freshness_hours,
                )
                raise AirflowSkipException(
                    f"{self.unique_id} is fresh "
                    f"(last ran {elapsed_hours:.2f}h ago, TTL {self.freshness_hours}h)"
                )
    # ... existing subprocess logic unchanged ...
    # Phase 3: record successful run (after _log_run_results(), on success path only)
    self._log_run_results()
    if self.freshness_hours is not None or True:  # record unconditionally on success
        from aircontroler.hooks.metadata_db import MetadataDBHook
        hook = MetadataDBHook()
        hook.record_run(
            unique_id=self.unique_id,
            dag_id=context["dag_run"].dag_id,
            run_id=context["run_id"],
        )
```

**`template_fields` extension** — add `"freshness_hours"` and `"trigger_rule"` if they should be templatable (they are plain scalars; adding them follows the existing pattern on line 40).

**Existing logging pattern** (lines 93-110) — unchanged; `self.log.info(...)` / `self.log.warning(...)` / `self.log.error(...)`.

**Existing error-raise pattern** (lines 100-107) — unchanged:
```python
if result.returncode != 0:
    if result.stdout:
        self.log.error("dbt stdout:\n%s", result.stdout)
    self.log.error("dbt stderr:\n%s", result.stderr)
    raise AirflowException(
        f"dbt build failed for {self.unique_id!r} "
        f"(exit {result.returncode})"
    )
```

---

### `aircontroler/factory.py` (factory, request-response) — MODIFY

**Analog:** `aircontroler/factory.py` (existing, lines 1-201)

**Function signature extension pattern** (existing lines 37-50; add `trigger_rule` param):
```python
def build_dbt_dag(
    dag: "DAG",
    manifest_path: str,
    dbt_command: str,
    project_dir: str,
    profiles_path: str,
    dbt_executable_path: str | None = None,
    service_account: str | None = None,
    cardinality_limit: int | None = None,
    max_active_tasks: int | None = None,
    pool: str | None = None,
    pool_slots: int = 1,
    target_path: str | None = None,
    trigger_rule: str = "none_failed",   # NEW Phase 3
) -> dict:
```

**Per-model meta extraction extension** (existing lines 143-158; extend with freshness_hours and trigger_rule):

The existing pattern (line 144) is:
```python
ac_meta = node.config.get("meta", {}).get("aircontroler", {})
task_pool = ac_meta.get("pool") or pool or None
```

Extend with (same `or` short-circuit idiom):
```python
ac_meta = node.config.get("meta", {}).get("aircontroler", {})
task_pool = ac_meta.get("pool") or pool or None
task_trigger_rule = ac_meta.get("trigger_rule") or trigger_rule   # NEW
```

**Operator constructor extension** (existing lines 146-158):
```python
tasks[uid] = DbtRunModelOperator(
    task_id=uid,
    dag=dag,
    unique_id=uid,
    project_dir=project_dir,
    profiles_path=profiles_path,
    dbt_executable_path=executable,
    target_path=target_path,
    retries=ac_meta.get("retries"),
    retry_delay=ac_meta.get("retry_delay"),
    pool=task_pool,
    pool_slots=pool_slots if task_pool else 1,
    freshness_hours=ac_meta.get("freshness_hours"),   # NEW
    trigger_rule=task_trigger_rule,                    # NEW
)
```

**Existing warning pattern** (lines 83-89) — unchanged; `log.warning(...)` for stub params.

---

### `aircontroler/__init__.py` (config) — MODIFY

**Analog:** `aircontroler/__init__.py` (existing, lines 1-21)

**Existing get_provider_info() pattern** (lines 7-21):
```python
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

**Extension — add db-managers key** (from RESEARCH.md Pattern 3 registration block):
```python
def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aircontroler",
        "name": "aircontroler",
        "description": (
            "Map each dbt model to a native Airflow task with "
            "accurate upstream/downstream wiring."
        ),
        "versions": [__version__],
        "db-managers": ["aircontroler.db_manager.AircontrolerDBManager"],   # NEW
    }
```

**Key format:** `"db-managers"` (hyphen, not underscore) — confirmed from `provider_info.schema.json` in RESEARCH.md sources. Value is a list of dotted-path strings.

---

### `tests/unit/test_metadata_db_hook.py` (test)

**Analog:** `tests/unit/test_operator.py` (closest structural match)

**File header pattern** (from `test_operator.py` lines 1-4):
```python
"""Unit tests for aircontroler.hooks.metadata_db — FRESH-04."""
from __future__ import annotations
import pytest
```

**Mock pattern for create_session** (from RESEARCH.md Code Examples — mock at module level):
```python
# Option A: patch get_last_run/record_run directly (simplest)
mocker.patch.object(MetadataDBHook, "get_last_run", return_value=None)
mocker.patch.object(MetadataDBHook, "record_run")

# Option B: patch create_session as context manager
mock_session = mocker.MagicMock()
mock_cm = mocker.MagicMock()
mock_cm.__enter__ = mocker.MagicMock(return_value=mock_session)
mock_cm.__exit__ = mocker.MagicMock(return_value=False)
mocker.patch("aircontroler.hooks.metadata_db.create_session", return_value=mock_cm)
```

**Test structure pattern** (from `test_operator.py` test function style — inline imports, direct assertions):
```python
def test_get_last_run_returns_none_when_no_rows(mocker):
    """FRESH-04: get_last_run returns None when no matching rows."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.execute.return_value.scalar.return_value = None
    mock_cm = mocker.MagicMock(__enter__=mocker.MagicMock(return_value=mock_session),
                                __exit__=mocker.MagicMock(return_value=False))
    mocker.patch("aircontroler.hooks.metadata_db.create_session", return_value=mock_cm)
    hook = MetadataDBHook()
    result = hook.get_last_run("model.proj.stg_a")
    assert result is None
```

**pytest.raises pattern** (from `test_operator.py` lines 79-83):
```python
with pytest.raises(AirflowSkipException) as exc_info:
    op.execute(context)
assert "model.proj.stg_a" in str(exc_info.value)
```

---

### `tests/unit/test_db_manager.py` (test)

**Analog:** `tests/unit/test_init.py` (same style — simple attribute/dict assertions, no fixtures)

**File header and import style** (from `test_init.py` lines 1-4):
```python
"""Unit tests for aircontroler.db_manager — FRESH-06."""
from __future__ import annotations
```

**Attribute assertion pattern** (from `test_init.py` lines 5-16):
```python
def test_version_table_not_alembic_version():
    """FRESH-06: version_table_name must not be 'alembic_version' (RunDBManager._validate() check)."""
    from aircontroler.db_manager import AircontrolerDBManager
    assert AircontrolerDBManager.version_table_name != "alembic_version"

def test_db_managers_key_in_provider_info():
    """FRESH-06: get_provider_info() includes db-managers key pointing to AircontrolerDBManager."""
    from aircontroler import get_provider_info
    info = get_provider_info()
    assert "db-managers" in info
    assert "aircontroler.db_manager.AircontrolerDBManager" in info["db-managers"]
```

---

### `tests/integration/test_migrations.py` (test, integration)

**Analog:** `tests/integration/test_dag_wiring.py` (structure, pytest.mark.integration, pytestmark, skip pattern)

**File header and mark pattern** (from `test_dag_wiring.py` lines 1-21):
```python
"""Integration tests for Alembic migration round-trip. Covers FRESH-06."""
from __future__ import annotations
import pytest

pytestmark = pytest.mark.integration
```

**Skip pattern** (from `test_dag_wiring.py` lines 23-26):
```python
# Integration tests use tmp_path (SQLite) for the round-trip; no dbt dependency
# Skip if Airflow metadata DB is not accessible (use SQLite in-memory via tmp_path)
```

**In-memory SQLite test pattern** (integration test should create a fresh SQLite DB, not depend on live Airflow DB):
```python
def test_migration_upgrade_creates_table(tmp_path):
    """FRESH-06: Alembic upgrade() creates aircontroler_model_runs with expected columns."""
    from sqlalchemy import create_engine, inspect
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    # Run migration via alembic op directly (not via BaseDBManager — avoid Airflow dep)
    from alembic.config import Config
    from alembic import command
    import os
    migrations_dir = os.path.join(
        os.path.dirname(__file__), "..", "..", "aircontroler", "migrations"
    )
    cfg = Config()
    cfg.set_main_option("script_location", migrations_dir)
    cfg.set_main_option("sqlalchemy.url", str(engine.url))
    command.upgrade(cfg, "head")
    insp = inspect(engine)
    assert "aircontroler_model_runs" in insp.get_table_names()
    cols = {c["name"] for c in insp.get_columns("aircontroler_model_runs")}
    assert cols == {"id", "unique_id", "dag_id", "run_id", "completed_at"}
```

---

### `tests/fixtures/manifest_v12_with_freshness.json` (fixture)

**Analog:** `tests/fixtures/manifest_v12_with_meta.json` (lines 1-113 — identical structure, different meta keys)

**Extension pattern** — copy `manifest_v12_with_meta.json` verbatim and modify the `mart` node's `config` block to add `freshness_hours` alongside the existing Phase 2 meta keys:

Existing `mart` config in `manifest_v12_with_meta.json` (lines 95-107):
```json
"config": {
  "materialized": "table",
  "meta": {
    "aircontroler": {
      "retries": 3,
      "retry_delay": 300,
      "pool": "heavy_models"
    }
  }
}
```

New `mart` config with freshness (keep existing Phase 2 keys, add `freshness_hours`):
```json
"config": {
  "materialized": "table",
  "meta": {
    "aircontroler": {
      "retries": 3,
      "retry_delay": 300,
      "pool": "heavy_models",
      "freshness_hours": 24
    }
  }
}
```

For the freshness scenario tests, add `freshness_hours` to `stg_a` as well (entry node with short TTL for skip testing):
```json
"config": {
  "materialized": "view",
  "meta": {
    "aircontroler": {
      "freshness_hours": 6
    }
  }
}
```

The full fixture must preserve all five nodes (`stg_a`, `stg_b`, `int_a`, `int_b`, `mart`) and the same `metadata` block as `manifest_v12_with_meta.json`.

---

## Shared Patterns

### `from __future__ import annotations`
**Source:** Every existing source file (operator.py line 11, factory.py line 21, etc.)
**Apply to:** All new Python files — this is a project-wide convention.

### Conditional kwargs injection (non-None values only)
**Source:** `aircontroler/operator.py` lines 57-67
```python
if retries is not None:
    kwargs["retries"] = retries
if retry_delay is not None:
    kwargs["retry_delay"] = timedelta(seconds=retry_delay)
if pool is not None:
    kwargs["pool"] = pool
```
**Apply to:** `operator.py` `__init__` for `freshness_hours` (store as instance attr, not forwarded to BaseOperator). `trigger_rule` is forwarded unconditionally (it has a non-None default).

### Compat shim import for Airflow-version-portable symbols
**Source:** `aircontroler/operator.py` line 16
```python
from airflow.providers.common.compat.sdk import BaseOperator
```
**Apply to:** `operator.py` for `AirflowSkipException` — use `from airflow.providers.common.compat.sdk import AirflowSkipException`.
**Do NOT apply to:** `create_session` — import directly from `airflow.utils.session` (compat shim does not expose it).

### `self.log.info/warning/error` in operators
**Source:** `aircontroler/operator.py` lines 93, 101-103, 110, 129, 133, 137
**Apply to:** All new logging in `operator.py` freshness check code.

### Inline imports inside functions (deferred imports)
**Source:** `aircontroler/operator.py` lines 55, 127-128 (`from pathlib import Path` inside `_run_results_path`)
**Apply to:** `cli.py` (all Airflow imports deferred inside `db_init()` function body), `db_manager.py` for the `BaseDBManager` import guard pattern.

### pytest fixture loading pattern (JSON files)
**Source:** `tests/conftest.py` lines 13-34
```python
FIXTURES_DIR = Path(__file__).parent / "fixtures"

@pytest.fixture
def manifest_v12_with_freshness_data() -> dict:
    """Load the v12 manifest fixture with freshness_hours."""
    return json.loads((FIXTURES_DIR / "manifest_v12_with_freshness.json").read_text())
```
**Apply to:** `tests/conftest.py` — add `manifest_v12_with_freshness_data` fixture following the exact same pattern as `manifest_with_meta_data` (line 32-34).

### Mock log helper (`_make_mock_log`)
**Source:** `tests/unit/test_operator.py` lines 201-216
```python
def _make_mock_log(mocker):
    from aircontroler.operator import DbtRunModelOperator
    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log", new_callable=mocker.PropertyMock, return_value=mock_log
    )
    return mock_log
```
**Apply to:** `test_operator.py` additions for freshness tests — reuse the existing `_make_mock_log` helper already in the file. For `test_metadata_db_hook.py`, no log patching needed (hook uses `create_session`, not `self.log`).

### `mocker.patch` at module level (not instance level)
**Source:** `tests/unit/test_operator.py` line 23
```python
mock_run = mocker.patch("aircontroler.operator.subprocess.run")
```
**Apply to:** `test_metadata_db_hook.py` — patch `create_session` at the module where it is used: `"aircontroler.hooks.metadata_db.create_session"`.

### `pytestmark = pytest.mark.integration`
**Source:** `tests/integration/test_dag_wiring.py` line 18
```python
pytestmark = pytest.mark.integration
```
**Apply to:** `tests/integration/test_migrations.py` — same module-level mark.

---

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `aircontroler/migrations/alembic.ini` | config | — | No Alembic config files exist in the project codebase |
| `aircontroler/migrations/env.py` | config | — | No Alembic env.py exists in the project codebase |
| `aircontroler/migrations/versions/0001_add_model_runs_table.py` | migration | CRUD | No Alembic migration revision files exist in the project codebase |

For all three, the patterns come exclusively from RESEARCH.md Patterns 4 and 5 (verified against live Airflow 3.2.0 env and `RunDBManager._validate()` source).

---

## Metadata

**Analog search scope:** `/workspace/aircontroler/aircontroler/`, `/workspace/aircontroler/tests/`, `.venv/lib/python3.13/site-packages/airflow/utils/db_manager.py`
**Files scanned:** 14 source files + 1 venv file
**Pattern extraction date:** 2026-04-19
