"""build_dbt_dag() — public factory that renders a dbt project as an Airflow DAG.

Parse-time only: called when the DAG file is imported by the Airflow scheduler.
Never performs I/O at task runtime.

Usage:
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag

    with DAG("my_dag", schedule="@daily") as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path="/path/to/target/manifest.json",
            dbt_command="dbt build --select tag:daily+",
            project_dir="/path/to/dbt_project",
            profiles_path="/path/to/profiles",
        )
        start >> result["entry_tasks"]
        result["exit_tasks"] >> notify
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from aircontroler.manifest import ManifestNode, parse_manifest
from aircontroler.operator import DbtRunModelOperator
from aircontroler.selector import SelectorResolver

if TYPE_CHECKING:
    from airflow.models import DAG

log = logging.getLogger(__name__)


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
) -> dict:
    """Render a dbt project as an Airflow DAG with one task per resolved model.

    Mutates the user-supplied DAG by adding one DbtRunModelOperator per resolved
    dbt model. Task IDs equal dbt model unique_ids. Edges mirror dbt lineage.

    Args:
        dag: The Airflow DAG to populate. Create it yourself and pass it in.
        manifest_path: Absolute path to a pre-built manifest.json. Use
            ManifestLoader(compile=True) if you need compile-on-parse behavior.
        dbt_command: Full dbt command string, e.g.
            'dbt build --select tag:daily+ --exclude raw_* --profile prod'.
            The factory extracts --select/--exclude/--selector flags to pass
            to 'dbt ls' for graph resolution. Per D-05, --project-dir and
            --profiles-dir are NOT extracted from this string.
        project_dir: Absolute path to the dbt project directory.
        profiles_path: Path to the directory containing profiles.yml.
        dbt_executable_path: Path to the dbt executable. Defaults to 'dbt'.
        service_account: GCP service account for per-model identity.
            Phase 1 stub: logs WARNING and continues (D-11).

    Returns:
        dict with keys:
            - 'entry_tasks': list of DbtRunModelOperator with no resolved upstream
            - 'exit_tasks': list of DbtRunModelOperator with no resolved downstream
            - 'tasks': dict[unique_id, DbtRunModelOperator] for all resolved models

    Security:
        - dbt_command is only used for selector flag extraction (T-01-04-01)
        - service_account repr-logged (email address, not a secret) (T-01-04-03)
        - cardinality guard deferred to Phase 2 (T-01-04-02 accepted risk)
    """
    # D-11: service_account is a Phase 1 stub — log and continue, do not crash
    if service_account is not None:
        log.warning(
            "service_account is not yet implemented — "
            "will be supported in a future release. "
            "Provided value %r will be ignored.",
            service_account,
        )

    executable = dbt_executable_path or "dbt"

    # --- Parse-time step 1: Load and parse the manifest ---
    all_nodes: list[ManifestNode] = parse_manifest(manifest_path)
    all_node_map: dict[str, ManifestNode] = {n.unique_id: n for n in all_nodes}

    # --- Parse-time step 2: Resolve the selector via dbt ls ---
    resolver = SelectorResolver(dbt_executable=executable)
    resolved_ids: set[str] = resolver.resolve(
        dbt_command=dbt_command,
        project_dir=project_dir,
        profiles_dir=profiles_path,
    )

    # EXEC-03: cardinality guard at DAG parse time.
    # Precedence: kwarg > env var > hardcoded default 800.
    effective_limit: int = (
        cardinality_limit
        if cardinality_limit is not None
        else int(os.environ.get("AIRCONTROLER_MAX_MODELS", 800))
    )
    if len(resolved_ids) > effective_limit:
        raise ValueError(
            f"Selector resolved to {len(resolved_ids):,} models "
            f"(limit: {effective_limit:,}). "
            "Narrow your --select filter or raise cardinality_limit=N in build_dbt_dag()."
        )

    # EXEC-04: DAG-level concurrency control.
    # Only mutate when explicitly set; None preserves Airflow's default (16).
    if max_active_tasks is not None:
        dag.max_active_tasks = max_active_tasks

    # --- Parse-time step 3: Filter manifest to resolved nodes ---
    node_map: dict[str, ManifestNode] = {
        uid: node
        for uid, node in all_node_map.items()
        if uid in resolved_ids
    }

    # --- Parse-time step 4: Static loop — one operator per resolved node ---
    # Never uses .expand() — preserves per-model graph wiring and lineage (D-06 key decision)
    tasks: dict[str, DbtRunModelOperator] = {}
    for uid, node in node_map.items():
        # EXEC-01: per-model overrides from meta.aircontroler in schema.yml.
        # Unrecognized keys (e.g., 'freshness' reserved for Phase 3) are
        # silently ignored — only the known keys are extracted via .get().
        ac_meta = node.config.get("meta", {}).get("aircontroler", {})
        # D-16: per-model pool overrides DAG-level pool.
        task_pool = ac_meta.get("pool") or pool or None
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
        )

    # --- Parse-time step 5: Wire edges from manifest depends_on ---
    # Only wire edges between tasks that are both in the resolved set
    for uid, node in node_map.items():
        for dep_uid in node.depends_on:
            if dep_uid in tasks:
                tasks[dep_uid].set_downstream(tasks[uid])

    # --- Parse-time step 6: Classify entry and exit tasks (set-difference approach) ---
    # entry_tasks: tasks where none of their depends_on are in the resolved set
    entry_tasks: list[DbtRunModelOperator] = [
        tasks[uid]
        for uid, node in node_map.items()
        if not any(dep in tasks for dep in node.depends_on)
    ]

    # exit_tasks: tasks that are not in any other resolved node's depends_on
    all_resolved_deps: set[str] = {
        dep
        for node in node_map.values()
        for dep in node.depends_on
        if dep in tasks
    }
    exit_tasks: list[DbtRunModelOperator] = [
        tasks[uid]
        for uid in node_map
        if uid not in all_resolved_deps
    ]

    log.info(
        "build_dbt_dag: %d tasks, %d entry, %d exit (DAG: %s)",
        len(tasks),
        len(entry_tasks),
        len(exit_tasks),
        dag.dag_id,
    )

    return {
        "entry_tasks": entry_tasks,
        "exit_tasks": exit_tasks,
        "tasks": tasks,
    }
