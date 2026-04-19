"""DbtRunModelOperator — executes a single dbt model via CLI subprocess.

This is the runtime boundary: parse-time (factory.py) creates instances;
Airflow scheduler calls execute() at task runtime.

Security:
- Command built as list[str]; no shell passthrough (T-01-03-01)
- profiles_path logged by path only; contents never read or logged (T-01-03-02)
- AirflowException on non-zero exit (dbt failure surfaced in Airflow UI)
"""
from __future__ import annotations

import subprocess

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseOperator

# Allowlist of adapter_response keys that are safe to emit in logs (T-2-01).
# Fields like 'table_id', 'schema', or 'relation_name' are intentionally excluded
# because they may expose fully-qualified relation references.
_SAFE_ADAPTER_KEYS = frozenset({"_message", "rows_affected", "bytes_processed", "slot_ms", "cache_hit"})


class DbtRunModelOperator(BaseOperator):
    """Airflow operator that runs 'dbt build --select <unique_id>' as a subprocess.

    One instance is created per resolved dbt model at DAG parse time.

    Args:
        unique_id: The dbt model unique_id (e.g. 'model.my_project.stg_orders').
            Used as the --select argument and in error messages.
        project_dir: Absolute path to the dbt project directory.
        profiles_path: Path to the directory containing profiles.yml.
            Passed as --profiles-dir to dbt subprocess.
        dbt_executable_path: Path to the dbt executable. Defaults to 'dbt'
            (resolved via PATH). Override if dbt is not on the default PATH.
    """

    # Declare non-standard constructor args for Airflow template resolution
    template_fields = ("unique_id", "project_dir", "profiles_path", "dbt_executable_path", "target_path")

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
        freshness_hours: float | None = None,
        trigger_rule: str = "none_failed",
        **kwargs,
    ) -> None:
        from datetime import timedelta
        # Conditional injection: only pass non-None values to BaseOperator,
        # so unset overrides fall back to BaseOperator defaults
        # (retries=0, retry_delay=timedelta(seconds=300), pool=None).
        # See RESEARCH.md Pitfall 3 (BaseOperator None semantics) and Pitfall 4
        # (retry_delay int → timedelta conversion).
        if retries is not None:
            kwargs["retries"] = retries
        if retry_delay is not None:
            kwargs["retry_delay"] = timedelta(seconds=retry_delay)
        if pool is not None:
            kwargs["pool"] = pool
        # Phase 3 / D-02 / Pitfall 4: trigger_rule always forwarded as plain string
        # ("none_failed" is the project default; don't import TriggerRule enum).
        kwargs["trigger_rule"] = trigger_rule
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.project_dir = project_dir
        self.profiles_path = profiles_path
        self.dbt_executable_path = dbt_executable_path
        self.target_path = target_path
        # Phase 3 / T-3-02: reject freshness_hours <= 0 (log WARNING, treat as None)
        if freshness_hours is not None and freshness_hours <= 0:
            self.log.warning(
                "freshness_hours=%s is not positive for %s — treating as None (always run dbt)",
                freshness_hours, unique_id,
            )
            freshness_hours = None
        self.freshness_hours = freshness_hours

    def execute(self, context: dict) -> None:
        """Run 'dbt build --select <unique_id>' as a subprocess.

        Phase 3 (FRESH-03): before the subprocess, consult MetadataDBHook.get_last_run()
        and raise AirflowSkipException if the model ran successfully within freshness_hours.
        On success (returncode 0, after _log_run_results), call record_run() to persist state.

        Security note: cmd is always list[str]; no shell passthrough is used.
        profiles_path is passed as a directory path flag; file contents are
        never read by this method and never appear in logs.
        """
        # Phase 3: FRESH-03 freshness TTL check — before subprocess
        if self.freshness_hours is not None:
            from datetime import datetime, timezone
            from aircontroler.hooks.metadata_db import MetadataDBHook
            from airflow.providers.common.compat.sdk import AirflowSkipException
            hook = MetadataDBHook()
            try:
                last_run = hook.get_last_run(self.unique_id)
            except Exception as exc:
                # Fail-open (Nyquist dim 8): on any DB error (e.g., table not yet created
                # on Airflow 2.x pre-`aircontroler db init`), log WARNING and proceed to dbt.
                self.log.warning(
                    "Freshness check failed for %s: %s — running dbt (fail-open)",
                    self.unique_id, exc,
                )
                last_run = None
            if last_run is not None:
                # Pitfall 5: normalize to UTC. SQLite and some drivers strip tzinfo;
                # if naive, treat as UTC (matches the hook's write-side UTC stamp).
                if last_run.tzinfo is None:
                    last_run = last_run.replace(tzinfo=timezone.utc)
                now = datetime.now(tz=timezone.utc)
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

        # Existing Phase 1 / 2 subprocess logic — unchanged structure
        executable = self.dbt_executable_path or "dbt"
        cmd = [
            executable,
            "build",
            "--select", self.unique_id,
            "--project-dir", self.project_dir,
            "--profiles-dir", self.profiles_path,
        ]
        # Log the command for auditability (path only, not credentials)
        self.log.info("Running dbt: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            if result.stdout:
                self.log.error("dbt stdout:\n%s", result.stdout)
            self.log.error("dbt stderr:\n%s", result.stderr)
            raise AirflowException(
                f"dbt build failed for {self.unique_id!r} "
                f"(exit {result.returncode})"
            )
        # Success: log stdout once at INFO.
        if result.stdout:
            self.log.info(result.stdout)
        self._log_run_results()

        # Phase 3 / FRESH-04 / Nyquist dims 4-6: record_run on success path ONLY.
        # NOT reached on failure (AirflowException above) or on skip (AirflowSkipException earlier).
        from aircontroler.hooks.metadata_db import MetadataDBHook
        hook = MetadataDBHook()
        try:
            hook.record_run(
                unique_id=self.unique_id,
                dag_id=context["dag_run"].dag_id,
                run_id=context["run_id"],
            )
        except Exception as exc:
            # Don't fail the task if state recording fails (table missing on Airflow 2.x, etc.)
            # — log WARNING. The model ran successfully; recording is best-effort.
            self.log.warning(
                "record_run failed for %s: %s — state table not updated",
                self.unique_id, exc,
            )

    def _run_results_path(self):
        """Resolve the path to run_results.json based on target_path or project_dir."""
        from pathlib import Path
        if self.target_path:
            return Path(self.target_path) / "run_results.json"
        return Path(self.project_dir) / "target" / "run_results.json"

    def _log_run_results(self) -> None:
        """Parse and log run_results.json for this node. Called only on subprocess success.

        Security constraint (T-2-01): MUST NOT log `compiled_code` or `relation_name`.
        Logs only `status`, `execution_time`, `adapter_response`, and `rows_affected`.
        """
        import json
        rr_path = self._run_results_path()
        if not rr_path.exists():
            self.log.warning(
                "run_results.json not found at %s — skipping structured logging",
                rr_path,
            )
            return
        try:
            with open(rr_path) as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            self.log.warning("Failed to read run_results.json: %s", exc)
            return
        results = data.get("results", [])
        node_result = next(
            (r for r in results if r.get("unique_id") == self.unique_id),
            None,
        )
        if node_result is None:
            self.log.warning(
                "run_results.json contained no entry for unique_id=%r — "
                "model may have been skipped",
                self.unique_id,
            )
            return
        adapter_response = node_result.get("adapter_response", {})
        rows_affected = adapter_response.get("rows_affected")
        self.log.info(
            "dbt result: unique_id=%s status=%s execution_time=%.3fs",
            self.unique_id,
            node_result.get("status", "unknown"),
            node_result.get("execution_time", 0.0),
        )
        if rows_affected is not None:
            self.log.info("dbt result: rows_affected=%s", rows_affected)
        safe_adapter_fields = {k: v for k, v in adapter_response.items() if k in _SAFE_ADAPTER_KEYS}
        self.log.info("dbt result: adapter_response=%s", safe_adapter_fields)
