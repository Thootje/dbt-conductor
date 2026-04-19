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
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.project_dir = project_dir
        self.profiles_path = profiles_path
        self.dbt_executable_path = dbt_executable_path
        self.target_path = target_path

    def execute(self, context: dict) -> None:
        """Run 'dbt build --select <unique_id>' as a subprocess.

        Streams stdout/stderr to the Airflow task log.
        Raises AirflowException on non-zero dbt exit code.

        Security note: cmd is always list[str]; no shell passthrough is used.
        profiles_path is passed as a directory path flag; file contents are
        never read by this method and never appear in logs.
        """
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
        self.log.info(result.stdout)
        if result.returncode != 0:
            if result.stdout:
                self.log.error("dbt stdout:\n%s", result.stdout)
            self.log.error("dbt stderr:\n%s", result.stderr)
            raise AirflowException(
                f"dbt build failed for {self.unique_id!r} "
                f"(exit {result.returncode})"
            )
