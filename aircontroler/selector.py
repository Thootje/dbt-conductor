"""Selector resolution for aircontroler.

Delegates to 'dbt ls' subprocess for full dbt selector grammar support.
Never reimplements dbt's selector DSL.

Security: command always built as list[str]; no shell passthrough (T-01-02-02).
"""
from __future__ import annotations

import json
import logging
import re
import subprocess

log = logging.getLogger(__name__)

# Regex patterns to extract selector flags from a dbt command string (D-04).
# Each flag is extracted independently. --models is the legacy alias for --select.
_SELECTOR_FLAGS: dict[str, re.Pattern] = {
    "--select": re.compile(r"--select\s+(\S+)"),
    "--exclude": re.compile(r"--exclude\s+(\S+)"),
    "--selector": re.compile(r"--selector\s+(\S+)"),
    "--models": re.compile(r"--models\s+(\S+)"),  # legacy alias; still works in 1.8-1.11
}


class SelectorResolver:
    """Resolve a dbt selector string to a set of model unique_ids via 'dbt ls'.

    Delegates full selector grammar to the dbt CLI — no reimplementation.
    """

    def __init__(self, dbt_executable: str = "dbt") -> None:
        self.dbt_executable = dbt_executable

    def resolve(
        self,
        dbt_command: str,
        project_dir: str,
        profiles_dir: str,
    ) -> set[str]:
        """Parse selector flags from dbt_command and delegate to 'dbt ls'.

        Args:
            dbt_command: Full dbt command string, e.g.
                'dbt build --select tag:daily+ --exclude raw_* --profile prod'
            project_dir: Path to the dbt project directory.
            profiles_dir: Directory containing profiles.yml.

        Returns:
            Set of unique_id strings for resolved models.

        Raises:
            RuntimeError: if dbt ls exits non-zero.
        """
        selector_args = self._extract_selector_args(dbt_command)
        cmd = [
            self.dbt_executable,
            "ls",
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
            "--output", "json",
            "--output-keys", "unique_id",
        ] + selector_args
        log.info("Resolving selector: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt ls failed (exit {result.returncode}): {result.stderr}"
            )
        return self._parse_ndjson(result.stdout)

    def _extract_selector_args(self, dbt_command: str) -> list[str]:
        """Extract --select/--exclude/--selector flags from a dbt command string.

        Flags are extracted in the order they appear in _SELECTOR_FLAGS.
        --project-dir and --profiles-dir are intentionally NOT extracted here
        (D-05: these come from explicit factory params, not the command string).
        """
        args: list[str] = []
        for flag, pattern in _SELECTOR_FLAGS.items():
            for match in pattern.finditer(dbt_command):
                args.extend([flag, match.group(1)])
        return args

    def _parse_ndjson(self, stdout: str) -> set[str]:
        """Parse newline-delimited JSON output from 'dbt ls --output json'.

        dbt sometimes emits log lines (e.g. 'Running with dbt=1.8.0') before
        the JSON objects. Non-JSON lines are silently skipped (Pitfall 2).
        """
        unique_ids: set[str] = set()
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                uid = obj.get("unique_id")
                if uid:
                    unique_ids.add(uid)
            except json.JSONDecodeError:
                # Non-JSON log prefix line from dbt — skip silently
                pass
        return unique_ids
