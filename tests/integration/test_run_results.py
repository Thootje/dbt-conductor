"""Integration tests for EXEC-02: real dbt build → run_results.json parsing.

Uses the fixture DuckDB project at tests/fixtures/dbt_project/. Runs an actual
dbt build subprocess and confirms that the operator parses the resulting
run_results.json and emits structured log lines. No mocks on the parsing path.

Skipped automatically when dbt is not on PATH.

Note on dbt selectors vs unique_id (dbt 1.11):
  dbt build --select <selector> accepts model names (e.g. 'stg_a') or
  package-qualified names ('fixture_project.stg_a'). The full unique_id format
  'model.fixture_project.stg_a' is NOT a valid selector in dbt 1.11 — using it
  results in exit 0 with 'Nothing to do'. The integration tests therefore run
  the dbt subprocess directly with a working selector, then invoke
  _log_run_results() with the correct unique_id that matches the run_results entry.
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "dbt_project"


def _dbt_available() -> bool:
    """Check if dbt is available on PATH."""
    return shutil.which("dbt") is not None


@pytest.mark.integration
def test_run_results_logged_after_real_build(tmp_path, mocker):
    """EXEC-02 integration: real dbt build writes run_results.json; operator parses and logs it.

    Strategy:
    1. Run 'dbt build --select stg_a' directly to produce a real run_results.json.
    2. Discover the unique_id written into run_results by dbt (e.g. 'model.fixture_project.stg_a').
    3. Create an operator whose unique_id matches that entry.
    4. Call _log_run_results() directly to exercise the parsing path.
    5. Assert structured log emitted; assert security constraint T-2-01 (no compiled_code/relation_name).
    """
    if not _dbt_available():
        pytest.skip("dbt not available on PATH")

    from aircontroler.operator import DbtRunModelOperator

    # Step 1: copy the fixture project to a writable tmp_path so we don't
    # pollute tests/fixtures/dbt_project/target/
    project_copy = tmp_path / "dbt_project"
    shutil.copytree(FIXTURE_DIR, project_copy)

    # Step 2: run dbt build --select stg_a (model name — works in dbt 1.11)
    build_result = subprocess.run(
        [
            "dbt", "build",
            "--select", "stg_a",
            "--project-dir", str(project_copy),
            "--profiles-dir", str(project_copy),
        ],
        capture_output=True,
        text=True,
    )
    if build_result.returncode != 0:
        pytest.fail(
            "dbt build failed:\nstdout: %s\nstderr: %s"
            % (build_result.stdout, build_result.stderr)
        )

    # Step 3: confirm run_results.json was written by dbt
    rr_path = project_copy / "target" / "run_results.json"
    assert rr_path.exists(), f"dbt build did not write run_results.json at {rr_path}"

    # Step 4: discover the actual unique_id written into run_results
    rr_data = json.loads(rr_path.read_text())
    stg_a_result = next(
        (r for r in rr_data.get("results", []) if "stg_a" in r.get("unique_id", "")),
        None,
    )
    assert stg_a_result is not None, (
        "run_results.json has no entry for stg_a. results: %s" % rr_data.get("results")
    )
    stg_a_uid = stg_a_result["unique_id"]  # e.g. "model.fixture_project.stg_a"

    # Step 5: create an operator with that unique_id and call _log_run_results()
    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log", new_callable=mocker.PropertyMock, return_value=mock_log
    )
    op = DbtRunModelOperator(
        task_id=stg_a_uid,
        unique_id=stg_a_uid,
        project_dir=str(project_copy),
        profiles_path=str(project_copy),
    )
    op._log_run_results()  # exercise the parsing path directly; no subprocess mock needed

    # Step 6: confirm the operator logged a structured line with the unique_id
    def _format_calls(call_list):
        msgs = []
        for call in call_list:
            fmt = call.args[0] if call.args else ""
            args = call.args[1:] if len(call.args) > 1 else ()
            try:
                msgs.append(str(fmt % args) if args else str(fmt))
            except Exception:
                msgs.append(str(fmt))
        return msgs

    info_messages = " ".join(_format_calls(mock_log.info.call_args_list))
    assert "dbt result:" in info_messages, (
        "Expected operator to emit 'dbt result:' log line. "
        "Got messages: %s" % info_messages
    )
    assert ("unique_id=%s" % stg_a_uid) in info_messages, (
        "Expected log to include the model unique_id '%s'. "
        "Got messages: %s" % (stg_a_uid, info_messages)
    )

    # Step 7: SECURITY (T-2-01) — confirm compiled_code/relation_name are NOT logged
    all_messages = " ".join(
        _format_calls(mock_log.info.call_args_list)
        + _format_calls(mock_log.warning.call_args_list)
        + _format_calls(mock_log.debug.call_args_list)
        + _format_calls(mock_log.error.call_args_list)
    )
    assert "compiled_code" not in all_messages, (
        "Security violation T-2-01: compiled_code key appeared in logs"
    )
    assert "relation_name" not in all_messages, (
        "Security violation T-2-01: relation_name key appeared in logs"
    )


@pytest.mark.integration
def test_run_results_missing_warns_in_real_run(tmp_path, mocker):
    """EXEC-02 integration: when run_results.json is absent (e.g., dbt was skipped),
    operator emits WARNING and does not crash.

    This test simulates the absent-artifact path by mocking subprocess at the
    integration level — we run the real operator code (no patching of operator
    methods) but stub the subprocess so no run_results.json is written.
    """
    if not _dbt_available():
        pytest.skip("dbt not available on PATH")
    import unittest.mock as _mock
    from aircontroler.operator import DbtRunModelOperator

    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log", new_callable=mocker.PropertyMock, return_value=mock_log
    )
    op = DbtRunModelOperator(
        task_id="model.fixture_project.stg_a",
        unique_id="model.fixture_project.stg_a",
        project_dir=str(tmp_path),  # tmp_path/target/ does not exist
        profiles_path=str(tmp_path),
    )
    with _mock.patch("aircontroler.operator.subprocess.run") as mr:
        mr.return_value = _mock.MagicMock(returncode=0, stdout="", stderr="")
        op.execute({})

    def _format_calls(call_list):
        msgs = []
        for call in call_list:
            fmt = call.args[0] if call.args else ""
            args = call.args[1:] if len(call.args) > 1 else ()
            try:
                msgs.append(str(fmt % args) if args else str(fmt))
            except Exception:
                msgs.append(str(fmt))
        return msgs

    warning_messages = " ".join(_format_calls(mock_log.warning.call_args_list))
    assert "run_results.json not found" in warning_messages
