"""Unit tests for aircontroler.operator — CORE-06, CORE-07, CORE-08."""
from __future__ import annotations
import pytest


def test_import_compat():
    """CORE-08: DbtRunModelOperator uses BaseOperator from common-compat shim."""
    from aircontroler.operator import DbtRunModelOperator
    from airflow.models import BaseOperator
    assert issubclass(DbtRunModelOperator, BaseOperator)


def test_subprocess_command(mocker):
    """CORE-06: execute() calls subprocess.run with correct list[str] command.

    Command must be:
      [dbt_executable, "build", "--select", unique_id,
       "--project-dir", project_dir, "--profiles-dir", profiles_path]
    shell=True must never be used.
    """
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        dbt_executable_path="dbt",
    )
    op.execute({})
    assert mock_run.call_count == 1
    call_kwargs = mock_run.call_args[1]
    assert call_kwargs.get("shell", False) is False
    cmd = mock_run.call_args[0][0]
    assert isinstance(cmd, list), "Command must be list[str], never a string"
    assert cmd[0] == "dbt"
    assert cmd[1] == "build"
    assert "--select" in cmd
    assert "model.proj.stg_a" in cmd
    assert "--project-dir" in cmd
    assert "/proj" in cmd
    assert "--profiles-dir" in cmd
    assert "/profiles" in cmd


def test_custom_executable(mocker):
    """CORE-06: dbt_executable_path parameter overrides default 'dbt'."""
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        dbt_executable_path="/custom/path/dbt",
    )
    op.execute({})
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "/custom/path/dbt"


def test_failure_raises(mocker):
    """CORE-06: Non-zero dbt exit code raises AirflowException with unique_id and code."""
    from aircontroler.operator import DbtRunModelOperator
    from airflow.exceptions import AirflowException
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(
        returncode=1,
        stdout="",
        stderr="Error: model not found",
    )
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    with pytest.raises(AirflowException) as exc_info:
        op.execute({})
    error_message = str(exc_info.value)
    assert "model.proj.stg_a" in error_message
    assert "1" in error_message


def test_profiles_path(mocker):
    """CORE-07: profiles_path passed as --profiles-dir; contents never logged."""
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.mart",
        unique_id="model.proj.mart",
        project_dir="/proj",
        profiles_path="/secure/profiles",
    )
    op.execute({})
    cmd = mock_run.call_args[0][0]
    # --profiles-dir must appear in the command
    assert "--profiles-dir" in cmd
    # The profiles_path value must follow --profiles-dir
    profiles_dir_idx = cmd.index("--profiles-dir")
    assert cmd[profiles_dir_idx + 1] == "/secure/profiles"
    # Verify subprocess.run does not receive file contents via env or stdin
    call_kwargs = mock_run.call_args[1]
    assert "env" not in call_kwargs or call_kwargs.get("env") is None


def test_retries_from_meta(mocker):
    """EXEC-01: retries kwarg is forwarded to BaseOperator."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.mart",
        unique_id="model.proj.mart",
        project_dir="/proj",
        profiles_path="/profiles",
        retries=3,
    )
    assert op.retries == 3


def test_retry_delay_conversion(mocker):
    """EXEC-01: retry_delay int (seconds) is converted to timedelta on the operator."""
    from datetime import timedelta
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.mart",
        unique_id="model.proj.mart",
        project_dir="/proj",
        profiles_path="/profiles",
        retry_delay=300,
    )
    assert op.retry_delay == timedelta(seconds=300)


def test_pool_from_meta(mocker):
    """EXEC-01: pool kwarg is forwarded to BaseOperator."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.mart",
        unique_id="model.proj.mart",
        project_dir="/proj",
        profiles_path="/profiles",
        pool="heavy_models",
    )
    assert op.pool == "heavy_models"


def test_target_path_stored(mocker):
    """EXEC-01: target_path kwarg is stored on the instance."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        target_path="/custom/target",
    )
    assert op.target_path == "/custom/target"


def test_target_path_default_none(mocker):
    """EXEC-01: target_path defaults to None when not passed."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    assert op.target_path is None


def test_no_meta_uses_defaults(mocker):
    """EXEC-01: With no retries/retry_delay/pool kwargs, BaseOperator defaults apply.

    BaseOperator default retries is 0; default retry_delay is timedelta(seconds=300);
    default pool is the string "default_pool" in modern Airflow (or None in some
    versions). The key assertion is that we did NOT corrupt these by passing None
    explicitly to BaseOperator (Pitfall 3). retries == 0 is the canonical proof.
    """
    from datetime import timedelta
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    assert op.retries == 0
    assert op.retry_delay == timedelta(seconds=300)


def test_template_fields_includes_target_path(mocker):
    """EXEC-01: template_fields exposes target_path for Jinja templating."""
    from aircontroler.operator import DbtRunModelOperator
    assert "target_path" in DbtRunModelOperator.template_fields


def test_run_results_logging(mocker, tmp_path, run_results_v6_data, caplog):
    """EXEC-02: _log_run_results() finds matching unique_id and emits structured log."""
    import json
    import logging
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_v6_data))
    op = DbtRunModelOperator(
        task_id="model.fixture_project.stg_a",
        unique_id="model.fixture_project.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    with caplog.at_level(logging.INFO):
        op.execute({})
    info_messages = " ".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "dbt result:" in info_messages
    assert "unique_id=model.fixture_project.stg_a" in info_messages
    assert "status=success" in info_messages
    assert "execution_time=0.028s" in info_messages


def test_run_results_no_match_warns(mocker, tmp_path, run_results_v6_data, caplog):
    """EXEC-02: When unique_id is not in run_results.results[], a WARNING is logged (no crash)."""
    import json
    import logging
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_v6_data))
    op = DbtRunModelOperator(
        task_id="model.proj.not_in_results",
        unique_id="model.proj.not_in_results",  # not in fixture results
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    with caplog.at_level(logging.WARNING):
        op.execute({})  # must not raise
    warning_messages = " ".join(r.message for r in caplog.records if r.levelno == logging.WARNING)
    assert "no entry for unique_id" in warning_messages


def test_run_results_missing_file_warns(mocker, tmp_path, caplog):
    """EXEC-02: When run_results.json does not exist, a WARNING is logged (no crash)."""
    import logging
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    # Note: no target/ directory created → run_results.json absent
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    with caplog.at_level(logging.WARNING):
        op.execute({})  # must not raise
    warning_messages = " ".join(r.message for r in caplog.records if r.levelno == logging.WARNING)
    assert "run_results.json not found" in warning_messages


def test_run_results_skipped_on_failure(mocker, tmp_path, caplog):
    """EXEC-02 D-10: When subprocess returns non-zero, AirflowException is raised and run_results parsing is skipped."""
    import logging
    import pytest as _pytest
    from airflow.exceptions import AirflowException
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=1, stdout="boom", stderr="error")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    with caplog.at_level(logging.WARNING):
        with _pytest.raises(AirflowException):
            op.execute({})
    # _log_run_results was NEVER called → no "run_results.json not found" warning
    warning_messages = " ".join(r.message for r in caplog.records if r.levelno == logging.WARNING)
    assert "run_results.json not found" not in warning_messages
    assert "no entry for unique_id" not in warning_messages


def test_target_path_controls_run_results_path(mocker, tmp_path):
    """EXEC-02 D-07: target_path override determines run_results.json location."""
    from pathlib import Path
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
        target_path="/explicit/target",
    )
    assert op._run_results_path() == Path("/explicit/target") / "run_results.json"


def test_default_run_results_path(mocker, tmp_path):
    """EXEC-02 D-07: Default path is {project_dir}/target/run_results.json."""
    from pathlib import Path
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    assert op._run_results_path() == Path(str(tmp_path)) / "target" / "run_results.json"


def test_run_results_does_not_log_compiled_code_or_relation(mocker, tmp_path, run_results_v6_data, caplog):
    """SECURITY T-2-01: compiled_code and relation_name MUST NOT appear in any log record."""
    import json
    import logging
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_v6_data))
    op = DbtRunModelOperator(
        task_id="model.fixture_project.stg_a",
        unique_id="model.fixture_project.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    with caplog.at_level(logging.DEBUG):
        op.execute({})
    all_messages = " ".join(r.getMessage() for r in caplog.records)
    # Fixture's compiled_code is "select 1 as id, 'foo' as val"
    assert "compiled_code" not in all_messages, f"Security violation T-2-01: compiled_code key logged. Messages: {all_messages}"
    assert "select 1 as id" not in all_messages, f"Security violation T-2-01: SQL contents logged. Messages: {all_messages}"
    assert "relation_name" not in all_messages, f"Security violation T-2-01: relation_name key logged. Messages: {all_messages}"
    # Fixture's relation_name is "memory.main.stg_a" — verify the inner string isn't logged either
    assert '"main"."stg_a"' not in all_messages, f"Security violation T-2-01: relation contents logged. Messages: {all_messages}"
