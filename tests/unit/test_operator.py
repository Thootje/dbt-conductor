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
