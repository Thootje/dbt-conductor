"""Unit tests for aircontroler.selector — CORE-03."""
from __future__ import annotations
import pytest


def test_extract_select(mocker):
    """CORE-03: --select flag extracted from dbt command string; dbt ls called as list[str]."""
    from aircontroler.selector import SelectorResolver
    mock_run = mocker.patch("aircontroler.selector.subprocess.run")
    mock_run.return_value = mocker.MagicMock(
        returncode=0,
        stdout='{"unique_id": "model.proj.stg_a"}\n',
        stderr="",
    )
    resolver = SelectorResolver(dbt_executable="dbt")
    result = resolver.resolve(
        dbt_command="dbt build --select tag:daily+ --profile prod",
        project_dir="/proj",
        profiles_dir="/profiles",
    )
    assert mock_run.call_count == 1
    cmd = mock_run.call_args[0][0]
    assert isinstance(cmd, list), "Command must be list[str], never a shell string"
    # shell=True must never appear
    kwargs = mock_run.call_args[1]
    assert kwargs.get("shell", False) is False
    assert "--select" in cmd
    assert "tag:daily+" in cmd
    assert result == {"model.proj.stg_a"}


def test_extract_exclude(mocker):
    """CORE-03: --exclude flag extracted alongside --select."""
    from aircontroler.selector import SelectorResolver
    mock_run = mocker.patch("aircontroler.selector.subprocess.run")
    mock_run.return_value = mocker.MagicMock(
        returncode=0,
        stdout='{"unique_id": "model.proj.mart"}\n',
        stderr="",
    )
    resolver = SelectorResolver()
    resolver.resolve(
        dbt_command="dbt build --select +mart --exclude raw_*",
        project_dir="/proj",
        profiles_dir="/profiles",
    )
    cmd = mock_run.call_args[0][0]
    assert "--select" in cmd
    assert "+mart" in cmd
    assert "--exclude" in cmd
    assert "raw_*" in cmd


def test_extract_selector(mocker):
    """CORE-03: --selector flag extracted from dbt command string."""
    from aircontroler.selector import SelectorResolver
    mock_run = mocker.patch("aircontroler.selector.subprocess.run")
    mock_run.return_value = mocker.MagicMock(
        returncode=0,
        stdout='{"unique_id": "model.proj.a"}\n',
        stderr="",
    )
    resolver = SelectorResolver()
    resolver.resolve(
        dbt_command="dbt build --selector my_yaml_selector",
        project_dir="/proj",
        profiles_dir="/profiles",
    )
    cmd = mock_run.call_args[0][0]
    assert "--selector" in cmd
    assert "my_yaml_selector" in cmd


def test_parse_ls_output(mocker):
    """CORE-03: NDJSON stdout parsed correctly; non-JSON log lines silently skipped."""
    from aircontroler.selector import SelectorResolver
    # dbt sometimes emits log lines before NDJSON output
    ndjson_with_log = (
        "Running with dbt=1.8.0\n"
        '{"unique_id": "model.proj.stg_a"}\n'
        '{"unique_id": "model.proj.stg_b"}\n'
    )
    mock_run = mocker.patch("aircontroler.selector.subprocess.run")
    mock_run.return_value = mocker.MagicMock(
        returncode=0,
        stdout=ndjson_with_log,
        stderr="",
    )
    resolver = SelectorResolver()
    result = resolver.resolve(
        dbt_command="dbt build --select +mart",
        project_dir="/proj",
        profiles_dir="/profiles",
    )
    assert result == {"model.proj.stg_a", "model.proj.stg_b"}


def test_ls_failure_raises(mocker):
    """CORE-03: Non-zero dbt ls exit code raises RuntimeError with stderr message."""
    from aircontroler.selector import SelectorResolver
    mock_run = mocker.patch("aircontroler.selector.subprocess.run")
    mock_run.return_value = mocker.MagicMock(
        returncode=1,
        stdout="",
        stderr="project not found: /bad/path",
    )
    resolver = SelectorResolver()
    with pytest.raises(RuntimeError, match="project not found"):
        resolver.resolve(
            dbt_command="dbt build --select +mart",
            project_dir="/bad/path",
            profiles_dir="/profiles",
        )
