"""Unit tests for aircontroler.cli — FRESH-06."""
from __future__ import annotations

from click.testing import CliRunner


def test_cli_help_exits_zero():
    """CLI entry point is discoverable and --help works without Airflow context."""
    from aircontroler.cli import cli
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "aircontroler" in result.output.lower()


def test_cli_db_group_exists():
    """`aircontroler db --help` lists the `init` subcommand."""
    from aircontroler.cli import cli
    runner = CliRunner()
    result = runner.invoke(cli, ["db", "--help"])
    assert result.exit_code == 0
    assert "init" in result.output


def test_db_init_invokes_db_manager_initdb(mocker):
    """`aircontroler db init` calls AircontrolerDBManager(session).initdb()."""
    from aircontroler.cli import cli
    # Patch create_session to yield a mock session; patch AircontrolerDBManager to spy on initdb
    mock_session = mocker.MagicMock()
    mock_cm = mocker.MagicMock()
    mock_cm.__enter__ = mocker.MagicMock(return_value=mock_session)
    mock_cm.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("airflow.utils.session.create_session", return_value=mock_cm)
    mock_manager_cls = mocker.patch("aircontroler.db_manager.AircontrolerDBManager")
    runner = CliRunner()
    result = runner.invoke(cli, ["db", "init"])
    assert result.exit_code == 0, result.output
    mock_manager_cls.assert_called_once_with(mock_session)
    mock_manager_cls.return_value.initdb.assert_called_once()
    assert "aircontroler_model_runs" in result.output
