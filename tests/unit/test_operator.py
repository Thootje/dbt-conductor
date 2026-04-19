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


def _make_mock_log(mocker):
    """Create a MagicMock logger and patch DbtRunModelOperator.log property to return it.

    Airflow 3 uses structlog (PrintLoggerFactory) rather than stdlib logging,
    so pytest's caplog fixture cannot capture operator log calls. This helper
    patches the 'log' property at the class level using mocker (auto-cleaned up
    after each test) so we can assert on info/warning/error calls directly.

    Returns the mock_log MagicMock instance. mocker handles teardown.
    """
    from aircontroler.operator import DbtRunModelOperator
    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log", new_callable=mocker.PropertyMock, return_value=mock_log
    )
    return mock_log


def _format_log_calls(call_list):
    """Reconstruct formatted log messages from a list of mock call objects.

    Each call has positional args (fmt, *args); we apply %-formatting to
    reproduce the final string that would have been emitted.
    """
    msgs = []
    for call in call_list:
        fmt = call.args[0] if call.args else ""
        args = call.args[1:] if len(call.args) > 1 else ()
        try:
            msgs.append(str(fmt % args) if args else str(fmt))
        except Exception:
            msgs.append(str(fmt))
    return msgs


def test_run_results_logging(mocker, tmp_path, run_results_v6_data):
    """EXEC-02: _log_run_results() finds matching unique_id and emits structured log.

    Airflow 3 uses structlog (not stdlib logging), so caplog does not capture
    operator log calls. We patch the 'log' property and assert on mock calls.
    """
    import json
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_v6_data))
    mock_log = _make_mock_log(mocker)
    op = DbtRunModelOperator(
        task_id="model.fixture_project.stg_a",
        unique_id="model.fixture_project.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    op.execute({})
    info_messages = " ".join(_format_log_calls(mock_log.info.call_args_list))
    assert "dbt result:" in info_messages
    assert "unique_id=model.fixture_project.stg_a" in info_messages
    assert "status=success" in info_messages
    assert "execution_time=0.028s" in info_messages


def test_run_results_no_match_warns(mocker, tmp_path, run_results_v6_data):
    """EXEC-02: When unique_id is not in run_results.results[], a WARNING is logged (no crash)."""
    import json
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_v6_data))
    mock_log = _make_mock_log(mocker)
    op = DbtRunModelOperator(
        task_id="model.proj.not_in_results",
        unique_id="model.proj.not_in_results",  # not in fixture results
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    op.execute({})  # must not raise
    warning_messages = " ".join(_format_log_calls(mock_log.warning.call_args_list))
    assert "no entry for unique_id" in warning_messages


def test_run_results_missing_file_warns(mocker, tmp_path):
    """EXEC-02: When run_results.json does not exist, a WARNING is logged (no crash)."""
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    # Note: no target/ directory created → run_results.json absent
    mock_log = _make_mock_log(mocker)
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    op.execute({})  # must not raise
    warning_messages = " ".join(_format_log_calls(mock_log.warning.call_args_list))
    assert "run_results.json not found" in warning_messages


def test_run_results_skipped_on_failure(mocker, tmp_path):
    """EXEC-02 D-10: When subprocess returns non-zero, AirflowException is raised and run_results parsing is skipped."""
    import pytest as _pytest
    from airflow.exceptions import AirflowException
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=1, stdout="boom", stderr="error")
    mock_log = _make_mock_log(mocker)
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    with _pytest.raises(AirflowException):
        op.execute({})
    # _log_run_results was NEVER called → no "run_results.json not found" warning
    warning_messages = " ".join(_format_log_calls(mock_log.warning.call_args_list))
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


def test_run_results_does_not_log_compiled_code_or_relation(mocker, tmp_path, run_results_v6_data):
    """SECURITY T-2-01: compiled_code and relation_name MUST NOT appear in any log record.

    Airflow 3 uses structlog; we capture all calls to the mocked logger and
    reconstruct the formatted message strings for the security assertion.
    """
    import json
    from aircontroler.operator import DbtRunModelOperator
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_v6_data))
    mock_log = _make_mock_log(mocker)
    op = DbtRunModelOperator(
        task_id="model.fixture_project.stg_a",
        unique_id="model.fixture_project.stg_a",
        project_dir=str(tmp_path),
        profiles_path="/profiles",
    )
    op.execute({})
    all_messages = " ".join(
        _format_log_calls(mock_log.info.call_args_list)
        + _format_log_calls(mock_log.warning.call_args_list)
        + _format_log_calls(mock_log.debug.call_args_list)
        + _format_log_calls(mock_log.error.call_args_list)
    )
    # Fixture's compiled_code is "select 1 as id, 'foo' as val"
    assert "compiled_code" not in all_messages, f"Security violation T-2-01: compiled_code key logged. Messages: {all_messages}"
    assert "select 1 as id" not in all_messages, f"Security violation T-2-01: SQL contents logged. Messages: {all_messages}"
    assert "relation_name" not in all_messages, f"Security violation T-2-01: relation_name key logged. Messages: {all_messages}"
    # Fixture's relation_name is '"memory"."main"."stg_a"' — verify inner strings not logged
    assert '"main"."stg_a"' not in all_messages, f"Security violation T-2-01: relation contents logged. Messages: {all_messages}"


# ----------------------------------------------------------------------
# Phase 3 — FRESH-03 / FRESH-04 — freshness check + record_run wiring
# ----------------------------------------------------------------------


def _make_fresh_context():
    """Return a mock Airflow context with dag_run.dag_id and run_id populated."""
    from unittest.mock import MagicMock
    ctx = {}
    dag_run = MagicMock()
    dag_run.dag_id = "test_dag"
    ctx["dag_run"] = dag_run
    ctx["run_id"] = "test_run_id"
    return ctx


def test_freshness_hours_stored_on_instance():
    """FRESH-01: freshness_hours kwarg is stored as instance attribute."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    assert op.freshness_hours == 6.0


def test_freshness_hours_default_is_none():
    """D-12: freshness_hours defaults to None → freshness check skipped entirely."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    assert op.freshness_hours is None


def test_freshness_hours_zero_treated_as_none(mocker):
    """T-3-02 / Security V5: freshness_hours <= 0 logged WARNING and coerced to None."""
    from aircontroler.operator import DbtRunModelOperator
    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log",
        new_callable=mocker.PropertyMock, return_value=mock_log,
    )
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=0,
    )
    assert op.freshness_hours is None
    assert mock_log.warning.called


def test_freshness_hours_negative_treated_as_none(mocker):
    """T-3-02: negative freshness_hours logged WARNING and coerced to None."""
    from aircontroler.operator import DbtRunModelOperator
    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log",
        new_callable=mocker.PropertyMock, return_value=mock_log,
    )
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=-5,
    )
    assert op.freshness_hours is None
    assert mock_log.warning.called


def test_trigger_rule_default_is_none_failed():
    """D-02: default trigger_rule='none_failed' so downstream runs when upstreams skipped."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    assert op.trigger_rule == "none_failed"


def test_trigger_rule_override():
    """D-03: trigger_rule kwarg overrides the default."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        trigger_rule="all_done",
    )
    assert op.trigger_rule == "all_done"


def test_skip_when_fresh(mocker):
    """Nyquist dim 1: FRESH-03 — last_run within TTL → AirflowSkipException, no subprocess."""
    from datetime import datetime, timedelta, timezone
    from aircontroler.operator import DbtRunModelOperator
    from airflow.providers.common.compat.sdk import AirflowSkipException
    # Last run 2 hours ago; TTL 6 hours → fresh → skip
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=datetime.now(tz=timezone.utc) - timedelta(hours=2),
    )
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    with pytest.raises(AirflowSkipException) as exc_info:
        op.execute(_make_fresh_context())
    assert "model.proj.stg_a" in str(exc_info.value)
    assert mock_run.call_count == 0, "subprocess.run MUST NOT be called on skip path"


def test_no_skip_when_stale(mocker):
    """Nyquist dim 2: FRESH-03 — last_run outside TTL → subprocess invoked."""
    from datetime import datetime, timedelta, timezone
    from aircontroler.operator import DbtRunModelOperator
    # Last run 10 hours ago; TTL 6 hours → stale → run dbt
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=datetime.now(tz=timezone.utc) - timedelta(hours=10),
    )
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    op.execute(_make_fresh_context())
    assert mock_run.call_count == 1


def test_no_skip_when_no_ttl(mocker):
    """Nyquist dim 3: D-12 — freshness_hours=None → no hook call, dbt always invoked."""
    from aircontroler.operator import DbtRunModelOperator
    hook_get = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        # freshness_hours omitted → None
    )
    op.execute(_make_fresh_context())
    assert hook_get.call_count == 0, "get_last_run MUST NOT be called when freshness_hours is None"
    assert mock_run.call_count == 1


def test_no_skip_when_no_last_run(mocker):
    """FRESH-03: last_run=None (never ran before) → no skip, dbt invoked."""
    from aircontroler.operator import DbtRunModelOperator
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=None,
    )
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    op.execute(_make_fresh_context())
    assert mock_run.call_count == 1


def test_record_run_called_on_success(mocker):
    """Nyquist dim 4: FRESH-04 — successful dbt → record_run(unique_id, dag_id, run_id)."""
    from aircontroler.operator import DbtRunModelOperator
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=None,
    )
    hook_rec = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    op.execute(_make_fresh_context())
    assert hook_rec.call_count == 1
    call_kwargs = hook_rec.call_args.kwargs
    assert call_kwargs["unique_id"] == "model.proj.stg_a"
    assert call_kwargs["dag_id"] == "test_dag"
    assert call_kwargs["run_id"] == "test_run_id"


def test_record_run_not_called_on_failure(mocker):
    """Nyquist dim 5: failed dbt (non-zero returncode) → record_run NOT called."""
    from aircontroler.operator import DbtRunModelOperator
    from airflow.exceptions import AirflowException
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=None,
    )
    hook_rec = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=1, stdout="", stderr="boom")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    with pytest.raises(AirflowException):
        op.execute(_make_fresh_context())
    assert hook_rec.call_count == 0


def test_record_run_not_called_on_skip(mocker):
    """Nyquist dim 6: skip path (fresh) → record_run NOT called."""
    from datetime import datetime, timedelta, timezone
    from aircontroler.operator import DbtRunModelOperator
    from airflow.providers.common.compat.sdk import AirflowSkipException
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=datetime.now(tz=timezone.utc) - timedelta(hours=1),
    )
    hook_rec = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mocker.patch("aircontroler.operator.subprocess.run")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    with pytest.raises(AirflowSkipException):
        op.execute(_make_fresh_context())
    assert hook_rec.call_count == 0


def test_fail_open_on_missing_table(mocker):
    """Nyquist dim 8: get_last_run raises DB error → log WARNING, run dbt (fail-open)."""
    from aircontroler.operator import DbtRunModelOperator
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        side_effect=Exception("no such table: aircontroler_model_runs"),
    )
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    mock_log = mocker.MagicMock()
    mocker.patch.object(
        DbtRunModelOperator, "log",
        new_callable=mocker.PropertyMock, return_value=mock_log,
    )
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    op.execute(_make_fresh_context())
    assert mock_run.call_count == 1
    # WARNING was logged for the failed hook call
    assert any("fail-open" in str(call).lower() or "freshness" in str(call).lower()
               for call in mock_log.warning.call_args_list)


def test_naive_last_run_treated_as_utc(mocker):
    """Pitfall 5: last_run without tzinfo is treated as UTC (SQLite strips tz)."""
    from datetime import datetime, timedelta, timezone
    from aircontroler.operator import DbtRunModelOperator
    from airflow.providers.common.compat.sdk import AirflowSkipException
    # Return a NAIVE datetime 1 hour ago (SQLite behavior). Must be treated as UTC.
    naive_last_run = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).replace(tzinfo=None)
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        return_value=naive_last_run,
    )
    mocker.patch("aircontroler.operator.subprocess.run")
    op = DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        freshness_hours=6.0,
    )
    # Must NOT raise TypeError on comparison; must raise AirflowSkipException (fresh)
    with pytest.raises(AirflowSkipException):
        op.execute(_make_fresh_context())


# ── Phase 4: LOCK-01 / LOCK-02 / LOCK-03 / LOCK-04 operator stubs ─────────


def _make_conflict_op(conflict_policy: str):
    """Helper: build a DbtRunModelOperator with conflict_policy set."""
    from aircontroler.operator import DbtRunModelOperator
    return DbtRunModelOperator(
        task_id="model.proj.stg_a",
        unique_id="model.proj.stg_a",
        project_dir="/proj",
        profiles_path="/profiles",
        conflict_policy=conflict_policy,
    )


def _make_conflict_context():
    """Helper: Airflow context with dag_run.dag_id and run_id for conflict tests."""
    from unittest.mock import MagicMock
    ctx: dict = {}
    dag_run = MagicMock()
    dag_run.dag_id = "test_dag"
    ctx["dag_run"] = dag_run
    ctx["run_id"] = "test_run_id"
    return ctx


def test_conflict_policy_stored_on_operator():
    """LOCK-01: conflict_policy param is stored as self.conflict_policy."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="t", unique_id="u", project_dir="/p", profiles_path="/pr",
        conflict_policy="skip",
    )
    assert op.conflict_policy == "skip"


def test_conflict_policy_none_by_default():
    """LOCK-01 / D-02: Default conflict_policy is None — no locking, backward-compatible."""
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="t", unique_id="u", project_dir="/p", profiles_path="/pr",
    )
    assert op.conflict_policy is None


def test_conflict_skip_raises_airflow_skip_exception(mocker):
    """LOCK-03: acquire_lock returns False + conflict_policy=skip → AirflowSkipException."""
    from airflow.providers.common.compat.sdk import AirflowSkipException
    op = _make_conflict_op("skip")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock", return_value=False)
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_lock_holder",
        return_value=None,
    )
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder")
    mock_session_ctx = mocker.MagicMock()
    mock_session_ctx.__enter__ = mocker.MagicMock(return_value=mocker.MagicMock())
    mock_session_ctx.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("aircontroler.operator.create_session", return_value=mock_session_ctx)
    with pytest.raises(AirflowSkipException) as exc_info:
        op.execute(_make_conflict_context())
    assert "model.proj.stg_a" in str(exc_info.value)


def test_conflict_fail_raises_airflow_exception(mocker):
    """LOCK-04: acquire_lock returns False + conflict_policy=fail → AirflowException with holder info."""
    from airflow.exceptions import AirflowException
    op = _make_conflict_op("fail")
    fake_holder = mocker.MagicMock()
    fake_holder.dag_id = "other_dag"
    fake_holder.run_id = "other_run"
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock", return_value=False)
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_lock_holder",
        return_value=fake_holder,
    )
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder")
    mock_session_ctx = mocker.MagicMock()
    mock_session_ctx.__enter__ = mocker.MagicMock(return_value=mocker.MagicMock())
    mock_session_ctx.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("aircontroler.operator.create_session", return_value=mock_session_ctx)
    with pytest.raises(AirflowException) as exc_info:
        op.execute(_make_conflict_context())
    assert "other_dag" in str(exc_info.value)
    assert "model.proj.stg_a" in str(exc_info.value)


def test_lock_acquired_before_freshness_check(mocker):
    """LOCK-02 / D-05: acquire_lock is called BEFORE get_last_run (call order)."""
    call_order = []
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder",
        side_effect=lambda **kw: call_order.append("upsert"),
    )
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock",
        side_effect=lambda *a, **kw: call_order.append("acquire") or True,
    )
    mocker.patch(
        "aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run",
        side_effect=lambda *a, **kw: call_order.append("freshness") or None,
    )
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    from aircontroler.operator import DbtRunModelOperator
    op = DbtRunModelOperator(
        task_id="t", unique_id="model.proj.stg_a", project_dir="/p",
        profiles_path="/pr", conflict_policy="skip", freshness_hours=1.0,
    )
    mock_session_ctx = mocker.MagicMock()
    mock_session_ctx.__enter__ = mocker.MagicMock(return_value=mocker.MagicMock())
    mock_session_ctx.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("aircontroler.operator.create_session", return_value=mock_session_ctx)
    op.execute(_make_conflict_context())
    acquire_idx = call_order.index("acquire")
    freshness_idx = call_order.index("freshness")
    assert acquire_idx < freshness_idx, f"acquire must come before freshness: {call_order}"


def test_finally_cleanup_on_success_path(mocker):
    """LOCK-04 / D-12: delete_lock_holder is called on the success path."""
    mock_delete = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock", return_value=True)
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.get_last_run", return_value=None)
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.record_run")
    mock_run = mocker.patch("aircontroler.operator.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stdout="", stderr="")
    mock_session_ctx = mocker.MagicMock()
    mock_session_ctx.__enter__ = mocker.MagicMock(return_value=mocker.MagicMock())
    mock_session_ctx.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("aircontroler.operator.create_session", return_value=mock_session_ctx)
    op = _make_conflict_op("skip")
    op.execute(_make_conflict_context())
    assert mock_delete.call_count == 1


def test_finally_cleanup_on_conflict_skip_path(mocker):
    """LOCK-04 / D-12: delete_lock_holder is called even when conflict raises AirflowSkipException."""
    from airflow.providers.common.compat.sdk import AirflowSkipException
    mock_delete = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock", return_value=False)
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.get_lock_holder", return_value=None)
    mock_session_ctx = mocker.MagicMock()
    mock_session_ctx.__enter__ = mocker.MagicMock(return_value=mocker.MagicMock())
    mock_session_ctx.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("aircontroler.operator.create_session", return_value=mock_session_ctx)
    op = _make_conflict_op("skip")
    with pytest.raises(AirflowSkipException):
        op.execute(_make_conflict_context())
    assert mock_delete.call_count == 1


def test_finally_cleanup_on_conflict_fail_path(mocker):
    """LOCK-04 / D-12: delete_lock_holder is called even when conflict raises AirflowException."""
    from airflow.exceptions import AirflowException
    mock_delete = mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.delete_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.upsert_lock_holder")
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.acquire_lock", return_value=False)
    mocker.patch("aircontroler.hooks.metadata_db.MetadataDBHook.get_lock_holder", return_value=None)
    mock_session_ctx = mocker.MagicMock()
    mock_session_ctx.__enter__ = mocker.MagicMock(return_value=mocker.MagicMock())
    mock_session_ctx.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("aircontroler.operator.create_session", return_value=mock_session_ctx)
    op = _make_conflict_op("fail")
    with pytest.raises(AirflowException):
        op.execute(_make_conflict_context())
    assert mock_delete.call_count == 1
