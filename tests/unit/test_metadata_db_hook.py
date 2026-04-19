"""Unit tests for aircontroler.hooks.metadata_db — FRESH-04.

These tests exercise MetadataDBHook.get_last_run() and record_run() via mocked
create_session. They are stubbed (marked xfail) until Plan 02 replaces the
Phase 1 stub with a full implementation.
"""
from __future__ import annotations

import pytest


@pytest.mark.xfail(reason="Plan 02 implements get_last_run; Wave 0 is test stub only")
def test_get_last_run_returns_none_when_no_rows(mocker):
    """FRESH-04: get_last_run() returns None when no matching row exists (Nyquist dim 3, 8)."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.execute.return_value.scalar.return_value = None
    mock_cm = mocker.MagicMock(
        __enter__=mocker.MagicMock(return_value=mock_session),
        __exit__=mocker.MagicMock(return_value=False),
    )
    mocker.patch("aircontroler.hooks.metadata_db.create_session", return_value=mock_cm)
    hook = MetadataDBHook()
    assert hook.get_last_run("model.proj.stg_a") is None


@pytest.mark.xfail(reason="Plan 02 implements get_last_run; Wave 0 is test stub only")
def test_get_last_run_returns_timestamp(mocker):
    """FRESH-04: get_last_run() returns MAX(completed_at) timestamp."""
    from datetime import datetime, timezone
    from aircontroler.hooks.metadata_db import MetadataDBHook
    expected = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)
    mock_session = mocker.MagicMock()
    mock_session.execute.return_value.scalar.return_value = expected
    mock_cm = mocker.MagicMock(
        __enter__=mocker.MagicMock(return_value=mock_session),
        __exit__=mocker.MagicMock(return_value=False),
    )
    mocker.patch("aircontroler.hooks.metadata_db.create_session", return_value=mock_cm)
    hook = MetadataDBHook()
    assert hook.get_last_run("model.proj.stg_a") == expected


@pytest.mark.xfail(reason="Plan 02 implements record_run; Wave 0 is test stub only")
def test_record_run_inserts_row(mocker):
    """FRESH-04: record_run() inserts a row with unique_id, dag_id, run_id, completed_at."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_cm = mocker.MagicMock(
        __enter__=mocker.MagicMock(return_value=mock_session),
        __exit__=mocker.MagicMock(return_value=False),
    )
    mocker.patch("aircontroler.hooks.metadata_db.create_session", return_value=mock_cm)
    hook = MetadataDBHook()
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_1", run_id="run_1")
    assert mock_session.execute.call_count == 1


@pytest.mark.xfail(reason="Plan 02 implements parameterized SQL; Wave 0 is test stub only")
def test_get_last_run_uses_parameterized_query(mocker):
    """Security T-3-01: get_last_run() uses SQLAlchemy ORM select (no string-formatted SQL).

    Calls with a hostile unique_id and asserts the statement object is an
    sqlalchemy Select (not a raw string), preventing SQL injection via unique_id.
    """
    import sqlalchemy as sa
    from aircontroler.hooks.metadata_db import MetadataDBHook
    captured = {}
    mock_session = mocker.MagicMock()

    def capture_execute(stmt, *args, **kwargs):
        captured["stmt"] = stmt
        result = mocker.MagicMock()
        result.scalar.return_value = None
        return result

    mock_session.execute.side_effect = capture_execute
    mock_cm = mocker.MagicMock(
        __enter__=mocker.MagicMock(return_value=mock_session),
        __exit__=mocker.MagicMock(return_value=False),
    )
    mocker.patch("aircontroler.hooks.metadata_db.create_session", return_value=mock_cm)
    hook = MetadataDBHook()
    hook.get_last_run("'; DROP TABLE aircontroler_model_runs; --")
    assert isinstance(captured["stmt"], sa.sql.expression.Select)
