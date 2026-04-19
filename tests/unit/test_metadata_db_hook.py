"""Unit tests for aircontroler.hooks.metadata_db — FRESH-04."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _mock_session_ctx(mocker, session_mock):
    """Build a MagicMock that behaves like `with create_session() as session:`."""
    mock_cm = mocker.MagicMock()
    mock_cm.__enter__ = mocker.MagicMock(return_value=session_mock)
    mock_cm.__exit__ = mocker.MagicMock(return_value=False)
    return mock_cm


def test_get_last_run_returns_none_when_no_rows(mocker):
    """FRESH-04: get_last_run returns None when no matching row exists (Nyquist dim 3/8 at hook level)."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.execute.return_value.scalar.return_value = None
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    assert hook.get_last_run("model.proj.stg_a") is None


def test_get_last_run_returns_timestamp(mocker):
    """FRESH-04: get_last_run returns the MAX(completed_at) timestamp."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    expected = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)
    mock_session = mocker.MagicMock()
    mock_session.execute.return_value.scalar.return_value = expected
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    assert hook.get_last_run("model.proj.stg_a") == expected


def test_get_last_run_uses_parameterized_query(mocker):
    """Security T-3-01: get_last_run uses SQLAlchemy Select, not string-formatted SQL.

    A hostile unique_id must reach the DB as a bind parameter, not embedded SQL.
    """
    import sqlalchemy as sa
    from aircontroler.hooks.metadata_db import MetadataDBHook
    captured: dict = {}
    def capture(stmt, *args, **kwargs):
        captured["stmt"] = stmt
        result = mocker.MagicMock()
        result.scalar.return_value = None
        return result
    mock_session = mocker.MagicMock()
    mock_session.execute.side_effect = capture
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    hostile = "'; DROP TABLE aircontroler_model_runs; --"
    hook.get_last_run(hostile)
    stmt = captured["stmt"]
    # Must be a Select (ORM core), not a str or TextClause
    assert isinstance(stmt, sa.sql.expression.Select), f"Expected Select, got {type(stmt)}"
    # Compiled SQL MUST NOT contain the hostile string — it should be a bind parameter
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    assert hostile not in compiled


def test_record_run_inserts_row(mocker):
    """FRESH-04: record_run inserts a row — Nyquist dim 4 (success records run)."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_1", run_id="run_1")
    assert mock_session.execute.call_count == 1


def test_record_run_values_include_all_fields(mocker):
    """FRESH-04 / D-05: record_run writes unique_id, dag_id, run_id, and completed_at."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    captured: dict = {}
    def capture(stmt, *args, **kwargs):
        # Insert statement: .values() sets parameters dict; extract via _values cache
        captured["params"] = stmt.compile().params
        return mocker.MagicMock()
    mock_session = mocker.MagicMock()
    mock_session.execute.side_effect = capture
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    hook.record_run(unique_id="model.proj.stg_a", dag_id="dag_1", run_id="run_1")
    params = captured["params"]
    assert params["unique_id"] == "model.proj.stg_a"
    assert params["dag_id"] == "dag_1"
    assert params["run_id"] == "run_1"
    assert isinstance(params["completed_at"], datetime)
    # D-04: completed_at is timezone-aware
    assert params["completed_at"].tzinfo is not None


def test_get_conn_not_supported():
    """MetadataDBHook uses create_session, not get_conn (documented NotImplementedError)."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    hook = MetadataDBHook()
    with pytest.raises(NotImplementedError, match="create_session"):
        hook.get_conn()


# ── Phase 4: LOCK-02 / LOCK-04 hook method stubs ─────────────────────────


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_acquire_lock_returns_true_on_success(mocker):
    """LOCK-02: acquire_lock returns True when pg_try_advisory_xact_lock returns True."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.get_bind.return_value.dialect.name = "postgresql"
    mock_session.execute.return_value.scalar.return_value = True
    hook = MetadataDBHook()
    assert hook.acquire_lock("model.proj.stg_a", mock_session) is True


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_acquire_lock_returns_false_on_contention(mocker):
    """LOCK-02: acquire_lock returns False when pg_try_advisory_xact_lock returns False."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.get_bind.return_value.dialect.name = "postgresql"
    mock_session.execute.return_value.scalar.return_value = False
    hook = MetadataDBHook()
    assert hook.acquire_lock("model.proj.stg_a", mock_session) is False


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_acquire_lock_sqlite_dialect_returns_true_with_warning(mocker):
    """LOCK-02 / D-13: On non-Postgres dialect, acquire_lock logs WARNING and returns True."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.get_bind.return_value.dialect.name = "sqlite"
    hook = MetadataDBHook()
    mock_log = mocker.MagicMock()
    mocker.patch.object(MetadataDBHook, "log", new_callable=mocker.PropertyMock, return_value=mock_log)
    result = hook.acquire_lock("model.proj.stg_a", mock_session)
    assert result is True
    mock_log.warning.assert_called_once()


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_acquire_lock_uses_adler32_hash_not_unique_id_in_sql(mocker):
    """Security T-4-01: acquire_lock passes unique_id hash as bind param, never raw string in SQL."""
    import sqlalchemy as sa
    from aircontroler.hooks.metadata_db import MetadataDBHook
    captured: dict = {}

    def capture(stmt, params=None, **kwargs):
        captured["stmt"] = stmt
        captured["params"] = params
        result = mocker.MagicMock()
        result.scalar.return_value = True
        return result

    mock_session = mocker.MagicMock()
    mock_session.get_bind.return_value.dialect.name = "postgresql"
    mock_session.execute.side_effect = capture
    hook = MetadataDBHook()
    hook.acquire_lock("model.proj.stg_a", mock_session)
    # unique_id must NOT appear verbatim in the SQL text
    stmt_str = str(captured["stmt"])
    assert "model.proj.stg_a" not in stmt_str


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_upsert_lock_holder_writes_row(mocker):
    """LOCK-04 / D-11: upsert_lock_holder commits identity row in a micro-transaction."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    hook.upsert_lock_holder(unique_id="model.proj.stg_a", dag_id="dag_1", run_id="run_1")
    assert mock_session.execute.call_count == 1


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_delete_lock_holder_removes_row(mocker):
    """LOCK-04 / D-12: delete_lock_holder executes a delete via its own micro-transaction."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mocker.patch(
        "aircontroler.hooks.metadata_db.create_session",
        return_value=_mock_session_ctx(mocker, mock_session),
    )
    hook = MetadataDBHook()
    hook.delete_lock_holder("model.proj.stg_a")
    assert mock_session.execute.call_count == 1


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_get_lock_holder_returns_row(mocker):
    """LOCK-04 / D-11: get_lock_holder returns a row-like object with dag_id and run_id."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    fake_row = mocker.MagicMock()
    fake_row.dag_id = "dag_1"
    fake_row.run_id = "run_1"
    mock_session.execute.return_value.fetchone.return_value = fake_row
    hook = MetadataDBHook()
    row = hook.get_lock_holder("model.proj.stg_a", mock_session)
    assert row is not None
    assert row.dag_id == "dag_1"


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_get_last_run_accepts_session_param(mocker):
    """LOCK-02 / D-08: get_last_run(unique_id, session=session) uses the provided session."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    mock_session.execute.return_value.scalar.return_value = None
    hook = MetadataDBHook()
    # When session is provided, create_session must NOT be called
    hook.get_last_run("model.proj.stg_a", session=mock_session)
    assert mock_session.execute.call_count == 1


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_record_run_accepts_session_param(mocker):
    """LOCK-02 / D-08: record_run(..., session=session) uses the provided session."""
    from aircontroler.hooks.metadata_db import MetadataDBHook
    mock_session = mocker.MagicMock()
    hook = MetadataDBHook()
    hook.record_run(unique_id="model.proj.stg_a", dag_id="d", run_id="r", session=mock_session)
    assert mock_session.execute.call_count == 1
