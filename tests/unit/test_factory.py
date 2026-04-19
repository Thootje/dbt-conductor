"""Unit tests for aircontroler.factory — CORE-04, CORE-05, D-11."""
from __future__ import annotations
import logging
import pytest


def _make_nodes(manifest_v11_data):
    """Helper: parse ManifestNodes from fixture data (calls real parse_manifest logic)."""
    from aircontroler.manifest import ManifestNode
    nodes = []
    for uid, node in manifest_v11_data["nodes"].items():
        deps = [n for n in node["depends_on"]["nodes"]
                if n.startswith(("model.", "seed.", "snapshot."))]
        nodes.append(ManifestNode(
            unique_id=node["unique_id"],
            name=node["name"],
            resource_type=node["resource_type"],
            depends_on=deps,
            fqn=node.get("fqn", []),
            tags=node.get("tags", []),
            original_file_path=node.get("original_file_path", ""),
            config=node.get("config", {}),
        ))
    return nodes


def test_task_per_model(mocker, manifest_v11_data, tmp_path):
    """CORE-04: One DbtRunModelOperator created per resolved model; task_id == unique_id."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    from aircontroler.operator import DbtRunModelOperator
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch(
        "aircontroler.factory.SelectorResolver.resolve",
        return_value=all_uids,
    )
    with DAG("test_dag", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    assert len(result["tasks"]) == 5
    for uid, task in result["tasks"].items():
        assert isinstance(task, DbtRunModelOperator)
        assert task.task_id == uid


def test_edge_wiring(mocker, manifest_v11_data, tmp_path):
    """CORE-05: Upstream/downstream edges mirror manifest depends_on."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_edges", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    tasks = result["tasks"]
    mart_upstream_ids = {t.task_id for t in tasks["model.fixture_project.mart"].upstream_list}
    assert "model.fixture_project.int_a" in mart_upstream_ids
    assert "model.fixture_project.int_b" in mart_upstream_ids
    int_a_upstream_ids = {t.task_id for t in tasks["model.fixture_project.int_a"].upstream_list}
    assert "model.fixture_project.stg_a" in int_a_upstream_ids
    stg_a_upstream_ids = {t.task_id for t in tasks["model.fixture_project.stg_a"].upstream_list}
    assert len(stg_a_upstream_ids) == 0


def test_entry_exit_classification(mocker, manifest_v11_data, tmp_path):
    """CORE-05: entry_tasks = models with no resolved upstream; exit_tasks = no resolved downstream."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_classify", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    entry_ids = {t.task_id for t in result["entry_tasks"]}
    assert entry_ids == {
        "model.fixture_project.stg_a",
        "model.fixture_project.stg_b",
    }
    exit_ids = {t.task_id for t in result["exit_tasks"]}
    assert exit_ids == {"model.fixture_project.mart"}


def test_service_account_warning(mocker, manifest_v11_data, tmp_path, caplog):
    """D-11: WARNING logged when service_account is not None; no exception raised."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch(
        "aircontroler.factory.SelectorResolver.resolve",
        return_value={n.unique_id for n in nodes},
    )
    with caplog.at_level(logging.WARNING, logger="aircontroler.factory"):
        with DAG("test_dag_sa", schedule=None) as dag:
            result = build_dbt_dag(
                dag=dag,
                manifest_path=str(tmp_path / "manifest.json"),
                dbt_command="dbt build --select +mart",
                project_dir=str(tmp_path),
                profiles_path=str(tmp_path),
                service_account="svc@project.iam.gserviceaccount.com",
            )
    warning_texts = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("service_account is not yet implemented" in t for t in warning_texts)
    # Must not raise; must return the result dict
    assert "tasks" in result


def test_cardinality_default_limit(mocker, manifest_v11_data, tmp_path):
    """EXEC-03: Default limit 800; ValueError raised when exceeded."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    fake_ids = {f"model.proj.model_{i}" for i in range(801)}
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=fake_ids)
    with DAG("test_dag_card_default", schedule=None) as dag:
        with pytest.raises(ValueError, match="801"):
            build_dbt_dag(
                dag=dag,
                manifest_path=str(tmp_path / "manifest.json"),
                dbt_command="dbt build --select +mart",
                project_dir=str(tmp_path),
                profiles_path=str(tmp_path),
            )


def test_cardinality_kwarg_override(mocker, manifest_v11_data, tmp_path):
    """EXEC-03: cardinality_limit kwarg overrides default (and env var)."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    fake_ids = {f"model.proj.model_{i}" for i in range(5)}
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=fake_ids)
    with DAG("test_dag_card_kwarg", schedule=None) as dag:
        with pytest.raises(ValueError, match=r"5.*\(limit: 3\)"):
            build_dbt_dag(
                dag=dag,
                manifest_path=str(tmp_path / "manifest.json"),
                dbt_command="dbt build --select +mart",
                project_dir=str(tmp_path),
                profiles_path=str(tmp_path),
                cardinality_limit=3,
            )


def test_cardinality_env_var(mocker, monkeypatch, manifest_v11_data, tmp_path):
    """EXEC-03: AIRCONTROLER_MAX_MODELS env var used when no kwarg."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    fake_ids = {f"model.proj.model_{i}" for i in range(11)}
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=fake_ids)
    monkeypatch.setenv("AIRCONTROLER_MAX_MODELS", "10")
    with DAG("test_dag_card_env", schedule=None) as dag:
        with pytest.raises(ValueError, match="11"):
            build_dbt_dag(
                dag=dag,
                manifest_path=str(tmp_path / "manifest.json"),
                dbt_command="dbt build --select +mart",
                project_dir=str(tmp_path),
                profiles_path=str(tmp_path),
            )


def test_cardinality_kwarg_overrides_env(mocker, monkeypatch, manifest_v11_data, tmp_path):
    """EXEC-03: kwarg precedence over env var (kwarg=20 > env=5; 10 ids → no raise)."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    fake_ids = {f"model.proj.model_{i}" for i in range(10)}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=fake_ids)
    monkeypatch.setenv("AIRCONTROLER_MAX_MODELS", "5")
    with DAG("test_dag_card_kwarg_over_env", schedule=None) as dag:
        # cardinality_limit=20 should win; 10 <= 20 → no raise
        build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            cardinality_limit=20,
        )


def test_cardinality_at_limit_passes(mocker, manifest_v11_data, tmp_path):
    """EXEC-03: Exactly at limit (count == limit) does NOT raise (strict >)."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    fake_ids = {f"model.proj.model_{i}" for i in range(5)}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=fake_ids)
    with DAG("test_dag_card_at_limit", schedule=None) as dag:
        # 5 == 5 → no raise
        build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            cardinality_limit=5,
        )


def test_cardinality_error_message(mocker, manifest_v11_data, tmp_path):
    """EXEC-03: Error message contains count and limit with comma formatting and helpful hint."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    fake_ids = {f"model.proj.model_{i}" for i in range(1500)}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=fake_ids)
    with DAG("test_dag_card_msg", schedule=None) as dag:
        with pytest.raises(ValueError) as exc_info:
            build_dbt_dag(
                dag=dag,
                manifest_path=str(tmp_path / "manifest.json"),
                dbt_command="dbt build --select +mart",
                project_dir=str(tmp_path),
                profiles_path=str(tmp_path),
                cardinality_limit=1000,
            )
    msg = str(exc_info.value)
    assert "1,500" in msg
    assert "1,000" in msg
    assert "Narrow your --select filter" in msg
    assert "cardinality_limit=N" in msg


def test_max_active_tasks_set(mocker, manifest_v11_data, tmp_path):
    """EXEC-04: max_active_tasks kwarg sets dag.max_active_tasks attribute."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_mat_set", schedule=None) as dag:
        build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            max_active_tasks=5,
        )
    assert dag.max_active_tasks == 5


def test_max_active_tasks_default_none(mocker, manifest_v11_data, tmp_path):
    """EXEC-04: max_active_tasks=None (default) does not modify dag attribute (stays at Airflow default 16)."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_mat_none", schedule=None) as dag:
        original_value = dag.max_active_tasks
        build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    # Attribute was NOT mutated by factory; equals whatever Airflow defaults to (16 in 3.2.0)
    assert dag.max_active_tasks == original_value
