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


def test_meta_forwarding_to_operator(mocker, manifest_with_meta_data, tmp_path):
    """EXEC-01: Factory reads meta.aircontroler from manifest and passes to operator."""
    from datetime import timedelta
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_with_meta_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_meta_fwd", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    mart_task = result["tasks"]["model.fixture_project.mart"]
    assert mart_task.retries == 3
    assert mart_task.retry_delay == timedelta(seconds=300)
    assert mart_task.pool == "heavy_models"


def test_no_meta_node_uses_defaults(mocker, manifest_with_meta_data, tmp_path):
    """EXEC-01: Nodes without meta.aircontroler get BaseOperator defaults (retries=0)."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_with_meta_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_no_meta", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    stg_a = result["tasks"]["model.fixture_project.stg_a"]
    # stg_a has no meta.aircontroler in the fixture → BaseOperator defaults
    assert stg_a.retries == 0


def test_per_model_pool_overrides_dag_pool(mocker, manifest_with_meta_data, tmp_path):
    """EXEC-01 D-16: per-model meta.aircontroler.pool overrides DAG-level pool param."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_with_meta_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_pool_override", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            pool="dag_default_pool",
        )
    # mart has meta.aircontroler.pool=heavy_models → wins over dag_default_pool
    assert result["tasks"]["model.fixture_project.mart"].pool == "heavy_models"
    # stg_a has no meta.aircontroler → falls back to DAG-level pool
    assert result["tasks"]["model.fixture_project.stg_a"].pool == "dag_default_pool"


def test_dag_pool_applied_to_all(mocker, manifest_v11_data, tmp_path):
    """EXEC-04 + EXEC-01: DAG-level pool applies to all tasks when no per-model overrides."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_pool_all", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            pool="dag_pool",
        )
    for task in result["tasks"].values():
        assert task.pool == "dag_pool"


def test_dag_pool_none_leaves_unset(mocker, manifest_v11_data, tmp_path):
    """EXEC-04: pool=None (default) leaves tasks at BaseOperator default pool."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_pool_unset", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    # BaseOperator default for pool is "default_pool" string in modern Airflow.
    # The key assertion: factory did NOT inject any custom pool.
    for task in result["tasks"].values():
        assert task.pool in ("default_pool", None)


def test_pool_slots_applied_when_pool_set(mocker, manifest_with_meta_data, tmp_path):
    """EXEC-04: pool_slots flows to tasks that have a pool; defaults to 1 for unpooled."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_with_meta_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_pool_slots", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            pool="dag_pool",
            pool_slots=4,
        )
    # All tasks get pool_slots=4 because they all have a pool (mart=heavy_models, others=dag_pool)
    for task in result["tasks"].values():
        assert task.pool_slots == 4


def test_target_path_forwarded_to_all_tasks(mocker, manifest_v11_data, tmp_path):
    """EXEC-01/EXEC-02: target_path on factory propagates to every operator instance."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_target_path", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            target_path="/custom/target",
        )
    for task in result["tasks"].values():
        assert task.target_path == "/custom/target"


def test_freshness_key_ignored(mocker, manifest_v11_data, tmp_path):
    """EXEC-01 D-04: Unrecognized meta.aircontroler keys (e.g., 'freshness') are ignored."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    # Inject a fake meta.aircontroler.freshness on one node (Phase 3 reserves this key)
    nodes = _make_nodes(manifest_v11_data)
    nodes[0].config["meta"] = {"aircontroler": {"freshness": 24}}  # unknown key
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    # Should not raise — freshness is silently ignored
    with DAG("test_dag_freshness_ignored", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    # Task is created normally; no retries from the unknown key
    target_uid = nodes[0].unique_id
    assert target_uid in result["tasks"]
    assert result["tasks"][target_uid].retries == 0


# ----------------------------------------------------------------------
# Phase 3 — FRESH-01 / FRESH-03 — factory wiring for freshness + trigger_rule
# ----------------------------------------------------------------------


def _stub_resolver(mocker, uids):
    """Patch SelectorResolver.resolve to return the given set of unique_ids."""
    mocker.patch(
        "aircontroler.factory.SelectorResolver.resolve",
        return_value=set(uids),
    )


def _stub_manifest(mocker, manifest_dict):
    """Patch parse_manifest to return ManifestNode list from the given manifest dict."""
    from aircontroler.manifest import ManifestNode
    nodes = []
    for uid, raw in manifest_dict["nodes"].items():
        nodes.append(
            ManifestNode(
                unique_id=uid,
                name=raw["name"],
                resource_type=raw["resource_type"],
                depends_on=raw.get("depends_on", {}).get("nodes", []),
                fqn=raw.get("fqn", []),
                tags=raw.get("tags", []),
                original_file_path=raw.get("original_file_path", ""),
                config=raw.get("config", {}),
            )
        )
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)


def test_freshness_hours_extracted_from_manifest(mocker, manifest_with_freshness_data):
    """FRESH-01 / D-10: factory extracts meta.aircontroler.freshness_hours per node."""
    from airflow import DAG
    from aircontroler.factory import build_dbt_dag
    _stub_manifest(mocker, manifest_with_freshness_data)
    _stub_resolver(mocker, manifest_with_freshness_data["nodes"].keys())
    dag = DAG("test_dag", schedule=None, start_date=None)
    result = build_dbt_dag(
        dag=dag,
        manifest_path="/not/used",
        dbt_command="dbt build --select tag:all",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    # stg_a has freshness_hours: 6 in the fixture
    assert result["tasks"]["model.fixture_project.stg_a"].freshness_hours == 6
    # mart has freshness_hours: 24
    assert result["tasks"]["model.fixture_project.mart"].freshness_hours == 24
    # stg_b has no meta.aircontroler → freshness_hours is None
    assert result["tasks"]["model.fixture_project.stg_b"].freshness_hours is None


def test_trigger_rule_default_is_none_failed(mocker, manifest_v12_data):
    """D-02: default trigger_rule for all generated tasks is 'none_failed'."""
    from airflow import DAG
    from aircontroler.factory import build_dbt_dag
    _stub_manifest(mocker, manifest_v12_data)
    _stub_resolver(mocker, manifest_v12_data["nodes"].keys())
    dag = DAG("test_dag", schedule=None, start_date=None)
    result = build_dbt_dag(
        dag=dag,
        manifest_path="/not/used",
        dbt_command="dbt build --select tag:all",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    for task in result["tasks"].values():
        assert task.trigger_rule == "none_failed"


def test_trigger_rule_dag_level_override(mocker, manifest_v12_data):
    """D-03: build_dbt_dag(trigger_rule=...) sets default for all tasks in that DAG."""
    from airflow import DAG
    from aircontroler.factory import build_dbt_dag
    _stub_manifest(mocker, manifest_v12_data)
    _stub_resolver(mocker, manifest_v12_data["nodes"].keys())
    dag = DAG("test_dag", schedule=None, start_date=None)
    result = build_dbt_dag(
        dag=dag,
        manifest_path="/not/used",
        dbt_command="dbt build --select tag:all",
        project_dir="/proj",
        profiles_path="/profiles",
        trigger_rule="all_success",
    )
    for task in result["tasks"].values():
        assert task.trigger_rule == "all_success"


def test_trigger_rule_per_model_overrides_dag_level(mocker):
    """D-03: meta.aircontroler.trigger_rule on a specific node overrides DAG-level."""
    from airflow import DAG
    from aircontroler.factory import build_dbt_dag
    # Build a minimal manifest inline where stg_a has meta.aircontroler.trigger_rule="all_done"
    manifest = {
        "nodes": {
            "model.p.stg_a": {
                "unique_id": "model.p.stg_a",
                "name": "stg_a",
                "resource_type": "model",
                "depends_on": {"nodes": []},
                "fqn": ["p", "stg_a"],
                "tags": [],
                "original_file_path": "models/stg_a.sql",
                "config": {
                    "materialized": "view",
                    "meta": {"aircontroler": {"trigger_rule": "all_done"}},
                },
            },
            "model.p.stg_b": {
                "unique_id": "model.p.stg_b",
                "name": "stg_b",
                "resource_type": "model",
                "depends_on": {"nodes": []},
                "fqn": ["p", "stg_b"],
                "tags": [],
                "original_file_path": "models/stg_b.sql",
                "config": {"materialized": "view"},
            },
        }
    }
    _stub_manifest(mocker, manifest)
    _stub_resolver(mocker, manifest["nodes"].keys())
    dag = DAG("test_dag", schedule=None, start_date=None)
    result = build_dbt_dag(
        dag=dag,
        manifest_path="/not/used",
        dbt_command="dbt build --select tag:all",
        project_dir="/proj",
        profiles_path="/profiles",
        trigger_rule="none_failed",
    )
    assert result["tasks"]["model.p.stg_a"].trigger_rule == "all_done"
    assert result["tasks"]["model.p.stg_b"].trigger_rule == "none_failed"


def test_none_failed_permits_skipped_upstream_topology(mocker, manifest_v12_data):
    """Nyquist dim 7: DAG topology — none_failed means downstream CAN run even when upstream is SKIPPED.

    This test verifies the *topology* — that the generated tasks carry the
    none_failed trigger_rule which Airflow treats as "run if no upstream failed,
    including when upstreams are skipped". The actual runtime behavior is an
    Airflow-scheduler concern; here we verify the operator configuration is correct.
    """
    from airflow import DAG
    from aircontroler.factory import build_dbt_dag
    _stub_manifest(mocker, manifest_v12_data)
    _stub_resolver(mocker, manifest_v12_data["nodes"].keys())
    dag = DAG("test_dag", schedule=None, start_date=None)
    result = build_dbt_dag(
        dag=dag,
        manifest_path="/not/used",
        dbt_command="dbt build --select tag:all",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    # mart depends on int_a and int_b (a diamond) — under none_failed, mart runs
    # even if int_a was skipped (fresh). Assert mart's trigger_rule and that it has
    # resolved upstreams in the generated DAG.
    mart = result["tasks"]["model.fixture_project.mart"]
    int_a = result["tasks"]["model.fixture_project.int_a"]
    int_b = result["tasks"]["model.fixture_project.int_b"]
    assert mart.trigger_rule == "none_failed"
    # Validate DAG edge wiring — mart's upstreams contain int_a and int_b
    upstream_ids = {t.task_id for t in mart.upstream_list}
    assert "model.fixture_project.int_a" in upstream_ids
    assert "model.fixture_project.int_b" in upstream_ids


def test_freshness_with_meta_preserves_phase2_overrides(mocker, manifest_with_freshness_data):
    """Phase 2 regression: freshness_hours extraction must coexist with retries/retry_delay/pool."""
    from airflow import DAG
    from aircontroler.factory import build_dbt_dag
    _stub_manifest(mocker, manifest_with_freshness_data)
    _stub_resolver(mocker, manifest_with_freshness_data["nodes"].keys())
    dag = DAG("test_dag", schedule=None, start_date=None)
    result = build_dbt_dag(
        dag=dag,
        manifest_path="/not/used",
        dbt_command="dbt build --select tag:all",
        project_dir="/proj",
        profiles_path="/profiles",
    )
    # mart has retries=3, retry_delay=300, pool=heavy_models, freshness_hours=24
    mart = result["tasks"]["model.fixture_project.mart"]
    assert mart.freshness_hours == 24
    assert mart.pool == "heavy_models"
    assert mart.retries == 3


# ── Phase 4: LOCK-01 factory conflict_policy stubs ────────────────────────


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_dag_level_conflict_policy_forwarded_to_all_tasks(mocker, manifest_v11_data, tmp_path):
    """LOCK-01 / D-01: DAG-level conflict_policy forwarded to all DbtRunModelOperator instances."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_conflict", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
            conflict_policy="skip",
        )
    for task in result["tasks"].values():
        assert task.conflict_policy == "skip"


@pytest.mark.xfail(strict=False, reason="Wave 1 implementation pending")
def test_conflict_policy_none_means_no_locking(mocker, manifest_v11_data, tmp_path):
    """LOCK-01 / D-02: Default conflict_policy=None means no locking on any task."""
    from airflow.models import DAG
    from aircontroler.factory import build_dbt_dag
    nodes = _make_nodes(manifest_v11_data)
    all_uids = {n.unique_id for n in nodes}
    mocker.patch("aircontroler.factory.parse_manifest", return_value=nodes)
    mocker.patch("aircontroler.factory.SelectorResolver.resolve", return_value=all_uids)
    with DAG("test_dag_no_lock", schedule=None) as dag:
        result = build_dbt_dag(
            dag=dag,
            manifest_path=str(tmp_path / "manifest.json"),
            dbt_command="dbt build --select +mart",
            project_dir=str(tmp_path),
            profiles_path=str(tmp_path),
        )
    for task in result["tasks"].values():
        assert task.conflict_policy is None
