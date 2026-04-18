"""Unit tests for aircontroler.factory — CORE-04, CORE-05."""
from __future__ import annotations
import pytest


@pytest.mark.skip(reason="implementation pending — Wave 3")
def test_task_per_model(mocker, manifest_v11_data, tmp_path):
    """CORE-04: build_dbt_dag() creates one DbtRunModelOperator per resolved model.

    Setup: mock SelectorResolver.resolve() returning all 5 unique_ids from fixture,
    mock parse_manifest() returning 5 ManifestNodes.
    Expected: result['tasks'] has exactly 5 entries, each task_id equals its unique_id.
    """
    from aircontroler.factory import build_dbt_dag
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 3")
def test_edge_wiring(mocker, manifest_v11_data, tmp_path):
    """CORE-05: build_dbt_dag() wires upstream/downstream edges from manifest depends_on.

    Setup: same 5-node diamond mocks.
    Expected: mart task has int_a and int_b as upstream tasks.
    int_a has stg_a as upstream. int_b has stg_b as upstream.
    stg_a and stg_b have no upstream tasks.
    """
    from aircontroler.factory import build_dbt_dag
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 3")
def test_entry_exit_classification(mocker, manifest_v11_data, tmp_path):
    """CORE-05: build_dbt_dag() correctly classifies entry_tasks and exit_tasks.

    Setup: same 5-node diamond mocks.
    Expected: entry_tasks = [stg_a task, stg_b task] (2 items, order not guaranteed).
    exit_tasks = [mart task] (1 item).
    """
    from aircontroler.factory import build_dbt_dag
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 3")
def test_service_account_warning(mocker, manifest_v11_data, tmp_path, caplog):
    """D-11: build_dbt_dag() logs WARNING when service_account is not None.

    Input: service_account='svc@project.iam.gserviceaccount.com'
    Expected: WARNING log contains 'service_account is not yet implemented'.
    No exception raised.
    """
    from aircontroler.factory import build_dbt_dag
    raise NotImplementedError
