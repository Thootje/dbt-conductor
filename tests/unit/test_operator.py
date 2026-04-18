"""Unit tests for aircontroler.operator — CORE-06, CORE-07, CORE-08."""
from __future__ import annotations
import pytest


@pytest.mark.skip(reason="implementation pending — Wave 2")
def test_import_compat():
    """CORE-08: BaseOperator imported from common-compat shim, not direct airflow.models.

    Expected: 'from airflow.providers.common.compat.sdk import BaseOperator' succeeds
    and DbtRunModelOperator is a subclass of that BaseOperator.
    """
    from aircontroler.operator import DbtRunModelOperator
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 2")
def test_subprocess_command(mocker):
    """CORE-06: execute() calls subprocess.run with correct list[str] command.

    Expected command: [dbt_executable, 'build', '--select', unique_id,
    '--project-dir', project_dir, '--profiles-dir', profiles_path]
    MUST NOT use shell=True (subprocess injection defense).
    """
    from aircontroler.operator import DbtRunModelOperator
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 2")
def test_custom_executable(mocker):
    """CORE-06: dbt_executable_path parameter is used when provided.

    Input: dbt_executable_path='/custom/path/dbt'
    Expected: subprocess.run first arg list starts with '/custom/path/dbt'
    """
    from aircontroler.operator import DbtRunModelOperator
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 2")
def test_failure_raises(mocker):
    """CORE-06: Non-zero dbt exit code raises AirflowException.

    Mock returncode=1
    Expected: AirflowException raised containing unique_id and exit code.
    """
    from aircontroler.operator import DbtRunModelOperator
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 2")
def test_profiles_path(mocker):
    """CORE-07: profiles_path is passed as --profiles-dir argument to subprocess.

    Expected: '--profiles-dir' and the profiles_path value appear in the subprocess
    command list. The profiles.yml file contents are NEVER logged (credential-leak defense).
    """
    from aircontroler.operator import DbtRunModelOperator
    raise NotImplementedError
