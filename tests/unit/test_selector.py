"""Unit tests for aircontroler.selector — CORE-03."""
from __future__ import annotations
import pytest


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_extract_select(mocker):
    """CORE-03: --select flag and value extracted from dbt command string.

    Input: 'dbt build --select tag:daily+ --profile prod'
    Expected: dbt ls called with ['--select', 'tag:daily+'] in args.
    """
    from aircontroler.selector import SelectorResolver
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_extract_exclude(mocker):
    """CORE-03: --exclude flag and value extracted from dbt command string.

    Input: 'dbt build --select +mart --exclude raw_*'
    Expected: dbt ls called with ['--select', '+mart', '--exclude', 'raw_*'] in args.
    """
    from aircontroler.selector import SelectorResolver
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_extract_selector(mocker):
    """CORE-03: --selector flag and value extracted from dbt command string.

    Input: 'dbt build --selector my_yaml_selector'
    Expected: dbt ls called with ['--selector', 'my_yaml_selector'] in args.
    """
    from aircontroler.selector import SelectorResolver
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_parse_ls_output(mocker):
    """CORE-03: SelectorResolver.resolve() parses NDJSON output from dbt ls correctly.

    Mock stdout: two JSON lines + one non-JSON log line (dbt sometimes emits these).
    Expected: set of two unique_ids returned, log line silently skipped.
    dbt ls command built as list[str] with no shell=True (subprocess-injection defense).
    """
    from aircontroler.selector import SelectorResolver
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_ls_failure_raises(mocker):
    """CORE-03: Non-zero dbt ls exit code raises RuntimeError with stderr message.

    Mock returncode=1, stderr='dbt ls failed: project not found'
    Expected: RuntimeError raised containing the stderr text.
    """
    from aircontroler.selector import SelectorResolver
    raise NotImplementedError
