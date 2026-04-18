"""Unit tests for aircontroler.__init__ — CORE-09."""
from __future__ import annotations
import pytest


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_provider_info():
    """CORE-09: get_provider_info() returns dict with required Airflow provider keys.

    Expected keys present: 'package-name', 'name', 'description', 'versions'
    Expected values:
      - package-name == 'airflow-provider-aircontroler'
      - name == 'aircontroler'
      - versions is a non-empty list
    """
    from aircontroler import get_provider_info
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_version_exported():
    """CORE-09: __version__ is exported from aircontroler package.

    Expected: aircontroler.__version__ is a non-empty string matching semver pattern.
    """
    import aircontroler
    raise NotImplementedError
