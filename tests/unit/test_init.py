"""Unit tests for aircontroler.__init__ — CORE-09."""
from __future__ import annotations


def test_provider_info():
    """CORE-09: get_provider_info() returns dict with required Airflow provider keys."""
    from aircontroler import get_provider_info
    info = get_provider_info()
    assert isinstance(info, dict)
    assert info["package-name"] == "airflow-provider-aircontroler"
    assert info["name"] == "aircontroler"
    assert "description" in info
    assert isinstance(info["versions"], list)
    assert len(info["versions"]) > 0
    assert "0.1.0" in info["versions"]


def test_version_exported():
    """CORE-09: __version__ is exported from aircontroler package."""
    import aircontroler
    assert hasattr(aircontroler, "__version__")
    assert isinstance(aircontroler.__version__, str)
    assert aircontroler.__version__ == "0.1.0"
