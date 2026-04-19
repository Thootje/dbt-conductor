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


def test_db_managers_key_registers_aircontroler_db_manager():
    """FRESH-06: get_provider_info() exposes db-managers pointing to AircontrolerDBManager."""
    from aircontroler import get_provider_info
    info = get_provider_info()
    assert "db-managers" in info, "db-managers key (with hyphen) must be present"
    assert info["db-managers"] == ["aircontroler.db_manager.AircontrolerDBManager"]


def test_db_managers_key_is_hyphenated():
    """FRESH-06: key is 'db-managers' (hyphen per provider_info.schema.json), NOT 'db_managers'."""
    from aircontroler import get_provider_info
    info = get_provider_info()
    assert "db_managers" not in info, "Must use hyphen (per Airflow schema), not underscore"
