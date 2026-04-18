"""Unit tests for aircontroler.manifest — CORE-01, CORE-02."""
from __future__ import annotations
import pytest


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_parse_v11(manifest_v11_data):
    """CORE-01: parse_manifest() with v11 schema returns correct ManifestNode list.

    Expected: 5 nodes returned, each with correct unique_id, name, resource_type,
    depends_on list, fqn, tags, original_file_path, config fields populated.
    """
    from aircontroler.manifest import parse_manifest
    # Will write to tmp file and call parse_manifest(path)
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_parse_v12(manifest_v12_data):
    """CORE-01: parse_manifest() with v12 schema returns same ManifestNode shape.

    Expected: 5 nodes, unit_tests key in raw data is ignored safely.
    """
    from aircontroler.manifest import parse_manifest
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_unknown_version_raises(tmp_path):
    """CORE-01: parse_manifest() raises ValueError for unknown manifest_version.

    Expected: ValueError with message containing the unknown version string.
    """
    from aircontroler.manifest import parse_manifest
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_compile_on_parse(tmp_path, mocker):
    """CORE-02: ManifestLoader.load() with compile=True calls dbt parse subprocess.

    Expected: subprocess.run called with list args including 'parse',
    '--project-dir', '--profiles-dir'. shell=True never used.
    After call, reads manifest from {project_dir}/target/manifest.json.
    """
    from aircontroler.manifest import ManifestLoader
    raise NotImplementedError


@pytest.mark.skip(reason="implementation pending — Wave 1")
def test_manifest_path_validated_at_parse_time(tmp_path):
    """Security: parse_manifest() raises FileNotFoundError for nonexistent path.

    Expected: FileNotFoundError (or ValueError) raised immediately when path
    does not exist. Resolved absolute path logged (not file contents).
    """
    from aircontroler.manifest import parse_manifest
    raise NotImplementedError
