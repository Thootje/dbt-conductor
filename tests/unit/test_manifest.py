"""Unit tests for aircontroler.manifest — CORE-01, CORE-02."""
from __future__ import annotations
import json
import pytest
from pathlib import Path


def test_parse_v11(manifest_v11_data, tmp_path):
    """CORE-01: parse_manifest() with v11 schema returns correct ManifestNode list."""
    from aircontroler.manifest import parse_manifest, ManifestNode
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest_v11_data))
    nodes = parse_manifest(str(manifest_file))
    assert len(nodes) == 5
    node_map = {n.unique_id: n for n in nodes}
    assert "model.fixture_project.mart" in node_map
    mart = node_map["model.fixture_project.mart"]
    assert isinstance(mart, ManifestNode)
    assert mart.name == "mart"
    assert mart.resource_type == "model"
    assert set(mart.depends_on) == {
        "model.fixture_project.int_a",
        "model.fixture_project.int_b",
    }
    stg_a = node_map["model.fixture_project.stg_a"]
    assert stg_a.depends_on == []


def test_parse_v12(manifest_v12_data, tmp_path):
    """CORE-01: parse_manifest() with v12 schema returns same ManifestNode shape."""
    from aircontroler.manifest import parse_manifest
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest_v12_data))
    nodes = parse_manifest(str(manifest_file))
    assert len(nodes) == 5
    # unit_tests key in raw data must not cause errors
    node_ids = {n.unique_id for n in nodes}
    assert "model.fixture_project.mart" in node_ids


def test_unknown_version_raises(tmp_path):
    """CORE-01: parse_manifest() raises ValueError for unknown manifest version."""
    from aircontroler.manifest import parse_manifest
    data = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v99/manifest.json"
        },
        "nodes": {}
    }
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="v99"):
        parse_manifest(str(manifest_file))


def test_manifest_path_validated_at_parse_time(tmp_path):
    """Security: parse_manifest() raises immediately for nonexistent path."""
    from aircontroler.manifest import parse_manifest
    with pytest.raises((FileNotFoundError, OSError)):
        parse_manifest(str(tmp_path / "nonexistent.json"))


def test_compile_on_parse(tmp_path, mocker):
    """CORE-02: ManifestLoader.load(compile=True) calls dbt parse subprocess."""
    from aircontroler.manifest import ManifestLoader, ManifestNode
    import json as _json
    # Prepare a fake manifest that dbt parse would have emitted
    fake_manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json"
        },
        "nodes": {},
        "unit_tests": {}
    }
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(_json.dumps(fake_manifest))
    mock_run = mocker.patch("aircontroler.manifest.subprocess.run")
    mock_run.return_value = mocker.MagicMock(returncode=0, stderr="")
    loader = ManifestLoader()
    nodes = loader.load(
        project_dir=str(tmp_path),
        profiles_dir=str(tmp_path),
        compile=True,
        dbt_executable="dbt",
    )
    # Must have called subprocess.run exactly once
    assert mock_run.call_count == 1
    call_args = mock_run.call_args[0][0]  # first positional arg is the command list
    assert isinstance(call_args, list), "shell=True must never be used"
    assert "parse" in call_args
    assert "--project-dir" in call_args
    assert "--profiles-dir" in call_args
    # Returns list (empty in this case since nodes={})
    assert isinstance(nodes, list)
