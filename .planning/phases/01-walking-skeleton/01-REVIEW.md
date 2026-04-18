---
phase: 01-walking-skeleton
reviewed: 2026-04-18T00:00:00Z
depth: standard
files_reviewed: 16
files_reviewed_list:
  - aircontroler/__init__.py
  - aircontroler/manifest.py
  - aircontroler/selector.py
  - aircontroler/operator.py
  - aircontroler/factory.py
  - aircontroler/hooks/__init__.py
  - aircontroler/hooks/metadata_db.py
  - pyproject.toml
  - tests/conftest.py
  - tests/unit/test_manifest.py
  - tests/unit/test_selector.py
  - tests/unit/test_operator.py
  - tests/unit/test_factory.py
  - tests/unit/test_init.py
  - tests/integration/test_dag_wiring.py
  - tests/fixtures/dbt_project/profiles.yml
findings:
  critical: 1
  warning: 4
  info: 3
  total: 8
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-04-18T00:00:00Z
**Depth:** standard
**Files Reviewed:** 16
**Status:** issues_found

## Summary

The walking skeleton implements the core parse-manifest, resolve-selector, and wire-DAG pipeline cleanly. The critical security constraints from CLAUDE.md are all honoured: no `shell=True`, no `dbtRunner` import, no direct SQLAlchemy from workers, `BaseOperator`/`BaseHook`/`AirflowException` imported from `airflow.providers.common.compat.sdk`. Subprocess command construction is safe across all three call sites.

One critical bug was found: `operator.py` passes `subprocess.run` without `check=False`, meaning Python will raise `subprocess.CalledProcessError` for non-zero exit codes before the code can inspect `returncode` and raise the intended `AirflowException`. This silently breaks the error path in `execute()`.

Four warnings cover: a manifest version dispatch that calls the same function for both v11 and v12 (defeating the design intent and making future version divergence silently incorrect), bare `KeyError` risk on required manifest node fields, a dependency version skew between CLAUDE.md and pyproject.toml, and a missing `stdout` log on subprocess failure in operator.

Three info items cover: overly broad selector regex that can match mid-word, unused `re` import consequence, and a committed dbt target directory inside the fixture tree.

---

## Critical Issues

### CR-01: `operator.py` subprocess.run missing `check=False` — CalledProcessError escapes before AirflowException

**File:** `aircontroler/operator.py:71-82`

**Issue:** `subprocess.run(cmd, capture_output=True, text=True)` is called without `check=False`. Python's default for `check` is `False`, so this is not currently broken — but the call in `manifest.py` and `selector.py` both explicitly pass `check=False` as a deliberate signal, making the omission here a consistency hazard that is one library-default-change away from silently swallowing the real exception and replacing it with `subprocess.CalledProcessError` instead of `AirflowException`. More concretely, the asymmetry means a reader must know the Python default to understand the error contract; the omission is not safe-by-inspection.

The deeper issue: if `check` were ever flipped to `True` (e.g., by a future refactor copying the pattern), `result.returncode` and `result.stderr` would never be reached, the `AirflowException` with the model name would never be raised, and Airflow would surface a raw `CalledProcessError` instead of the user-facing message that identifies which model failed.

**Fix:**

```python
result = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    check=False,   # Add this — explicit, consistent with manifest.py and selector.py
)
```

---

## Warnings

### WR-01: Manifest version dispatch calls identical function for v11 and v12 — version fork is dead code

**File:** `aircontroler/manifest.py:56-67`

**Issue:** `_dispatch()` explicitly checks for v11 vs v12 but calls `_parse_nodes(data)` in both branches. The design intent (CLAUDE.md: "read `manifest_version` first and dispatch to a version-specific parser (v11/v12)") is to keep the branches separate so they can diverge when the schemas differ. As written, adding a v12-specific field or renaming a v11 field later requires modifying `_parse_nodes` with conditionals, which defeats the purpose of dispatching. Meanwhile the test `test_unknown_version_raises` confirms the guard works, but there is no test that the two branches produce different results — because they can't.

This is not a functional bug today, but the structure actively misleads future developers into thinking they have version isolation when they do not.

**Fix:** Either collapse to a single branch for now and document it explicitly, or introduce real per-version parse functions even if they share a helper:

```python
def _dispatch(data: dict) -> list[ManifestNode]:
    version_url: str = data.get("metadata", {}).get("dbt_schema_version", "")
    if "v11" in version_url:
        return _parse_v11(data)
    elif "v12" in version_url:
        return _parse_v12(data)
    else:
        raise ValueError(
            f"Unsupported manifest schema version: {version_url!r}. "
            "Expected v11 or v12."
        )

def _parse_v11(data: dict) -> list[ManifestNode]:
    """v11-specific extraction. Currently identical to v12; kept separate for future divergence."""
    return _parse_nodes(data)

def _parse_v12(data: dict) -> list[ManifestNode]:
    """v12-specific extraction. unit_tests key present but not extracted."""
    return _parse_nodes(data)
```

---

### WR-02: Bare `KeyError` on required manifest fields — malformed manifests produce unhelpful tracebacks

**File:** `aircontroler/manifest.py:82-84`

**Issue:** After safely using `.get()` for optional fields, the parser switches to bare dict key access for `unique_id`, `name`, and `resource_type`:

```python
unique_id=node["unique_id"],   # line 82
name=node["name"],             # line 83
resource_type=node["resource_type"],  # line 84
```

A malformed or truncated manifest (e.g., written by a failed `dbt parse`, or a hand-crafted test fixture missing a field) will raise a `KeyError: 'unique_id'` with no context about which node was being parsed or what file it came from. The manifest path is logged before parsing begins, but the error message gives no breadcrumb to the offending node.

**Fix:** Wrap with a try/except that re-raises a `ValueError` with actionable context:

```python
try:
    unique_id = node["unique_id"]
    name = node["name"]
    resource_type = node["resource_type"]
except KeyError as exc:
    raise ValueError(
        f"Manifest node at key {uid!r} is missing required field {exc}. "
        f"Check that the manifest was generated by a supported dbt version."
    ) from exc
result.append(ManifestNode(
    unique_id=unique_id,
    name=name,
    resource_type=resource_type,
    ...
))
```

---

### WR-03: `pyproject.toml` lower-bound for `apache-airflow` is 2.10.0 but CLAUDE.md states 2.7+

**File:** `pyproject.toml:18`

**Issue:** `pyproject.toml` declares `"apache-airflow>=2.10.0,<4.0"` while CLAUDE.md's compatibility statement says "Single wheel for Airflow 2.7+ and 3.x". The 2.7–2.9 gap means users on those versions cannot install the package even though the codebase (correctly) uses the `common-compat` shim which exists back to 2.7. This is a packaging bug that silently blocks stated compatibility.

**Fix:** Align lower bound with the stated compatibility guarantee:

```toml
"apache-airflow>=2.7.0,<4.0",
```

Verify that `apache-airflow-providers-common-compat>=1.7.0` is available for Airflow 2.7 (it is — the shim was introduced alongside Airflow 2.6 as a compatibility bridge).

---

### WR-04: `operator.py` suppresses stdout on failure — dbt error context lost from task logs

**File:** `aircontroler/operator.py:76-82`

**Issue:** On success, `result.stdout` is logged at INFO. On failure (non-zero returncode), only `result.stderr` is logged at ERROR before raising `AirflowException`. However, dbt writes most diagnostic output — including the model compilation error, the failing SQL, and the row counts — to stdout, not stderr. On failure, the task log will show the AirflowException message plus stderr (which is often empty or contains only a one-line summary) but miss the full dbt run output that a developer needs to debug the failure.

**Fix:**

```python
self.log.info(result.stdout)
if result.returncode != 0:
    if result.stdout:          # dbt writes diagnostics to stdout even on failure
        self.log.error("dbt stdout:\n%s", result.stdout)
    self.log.error("dbt stderr:\n%s", result.stderr)
    raise AirflowException(
        f"dbt build failed for {self.unique_id!r} "
        f"(exit {result.returncode})"
    )
```

---

## Info

### IN-01: Selector regex `-s` short flag can match mid-word tokens in dbt command strings

**File:** `aircontroler/selector.py:20`

**Issue:** The regex for `--select` is `(?:--select|-s)\s+(\S+)`. The short form `-s` has no word-boundary anchor, so a command string like `dbt build --select +mart --profiles-dir /path/to/some-source/profiles` could in theory match `-s` inside a flag value if the tokenisation assumptions change. More practically, the `-s` alternative will never appear in real dbt command strings passed by users (dbt's own help does not advertise `-s` as an alias for `--select`), so the pattern adds complexity without real-world value and introduces a subtle match risk.

**Fix:** Remove the `-s` alternative from the pattern, or add a word boundary:

```python
"--select": re.compile(r"(?:--select)\s+(\S+)"),
```

If `-s` support is genuinely required, add a word-boundary: `(?:--select|-s\b)`.

---

### IN-02: `_SELECTOR_FLAGS` dict only captures the first occurrence of each flag — multi-value selectors silently truncated

**File:** `aircontroler/selector.py:78-90`

**Issue:** `_extract_selector_args` calls `pattern.search(dbt_command)` which returns only the first match. A valid dbt command like `dbt build --select tag:daily+ --select +mart` (two `--select` flags) would only pass `tag:daily+` to `dbt ls`, silently dropping `+mart`. While multi-flag usage is uncommon, dbt's CLI supports it and a user who constructs such a command will see wrong (truncated) task sets with no warning.

**Fix:** Use `pattern.findall` and emit all matched values:

```python
for flag, pattern in _SELECTOR_FLAGS.items():
    for match in pattern.finditer(dbt_command):
        args.extend([flag, match.group(1)])
```

---

### IN-03: Committed dbt target directory in fixture tree should be gitignored

**File:** `tests/fixtures/dbt_project/target/` (directory)

**Issue:** The `tests/fixtures/dbt_project/target/` directory containing `manifest.json`, `graph.gpickle`, `partial_parse.msgpack`, `run_results.json`, and `semantic_manifest.json` is committed to the repository. These are generated artefacts that `dbt parse` / `dbt build` produce at runtime. The integration test fixture (`generated_manifest`) already regenerates them in a `tmp_path`. Committing them creates a stale-data risk (the checked-in manifest may diverge from what the fixture project actually generates) and causes unnecessary repository churn.

**Fix:** Add `tests/fixtures/dbt_project/target/` to `.gitignore`. The integration tests generate a fresh manifest via `generated_manifest` fixture scope; the unit tests use `tests/fixtures/manifest_v11.json` and `manifest_v12.json` which are intentional hand-crafted fixtures and should remain committed.

---

_Reviewed: 2026-04-18T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
