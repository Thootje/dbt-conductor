# Phase 3: Freshness Policies - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-19
**Phase:** 03-freshness-policies
**Areas discussed:** Skip signal behavior, State table design, Per-DAG TTL override API, Hook DB access internals

---

## Skip signal behavior

| Option | Description | Selected |
|--------|-------------|----------|
| AirflowSkipException | Task shows yellow 'skipped' in Airflow UI. Visually distinct from executions. | ✓ |
| Return early (success) | Task shows green 'success'. Indistinguishable from real dbt run. | |

**User's choice:** AirflowSkipException
**Notes:** None

---

## Skip cascade (trigger_rule)

| Option | Description | Selected |
|--------|-------------|----------|
| none_failed as default | Downstream runs even if some upstreams were skipped (fresh). | ✓ |
| all_success (Airflow default) | Skipped upstream cascades skip to downstream. | |

**User's choice:** `none_failed` as the default trigger_rule for all generated tasks

**User clarification:** "If a model A depends on 2 models, 1 is fresh and will be skipped, the other is not fresh, I want the operator to skip the upstream fresh model but do run the downstream model A as there was new data coming in. I also want the user to be able to configure such rules themselves."

---

## trigger_rule config surface

| Option | Description | Selected |
|--------|-------------|----------|
| DAG-level param + per-model meta override | build_dbt_dag(trigger_rule=...) + meta.aircontroler.trigger_rule | ✓ |
| DAG-level param only | Global override only, no per-model granularity | |
| Claude's discretion | | |

**User's choice:** DAG-level param + per-model meta override (same pattern as pool/retries from Phase 2)

---

## State table design

| Option | Description | Selected |
|--------|-------------|----------|
| Latest-only: one row per unique_id | Updated in-place. Simple freshness check. | |
| Full history: one row per execution | Append-only. Enables audit trail and future cleanup CLI. | ✓ |

**User's choice:** Full history (append-only)
**Notes:** Enables v2 cleanup command (OBS-V2-02) without schema changes.

---

## dag_id / run_id columns

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — store dag_id and run_id | Enables cross-DAG attribution. | ✓ |
| No — just unique_id and timestamp | Minimal schema. | |

**User's choice:** Yes — store dag_id and run_id

---

## When to write a row

| Option | Description | Selected |
|--------|-------------|----------|
| Success only — after dbt exits 0 | Lean table; freshness query needs no status filter. | ✓ |
| Success and failure | More complete audit; status column required. | |

**User's choice:** Success only

---

## Per-DAG TTL override API (FRESH-02)

| Option | Description | Selected |
|--------|-------------|----------|
| Dict param: freshness_overrides={'unique_id': hours} | Python override per-DAG | |
| No Python override — schema.yml only | Defer FRESH-02 from Phase 3 | ✓ |

**User's choice:** Defer FRESH-02 from Phase 3. Schema.yml `freshness_hours` only.

**User clarification:** Asked "What is TTL?" — explained as the freshness window in hours. After clarification, user confirmed deferral of per-DAG override.

---

## FRESH-05 (longest TTL wins)

Deferred as a consequence of deferring FRESH-02. With only one TTL per model (from schema.yml), there is no cross-DAG TTL conflict to resolve. Trivially satisfied in Phase 3.

---

## Hook DB access internals

| Option | Description | Selected |
|--------|-------------|----------|
| Airflow internal create_session | Zero-config. Works like Airflow's own providers. | ✓ |
| Require aircontroler_default connection | Explicit but requires user setup of redundant connection. | |
| AIRFLOW__DATABASE__SQL_ALCHEMY_CONN env var | Bypasses Airflow's abstraction entirely. | |

**User's choice:** Internal `create_session`

**User clarification:** Asked "Would user be happy if we choose the internal route?" — confirmed yes, zero-config is the better user experience. User was also choosing between requiring them to configure a connection pointing to their own Airflow metadata DB vs just using the internal API.

---

## Airflow 3 BaseDBManager

| Option | Description | Selected |
|--------|-------------|----------|
| BaseDBManager for Airflow 3, create_session for Airflow 2 | Official provider pattern per FRESH-06 | ✓ |
| create_session on both versions | Simpler but non-standard on Airflow 3 | |

**User's choice:** BaseDBManager for Airflow 3 (as required by FRESH-06)

---

## Claude's Discretion

- Alembic migration file structure and revision ID
- Whether `aircontroler db init` uses `click` CLI or Airflow native CLI extension
- Index strategy for `aircontroler_model_runs`
- Session management (single vs separate sessions for read+write)

## Deferred Ideas

- FRESH-02: `freshness_overrides` dict param on `build_dbt_dag()` — future phase
- FRESH-05: Cross-DAG longest-TTL-wins registry — follows FRESH-02
- OBS-V2-02: `aircontroler db cleanup` pruning command — v2 scope
