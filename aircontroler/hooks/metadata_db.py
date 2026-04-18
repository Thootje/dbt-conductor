"""MetadataDBHook — Phase 1 stub for Airflow metadata DB access.

Full implementation deferred to Phase 3 (Freshness Policies).
Extends BaseHook so Airflow 3 worker isolation is respected from day one
(CLAUDE.md: never use direct SQLAlchemy from workers).
"""
from __future__ import annotations

from airflow.hooks.base import BaseHook


class MetadataDBHook(BaseHook):
    """Hook for aircontroler state table access.

    Phase 1: stub only. Phase 3 will implement get_last_run() and record_run().
    Using BaseHook ensures this is safe in Airflow 3.x worker isolation mode.
    """

    conn_name_attr = "aircontroler_conn_id"
    default_conn_name = "aircontroler_default"
    conn_type = "aircontroler"
    hook_name = "Aircontroler"

    def get_conn(self):
        """Phase 3 will return a DB connection. Raises NotImplementedError in Phase 1."""
        raise NotImplementedError(
            "MetadataDBHook.get_conn() is not implemented in Phase 1. "
            "Full implementation ships in Phase 3 (Freshness Policies)."
        )
