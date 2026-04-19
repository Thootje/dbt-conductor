"""Add aircontroler_model_runs table.

Revision ID: 0001
Revises: None
Create Date: 2026-04-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "aircontroler_model_runs",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("unique_id", sa.String(length=255), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_acr_unique_id", "aircontroler_model_runs", ["unique_id"])
    op.create_index("ix_acr_completed_at", "aircontroler_model_runs", ["completed_at"])


def downgrade():
    op.drop_index("ix_acr_completed_at", table_name="aircontroler_model_runs")
    op.drop_index("ix_acr_unique_id", table_name="aircontroler_model_runs")
    op.drop_table("aircontroler_model_runs")
