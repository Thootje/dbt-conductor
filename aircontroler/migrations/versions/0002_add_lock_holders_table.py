"""Add aircontroler_lock_holders table.

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "aircontroler_lock_holders",
        sa.Column("unique_id", sa.String(length=255), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("unique_id"),
    )


def downgrade():
    op.drop_table("aircontroler_lock_holders")
