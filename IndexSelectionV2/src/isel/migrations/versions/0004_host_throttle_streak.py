"""Count a host's batches in a row with a 429 or 503, so its pause can grow.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-08
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("host", sa.Column("throttle_streak", sa.SmallInteger, nullable=False,
                                    server_default=sa.text("0")), schema="recrawl")


def downgrade() -> None:
    op.drop_column("host", "throttle_streak", schema="recrawl")
