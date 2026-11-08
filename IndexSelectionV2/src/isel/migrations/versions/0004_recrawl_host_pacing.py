"""Host pacing: a 429/503 streak per host, and due docs split by first fetch or revisit.

`throttle_streak` counts a host's batches in a row with a 429 or 503 and no page, so its
pause can grow. `doc_due` splits into `doc_due_first` (never fetched) and
`doc_due_revisit`, so a host's first fetches go out before its revisits without a sort.

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

DUE = "rank IS NOT NULL AND state IN ('pending', 'failed')"


def upgrade() -> None:
    op.add_column("host", sa.Column("throttle_streak", sa.SmallInteger, nullable=False,
                                    server_default=sa.text("0")), schema="recrawl")
    op.create_index("doc_due_first", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(f"{DUE} AND fetched_at IS NULL"))
    op.create_index("doc_due_revisit", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(f"{DUE} AND fetched_at IS NOT NULL"))
    op.drop_index("doc_due", "doc", schema="recrawl")


def downgrade() -> None:
    op.create_index("doc_due", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(DUE))
    op.drop_index("doc_due_revisit", "doc", schema="recrawl")
    op.drop_index("doc_due_first", "doc", schema="recrawl")
    op.drop_column("host", "throttle_streak", schema="recrawl")
