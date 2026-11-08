"""Host pacing: a 429/503 streak per host, and due docs split by first fetch or revisit.

`throttle_streak` counts a host's batches in a row with a 429 or 503 and no page, so its
pause can grow. `doc_due` splits into `doc_due_first` (never fetched) and
`doc_due_revisit`, so a host's first fetches go out before its revisits without a sort.
`best_rank` of a host with a revisit due is recomputed to the rule of the target revision:
0004 adds REVISIT_RANK when the host has only revisits due, 0003 takes min(rank).

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
REVISIT_RANK = 1 << 40           # isel.schema.REVISIT_RANK, frozen for this revision


def _due(test: str = "") -> str:
    """min(rank) over the due docs of host h, with `test` on top."""
    return ("(SELECT min(d.rank) FROM recrawl.doc d WHERE d.host_id = h.host_id "
            f"AND d.rank IS NOT NULL AND d.state IN ('pending', 'failed'){test})")


# best_rank changes only on hosts with a revisit due, found through doc_due_revisit.
_HAS_REVISIT = (f"h.host_id IN (SELECT host_id FROM recrawl.doc WHERE {DUE} "
                "AND fetched_at IS NOT NULL)")


def upgrade() -> None:
    op.add_column("host", sa.Column("throttle_streak", sa.SmallInteger, nullable=False,
                                    server_default=sa.text("0")), schema="recrawl")
    op.create_index("doc_due_first", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(f"{DUE} AND fetched_at IS NULL"))
    op.create_index("doc_due_revisit", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(f"{DUE} AND fetched_at IS NOT NULL"))
    op.execute(f"UPDATE recrawl.host h SET best_rank = coalesce("
               f"{_due(' AND d.fetched_at IS NULL')}, "
               f"{_due(' AND d.fetched_at IS NOT NULL')} + {REVISIT_RANK}) "
               f"WHERE {_HAS_REVISIT}")
    op.drop_index("doc_due", "doc", schema="recrawl")


def downgrade() -> None:
    op.create_index("doc_due", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(DUE))
    op.execute(f"UPDATE recrawl.host h SET best_rank = {_due()} WHERE {_HAS_REVISIT}")
    op.drop_index("doc_due_revisit", "doc", schema="recrawl")
    op.drop_index("doc_due_first", "doc", schema="recrawl")
    op.drop_column("host", "throttle_streak", schema="recrawl")
