"""The recrawl frontier: one back queue per hostname, one row per page, one row per job.

The `select` role gets no USAGE on this schema, so the metric cannot see it.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-12
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ARRAY

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

TSTZ = sa.TIMESTAMP(timezone=True)


def upgrade() -> None:
    op.execute("CREATE SCHEMA recrawl")
    op.execute("CREATE SEQUENCE recrawl.batch_seq")
    op.create_table(
        "host",
        sa.Column("host_id", sa.BigInteger, nullable=False, autoincrement=False),
        sa.Column("host", sa.Text, nullable=False),
        sa.Column("best_rank", sa.BigInteger),
        sa.Column("next_at", TSTZ),
        sa.Column("batch_id", sa.BigInteger),
        sa.PrimaryKeyConstraint("host_id", name="host_pkey"),
        sa.UniqueConstraint("host", name="host_host_key"),
        schema="recrawl",
        postgresql_with={"fillfactor": 80},
    )
    op.create_index("host_ready", "host", ["best_rank"], schema="recrawl",
                    postgresql_where=sa.text("batch_id IS NULL AND best_rank IS NOT NULL"))
    op.create_index("host_queued", "host", ["batch_id"], schema="recrawl",
                    postgresql_where=sa.text("batch_id IS NOT NULL"))
    op.create_table(
        "doc",
        sa.Column("doc_id", sa.BigInteger, nullable=False, autoincrement=False),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("host_id", sa.BigInteger, nullable=False),
        sa.Column("rank", sa.BigInteger),
        sa.Column("model_run_id", sa.BigInteger),
        sa.Column("state", sa.Text, nullable=False, server_default=sa.text("'pending'")),
        sa.Column("due_at", TSTZ, nullable=False, server_default=sa.func.now()),
        sa.Column("attempts", sa.SmallInteger, nullable=False, server_default=sa.text("0")),
        sa.Column("batch_id", sa.BigInteger),
        sa.Column("fetched_at", TSTZ),
        sa.Column("http_status", sa.SmallInteger),
        sa.Column("final_url", sa.Text),
        sa.Column("fail_reason", sa.Text),
        sa.Column("payload_sha1", sa.Text),
        sa.Column("text_file", sa.Text),
        sa.Column("text_len", sa.Integer),
        sa.Column("lang", sa.Text),
        sa.PrimaryKeyConstraint("doc_id", name="doc_pkey"),
        sa.ForeignKeyConstraint(["host_id"], ["recrawl.host.host_id"], name="doc_host_id_fkey"),
        sa.ForeignKeyConstraint(["model_run_id"], ["public.model_runs.id"], ondelete="RESTRICT",
                                name="doc_model_run_id_fkey"),
        sa.CheckConstraint("state IN ('pending', 'dispatched', 'fetched', 'failed', 'gone')",
                           name="doc_state_check"),
        schema="recrawl",
        postgresql_with={"fillfactor": 80},
    )
    op.create_index("doc_due", "doc", ["host_id", "rank"], schema="recrawl",
                    postgresql_where=sa.text(
                        "rank IS NOT NULL AND state IN ('pending', 'failed')"))
    op.create_index("doc_inflight", "doc", ["batch_id"], schema="recrawl",
                    postgresql_where=sa.text("state = 'dispatched'"))
    op.create_index("doc_stale", "doc", ["due_at"], schema="recrawl",
                    postgresql_where=sa.text("state = 'fetched' AND rank IS NOT NULL"))
    op.create_table(
        "job",
        sa.Column("batch_id", sa.BigInteger, nullable=False, autoincrement=False),
        sa.Column("job_id", sa.Text),
        sa.Column("doc_ids", ARRAY(sa.BigInteger), nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("claimed_at", TSTZ, nullable=False),
        sa.Column("submitted_at", TSTZ),
        sa.Column("checked_at", TSTZ),
        sa.Column("finished_at", TSTZ),
        sa.Column("deleted_at", TSTZ),
        sa.Column("records", sa.Integer),
        sa.Column("error", sa.Text),
        sa.PrimaryKeyConstraint("batch_id", name="job_pkey"),
        sa.UniqueConstraint("job_id", name="job_job_id_key"),
        sa.CheckConstraint("state IN ('claimed', 'submitted', 'applied', 'lost', 'error')",
                           name="job_state_check"),
        schema="recrawl",
    )
    op.create_index("job_open", "job", ["state"], schema="recrawl",
                    postgresql_where=sa.text("state IN ('claimed', 'submitted', 'error')"))
    op.create_index("job_undeleted", "job", ["batch_id"], schema="recrawl",
                    postgresql_where=sa.text("state = 'applied' AND deleted_at IS NULL"))


def downgrade() -> None:
    op.execute("DROP SCHEMA recrawl CASCADE")
