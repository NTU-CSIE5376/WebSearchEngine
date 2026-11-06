"""Requeue the docs that the old retry rule held back for 30 days. Data only.

A WAF challenge (HTTP 202) counted as `fetched`, and a doc that used up its five tries
waited `stale_days`. Both waits outlast the golden cycle of about 14 days; see
docs/RECRAWL.md §8. robots.txt refusals keep their wait, since the site decides those.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-04
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SELECT pg_advisory_xact_lock(367123202)")   # recrawl.LOCK_CYCLE
    op.execute("CREATE TEMP TABLE requeued (host_id BIGINT) ON COMMIT DROP")
    op.execute("""
        WITH done AS (
            UPDATE recrawl.doc
            SET state = 'failed', fail_reason = 'http_202', due_at = now(), text_file = NULL,
                text_len = NULL, lang = NULL, payload_sha1 = NULL
            WHERE state = 'fetched' AND http_status = 202
            RETURNING host_id)
        INSERT INTO requeued SELECT host_id FROM done""")
    op.execute("""
        WITH done AS (
            UPDATE recrawl.doc SET due_at = now()
            WHERE state = 'failed' AND attempts >= 5 AND due_at > now()
              AND fail_reason IS DISTINCT FROM 'ignore_request'
            RETURNING host_id)
        INSERT INTO requeued SELECT host_id FROM done""")
    op.execute("""
        UPDATE recrawl.host h
        SET next_at = NULL,
            best_rank = (SELECT min(d.rank) FROM recrawl.doc d
                         WHERE d.host_id = h.host_id AND d.rank IS NOT NULL
                           AND d.state IN ('pending', 'failed'))
        WHERE h.host_id IN (SELECT host_id FROM requeued)""")


def downgrade() -> None:
    """Moves the revision only: the older due dates are not worth keeping."""
