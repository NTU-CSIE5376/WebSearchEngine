"""Baseline: the V1-compatible tables that `init-selectdb` created before Alembic.

The live selectdb (Host1 :5445) already has them; `migrate-selectdb
--stamp-baseline` records this revision there without running it.

Revision ID: 0001
Revises:
Create Date: 2026-10-12
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
CREATE TABLE public.model_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cutoff_datetime TIMESTAMPTZ,
    model_path TEXT,
    train_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    eval_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    selection_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_paths JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'loading',
    activated_at TIMESTAMPTZ,
    config_sha256 TEXT,
    code_version TEXT
)""")
    op.execute("""
CREATE TABLE public.selected_urls_current (
    url TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    first_seen TIMESTAMPTZ,
    run_id BIGINT NOT NULL REFERENCES public.model_runs(id) ON DELETE CASCADE,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)""")


def downgrade() -> None:
    op.execute("DROP TABLE public.selected_urls_current")
    op.execute("DROP TABLE public.model_runs")
