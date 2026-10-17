"""Harden the metric's contract tables.

- `model_runs.status` only takes known values, and at most one run is live.
- Deleting a `model_runs` row no longer empties the list that points at it.
- The list tables lose the per-run `run_id` default that a load left behind,
  and their constraints follow `<table>_pkey` / `<table>_run_id_fkey`, which
  `swap` keeps when it renames tables. `prev` goes first: the live current
  table may still carry another run's constraint names.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-12
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

STATUSES = "'loading', 'validated', 'live', 'retired', 'abandoned', 'rolled_back'"


def _relink(on_delete: str) -> str:
    return f"""
DO $$
DECLARE
    t text;
    old text;
BEGIN
    FOREACH t IN ARRAY ARRAY['selected_urls_prev', 'selected_urls_current'] LOOP
        CONTINUE WHEN to_regclass('public.' || t) IS NULL;
        SELECT conname INTO old FROM pg_constraint
            WHERE conrelid = ('public.' || t)::regclass AND contype = 'p';
        IF old <> t || '_pkey' THEN
            EXECUTE 'ALTER TABLE public.' || quote_ident(t) || ' RENAME CONSTRAINT '
                || quote_ident(old) || ' TO ' || quote_ident(t || '_pkey');
        END IF;
        FOR old IN SELECT conname FROM pg_constraint
                   WHERE conrelid = ('public.' || t)::regclass AND contype = 'f' LOOP
            EXECUTE 'ALTER TABLE public.' || quote_ident(t) || ' DROP CONSTRAINT '
                || quote_ident(old);
        END LOOP;
        EXECUTE 'ALTER TABLE public.' || quote_ident(t) || ' ADD CONSTRAINT '
            || quote_ident(t || '_run_id_fkey') || ' FOREIGN KEY (run_id) '
            || 'REFERENCES public.model_runs(id) ON DELETE {on_delete}';
        EXECUTE 'ALTER TABLE public.' || quote_ident(t) || ' ALTER COLUMN run_id DROP DEFAULT';
    END LOOP;
END $$"""


def upgrade() -> None:
    op.execute("ALTER TABLE public.model_runs ADD CONSTRAINT model_runs_status_check "
               f"CHECK (status IN ({STATUSES}))")
    op.execute("CREATE UNIQUE INDEX model_runs_one_live ON public.model_runs (status) "
               "WHERE status = 'live'")
    op.execute(_relink("RESTRICT"))


def downgrade() -> None:
    op.execute(_relink("CASCADE"))
    op.execute("DROP INDEX public.model_runs_one_live")
    op.execute("ALTER TABLE public.model_runs DROP CONSTRAINT model_runs_status_check")
