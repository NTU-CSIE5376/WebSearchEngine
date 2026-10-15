"""The selectdb schema, the single definition that Alembic revisions must match.

`public` is the official metric's contract and keeps V1's columns
(WebSearchEngine `IndexSelection/selectdb_common.py`): the metric runs
`SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)` as the
`select` role. `run_id`, `selected_at` and `first_seen` repeat per row because
V1 readers expect them there.

Only the live list table is defined here. `publish` creates each run's table
with `LIKE public.selected_urls_current`, and `swap` renames tables and their
constraints, so `selected_urls_prev` and `selected_urls_r<id>` stay out of the
metadata (`migrations/env.py` skips them).
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB

RUN_STATUSES = ("loading", "validated", "live", "retired", "abandoned", "rolled_back")
LIST_TABLE = "selected_urls_current"

metadata = sa.MetaData()


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _tstz(name: str, **kw) -> sa.Column:
    return sa.Column(name, sa.TIMESTAMP(timezone=True), **kw)


def _json(name: str) -> sa.Column:
    return sa.Column(name, JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))


model_runs = sa.Table(
    "model_runs", metadata,
    sa.Column("id", sa.BigInteger, primary_key=True),
    sa.Column("run_label", sa.Text, nullable=False),
    _tstz("created_at", nullable=False, server_default=sa.func.now()),
    _tstz("cutoff_datetime"),
    sa.Column("model_path", sa.Text),
    _json("train_metrics"),
    _json("eval_metrics"),
    _json("selection_metrics"),
    _json("source_paths"),
    sa.Column("status", sa.Text, nullable=False, server_default=sa.text("'loading'")),
    _tstz("activated_at"),
    sa.Column("config_sha256", sa.Text),
    sa.Column("code_version", sa.Text),
    sa.PrimaryKeyConstraint("id", name="model_runs_pkey"),
    sa.UniqueConstraint("run_label", name="model_runs_run_label_key"),
    sa.CheckConstraint(_in("status", RUN_STATUSES), name="model_runs_status_check"),
    sa.Index("model_runs_one_live", "status", unique=True,
             postgresql_where=sa.text("status = 'live'")),
)

selected_urls_current = sa.Table(
    LIST_TABLE, metadata,
    sa.Column("url", sa.Text, nullable=False),
    sa.Column("score", DOUBLE_PRECISION, nullable=False),
    _tstz("first_seen"),
    sa.Column("run_id", sa.BigInteger, nullable=False),
    _tstz("selected_at", nullable=False, server_default=sa.func.now()),
    sa.PrimaryKeyConstraint("url", name=f"{LIST_TABLE}_pkey"),
    sa.ForeignKeyConstraint(["run_id"], ["model_runs.id"], ondelete="RESTRICT",
                            name=f"{LIST_TABLE}_run_id_fkey"),
)
