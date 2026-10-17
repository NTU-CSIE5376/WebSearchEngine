"""The selectdb schema, the single definition that Alembic revisions must match.

`public` is the official metric's contract and keeps V1's columns
(WebSearchEngine `IndexSelection/selectdb_common.py`): the metric runs
`SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)` as the
`select` role. `run_id`, `selected_at` and `first_seen` repeat per row because
V1 readers expect them there.

`recrawl` holds the recrawl frontier and is not readable by the `select` role.
`host` is one back queue per hostname, the unit of the 0.2 qps rule; `doc` is
one row per page ever selected, keyed by `doc_id` as a signed BIGINT. A doc is
on the live list while `rank` is set; `fetched` docs come due again through
`doc_stale`. `log` lists the crawler fetch logs the archive has applied; the
crawler deletes its own files, so this is the archive's only progress record.

Only the live list table is defined here. `publish` creates each run's table
with `LIKE public.selected_urls_current`, and `swap` renames tables and their
constraints, so `selected_urls_prev` and `selected_urls_r<id>` stay out of the
metadata (`migrations/env.py` skips them).
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB

RUN_STATUSES = ("loading", "validated", "live", "retired", "abandoned", "rolled_back")
DOC_STATES = ("pending", "dispatched", "fetched", "failed", "gone")
# Doc states the planner may hand out again once `due_at` has passed.
DUE_STATES = ("pending", "failed")
LIST_TABLE = "selected_urls_current"
RECRAWL_SCHEMA = "recrawl"

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

batch_seq = sa.Sequence("batch_seq", schema=RECRAWL_SCHEMA, metadata=metadata)

recrawl_host = sa.Table(
    "host", metadata,
    sa.Column("host_id", sa.BigInteger, nullable=False, autoincrement=False),
    sa.Column("host", sa.Text, nullable=False),
    sa.Column("crawl_shard", sa.SmallInteger, nullable=False),
    sa.Column("best_rank", sa.BigInteger),          # min rank of its due docs; NULL: none
    _tstz("next_at"),                               # no batch before this (backoff, pause)
    sa.Column("batch_id", sa.BigInteger),           # its batch queued at the API; NULL: none
    sa.PrimaryKeyConstraint("host_id", name="host_pkey"),
    sa.UniqueConstraint("host", name="host_host_key"),
    sa.CheckConstraint("crawl_shard BETWEEN 0 AND 255", name="host_crawl_shard_check"),
    sa.Index("host_ready", "best_rank",
             postgresql_where=sa.text("batch_id IS NULL AND best_rank IS NOT NULL")),
    sa.Index("host_queued", "host_id", postgresql_where=sa.text("batch_id IS NOT NULL")),
    schema=RECRAWL_SCHEMA,
    postgresql_with={"fillfactor": 80},
)

recrawl_doc = sa.Table(
    "doc", metadata,
    sa.Column("doc_id", sa.BigInteger, nullable=False, autoincrement=False),
    sa.Column("url", sa.Text, nullable=False),      # canonical spelling sent to the crawler
    sa.Column("host_id", sa.BigInteger, nullable=False),
    sa.Column("rank", sa.BigInteger),               # NULL: not on the live list
    sa.Column("model_run_id", sa.BigInteger),
    sa.Column("state", sa.Text, nullable=False, server_default=sa.text("'pending'")),
    _tstz("due_at", nullable=False, server_default=sa.func.now()),
    sa.Column("attempts", sa.SmallInteger, nullable=False, server_default=sa.text("0")),
    sa.Column("batch_id", sa.BigInteger),
    _tstz("dispatched_at"),                         # when the crawler took the batch
    _tstz("fetched_at"),
    sa.Column("http_status", sa.SmallInteger),
    sa.Column("content_type", sa.Text),
    sa.Column("final_url", sa.Text),
    sa.Column("fail_reason", sa.Text),
    sa.Column("payload_sha1", sa.Text),
    sa.Column("etag", sa.Text),
    _tstz("last_modified"),
    sa.Column("text_file", sa.Text),                # relative to recrawl.text_root
    sa.Column("text_len", sa.Integer),
    sa.Column("lang", sa.Text),
    sa.PrimaryKeyConstraint("doc_id", name="doc_pkey"),
    sa.ForeignKeyConstraint(["host_id"], [f"{RECRAWL_SCHEMA}.host.host_id"],
                            name="doc_host_id_fkey"),
    sa.ForeignKeyConstraint(["model_run_id"], ["model_runs.id"], ondelete="RESTRICT",
                            name="doc_model_run_id_fkey"),
    sa.CheckConstraint(_in("state", DOC_STATES), name="doc_state_check"),
    sa.Index("doc_due", "host_id", "rank",
             postgresql_where=sa.text(f"rank IS NOT NULL AND {_in('state', DUE_STATES)}")),
    sa.Index("doc_inflight", "batch_id", postgresql_where=sa.text("state = 'dispatched'")),
    sa.Index("doc_stale", "due_at",
             postgresql_where=sa.text("state = 'fetched' AND rank IS NOT NULL")),
    schema=RECRAWL_SCHEMA,
    postgresql_with={"fillfactor": 80},
)

recrawl_log = sa.Table(
    "log", metadata,
    sa.Column("name", sa.Text, nullable=False),     # recrawl-NN-<ts>Z-<seq>.jsonl
    _tstz("processed_at", nullable=False, server_default=sa.func.now()),
    sa.Column("records", sa.Integer, nullable=False),
    sa.Column("error", sa.Text),                    # first malformed line, if any
    sa.PrimaryKeyConstraint("name", name="log_pkey"),
    schema=RECRAWL_SCHEMA,
)
