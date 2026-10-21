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
`doc_stale`. `job` is one manual-bulk-crawl job at the crawler API (WebCrawler
PR #84): the docs it carries, in the order their URLs were sent, and how far it got.
A job is `completed` once the API's status says so; the archive then applies it.
An applied job's results are deleted at the API; `deleted_at` records that.

Only the live list table is defined here. `publish` creates each run's table
with `LIKE public.selected_urls_current`, and `swap` renames tables and their
constraints, so `selected_urls_prev` and `selected_urls_r<id>` stay out of the
metadata (`migrations/env.py` skips them).
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION, JSONB

RUN_STATUSES = ("loading", "validated", "live", "retired", "abandoned", "rolled_back")
DOC_STATES = ("pending", "dispatched", "fetched", "failed", "gone")
# Doc states the planner may hand out again once `due_at` has passed.
DUE_STATES = ("pending", "failed")
JOB_STATES = ("claimed", "submitted", "completed", "applied", "lost", "error")
# Jobs whose docs stay `dispatched`; an `error` job waits for a fix or for its results to expire.
OPEN_JOB_STATES = ("claimed", "submitted", "completed", "error")
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
    sa.Column("best_rank", sa.BigInteger),          # min rank of its due docs; NULL: none
    _tstz("next_at"),                               # no job before this (pacing, backoff)
    sa.Column("batch_id", sa.BigInteger),           # its open job; NULL: none
    sa.PrimaryKeyConstraint("host_id", name="host_pkey"),
    sa.UniqueConstraint("host", name="host_host_key"),
    sa.Index("host_ready", "best_rank",
             postgresql_where=sa.text("batch_id IS NULL AND best_rank IS NOT NULL")),
    sa.Index("host_queued", "batch_id", postgresql_where=sa.text("batch_id IS NOT NULL")),
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
    sa.Column("batch_id", sa.BigInteger),           # the job it was last sent in
    _tstz("fetched_at"),
    sa.Column("http_status", sa.SmallInteger),
    sa.Column("final_url", sa.Text),
    sa.Column("fail_reason", sa.Text),
    sa.Column("payload_sha1", sa.Text),             # SHA-1 of the HTML bytes
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

recrawl_job = sa.Table(
    "job", metadata,
    sa.Column("batch_id", sa.BigInteger, nullable=False, autoincrement=False),
    sa.Column("job_id", sa.Text),                   # the API's id; NULL until it answers
    sa.Column("doc_ids", ARRAY(sa.BigInteger), nullable=False),   # in the order sent
    sa.Column("state", sa.Text, nullable=False),
    _tstz("claimed_at", nullable=False),
    _tstz("submitted_at"),
    _tstz("checked_at"),                            # last status query
    _tstz("completed_at"),                          # the API's completed_at
    _tstz("finished_at"),                           # applied or lost
    _tstz("deleted_at"),                            # results deleted at the API
    sa.Column("records", sa.Integer),               # result records applied
    sa.Column("error", sa.Text),
    sa.PrimaryKeyConstraint("batch_id", name="job_pkey"),
    sa.UniqueConstraint("job_id", name="job_job_id_key"),
    sa.CheckConstraint(_in("state", JOB_STATES), name="job_state_check"),
    sa.Index("job_open", "state", postgresql_where=sa.text(_in("state", OPEN_JOB_STATES))),
    sa.Index("job_undeleted", "batch_id",
             postgresql_where=sa.text("state = 'applied' AND deleted_at IS NULL")),
    schema=RECRAWL_SCHEMA,
)
