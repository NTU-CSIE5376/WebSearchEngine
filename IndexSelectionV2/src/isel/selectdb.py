"""Connections to OUR selectdb and its schema migrations.

Every write to a database goes through `connect`, which refuses a target whose
port is in `selectdb.forbidden_ports` (crawlerdb, metricdb, V1 selectdb) or
that holds crawler or metric tables.

The schema lives in `isel.schema` and evolves through the Alembic revisions in
`isel/migrations/versions/`. `migrate` runs them on a `connect` connection in
one transaction, under an advisory lock, and then compares the database with
`isel.schema`, so a database that drifted from the revisions rolls back.
"""

from __future__ import annotations

import io
from typing import Any

import psycopg
import sqlalchemy as sa
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from sqlalchemy.pool import NullPool

from isel.config import Config
from isel.pg import require_env

BASELINE = "0001"
# A database with any of these is crawlerdb or metricdb, whatever its port.
_FOREIGN_TABLES = ("public.url_state_current_000", "public.metric_url")
_MIGRATE_LOCK = 0x15E1_DB01   # pg_advisory_xact_lock key shared by every migrate


class ForbiddenTarget(RuntimeError):
    """Raised when a write connection would reach a database other than our selectdb."""


class NeedsBaselineStamp(RuntimeError):
    """Raised when selectdb has the pre-Alembic tables but no Alembic version yet."""


def connect(cfg: Config, dsn_env: str, read_only: bool) -> psycopg.Connection:
    """Opens our selectdb in autocommit mode after checking the target.

    Raises:
        ForbiddenTarget: If the port is forbidden or the database holds
            crawler or metric tables.
    """
    s = cfg.selectdb
    dsn = require_env(dsn_env)
    if int(conninfo_to_dict(dsn).get("port") or 5432) in s.forbidden_ports:
        raise ForbiddenTarget(f"{dsn_env} points at a forbidden port")
    conn = psycopg.connect(dsn, autocommit=True, application_name=f"{cfg.run.app_name}_select",
                           connect_timeout=s.connect_timeout_s,
                           options="-c default_transaction_read_only=on" if read_only else "")
    try:
        if conn.info.port in s.forbidden_ports:
            raise ForbiddenTarget(f"{dsn_env} reached a forbidden port")
        with conn.cursor() as cur:
            cur.execute("SELECT " + " OR ".join(
                f"to_regclass('{t}') IS NOT NULL" for t in _FOREIGN_TABLES))
            if cur.fetchone()[0]:
                raise ForbiddenTarget(f"{dsn_env} reaches a database with crawler or metric tables")
    except BaseException:
        conn.close()
        raise
    return conn


def _alembic_config(out: io.TextIOBase | None = None) -> AlembicConfig:
    acfg = AlembicConfig(output_buffer=out)
    acfg.set_main_option("script_location", "isel:migrations")
    return acfg


def render_sql(stamp_baseline: bool, out: io.TextIOBase) -> None:
    """Writes the DDL that `migrate` would run, without connecting.

    With `stamp_baseline` the script starts after the baseline, as on the live
    selectdb; otherwise it builds an empty database.
    """
    command.upgrade(_alembic_config(out), f"{BASELINE}:head" if stamp_baseline else "head",
                    sql=True)


def migrate(admin: psycopg.Connection, stamp_baseline: bool = False) -> dict[str, Any]:
    """Brings selectdb to the newest revision and checks it against `isel.schema`.

    Args:
        admin: A `connect` connection; it is closed on return.
        stamp_baseline: Record the baseline without running it. Only valid on a
            database that has the pre-Alembic tables and no Alembic version.

    Raises:
        NeedsBaselineStamp: If the pre-Alembic tables exist and `stamp_baseline` is unset.
        RuntimeError: If `stamp_baseline` does not fit the database, or the
            migrated database differs from `isel.schema` (nothing is committed).
    """
    admin.autocommit = False
    engine = sa.create_engine("postgresql+psycopg://", creator=lambda: admin,
                              poolclass=NullPool)
    acfg = _alembic_config()
    try:
        with engine.begin() as conn:
            conn.execute(sa.text("SELECT pg_advisory_xact_lock(:k)"), {"k": _MIGRATE_LOCK})
            before = MigrationContext.configure(conn).get_current_revision()
            legacy = conn.execute(sa.text(
                "SELECT to_regclass('public.model_runs') IS NOT NULL")).scalar_one()
            if stamp_baseline and (before is not None or not legacy):
                raise RuntimeError("--stamp-baseline only fits a pre-Alembic selectdb "
                                   f"(revision {before}, model_runs present: {legacy})")
            if legacy and before is None and not stamp_baseline:
                raise NeedsBaselineStamp(
                    "selectdb has the pre-Alembic tables; review `migrate-selectdb --sql "
                    "--stamp-baseline`, then rerun with --stamp-baseline")
            acfg.attributes["connection"] = conn
            if stamp_baseline:
                command.stamp(acfg, BASELINE)
            command.upgrade(acfg, "head")
            command.check(acfg)
            after = MigrationContext.configure(conn).get_current_revision()
    finally:
        engine.dispose()
    return {"revision_before": before, "revision": after, "stamped_baseline": stamp_baseline}


def assert_ready(cfg: Config, conn: psycopg.Connection) -> None:
    """Refuses a selectdb that is behind the newest revision or lacks the reader role.

    Raises:
        RuntimeError: If `migrate-selectdb` has not brought the database to head.
    """
    head = ScriptDirectory.from_config(_alembic_config()).get_current_head()
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.alembic_version') IS NOT NULL")
        versions = []
        if cur.fetchone()[0]:
            cur.execute("SELECT version_num FROM public.alembic_version")
            versions = [v for (v,) in cur.fetchall()]
        if versions != [head]:
            raise RuntimeError(f"selectdb is at {versions or 'no revision'}, not {head}; "
                               "run `isel migrate-selectdb`")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (cfg.selectdb.reader_role,))
        if cur.fetchone() is None:
            raise RuntimeError(f"role {cfg.selectdb.reader_role!r} is missing; "
                               "run `isel migrate-selectdb`")


def init_role(cfg: Config, admin: psycopg.Connection) -> None:
    """Creates or updates the read-only role the metric connects as. Idempotent.

    The password is the metric's hardcoded one, so it comes from a secret
    and never sits in a revision.
    """
    role = cfg.selectdb.reader_role
    password = require_env(cfg.selectdb.reader_password_env)
    with admin.transaction(), admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        verb = "ALTER" if cur.fetchone() else "CREATE"
        cur.execute(sql.SQL("{} ROLE {} LOGIN PASSWORD {}").format(
            sql.SQL(verb), sql.Identifier(role), sql.Literal(password)))
        cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(admin.info.dbname), sql.Identifier(role)))
        cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
        cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(
            sql.Identifier(role)))
        cur.execute(sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}").format(
            sql.Identifier(role)))
