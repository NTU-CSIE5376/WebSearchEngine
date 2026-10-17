"""Alembic environment for selectdb.

Online runs take the connection that `isel.selectdb.migrate` opened through
the forbidden-target check, so there is no second way in. `--sql` runs render
DDL for review and never connect.
"""

from __future__ import annotations

from alembic import context

from isel.schema import metadata

SCHEMAS = (None, "public", "recrawl")   # None is the default schema during reflection


def include_name(name: str | None, type_: str, parent_names: dict) -> bool:
    """Compares only our schemas and the tables in `metadata`.

    Run tables (`selected_urls_r<id>`), the previous list and the swap's
    temporary name come and go at runtime and are never migrated.
    """
    if type_ == "schema":
        return name in SCHEMAS
    if type_ == "table":
        schema = parent_names.get("schema_name")
        key = name if schema in (None, "public") else f"{schema}.{name}"
        return key in metadata.tables
    return True


def _configure(**kw) -> None:
    context.configure(target_metadata=metadata, include_schemas=True,
                      include_name=include_name, compare_type=True, **kw)


if context.is_offline_mode():
    _configure(dialect_name="postgresql", literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    connection = context.config.attributes.get("connection")
    if connection is None:
        raise RuntimeError("run migrations through `isel migrate-selectdb`")
    _configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()
