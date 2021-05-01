from __future__ import annotations

from collections.abc import Callable
from typing import Any, TYPE_CHECKING

from .insertable import Insertable
from .utils import MISSING, not_creatable, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection, Record  # type: ignore


__all__ = ("Table",)


@not_creatable
class Table(Insertable):
    @classmethod
    @query_builder
    def _query_create(cls, if_not_exists: bool) -> list[str]:
        builder = ["CREATE TABLE"]

        if if_not_exists:
            builder.append("IF NOT EXISTS")

        builder.append(cls._name)
        builder.append("(")

        for column in cls._columns:
            builder.append(column.name)

            builder.append(column.sql_type.sql_type)

            if not column.nullable:
                builder.append("NOT NULL")

            if column.unique:
                builder.append("UNIQUE")

            if column.default is not MISSING:
                builder.append("DEFAULT")
                builder.append(str(column.default))

            if column.references is not None:
                builder.append("REFERENCES")
                builder.append(column.references.table._name)
                builder.append("(")
                builder.append(column.references.name)
                builder.append(")")

            builder.append(",")

        if cls._primary_keys:
            builder.append("PRIMARY KEY (")
            for column in cls._primary_keys:
                builder.append(column.name)
                builder.append(",")

            builder.pop(-1)
            builder.append(")")
        else:
            builder.pop(-1)

        builder.append(")")

        return builder

    @classmethod
    def _query_drop(cls, if_exists: bool, cascade: bool) -> str:
        return super()._query_drop("TABLE", if_exists, cascade)

    @classmethod
    async def migrate_to(
        cls,
        connection: Connection,
        table: type[Table],
        migration: Callable[[Record], dict[str, Any]],
        *,
        create_new_table: bool = True,
        drop_table: bool = False,
    ) -> None:

        if create_new_table:
            await table.create(connection)

        records = await cls.fetch(connection)
        await table.insert_many(connection, None, *(migration(record) for record in records))

        if drop_table:
            await cls.drop(connection)

    @classmethod
    async def migrate_from(
        cls,
        connection: Connection,
        table: type[Table],
        migration: Callable[[Record], dict[str, Any]],
        *,
        create_table: bool = True,
        drop_old_table: bool = False,
    ) -> None:
        return await table.migrate_to(
            connection,
            cls,
            migration,
            create_new_table=create_table,
            drop_table=drop_old_table,
        )
