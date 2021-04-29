from __future__ import annotations
from donphan.column import Column

from typing import TYPE_CHECKING

from .selectable import Selectable
from .types import CustomType
from .utils import MISSING, not_creatable, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = ("Table",)


@not_creatable
class Table(Selectable):
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

            if issubclass(column.sql_type, CustomType):
                builder.append(column.sql_type._name)
            else:
                builder.append(column.sql_type.__name__)

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
    async def migrate_to(
        cls,
        connection: Connection,
        table: type[Table],
        *,
        create_new_table: bool = True,
        drop_table: bool = False,
    ) -> None:

        if create_new_table:
            await table.create(connection)

        # TODO: FETCH -> MODIFY -> INSERT

        if drop_table:
            await cls.drop(connection)

    @classmethod
    async def migrate_from(
        cls,
        connection: Connection,
        table: type[Table],
        *,
        create_table: bool = True,
        drop_old_table: bool = False,
    ) -> None:
        return await table.migrate_to(
            connection,
            cls,
            create_new_table=create_table,
            drop_table=drop_old_table,
        )
