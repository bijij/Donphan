from __future__ import annotations
from types import BuiltinMethodType
from donphan.types import CustomType

from typing import TYPE_CHECKING

from .selectable import Selectable
from .utils import not_creatable, query_builder

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

        for name, type in cls._columns.items():
            builder.append(name)

            if issubclass(type.sql_type, CustomType):
                builder.append(type.sql_type._name)
            else:
                builder.append(type.sql_type.__name__)

            if type.references is not None:
                builder.append("REFERENCES")
                builder.append(type.references.table._name)
                builder.append("(")
                builder.append(type.references.name)
                builder.append(")")

            builder.append(",")

        if cls._primary_keys:
            builder.append("PRIMARY KEY (")
            for key in cls._primary_keys:
                builder.append(key)
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
