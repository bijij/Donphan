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
    """Base class for creating representations of SQL Database Tables.

    Attributes
    ----------
        _name: :class:`str`
            The name of the table.
        _schema: :class:`str`
            The tables schema.
        _columns: Iterable[:class:`~.Column`]
            The columns contained in the table.
        _columns_dict: Dict[:class:`str`, :class:`~.Column`]
            A mapping between a column's name and itself.
        _primary_keys: Iterable[:class:`~.Column`]
            The primary key columns of the table.
    """

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
        /,
        table: type[Table],
        migration: Callable[[Record], dict[str, Any]],
        *,
        create_new_table: bool = False,
        drop_table: bool = False,
    ) -> None:
        """|coro|

        Helper function for migrating data in a table to another.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection`
            The database connection to use for transactions.
        table: Type[:class:`.Table`]
            The new table to migrate to.
        migration: Callable[[:class:`asyncpg.Record`], Dict[:class:`str`, Any]]
            The function used to migrate data between tables.
        create_new_table: :class:`bool`
            Sets whether the table to migrate to should be created.
            Defaults to ``False``.
        drop_table: :class:`bool`
            Sets whether this table should be dropped after migrating.
            Defaults to ``False``.
        """

        if create_new_table:
            await table.create(connection)

        records = await cls.fetch(connection)
        await table.insert_many(connection, None, *(migration(record) for record in records))

        if drop_table:
            await cls.drop(connection)
