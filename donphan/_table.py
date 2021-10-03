"""
MIT License

Copyright (c) 2019-present Josh B

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THEdefault
SOFTWARE.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, ClassVar

from ._creatable import Creatable
from ._insertable import Insertable
from .utils import MISSING, not_creatable, optional_pool, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection, Record  # type: ignore
    from ._column import Column


__all__ = ("Table",)


@not_creatable
class Table(Insertable, Creatable):
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

    _type: ClassVar[str] = "TABLE"

    @classmethod
    def _query_exists(
        cls,
    ) -> str:
        return super()._query_exists("tables")

    @classmethod
    @query_builder
    def _query_create(cls, if_not_exists: bool) -> list[str]:
        builder = ["CREATE TABLE"]

        if if_not_exists:
            builder.append("IF NOT EXISTS")

        builder.append(cls._name)
        builder.append("(")

        for column in cls._columns:
            builder.append(column._query())
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
    @query_builder
    def _query_drop_column(cls, column: Column) -> list[str]:
        return ["ALTER TABLE", cls._name, "DROP COLUMN", column.name]

    @classmethod
    @query_builder
    def _query_add_column(cls, column: Column) -> list[str]:
        return ["ALTER TABLE", cls._name, "ADD COLUMN", column._query()]

    @classmethod
    @optional_pool
    async def drop_column(
        cls,
        connection: Connection,
        /,
        column: Column,
    ) -> None:
        """Drops a column from the table.

        Parameters
        ----------
            connection: :class:`~asyncpg.Connection`
                The connection to use.
            column: :class:`~.Column`
                The column to drop.
        """
        if getattr(column, "table", MISSING) is not cls:
            raise ValueError("Column does not belong to this table.")
        await connection.execute(cls._query_drop_column(column))
        del column.table

    @classmethod
    @optional_pool
    async def add_column(
        cls,
        connection: Connection,
        /,
        column: Column,
    ) -> None:
        """Adds a new column to the table.

        Parameters
        ----------
            connection: :class:`~asyncpg.Connection`
                The connection to use.
            column: :class:`~.Column`
                The column to add. This column must not be associated with
                another table.
        """
        if hasattr(column, "table"):
            raise ValueError(f"Column {column.name} already belongs to {column.table}")
        if column.name in cls._columns_dict:
            raise ValueError(f"Column {column.name} already exists.")
        cls._columns_dict[column.name] = column
        await connection.execute(cls._query_add_column(column))
        column.table = cls

    @classmethod
    @optional_pool
    async def migrate(
        cls,
        connection: Connection,
        /,
        columns: Iterable[Column],
    ) -> None:
        """|coro|

        Migrates the table to the given columns.
        If an error occurs, the transaction will be rolled back.

        Parameters
        ----------
            connection: :class:`~asyncpg.Connection`
                The connection to use.
            columns: Iterable[:class:`~.Column`]
                The new list of columns for the table.
        """
        columns_dict = {column.name: column for column in columns}

        async with connection.transaction():
            for column in cls._columns:
                if column.name not in columns_dict:
                    await cls.drop_column(connection, column)

            for column in columns_dict:
                if column not in cls._columns_dict:
                    await cls.add_column(connection, columns_dict[column])

    @classmethod
    @optional_pool
    async def migrate_to(
        cls,
        connection: Connection,
        /,
        table: type[Table],
        migration: Callable[[Record], dict[str, Any]] = MISSING,
        *,
        create_new_table: bool = False,
        drop_table: bool = False,
    ) -> None:
        """|coro|

        Helper function for migrating data in a table to another.
        If an error occurs, the transaction will be rolled back.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        table: Type[:class:`.Table`]
            The new table to migrate to.
        migration: Callable[[:class:`asyncpg.Record`], Dict[:class:`str`, Any]]
            The function used to migrate data between tables.
            By default this passes the record unchanged.
        create_new_table: :class:`bool`
            Sets whether the table to migrate to should be created.
            Defaults to ``False``.
        drop_table: :class:`bool`
            Sets whether this table should be dropped after migrating.
            Defaults to ``False``.
        """
        if cls._name == table._name:
            if migration is not MISSING:
                raise ValueError("Custom migration functions are not supported for table alterations.")
            await cls.migrate(connection, (column._copy() for column in table._columns))
            return

        async with connection.transaction():

            if migration is MISSING:
                migration = dict

                if create_new_table:
                    await table.create(connection)

                records = await cls.fetch(connection)
                await table.insert_many(connection, None, *(migration(record) for record in records))

                if drop_table:
                    await cls.drop(connection)
