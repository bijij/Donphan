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

from ._consts import CUSTOM_TYPES
from ._creatable import Creatable
from ._custom_types import CustomType
from ._insertable import Insertable
from .utils import MISSING, not_creatable, optional_pool, optional_transaction, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection, Record

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

        unique_columns = []

        for column in cls._columns:
            builder.append(column._query())
            builder.append(",")

            if column.unique:
                unique_columns.append(column)

        if unique_columns:
            builder.append("UNIQUE (")
            for column in unique_columns:
                builder.append(column.name)
                builder.append(",")

            builder.pop(-1)
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
    @query_builder
    def _query_drop_column(cls, column: Column) -> list[str]:
        return ["ALTER TABLE", cls._name, "DROP COLUMN", column.name]

    @classmethod
    @query_builder
    def _query_add_column(cls, column: Column) -> list[str]:
        return ["ALTER TABLE", cls._name, "ADD COLUMN", column._query()]

    @classmethod
    @optional_pool
    async def create(
        cls,
        connection: Connection,
        *,
        if_not_exists: bool = True,
        create_schema: bool = True,
        automatic_migrations: bool = False,
    ) -> None:
        """|coro|

        Creates this database object.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        if_not_exists: :class:`bool`
            Sets whether creation should continue if the object already exists.
            Defaults to ``True``.
        create_schema: :class:`bool`
            Sets whether the database schema should also be created.
            Defaults to ``True``.
        automatic_migrations: :class:`bool`
            Sets whether migrations should be automatically run.
            Defaults to ``False``.
        """
        # create custom types if needed
        for column in cls._columns:
            if issubclass(column.sql_type, CustomType):
                if column.sql_type._name not in CUSTOM_TYPES:
                    await column.sql_type.create(connection, if_not_exists=if_not_exists)

        return await super().create(
            connection,
            if_not_exists=if_not_exists,
            create_schema=create_schema,
            automatic_migrations=automatic_migrations,
        )

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
        with_transaction: bool = True,
    ) -> None:
        """|coro|

        Migrates the table to the given columns.

        Parameters
        ----------
        connection: :class:`~asyncpg.Connection`
            The connection to use.
        columns: Iterable[:class:`~.Column`]
            The new list of columns for the table.
        with_transaction: :class:`bool`
            Sets whether the database should be wrapped in a transaction.
            Defaults to ``True``.
        """
        columns_dict = {column.name: column for column in columns}

        async with optional_transaction(connection, with_transaction):
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
        with_transaction: bool = True,
    ) -> None:
        """|coro|

        Helper function for migrating data in a table to another.

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
        with_transaction: :class:`bool`
            Sets whether the database should be wrapped in a transaction.
            Defaults to ``True``.
        """
        if cls._name == table._name:
            if migration is not MISSING:
                raise ValueError("Custom migration functions are not supported for table alterations.")
            await cls.migrate(connection, (column._copy() for column in table._columns))
            return

        async with optional_transaction(connection, with_transaction):

            if migration is MISSING:
                migration = dict

                if create_new_table:
                    await table.create(connection)

                records = await cls.fetch(connection)
                await table.insert_many(connection, None, *(migration(record) for record in records))

                if drop_table:
                    await cls.drop(connection)
