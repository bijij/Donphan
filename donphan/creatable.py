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
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

from typing import Protocol, TYPE_CHECKING

from .consts import DEFAULT_SCHEMA, NOT_CREATABLE
from .utils import MISSING, normalise_name, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = ("Creatable",)


class Creatable(Protocol):
    _schema: str
    _name: str

    def __init_subclass__(
        cls,
        *,
        _name: str = MISSING,
        schema: str = MISSING,
    ) -> None:
        if schema is MISSING:
            schema = DEFAULT_SCHEMA

        if _name is MISSING:
            _name = normalise_name(cls.__name__)

        cls._schema = schema
        cls._name = f"{schema}.{_name}"
        super().__init_subclass__()

    # region: query generation

    @classmethod
    def _query_create(
        cls,
        if_not_exists: bool,
    ) -> str:
        raise NotImplementedError()

    @classmethod
    @query_builder
    def _query_create_schema(
        cls,
        if_not_exists: bool,
    ) -> list[str]:
        builder = ["CREATE SCHEMA"]
        if if_not_exists:
            builder.append("IF NOT EXISTS")
        builder.append(cls._schema)
        return builder

    @classmethod
    @query_builder
    def _query_drop(
        cls,
        type: str,
        if_exists: bool,
        cascade: bool,
    ) -> list[str]:
        builder = ["DROP", type]

        if if_exists:
            builder.append("IF EXISTS")

        builder.append(cls._name)

        if cascade:
            builder.append("CASCADE")

        return builder

    # endregion

    # region: public methods

    @classmethod
    async def create(
        cls,
        connection: Connection,
        *,
        if_not_exists: bool = True,
        create_schema: bool = True,
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
        """
        if create_schema:
            await cls.create_schema(connection, if_not_exists=if_not_exists)
        query = cls._query_create(if_not_exists)
        await connection.execute(query)

    @classmethod
    async def create_schema(
        cls,
        connection: Connection,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """|coro|

        Creates the schema this database object uses.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        if_not_exists: :class:`bool`
            Sets whether creation should continue if the schema already exists.
            Defaults to ``True``.
        """
        query = cls._query_create_schema(if_not_exists)
        await connection.execute(query)

    @classmethod
    async def create_all(
        cls,
        connection: Connection,
        /,
        *,
        if_not_exists: bool = True,
        create_schema: bool = True,
    ) -> None:
        """|coro|

        Creates all subclasses of this database object.

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
        """
        for subcls in cls.__subclasses__():
            if subcls in NOT_CREATABLE:
                await subcls.create_all(
                    connection,
                    if_not_exists=if_not_exists,
                    create_schema=create_schema,
                )
            else:
                await subcls.create(
                    connection,
                    if_not_exists=if_not_exists,
                    create_schema=create_schema,
                )

    @classmethod
    async def drop(
        cls,
        connection: Connection,
        /,
        *,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        """|coro|

        Drops this object from the database.

        Parameters
        ----------
        if_exists: :class:`bool`
            Sets whether dropping should not error if the object does not exist.
            Defaults to ``True``.
        cascade: :class:`bool`
            Sets whether dropping should cascade.
            Defaults to ``False``.
        """
        query = cls._query_drop(if_exists, cascade)
        await connection.execute(query)

    # endregion
