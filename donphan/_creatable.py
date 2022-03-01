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

from typing import TYPE_CHECKING, ClassVar, Optional, TextIO, Union, overload

from ._column import Column
from ._consts import DEFAULT_SCHEMA, NOT_CREATABLE
from ._object import Object
from .utils import MISSING, optional_pool, optional_transaction, query_builder, write_to_file

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = ("Creatable",)


class Creatable(Object):
    _type: ClassVar[str] = MISSING

    # region: query generation

    @classmethod
    def _query_exists(
        cls,
    ) -> str:
        raise NotImplementedError()

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
        if_exists: bool,
        cascade: bool,
    ) -> list[str]:
        builder = ["DROP", cls._type]

        if if_exists:
            builder.append("IF EXISTS")

        builder.append(cls._name)

        if cascade:
            builder.append("CASCADE")

        return builder

    # endregion

    @classmethod
    def _find_schemas(cls) -> list[type[Creatable]]:
        schema_names: set[str] = set()
        schemas: list[type[Creatable]] = []

        for subcls in cls.__subclasses__():

            if subcls in NOT_CREATABLE:
                for schema in subcls._find_schemas():
                    if schema._schema not in schema_names:
                        schema_names.add(schema._schema)
                        schemas.append(schema)
            else:
                if subcls._schema not in schema_names:
                    schema_names.add(subcls._schema)
                    schemas.append(subcls)

        return schemas

    # region: public methods

    @classmethod
    @optional_pool
    async def exists(
        cls,
        connection: Connection,
    ) -> bool:
        """
        Check if the table exists.

        :param conn: The connection to use.
        :return: True if the table exists, False otherwise.
        """
        record = await connection.fetchrow(cls._query_exists(), cls._schema, cls._local_name)
        if record is None:
            return False
        (result,) = record
        return result

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
        if create_schema:
            await cls.create_schema(connection, if_not_exists=if_not_exists)

        if automatic_migrations:
            # this is a hack because >circular imports<
            from ._table import Table

            if issubclass(cls, Table) and await cls.exists(connection):
                new_columns = [column._copy() for column in cls._columns]
                old_columns: set[str] = {
                    record["column_name"]
                    for record in await connection.fetch(
                        "SELECT * FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2;",
                        cls._schema,
                        cls._local_name,
                    )
                }

                cls._columns_dict = {column.name: column for column in cls._columns if column.name in old_columns}
                for column_name in old_columns:
                    if column_name not in cls._columns_dict:
                        column = Column.create(column_name, MISSING)
                        column.table = cls
                        cls._columns_dict[column_name] = column

                await cls.migrate(connection, new_columns)
                return

        query = cls._query_create(if_not_exists)
        await connection.execute(query)

    @classmethod
    @optional_pool
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
    @optional_pool
    async def create_all(
        cls,
        connection: Connection,
        /,
        *,
        if_not_exists: bool = True,
        create_schema: bool = True,
        automatic_migrations: bool = False,
        with_transaction: bool = True,
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
        automatic_migrations: :class:`bool`
            Sets whether migrations should be automatically performed.
            Defaults to ``False``.
        with_transaction: :class:`bool`
            Sets whether the database should be wrapped in a transaction.
            Defaults to ``True``.
        """
        async with optional_transaction(connection, with_transaction):

            for subcls in cls.__subclasses__():
                if create_schema:
                    for schema in cls._find_schemas():
                        await schema.create_schema(connection, if_not_exists=if_not_exists)

                if subcls in NOT_CREATABLE:
                    await subcls.create_all(
                        connection,
                        if_not_exists=if_not_exists,
                        automatic_migrations=automatic_migrations,
                        with_transaction=False,
                    )
                else:
                    await subcls.create(
                        connection,
                        if_not_exists=if_not_exists,
                        automatic_migrations=automatic_migrations,
                    )

    @overload
    @classmethod
    def export(cls, *, if_not_exists: bool = ..., export_schema: bool = True, fp: None = ...) -> str:
        ...

    @overload
    @classmethod
    def export(cls, *, if_not_exists: bool = ..., export_schema: bool = True, fp: Union[str, TextIO] = ...) -> TextIO:
        ...

    @classmethod
    def export(
        cls, *, if_not_exists: bool = False, export_schema: bool = True, fp: Optional[Union[str, TextIO]] = None
    ) -> Union[TextIO, str]:
        """
        A function which exports this database object.

        Parameters
        ----------
        if_not_exists: :class:`bool`
            Sets whether the if_not_exists clause should be set on
            exported objects. Defaults to ``False``.
        export_schema: :class:`bool`
            Sets whether to additionally export the schema used by this object.
            Defaults to ``True``.
        fp: Optional[:class:`os.PathLike`, :class:`io.TextIOBase`]
            A file-like object opened in text mode and write mode.
            or a filename representing a file on disk to write to.

            .. note::
                If the file-like object passed is opened via :func:`open`
                ensure the object is in a text-writing mode such as ``"w"``.

        Returns
        -------
        Union[:class:`io.TextIOBase`, :class:`str`]
            The file-like object which was provided or a string containing the
            exported database object.
        """
        output = ""

        if export_schema and cls._schema != DEFAULT_SCHEMA:
            output += cls.export_schema(if_not_exists=if_not_exists)
            output += "\n\n"

        output += cls._query_create(if_not_exists)
        output += ";"

        if fp is None:
            return output
        return write_to_file(fp, output)

    @overload
    @classmethod
    def export_schema(cls, *, if_not_exists: bool = ..., fp: None = ...) -> str:
        ...

    @overload
    @classmethod
    def export_schema(cls, *, if_not_exists: bool = ..., fp: Union[str, TextIO] = ...) -> TextIO:
        ...

    @classmethod
    def export_schema(
        cls, *, if_not_exists: bool = False, fp: Optional[Union[str, TextIO]] = None
    ) -> Union[TextIO, str]:
        """|coro|

        A function which exports this database object's schema.

        Parameters
        ----------
        if_not_exists: :class:`bool`
            Sets whether the if_not_exists clause should be set on
            the exported database schema. Defaults to ``False``.
        fp: Optional[:class:`os.PathLike`, :class:`io.TextIOBase`]
            A file-like object opened in text mode and write mode.
            or a filename representing a file on disk to write to.

            .. note::
                If the file-like object passed is opened via :func:`open`
                ensure the object is in a text-writing mode such as ``"w"``.

        Returns
        -------
        Union[:class:`io.TextIOBase`, :class:`str`]
            The file-like object which was provided or a string containing the
            exported database schema.
        """
        output = cls._query_create_schema(if_not_exists)
        output += ";"

        if fp is None:
            return output
        return write_to_file(fp, output)

    @overload
    @classmethod
    def export_all(cls, *, if_not_exists: bool = ..., export_schema: bool = True, fp: None = ...) -> str:
        ...

    @overload
    @classmethod
    def export_all(
        cls, *, if_not_exists: bool = ..., export_schema: bool = True, fp: Union[str, TextIO] = ...
    ) -> TextIO:
        ...

    @classmethod
    def export_all(
        cls, *, if_not_exists: bool = False, export_schema: bool = True, fp: Optional[Union[str, TextIO]] = None
    ) -> Union[TextIO, str]:
        """
        A function which exports all database objects that subclass this object.

        Parameters
        ----------
        if_not_exists: :class:`bool`
            Sets whether the if_not_exists clause should be set on
            exported objects. Defaults to ``False``.
        export_schema: :class:`bool`
            Sets whether to additionally export any schemas used by these objects.
            Defaults to ``True``.
        fp: Optional[:class:`os.PathLike`, :class:`io.TextIOBase`]
            A file-like object opened in text mode and write mode.
            or a filename representing a file on disk to write to.

            .. note::
                If the file-like object passed is opened via :func:`open`
                ensure the object is in a text-writing mode such as ``"w"``.

        Returns
        -------
        Union[:class:`io.TextIOBase`, :class:`str`]
            The file-like object which was provided or a string containing the
            exported database objects.
        """
        output = ""

        if export_schema:
            for schema in cls._find_schemas():
                if schema._schema != DEFAULT_SCHEMA:
                    output += schema.export_schema(if_not_exists=if_not_exists)
                    output += "\n\n"

        for subcls in cls.__subclasses__():
            if subcls in NOT_CREATABLE:
                output += subcls.export_all(if_not_exists=if_not_exists, export_schema=False)
            else:
                output += subcls.export(if_not_exists=if_not_exists, export_schema=False)
                output += "\n\n"

        if fp is None:
            return output
        return write_to_file(fp, output)

    @classmethod
    @optional_pool
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
