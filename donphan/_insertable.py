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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar, Union, cast, overload

from ._column import Column, SQLType
from ._selectable import Selectable
from .utils import optional_pool, query_builder, resolve_annotation

if TYPE_CHECKING:
    from asyncpg import Connection, Record


__all__ = ("Insertable",)


T = TypeVar("T")


class Insertable(Selectable):

    if TYPE_CHECKING:
        _primary_keys: ClassVar[list[Column]]

    @classmethod
    def _setup_column(
        cls,
        name: str,
        type: Any,
        globals: dict[str, Any],
        locals: dict[str, Any],
        cache: dict[str, Any],
    ) -> None:

        type = resolve_annotation(type, globals, locals, cache)

        if getattr(type, "__origin__", None) is not Column:
            raise TypeError("Column typings must be of type Column.")

        type = type.__args__[0]
        is_array = False

        if getattr(type, "__origin__", None) is list:
            is_array = True
            type = type.__args__[0]

        try:
            if not issubclass(type, SQLType):
                type = SQLType._from_type(list[type] if is_array else type)
            elif is_array:
                type = SQLType._from_type(list[type.py_type])

        except TypeError:
            if getattr(type, "__origin__", None) is not SQLType:
                raise TypeError("Column typing generics must be a valid SQLType.")
            type = type.__args__[0]  # type: ignore

            type = SQLType._from_type(list[type] if is_array else type)

        if not hasattr(cls, name):
            column = Column._with_type(type)
            setattr(cls, name, column)
        else:
            column = getattr(cls, name)
            if not isinstance(column, Column):
                raise ValueError("Column values must be an instance of Column.")

            column._sql_type = type

        column.name = name
        column.table = cls

        cls._columns_dict[name] = column
        if column.primary_key:
            cls._primary_keys.append(column)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._primary_keys = []

        super().__init_subclass__(**kwargs)

    # region: query generation

    @classmethod
    def _get_primary_keys(
        cls,
        record: Record,
    ) -> dict[str, Any]:
        return {column.name: record[column.name] for column in cls._primary_keys}

    @classmethod
    @query_builder
    def _build_query_insert(
        cls,
        columns: Union[Iterable[Union[Column, str]], str],
        ignore_on_conflict: bool,
        update_on_conflict: Union[Iterable[Union[Column, str]], str],
        returning: Union[Iterable[Union[Column, str]], str],
    ) -> list[str]:
        builder = [f"INSERT INTO", cls._name, "("]

        if isinstance(columns, str):
            builder.append(columns)
        else:
            for column in columns:
                if isinstance(column, Column):
                    column = column.name
                builder.append(column)
                builder.append(",")

            builder.pop(-1)

        builder.append(") VALUES (")

        for i, _ in enumerate(columns, 1):
            builder.append(f"${i}")
            builder.append(",")

        builder.pop(-1)

        builder.append(")")

        if ignore_on_conflict and update_on_conflict:
            raise ValueError("")

        elif ignore_on_conflict:
            builder.append("ON CONFLICT DO NOTHING")

        elif update_on_conflict:
            builder.append("ON CONFLICT (")

            for column in cls._primary_keys:
                builder.append(column.name)
                builder.append(",")

            builder.pop(-1)

            builder.append(") DO UPDATE SET")

            if isinstance(update_on_conflict, str):
                builder.append(update_on_conflict)
            else:
                for column in update_on_conflict:
                    if isinstance(column, Column):
                        column = column.name
                    builder.append(f"{column} = EXCLUDED.{column}")
                    builder.append(",")

                builder.pop(-1)

        if returning:
            builder.append("RETURNING")

            if isinstance(returning, str):
                builder.append(returning)
            else:
                for column in returning:
                    if isinstance(column, Column):
                        column = column.name
                    builder.append(column)
                    builder.append(",")

                builder.pop(-1)

        return builder

    @classmethod
    @query_builder
    def _build_query_update(
        cls,
        where: str,
        offset: int,
        columns: Union[Iterable[Union[Column, str]], str],
    ) -> list[str]:
        builder = [f"UPDATE", cls._name, "SET"]

        if isinstance(columns, str):
            columns = [column.strip() for column in columns.split(",")]

        for i, column in enumerate(columns, offset):
            if isinstance(column, Column):
                column = column.name
            builder.append(column)
            builder.append(f"= ${i}")
            builder.append(",")

        builder.pop(-1)

        builder.append("WHERE")
        builder.append(where)

        return builder

    @classmethod
    @query_builder
    def _build_query_delete(
        cls,
        where: str,
    ) -> list[str]:
        builder = ["DELETE FROM", cls._name]
        if where:
            builder.append("WHERE")
            builder.append(where)
        return builder

    # endregion

    # region: public methods

    async def _insert(
        cls,  # type: ignore
        connection: Connection,
        /,
        *,
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = None,
        returning: Optional[Union[Iterable[Union[Column, str]], str]] = None,
        **values: Any,
    ) -> Optional[Record]:
        r"""|coro|

        Inserts a new record into the database.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        ignore_on_conflict: :class:`bool`
            Sets whether to ignore errors when inserting, defaults to ``False``.
        update_on_conflict: Optional[Union[Iterable[Union[:class:`~Column`, :class:`str`]], :class:`str`]]
            An Optional list of or string representing columns to update with new data if a conflict occurs.
        returning: Optional[Union[Iterable[Union[:class:`~Column`, :class:`str`]], :class:`str`]]
            An optional list of or string representing columns to return from the inserted record.
        \*\*values: Any
            The column to value mapping for the record to insert.

        Returns
        -------
        Optional[:class:`asyncpg.Record`]
            A record containing information from the inserted record.
        """
        columns = cls._get_columns(values)
        query = cls._build_query_insert(columns, ignore_on_conflict, update_on_conflict or [], returning or [])
        if returning is not None:
            return await connection.fetchrow(query, *values.values())
        await connection.execute(query, *values.values())

    @classmethod
    @overload
    async def insert(
        cls,
        connection: Optional[Connection],
        /,
        *,
        ignore_on_conflict: bool = ...,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = ...,
        returning: Union[Iterable[Union[Column, str]], str] = ...,
        **values: Any,
    ) -> Record:
        ...

    @classmethod
    @overload
    async def insert(
        cls,
        connection: Optional[Connection],
        /,
        *,
        ignore_on_conflict: bool = ...,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = ...,
        returning: None = ...,
        **values: Any,
    ) -> None:
        ...

    @classmethod
    @overload
    async def insert(
        cls,
        /,
        *,
        ignore_on_conflict: bool = ...,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = ...,
        returning: Union[Iterable[Union[Column, str]], str] = ...,
        **values: Any,
    ) -> Record:
        ...

    @classmethod
    @overload
    async def insert(
        cls,
        /,
        *,
        ignore_on_conflict: bool = ...,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = ...,
        returning: None = ...,
        **values: Any,
    ) -> None:
        ...

    @classmethod
    async def insert(cls, *args: Any, **kwargs: Any) -> None:
        ...

    insert = classmethod(optional_pool(_insert))  # type: ignore
    del _insert

    async def _insert_many(
        cls,  # type: ignore
        connection: Connection,
        /,
        columns: Optional[Union[Iterable[Union[Column, str]], str]],
        *values: Union[Iterable[Any], dict[str, Any]],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = None,
    ) -> None:
        r"""|coro|

        Inserts a set of new records into the database.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        \*values: Dict[:class:`str`, Any]
            The column to value mappings for each record to insert.
        ignore_on_conflict: :class:`bool`
            Sets whether to ignore errors when inserting, defaults to ``False``.
        update_on_conflict: Optional[Union[Iterable[Union[:class:`~Column`, :class:`str`]], :class:`str`]]
            An Optional list of or string representing columns to update with new data if a conflict occurs.
        """
        if columns is None:
            values = cast(tuple[dict[str, Any], ...], values)
            columns = cls._get_columns(values[0])
            values = cast(tuple[list[Any]], (list(value.values()) for value in values))

        query = cls._build_query_insert(columns, ignore_on_conflict, update_on_conflict or [], [])
        await connection.executemany(query, values)

    @classmethod
    @overload
    async def insert_many(
        cls,
        connection: Optional[Connection],
        /,
        columns: Union[Iterable[Union[Column, str]], str],
        *values: Iterable[Any],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = None,
    ) -> None:
        ...

    @classmethod
    @overload
    async def insert_many(
        cls,
        connection: Optional[Connection],
        /,
        columns: None,
        *values: dict[str, Any],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = None,
    ) -> None:
        ...

    @classmethod
    @overload
    async def insert_many(
        cls,
        /,
        columns: Union[Iterable[Union[Column, str]], str],
        *values: Iterable[Any],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = None,
    ) -> None:
        ...

    @classmethod
    @overload
    async def insert_many(
        cls,
        /,
        columns: None,
        *values: dict[str, Any],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Union[Iterable[Union[Column, str]], str]] = None,
    ) -> None:
        ...

    @classmethod
    async def insert_many(cls, *args: Any, **kwargs: Any) -> None:
        ...

    insert_many = classmethod(optional_pool(_insert_many))  # type: ignore
    del _insert_many

    @classmethod
    @optional_pool
    async def update_where(
        cls,
        connection: Connection,
        /,
        where: str,
        *values: Any,
        **_values: Any,
    ) -> None:
        r"""|coro|

        Updates records in the database which match a given WHERE clause.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        where: :class:`str`
            An SQL WHERE clause.
        \*values: Any
            Values to be substituted into the WHERE clause.
        \*\*values: Any
            The column to value mapping to assign to updated records.
        """
        columns = cls._get_columns(_values)
        query = cls._build_query_update(where, len(values) + 1, columns)
        await connection.execute(query, *values, *_values.values())

    @classmethod
    @optional_pool
    async def update_record(
        cls,
        connection: Connection,
        /,
        record: Record,
        **values: Any,
    ) -> None:
        r"""|coro|

        Updates a record in the database.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        record: :class:`asyncpg.Record`
            The record to update.
        \*\*values: Any
            The column to value mapping to assign to updated record.
        """
        primary_keys = cls._get_primary_keys(record)
        where = cls._build_where_clause(primary_keys)
        return await cls.update_where(connection, where, *primary_keys.values(), **values)

    @classmethod
    @optional_pool
    async def delete_where(
        cls,
        connection: Connection,
        /,
        where: str,
        *values: Any,
    ) -> None:
        """|coro|

        Deletes records in the database which match the given WHERE clause.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        where: :class:`str`
            An SQL WHERE clause.
        *values: Any
            Values to be substituted into the WHERE clause.
        """
        query = cls._build_query_delete(where)
        await connection.execute(query, *values)

    @classmethod
    @optional_pool
    async def delete(
        cls,
        connection: Connection,
        /,
        **values: Any,
    ) -> None:
        r"""|coro|

        Deletes records in the database which contain the given values.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        \*\*values: Any
            The column to value mapping to filter records with.
        """
        where = cls._build_where_clause(values)
        return await cls.delete_where(connection, where, *filter(lambda v: v is not None, values.values()))

    @classmethod
    @optional_pool
    async def delete_record(
        cls,
        connection: Connection,
        /,
        record: Record,
    ) -> None:
        """|coro|

        Deletes a given record from the database.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        record: :class:`asyncpg.Record`
            The record to delete.
        """
        primary_keys = cls._get_primary_keys(record)
        where = cls._build_where_clause(primary_keys)
        return await cls.delete_where(connection, where, *primary_keys.values())

    # endregion
