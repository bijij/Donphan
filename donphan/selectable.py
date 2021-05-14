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

import inspect
import sys

from collections.abc import Iterable
from typing import Any, Literal, Optional, TYPE_CHECKING

from .column import Column, SQLType
from .creatable import Creatable
from .utils import not_creatable, query_builder, resolve_annotation

if TYPE_CHECKING:
    from asyncpg import Connection, Record  # type: ignore


__all__ = ("Selectable",)


OPERATORS: dict[str, str] = {
    "eq": "=",
    "lt": "<",
    "le": "<=",
    "ne": "<>",
    "ge": ">=",
    "gt": ">",
}


OrderBy = tuple[Column, Literal["ASC", "DESC"]]


@not_creatable
class Selectable(Creatable):
    if TYPE_CHECKING:
        _columns: Iterable[Column]
        _columns_dict: dict[str, Column]
        _primary_keys: Iterable[Column]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cache: dict[str, Any] = {}
        cls._columns = []
        cls._columns_dict = {}
        cls._primary_keys = []

        # Get global namespaces
        try:
            globals = sys.modules[cls.__module__].__dict__
        except KeyError:
            globals = {}

        # Get local namespace
        frame = inspect.currentframe()
        try:
            if frame is None:
                locals = {}
            else:
                if frame.f_back is None:
                    locals = frame.f_locals
                else:
                    locals = frame.f_back.f_locals
        finally:
            del frame

        for name, type in cls.__annotations__.items():
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
                    type = SQLType._from_type(list[type] if is_array else type)  # type: ignore
                elif is_array:
                    type = SQLType._from_type(list[type.py_type])  # type: ignore

            except TypeError:
                if getattr(type, "__origin__", None) is not SQLType:
                    raise TypeError("Column typing generics must be a valid SQLType.")
                type = type.__args__[0]  # type: ignore

                type = SQLType._from_type(list[type] if is_array else type)  # type: ignore

            if not hasattr(cls, name):
                column = Column._with_type(type)  # type: ignore
                setattr(cls, name, column)
            else:
                column = getattr(cls, name)
                if not isinstance(column, Column):
                    raise ValueError("Column values must be an instance of Column.")

                column._sql_type = type

            column.name = name
            column.table = cls

            cls._columns.append(column)
            cls._columns_dict[name] = column
            if column.primary_key:
                cls._primary_keys.append(column)

        super().__init_subclass__(**kwargs)

    # region: query generation

    @classmethod
    @query_builder
    def _build_where_clause(
        cls,
        values: dict[str, Any],
    ) -> list[str]:
        builder = []

        if len(values) > 1:
            builder.append("(" * (len(values) - 1))

        for i, name in enumerate(values, 1):
            is_or = False
            operator = OPERATORS["eq"]
            if name.startswith("or_"):
                if i == 1:
                    raise NameError("")
                is_or = True
                name = name[3:]

            if i > 1:
                builder.append("OR" if is_or else "AND")

            if name[-4:-2] == "__":
                name, operator = name.rsplit("__", 1)
                if operator not in OPERATORS:
                    raise NameError(f"Unknown operator {operator}.")
                operator = OPERATORS[operator]

            if name not in cls._columns_dict:
                raise NameError(f"Unknown column {name} in table {cls._name}.")

            builder.append(name)
            builder.append(operator)

            builder.append(f"${i}")

            if i > 1:
                builder.append(")")

        return builder

    @classmethod
    @query_builder
    def _build_query_fetch(
        cls,
        where: str,
        limit: Optional[int],
        order_by: Optional[OrderBy],
    ) -> list[str]:
        builder = ["SELECT * FROM", cls._name]
        if where:
            builder.append("WHERE")
            builder.append(where)

        if order_by is not None:
            column, direction = order_by
            builder.append("ORDER BY")
            builder.append(column.name)
            builder.append(direction)

        if limit is not None:
            builder.append("LIMIT")
            builder.append(str(limit))

        return builder

    # endregion

    # region: public methods

    @classmethod
    async def fetch_where(
        cls,
        connection: Connection,
        /,
        where: str,
        *values: Any,
        limit: Optional[int] = None,
        order_by: Optional[OrderBy] = None,
    ) -> Iterable[Record]:
        r"""|coro|

        Fetches records in the database which match a given WHERE clause.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        where: :class:`str`
            An SQL WHERE clause.
        \*values: Any
            Values to be substituted into the WHERE clause.
        limit: Optional[:class:`int`]
            If provided, sets the maxmimum number of records to be returned.
        order_by: Optional[Tuple[:class:`Column`, Literal[`"ASC"``, ``"DESC"``]]]
            Sets the ORDER BY condition on the database query, takes a tuple
            concisting of the column and direction.

        Returns
        -------
        Iterable[:class:`asyncpg.Record`]
            Records which match the WHERE clause.
        """
        query = cls._build_query_fetch(where, limit, order_by)
        return await connection.fetch(query, *values)

    @classmethod
    async def fetch(
        cls,
        connection: Connection,
        /,
        *,
        limit: Optional[int] = None,
        order_by: Optional[OrderBy] = None,
        **values: Any,
    ) -> Iterable[Record]:
        r"""|coro|

        Fetches records in the database which contain the given values.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        limit: Optional[:class:`int`]
            If provided, sets the maxmimum number of records to be returned.
        order_by: Optional[Tuple[:class:`Column`, Literal["ASC", "DESC"]]]
            Sets the ORDER BY condition on the database query, takes a tuple
            concisting of the column and direction.
        \*\*values: Any
            The column to value mapping to filter records with.

        Returns
        -------
        Iterable[:class:`asyncpg.Record`]
            Records which which contain the given values.
        """
        where = cls._build_where_clause(values)
        return await cls.fetch_where(connection, where, *values.values(), limit=limit, order_by=order_by)

    @classmethod
    async def fetch_row_where(
        cls,
        connection: Connection,
        /,
        where: str,
        *values: Any,
        order_by: Optional[OrderBy] = None,
    ) -> Optional[Record]:
        r"""|coro|

        Fetches a record in the database which match a given WHERE clause.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        where: :class:`str`
            An SQL WHERE clause.
        \*values: Any
            Values to be substituted into the WHERE clause.
        order_by: Optional[Tuple[:class:`Column`, Literal["ASC", "DESC"]]]
            Sets the ORDER BY condition on the database query, takes a tuple
            concisting of the column and direction.

        Returns
        -------
        Optional[:class:`asyncpg.Record`]
            A record which matches the WHERE clause if found.
        """
        query = cls._build_query_fetch(where, None, order_by)
        return await connection.fetchrow(query, *values)

    @classmethod
    async def fetch_row(
        cls,
        connection: Connection,
        /,
        *,
        order_by: Optional[OrderBy] = None,
        **values: Any,
    ) -> Optional[Record]:
        r"""|coro|

        Fetches a records in the database which contains the given values.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        order_by: Optional[Tuple[:class:`Column`, Literal["ASC", "DESC"]]]
            Sets the ORDER BY condition on the database query, takes a tuple
            concisting of the column and direction.
        \*\*values: Any
            The column to value mapping to filter records with.

        Returns
        -------
        Optional[:class:`asyncpg.Record`]
            A record which contains the given values if found.
        """
        where = cls._build_where_clause(values)
        return await cls.fetch_row_where(connection, where, *values.values(), order_by=order_by)

    # endregion
