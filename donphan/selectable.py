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
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from .column import BaseColumn, Column
from .creatable import Creatable
from .utils import not_creatable, query_builder

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


OrderBy = tuple[BaseColumn, Literal["ASC", "DESC"]]


@not_creatable
class Selectable(Creatable):
    if TYPE_CHECKING:
        _columns: ClassVar[list[Column]]
        _columns_dict: ClassVar[dict[str, Column]]

    @classmethod
    def _get_columns(
        cls,
        values: dict[str, Any],
    ) -> Iterable[Column]:
        return [cls._columns_dict[column] for column in values]

    @classmethod
    def _setup_column(
        cls,
        name: str,
        type: Any,
        globals: dict[str, Any],
        locals: dict[str, Any],
        cache: dict[str, Any],
    ) -> None:
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._columns = []
        cls._columns_dict = {}

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

        cache: dict[str, Any] = {}

        for name, type in cls.__annotations__.items():
            if not name.startswith("_"):
                cls._setup_column(name, type, globals, locals, cache)

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
        order_by: Optional[Union[OrderBy, str]],
    ) -> list[str]:
        builder = ["SELECT * FROM", cls._name]
        if where:
            builder.append("WHERE")
            builder.append(where)

        if order_by is not None:
            builder.append("ORDER BY")

            if isinstance(order_by, str):
                builder.append(order_by)

            else:
                column, direction = order_by
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
        order_by: Optional[Union[OrderBy, str]] = None,
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
        order_by: Optional[Union[Tuple[:class:`BaseColumn`, Literal["ASC", "DESC"]], str]
            Sets the ORDER BY condition on the database query. Takes a tuple
            concisting of the column and direction, or a string defining the condition.

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
        order_by: Optional[Union[OrderBy, str]] = None,
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
        order_by: Optional[Union[Tuple[:class:`BaseColumn`, Literal["ASC", "DESC"]], str]
            Sets the ORDER BY condition on the database query. Takes a tuple
            concisting of the column and direction, or a string defining the condition.
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
        order_by: Optional[Union[OrderBy, str]] = None,
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
        order_by: Optional[Union[Tuple[:class:`BaseColumn`, Literal["ASC", "DESC"]], str]
            Sets the ORDER BY condition on the database query. Takes a tuple
            concisting of the column and direction, or a string defining the condition.

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
        order_by: Optional[Union[OrderBy, str]] = None,
        **values: Any,
    ) -> Optional[Record]:
        r"""|coro|

        Fetches a records in the database which contains the given values.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
        order_by: Optional[Union[Tuple[:class:`BaseColumn`, Literal["ASC", "DESC"]], str]
            Sets the ORDER BY condition on the database query. Takes a tuple
            concisting of the column and direction, or a string defining the condition.
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
