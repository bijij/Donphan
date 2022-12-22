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
import types
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from ._column import BaseColumn, Column, OnClause
from ._consts import OPERATORS, NULL_OPERATORS
from ._object import Object
from .utils import generate_alias, optional_pool, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection, Record

    from ._join import Join, JoinType


__all__ = ("Selectable",)


OrderBy = tuple[BaseColumn, Literal["ASC", "DESC"]]


class Selectable(Object):
    if TYPE_CHECKING:
        _columns_dict: ClassVar[dict[str, Column]]

    @classmethod
    @property
    def _columns(cls) -> Iterable[Column]:
        return cls._columns_dict.values()

    @classmethod
    def _query_exists(
        cls,
        type: str,
    ) -> str:
        return f"SELECT EXISTS ( SELECT FROM information_schema.{type} WHERE table_schema = $1 AND table_name = $2);"

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

        i = 1
        first = True
        for name, value in values.items():
            is_or = False
            if name.startswith("or_"):
                if first:
                    raise NameError("Query cannot accept OR as first clause.")
                is_or = True
                name = name[3:]

            if not first:
                builder.append("OR" if is_or else "AND")

            # set default operator
            if value is None:
                operator = NULL_OPERATORS["eq"]
            else:
                operator = OPERATORS["eq"]

            for key in OPERATORS:
                if name.endswith(f"__{key}"):
                    name, operator = name.rsplit("__", 1)
                    if value is None:
                        if operator not in NULL_OPERATORS:
                            raise NameError(f"Unknown null operator {operator}.")
                        operator = NULL_OPERATORS[operator]
                    else:
                        if operator not in OPERATORS:
                            raise NameError(f"Unknown operator {operator}.")
                        operator = OPERATORS[operator]

            if name not in cls._columns_dict:
                raise NameError(f"Unknown column {name} in selectable {cls._name}.")

            builder.append(name)

            if operator == "_IN":
                column = cls._columns_dict[name]
                builder.append("=")
                builder.append(f"any(${i}::{column.sql_type.sql_type}[])")
                i += 1
            elif operator == "_NULL_EQ":
                builder.append("IS NULL")
            elif operator == "_NULL_NE":
                builder.append("IS NOT NULL")
            else:
                builder.append(operator)
                builder.append(f"${i}")
                i += 1

            if not first:
                builder.append(")")

            first = False

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
    @optional_pool
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
    @optional_pool
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
        return await cls.fetch_where(
            connection, where, *filter(lambda v: v is not None, values.values()), limit=limit, order_by=order_by
        )

    @classmethod
    @optional_pool
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
    @optional_pool
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
        return await cls.fetch_row_where(
            connection, where, *filter(lambda v: v is not None, values.values()), order_by=order_by
        )

    # endregion

    @classmethod
    def _join(
        cls,
        other: Union[type[Selectable], BaseColumn],
        type: JoinType,
        on: Optional[Union[OnClause, Iterable[BaseColumn]]] = None,
    ) -> type[Join]:
        # this is a hack because >circular imports<
        from ._join import Join

        if isinstance(on, tuple):
            if isinstance(on[0], BaseColumn):
                on = [on]  # type: ignore

        name = generate_alias()

        def exec_body(ns: dict[str, Any]) -> None:
            ns["_pool"] = cls._pool
            ns["_alias"] = name
            ns["_type"] = type
            ns["_selectables"] = (cls, other)
            ns["_on"] = on

        return types.new_class(name, (Join,), {}, exec_body)  # type: ignore

    @classmethod
    def inner_join(
        cls,
        other: type[Selectable],
        on: Union[OnClause, Iterable[BaseColumn]],
    ) -> type[Join]:
        """A chainable method to join with another database object utilising an ``INNER JOIN``.

        Parameters
        ----------
        other: Union[:class:`Table`, :class:`View`, :class:`Join`]
            The other database object to join with.
        on: Union[:class:`OnClause`, Iterable[:class:`OnClause`]]
            The column or columns to join on

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return cls._join(other, "INNER", on)

    @classmethod
    def left_join(
        cls,
        other: type[Selectable],
        on: Union[OnClause, Iterable[BaseColumn]],
    ) -> type[Join]:
        """A chainable method to join with another database object utilising a ``LEFT JOIN``.

        Parameters
        ----------
        other: Union[:class:`Table`, :class:`View`, :class:`Join`]
            The other database object to join with.
        on: Union[:class:`OnClause`, Iterable[:class:`BaseColumn`]]
            The column or columns to join on

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return cls._join(other, "LEFT", on)

    @classmethod
    def right_join(
        cls,
        other: type[Selectable],
        on: Union[OnClause, Iterable[BaseColumn]],
    ) -> type[Join]:
        """A chainable method to join with another database object utilising a ``RIGHT JOIN``.

        Parameters
        ----------
        other: Union[:class:`Table`, :class:`View`, :class:`Join`]
            The other database object to join with.
        on: Union[:class:`OnClause`, Iterable[:class:`BaseColumn`]]
            The column or columns to join on

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return cls._join(other, "LEFT", on)

    @classmethod
    def full_outer_join(
        cls,
        other: type[Selectable],
        on: Union[OnClause, Iterable[BaseColumn]],
    ) -> type[Join]:
        """A chainable method to join with another database object utilising a ``FULL OUTER JOIN``.

        Parameters
        ----------
        other: Union[:class:`Table`, :class:`View`, :class:`Join`]
            The other database object to join with.
        on: Union[:class:`OnClause`, Iterable[:class:`BaseColumn`]]
            The column or columns to join on

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return cls._join(other, "FULL OUTER", on)
