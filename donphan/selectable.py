from __future__ import annotations

import inspect
import sys

from collections.abc import Iterable
from typing import Any, Optional, TYPE_CHECKING

from .column import Column, SQLType
from .creatable import Creatable
from .utils import MISSING, not_creatable, query_builder, resolve_annotation

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


@not_creatable
class Selectable(Creatable):
    if TYPE_CHECKING:
        _columns: Iterable[Column]
        _columns_dict: dict[str, Column]
        _primary_keys: Iterable[Column]

    def __init_subclass__(cls, schema: str = MISSING) -> None:
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
                    type = SQLType.from_type(list[type] if is_array else type)  # type: ignore
                elif is_array:
                    type = SQLType.from_type(list[type.py_type])  # type: ignore

            except TypeError:
                if getattr(type, "__origin__", None) is not SQLType:
                    raise TypeError("Column typing generics must be a valid SQLType.")
                type = type.__args__[0]  # type: ignore

                type = SQLType.from_type(list[type] if is_array else type)  # type: ignore

            if not hasattr(cls, name):
                column = Column.with_type(type)  # type: ignore
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

        return super().__init_subclass__(schema=schema)

    # region query generation

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
    ) -> list[str]:
        builder = ["SELECT * FROM", cls._name]
        if where:
            builder.append("WHERE")
            builder.append(where)
        return builder

    # endregion

    # region public methods

    @classmethod
    async def fetch_where(
        cls,
        connection: Connection,
        where: str,
        *values: Any,
    ) -> Iterable[Record]:
        query = cls._build_query_fetch(where)
        return await connection.fetch(query, *values)

    @classmethod
    async def fetch(
        cls,
        connection: Connection,
        **values: Any,
    ) -> Iterable[Record]:
        where = cls._build_where_clause(values)
        return await cls.fetch_where(connection, where, *values.values())

    @classmethod
    async def fetch_row_where(
        cls,
        connection: Connection,
        where: str,
        *values: Any,
    ) -> Optional[Record]:
        query = cls._build_query_fetch(where)
        return await connection.fetchrow(query, *values)

    @classmethod
    async def fetch_row(
        cls,
        connection: Connection,
        **values: Any,
    ) -> Optional[Record]:
        where = cls._build_where_clause(values)
        return await cls.fetch_row_where(connection, where, *values.values())

    # endregion
