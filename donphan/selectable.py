from __future__ import annotations

import inspect
import sys

from collections.abc import Iterable

from typing import Any, TYPE_CHECKING

from .column import Column, SQLType
from .creatable import Creatable
from .utils import MISSING, not_creatable, resolve_annotation

if TYPE_CHECKING:
    from asyncpg import Connection, Record  # type: ignore


__all__ = ("Selectable",)


@not_creatable
class Selectable(Creatable):
    if TYPE_CHECKING:
        _columns: Iterable[Column]
        _primary_keys: Iterable[Column]

    def __init_subclass__(cls, schema: str = MISSING) -> None:
        cache: dict[str, Any] = {}
        cls._columns = []
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
            if column.primary_key:
                cls._primary_keys.append(column)

        return super().__init_subclass__(schema=schema)

    # region query generation

    @classmethod
    def _query_fetch(cls) -> str:
        raise NotImplementedError()

    # endregion

    # region public methods

    @classmethod
    async def create(cls, connection: Connection, *, if_not_exists: bool = False):
        query = cls._query_create(if_not_exists)
        await connection.execute(query)

    @classmethod
    async def fetch(cls, connection: Connection) -> Iterable[Record]:
        query = cls._query_fetch()
        return await connection.fetch(query)

    # endregion
