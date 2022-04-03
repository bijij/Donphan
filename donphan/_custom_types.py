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

from typing import ClassVar, TYPE_CHECKING, Any, TypeVar, Union

import asyncpg

from ._consts import CUSTOM_TYPES, POOLS
from ._creatable import Creatable
from ._enums import Enum
from ._sqltype import SQLType
from .utils import not_creatable, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = (
    "CustomType",
    "EnumType",
)


T = TypeVar("T")
ET = TypeVar("ET", bound=Enum)


@not_creatable
class CustomType(SQLType[T], Creatable):
    """A representation of a Custom SQL type.

    Attributes
    ----------
        py_type: Type[Any]
            The python type associated with the column.
        sql_type: str
            The SQL type associated with the column.
        _name: :class:`str`
            The name of the table.
        _schema: :class:`str`
            The tables schema.
    """

    _type: ClassVar[str] = "TYPE"
    sql_type: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.sql_type = cls._name

    @classmethod
    async def _set_codec(cls, connection: Connection) -> None:
        raise NotImplementedError()

    @classmethod
    async def register(
        cls,
        connection: Connection,
    ) -> None:
        for pool in POOLS:
            for holder in pool._holders:
                if holder._con is not None:
                    await cls._set_codec(holder._con)

    @classmethod
    async def create(
        cls,
        connection: Connection,
        /,
        *args: Any,
        if_not_exists: bool = True,
        **kwargs: Any,
    ) -> None:
        try:
            await super().create(connection, *args, **kwargs)
        except asyncpg.exceptions.DuplicateObjectError:
            if not if_not_exists:
                raise
        await cls.register(connection)
        CUSTOM_TYPES[cls._name] = cls

    @classmethod
    async def unregister(
        cls,
        connection: Connection,
    ) -> None:
        for pool in POOLS:
            for holder in pool._holders:
                if holder._con is not None:
                    await holder._con.reset_type_codec(cls._name[len(cls._schema) + 1 :], schema=cls._schema)

    @classmethod
    async def drop(cls, connection: Connection, /, *args: Any, **kwargs: Any) -> None:
        await cls.unregister(connection)
        await super().drop(connection, *args, **kwargs)
        CUSTOM_TYPES.pop(cls._name, None)


@not_creatable
class EnumType(CustomType[ET], sql_type=""):
    """A representations of an SQL Enum type.

    Attributes
    ----------
        py_type: Type[:class:`~.Enum`]
            The python type associated with the column.
        sql_type: Type[:class:`~.SQLType`]
            The SQL type associated with the column.
        _name: :class:`str`
            The name of the table.
        _schema: :class:`str`
            The tables schema.
    """

    @classmethod
    @query_builder
    def _query_create(cls, if_not_exists: bool) -> list[str]:
        builder = ["CREATE TYPE"]
        builder.append(cls._name)
        builder.append("AS ENUM (")

        for key in cls.py_type:
            builder.append(f"'{key.name}'")
            builder.append(",")

        builder.pop(-1)

        builder.append(")")
        return builder

    @classmethod
    def _encoder(cls, value: Union[ET, Any]) -> str:
        return cls.py_type.try_value(value).name

    @classmethod
    def _decoder(cls, value: str) -> ET:
        return getattr(cls.py_type, value)

    @classmethod
    async def _set_codec(cls, connection: Connection) -> None:
        await connection.set_type_codec(
            cls._name[len(cls._schema) + 1 :],
            schema=cls._schema,
            encoder=cls._encoder,
            decoder=cls._decoder,
            format="text",
        )
