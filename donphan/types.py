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

from typing import (
    cast,
    List,
    Optional,
    Type,
)

import asyncpg

from .abc import Creatable
from .connection import MaybeAcquire
from .consts import DEFAULT_SCHEMA
from .sqltype import SQLTypeMeta, _SQLType


__all__ = (
    "create_types",
    "enum",
    "Enum",
)


class CustomTypeMeta(SQLTypeMeta):
    def __new__(cls, name, bases, attrs, **kwargs):
        return super().__new__(cls, name, bases, attrs, **kwargs)

    @property
    def _sql(cls) -> str:
        return cls.__name__


class CustomType(Creatable, _SQLType, metaclass=CustomTypeMeta):
    @classmethod
    def _query_drop(cls, if_exists=True, cascade=False):
        return cls._base_query_drop("TYPE", if_exists, cascade)


class EnumMeta(CustomTypeMeta):
    _values: List[str]
    _python: Type

    def __new__(cls, name, bases, attrs, *, values: List[str], **kwargs) -> Type[Enum]:  # type: ignore[misc]

        attrs.update({"_values": values})

        obj = cast(EnumMeta, super().__new__(cls, name, bases, attrs, **kwargs))

        if not obj._values:
            raise ValueError(f"Enum type {obj._name} cannot have 0 values.")

        for _name in obj._values:
            if obj._values.count(name) > 1:
                raise ValueError(f"Enum cannot have duplicate value {_name}.")

        obj._python = str

        return obj  # type: ignore[return-value]

    def __getattr__(cls, key):
        if key in cls._values:
            return key

        return super().__getattr__(key)


class Enum(CustomType):
    _values: List[str]

    @classmethod
    def _query_create(cls, drop_if_exists=True, if_not_exists=True):
        builder = [
            "CREATE TYPE",
            cls._name,
            "AS ENUM (",
            ", ".join(f"'{value}'" for value in cls._values),
            ")",
        ]

        return " ".join(builder)


def enum(name: str, values: str, *, schema: str = DEFAULT_SCHEMA) -> Type[Enum]:
    """Create a new enum type.

    Args:
        name (str): The name of the new enum type.
        values (str): Space separated list of enum values.
        schema (str, optional): The schema for the type.
    Returns:
        (Type): The new enum type.
    """
    return EnumMeta.__new__(EnumMeta, name, (Enum,), {}, values=values.split(), schema=schema)


async def create_types(
    connection: Optional[asyncpg.Connection] = None, drop_if_exists: bool = False, if_not_exists: bool = True
):
    """Create all defined types.

    Args:
        connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        drop_if_exists (bool, optional): Specifies wether the types should be
                first dropped from the database if they already exists.
    """
    async with MaybeAcquire(connection=connection) as connection:
        for _enum in Enum.__subclasses__():
            if drop_if_exists:
                await _enum.drop(connection=connection, if_exists=True, cascade=True)

            try:
                await _enum.create(connection=connection, if_not_exists=if_not_exists)
            except asyncpg.exceptions.DuplicateObjectError:
                if not if_not_exists:
                    raise
