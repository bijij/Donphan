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
import datetime
import decimal
import ipaddress
import uuid

from typing import (
    Any,
    TYPE_CHECKING,
    Union,
    cast,
    NamedTuple,
    Optional,
    Callable,
    Dict,
    Type,
    TypeVar,
)

from .meta import ObjectMeta

if TYPE_CHECKING:
    from .column import Column


__all__ = ("SQLType",)


T = TypeVar("T")
SqlT = TypeVar("SqlT", bound=Callable[..., "SQLType"])


class TypeDefinition(NamedTuple):
    python: Type
    sql: Union[str, Callable[..., str]]


def _varchar_sql(n: int = 2000):
    return f"CHARACTER VARYING({n})"


def _timestamp_sql(with_timezone: bool = False):
    return "TIMESTAMP WITH TIME ZONE" if with_timezone else "TIMESTAMP"


SQL_TYPES: Dict[str, TypeDefinition] = {
    # 8.1 Numeric
    "Integer": TypeDefinition(int, "INTEGER"),
    "SmallInt": TypeDefinition(int, "SMALLINT"),
    "BigInt": TypeDefinition(int, "BIGINT"),
    "Serial": TypeDefinition(int, "SERIAL"),
    "Float": TypeDefinition(float, "FLOAT"),
    "DoublePrecision": TypeDefinition(float, "DOUBLE PRECISION"),
    "Numeric": TypeDefinition(decimal.Decimal, "NUMERIC"),
    # 8.2 Monetary
    "Money": TypeDefinition(str, "MONEY"),
    # 8.14 JSON
    "JSON": TypeDefinition(dict, "JSON"),
    "JSONB": TypeDefinition(dict, "JSONB"),
    # 8.3 Character
    "CharacterVarying": TypeDefinition(str, _varchar_sql),
    "Character": TypeDefinition(str, "CHARACTER"),
    "Text": TypeDefinition(str, "TEXT"),
    # 8.4 Binary
    "Bytea": TypeDefinition(bytes, "BYTEA"),
    # 8.5 Date/Time
    "Timestamp": TypeDefinition(datetime.datetime, _timestamp_sql),
    "Date": TypeDefinition(datetime.date, "DATE"),
    "Interval": TypeDefinition(datetime.timedelta, "INTERVAL"),
    # 8.6 Boolean
    "Boolean": TypeDefinition(bool, "BOOLEAN"),
    # 8.9 Network Adress
    "CIDR": TypeDefinition(ipaddress._BaseNetwork, "CIDR"),
    "Inet": TypeDefinition(ipaddress._BaseNetwork, "INET"),
    "MACAddr": TypeDefinition(str, "MACADDR"),
    # 8.12 UUID
    "UUID": TypeDefinition(uuid.UUID, "UUID"),
}

SQL_TYPE_ALIASES: Dict[str, str] = {
    "Int": "Integer",
    "Char": "Character",
    "VarChar": "CharacterVarying",
}

DEFAULT_TYPES: Dict[Type, str] = {
    int: "Integer",
    float: "Float",
    decimal.Decimal: "Numeric",
    str: "Text",
    bytes: "Bytea",
    datetime.datetime: "Timestamp",
    datetime.date: "Date",
    datetime.timedelta: "Interval",
    bool: "Boolean",
    ipaddress.IPv4Network: "CIDR",
    ipaddress.IPv6Network: "CIDR",
    ipaddress.IPv4Address: "Inet",
    ipaddress.IPv6Address: "Inet",
    uuid.UUID: "UUID",
    dict: "JSONB",
}


class SQLTypeMeta(ObjectMeta):
    _python: Type
    _preformatted_sql: Union[str, Callable[..., str]]
    _format_values: Dict[str, Any]

    def __new__(
        cls,
        name,
        bases,
        attrs,
        *,
        python: Type[Any] = type(None),
        sql: Union[str, Callable[..., str]] = "NULL",
        values: Dict[str, Any] = {},
        **kwargs: Any,
    ):
        obj = cast(SQLTypeMeta, super().__new__(cls, name, bases, attrs, **kwargs))
        obj._python = python
        obj._preformatted_sql = sql
        obj._format_values = values

        return obj

    def __repr__(cls) -> str:
        return f'<donphan.SQLType python="{cls._python}" sql="{cls._sql}">'

    def __call__(cls, **kwargs) -> Type[SQLType]:  # type: ignore
        return cast(
            Type[SQLType],
            type(
                cls.__name__,
                (SQLType,),
                {
                    "python": cls._python,
                    "sql": cls._preformatted_sql,
                    "values": kwargs,
                },
            ),
        )

    def __getattribute__(self, name: str) -> Type[Column]:
        return super().__getattribute__(name)  # type: ignore

    @property
    def _sql(cls) -> str:
        if inspect.isfunction(cls._preformatted_sql):
            return cls._preformatted_sql(**cls._format_values)  # type: ignore
        return cls._preformatted_sql  # type: ignore


class BaseSQLType:
    ...


class SQLType(BaseSQLType, metaclass=SQLTypeMeta):
    _python: Type
    _sql: str

    def __repr__(self):
        return f"<SQLType sql='{self._sql}' python='{self._python.__name__}'>"

    def __eq__(self, other) -> bool:
        return self._sql == other._sql

    @classmethod
    def _from_python_type(cls, python_type: type):
        """Dynamically determines an SQL type given a python type.
        Args:
            python_type (type): The python type.
        """

        if DEFAULT_TYPES.get(python_type):
            return getattr(cls, DEFAULT_TYPES[python_type])

        raise TypeError(f"Could not find an applicable SQL type for Python type {python_type!r}.")


for name, definition in SQL_TYPES.items():
    python_type, sql_type = definition
    type_cls = SQLTypeMeta.__new__(SQLTypeMeta, name, (SQLType,), {}, python=python_type, sql=sql_type)

    setattr(SQLType, name, type_cls)


for alias, sqltype in SQL_TYPE_ALIASES.items():
    setattr(SQLType, alias, getattr(SQLType, sqltype))
