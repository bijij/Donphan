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

import datetime
import decimal
import ipaddress
import uuid

from typing import (
    Any,
    cast,
    Generic,
    Union,
    Callable,
    Dict,
    Type,
    TypeVar,
    TYPE_CHECKING,
)

from .meta import ObjectMeta

if TYPE_CHECKING:
    from .column import _Column


T = TypeVar("T")


__all__ = ("SQLType",)


class SQLTypeMeta(ObjectMeta):
    if TYPE_CHECKING:
        _python: Type
        _preformatted_sql: Union[str, Callable[..., str]]
        _format_values: Dict[str, Any]

    def __new__(cls, name, bases, attrs, *, values: Dict[str, Any] = {}, **kwargs: Any) -> Type[_Column]:  # type: ignore[misc]
        obj = cast(SQLTypeMeta, super().__new__(cls, name, bases, attrs, **kwargs))
        obj._format_values = values
        return obj  # type: ignore[return-value]

    def __repr__(cls) -> str:
        return f'<donphan.SQLType python="{cls._python}" sql="{cls._sql}">'

    def __call__(cls, **kwargs) -> Type[_Column]:  # type: ignore[override]
        return SQLTypeMeta.__new__(
            SQLTypeMeta,
            cls.__name__,
            (_SQLType,),
            {
                "_python": cls._python,
                "_preformatted_sql": cls._preformatted_sql,
            },
            values=kwargs,
        )

    def __getattribute__(self, name: str) -> Type[_Column]:
        return super().__getattribute__(name)

    @property
    def _sql(cls) -> str:
        if callable(cls._preformatted_sql):
            return cls._preformatted_sql(**cls._format_values)
        return cls._preformatted_sql


class BaseSQLType:
    ...


class _SQLType(BaseSQLType, metaclass=SQLTypeMeta):
    _python: Type[Any]
    _sql: str

    def __repr__(self):
        return f"<SQLType sql='{self._sql}' python='{self._python.__name__}'>"

    def __eq__(self, other) -> bool:
        return self._sql == other._sql

    @classmethod
    def _from_python_type(cls, python_type: Type[Any]):
        """Dynamically determines an SQL type given a python type.
        Args:
            python_type (type): The python type.
        """

        if python_type in DEFAULT_TYPES:
            return DEFAULT_TYPES[python_type]

        raise TypeError(f"Could not find an applicable SQL type for Python type {python_type!r}.")


def _create_sqltype(name: str, py_type: Type, sql_type: Union[str, Callable[..., str]]) -> Type[_Column]:
    return SQLTypeMeta.__new__(
        SQLTypeMeta,
        name,
        (_SQLType,),
        {
            "_python": py_type,
            "_preformatted_sql": sql_type,
        },
    )


def _varchar_sql(n: int = 2000):
    return f"CHARACTER VARYING({n})"


def _timestamp_sql(with_timezone: bool = False):
    return "TIMESTAMP WITH TIME ZONE" if with_timezone else "TIMESTAMP"


class SQLType:

    # 8.1 Numeric
    Int = Integer = _create_sqltype("Integer", int, "INTEGER")
    SmallInt = _create_sqltype("SmallInt", int, "SMALLINT")
    BigInt = _create_sqltype("BigInt", int, "BIGINT")
    Serial = _create_sqltype("Serial", int, "SERIAL")
    Float = _create_sqltype("Float", float, "FLOAT")
    DoublePrecision = _create_sqltype("DoublePrecision", float, "DOUBLE PRECISION")
    Numeric = _create_sqltype("Numeric", decimal.Decimal, "NUMERIC")

    # 8.2 Monetary
    Money = _create_sqltype("Money", str, "MONEY")

    # 8.14 JSON
    JSON = _create_sqltype("JSON", dict, "JSON")
    JSONB = _create_sqltype("JSONB", dict, "JSONB")

    # 8.3 Character
    VarChar = CharacterVarying = _create_sqltype("CharacterVarying", str, _varchar_sql)
    Char = Character = _create_sqltype("Character", str, "CHARACTER")
    Text = _create_sqltype("Text", str, "TEXT")

    # 8.4 Binary
    Bytea = _create_sqltype("Bytea", bytes, "BYTEA")

    # 8.5 Date/Time
    Timestamp = _create_sqltype("Timestamp", datetime.datetime, _timestamp_sql)
    Date = _create_sqltype("Date", datetime.date, "DATE")
    Interval = _create_sqltype("Interval", datetime.timedelta, "INTERVAL")

    # 8.6 Boolean
    Boolean = _create_sqltype("Boolean", bool, "BOOLEAN")

    # 8.9 Network Adress
    CIDR = _create_sqltype("CIDR", ipaddress._BaseNetwork, "CIDR")
    Inet = _create_sqltype("Inet", ipaddress._BaseNetwork, "INET")
    MACAddr = _create_sqltype("MACAddr", str, "MACADDR")

    # 8.12 UUID
    UUID = _create_sqltype("UUID", uuid.UUID, "UUID")


DEFAULT_TYPES: Dict[Type, Type[BaseSQLType]] = {
    int: SQLType.Integer,
    float: SQLType.Float,
    decimal.Decimal: SQLType.Numeric,
    str: SQLType.Text,
    bytes: SQLType.Bytea,
    datetime.datetime: SQLType.Timestamp,
    datetime.date: SQLType.Date,
    datetime.timedelta: SQLType.Interval,
    bool: SQLType.Boolean,
    ipaddress.IPv4Network: SQLType.CIDR,
    ipaddress.IPv6Network: SQLType.CIDR,
    ipaddress.IPv4Address: SQLType.Inet,
    ipaddress.IPv6Address: SQLType.Inet,
    uuid.UUID: SQLType.UUID,
    dict: SQLType.JSONB,
}
