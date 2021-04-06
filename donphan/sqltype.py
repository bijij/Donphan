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

from typing import Any, Callable, Dict, Type, TypeVar


T = TypeVar('T', bound=Callable[..., 'SQLType'])


DEFAULT_TYPES: Dict[type, Callable[..., SQLType]] = {}


def default_for(python_type: Type[Any]) -> Callable[[T], T]:
    """Sets a specified python type's default SQL type.
    Args:
        python_type (type): Python type to set the specified sqltype as default for.
    """
    def func(sql_type: T) -> T:
        DEFAULT_TYPES[python_type] = sql_type
        return sql_type
    return func


class SQLType:

    def __init__(self, python: Type[Any], sql: str):
        self._python = python
        self._sql = sql

    def __repr__(self):
        return f'<SQLType sql=\'{self._sql}\' python=\'{self.__name__}\'>'

    def __eq__(self, other) -> bool:
        return self._sql == other._sql

    @property
    def __name__(self) -> str:
        return self._python.__name__

    # 8.1 Numeric

    @classmethod
    @default_for(int)
    def Integer(cls):
        """Postgres Integer Type"""
        return cls(int, 'INTEGER')

    @classmethod
    def SmallInt(cls):
        """Postgres SmallInt Type"""
        return cls(int, 'SMALLINT')

    @classmethod
    def BigInt(cls):
        """Postgres BigInt Type"""
        return cls(int, 'BIGINT')

    @classmethod
    def Serial(cls):
        """Postgres Serial Type"""
        return cls(int, 'SERIAL')

    @classmethod
    @default_for(float)
    def Float(cls):
        """Postgres Float Type"""
        return cls(float, 'FLOAT')

    @classmethod
    def DoublePrecision(cls):
        """Postgres DoublePrecision Type"""
        return cls(float, 'DOUBLE PRECISION')

    @classmethod
    @default_for(decimal.Decimal)
    def Numeric(cls):
        """Postgres Numeric Type"""
        return cls(decimal.Decimal, 'NUMERIC')

    # 8.2 Monetary

    @classmethod
    def Money(cls):
        """Postgres Money Type"""
        return cls(str, 'MONEY')

    # 8.3 Character

    @classmethod
    def CharacterVarying(cls, n: int = 2000):
        return cls(str, f'CHARACTER VARYING({n})')

    @classmethod
    def Character(cls):
        """Postgres Character Type"""
        return cls(str, 'CHARACTER')

    @classmethod
    @default_for(str)
    def Text(cls):
        """Postgres Text Type"""
        return cls(str, 'TEXT')

    # 8.4 Binary

    @classmethod
    @default_for(bytes)
    def Bytea(cls):
        """Postgres Bytea Type"""
        return cls(bytes, 'BYTEA')

    # 8.5 Date/Time

    @classmethod
    @default_for(datetime.datetime)
    def Timestamp(cls, with_timezone: bool = True):
        """Postgres Timestamp Type"""
        return cls(datetime.datetime, 'TIMESTAMP WITH TIME ZONE' if with_timezone else 'TIMESTAMP')

    @classmethod
    @default_for(datetime.date)
    def Date(cls):
        """Postgres Date Type"""
        return cls(datetime.date, 'DATE')

    @classmethod
    @default_for(datetime.timedelta)
    def Interval(cls):
        """Postgres Interval Type"""
        return cls(datetime.timedelta, 'INTERVAL')

    # 8.6 Boolean

    @classmethod
    @default_for(bool)
    def Boolean(cls):
        """Postgres Boolean Type"""
        return cls(bool, 'BOOLEAN')

    # 8.9 Network Adress

    @classmethod
    @default_for(ipaddress.IPv4Network)
    @default_for(ipaddress.IPv6Network)
    def CIDR(cls):
        """Postgres CIDR Type"""
        return cls(ipaddress._BaseNetwork, 'CIDR')

    @classmethod
    @default_for(ipaddress.IPv4Address)
    @default_for(ipaddress.IPv6Address)
    def Inet(cls):
        """Postgres Inet Type"""
        return cls(ipaddress._BaseNetwork, 'INET')

    @classmethod
    def MACAddr(cls):
        """Postgres MACAddr Type"""
        return cls(str, 'MACADDR')

    # 8.12 UUID

    @classmethod
    @default_for(uuid.UUID)
    def UUID(cls):
        """Postgres UUID Type"""
        return cls(uuid.UUID, 'UUID')

    # 8.14 JSON

    @classmethod
    def JSON(cls):
        """Postgres JSON Type"""
        return cls(dict, 'JSON')

    @classmethod
    @default_for(dict)
    def JSONB(cls):
        """Postgres JSONB Type"""
        return cls(dict, 'JSONB')

    # Aliases
    Char = Character
    VarChar = CharacterVarying

    @classmethod
    def _from_python_type(cls, python_type: type):
        """Dynamically determines an SQL type given a python type.
        Args:
            python_type (type): The python type.
        """

        if DEFAULT_TYPES.get(python_type):
            return DEFAULT_TYPES[python_type](cls)

        raise TypeError(
            f'Could not find an applicable SQL type for Python type {python_type!r}.')
