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
import types
import uuid
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeVar

from ._enums import Enum
from .utils import DOCS_BUILDING, MISSING

__all__ = ("SQLType",)


T = TypeVar("T")
OT = TypeVar("OT")


class SQLType(Generic[T]):
    """A representation of an SQL type

    Attributes
    ----------
        py_type: Type[Any]
            The python type associated with the column.
        sql_type: str
            The SQL type associated with the column.
    """

    py_type: type[T]
    sql_type: str
    __defaults: dict[type[Any], type[SQLType[Any]]] = {}
    __enum_types: dict[type[Any], type[SQLType[Any]]] = {}

    def __init_subclass__(
        cls,
        *,
        sql_type: str = MISSING,
        default: bool = False,
        **kwargs: Any,
    ) -> None:
        cls.py_type = cls.__orig_bases__[0].__args__[0]  # type: ignore
        if default:
            cls.__defaults[cls.py_type] = cls
        super().__init_subclass__(**kwargs)
        cls.sql_type = sql_type

    @classmethod
    def _from_type(cls, type: type[OT]) -> type[SQLType[OT]]:
        if issubclass(type, Enum):
            # this is a hack because >circular imports<
            from ._custom_types import EnumType

            if type in cls.__enum_types:
                return cls.__enum_types[type]

            enum_type = types.new_class(type.__name__, (EnumType[type],))
            cls.__enum_types[type] = enum_type  # type: ignore
            return enum_type  # type: ignore

        return cls.__defaults[type]


if not TYPE_CHECKING:

    class SQLTypeConfig(NamedTuple):
        py_type: type[Any]
        sql_type: str
        is_default: bool = False
        aliases: tuple[str, ...] = ()

    for name, (py_type, sql_type, is_default, aliases) in {
        # 8.1 Numeric
        "Integer": SQLTypeConfig(int, "INTEGER", True, ("Int",)),
        "SmallInt": SQLTypeConfig(int, "SMALLINT"),
        "BigInt": SQLTypeConfig(int, "BIGINT"),
        "Serial": SQLTypeConfig(int, "SERIAL"),
        "SmallSerial": SQLTypeConfig(int, "SMALLSERIAL"),
        "BigSerial": SQLTypeConfig(int, "BIGSERIAL"),
        "Float": SQLTypeConfig(float, "FLOAT", True),
        "DoublePrecision": SQLTypeConfig(float, "DOUBLE PRECISION", False, ("Double",)),
        "Numeric": SQLTypeConfig(decimal.Decimal, "NUMERIC", True),
        # 8.2 Monetary
        "Money": SQLTypeConfig(str, "MONEY"),
        # 8.3 Character
        # "CharacterVarying": SQLTypeConfig(str, "CHARACTER VARYING({n})", False, ("VarChar",)),
        "Character": SQLTypeConfig(str, "CHARACTER", False, ("Char",)),
        "Text": SQLTypeConfig(str, "TEXT", True),
        # 8.4 Binary
        "Bytea": SQLTypeConfig(bytes, "BYTEA", True),
        # 8.5 Date/Time
        "Timestamp": SQLTypeConfig(datetime.datetime, "TIMESTAMP", True, ("NaieveTimestamp", "Datetime")),
        "AwareTimestamp": SQLTypeConfig(datetime.datetime, "TIMESTAMP WITH TIME ZONE", True),
        "Date": SQLTypeConfig(datetime.date, "DATE", True),
        "Interval": SQLTypeConfig(datetime.timedelta, "INTERVAL", True),
        # 8.6 Boolean
        "Boolean": SQLTypeConfig(bool, "BOOLEAN", True),
        # 8.9 Network Adress
        "CIDR": SQLTypeConfig(ipaddress._BaseNetwork, "CIDR"),
        "Inet": SQLTypeConfig(ipaddress._BaseNetwork, "INET"),
        "MACAddr": SQLTypeConfig(str, "MACADDR"),
        # 8.12 UUID
        "UUID": SQLTypeConfig(uuid.UUID, "UUID", True),
        # 8.14 JSON
        "JSON": SQLTypeConfig(dict, "JSON"),
        "JSONB": SQLTypeConfig(dict, "JSONB", True),
    }.items():
        # generate SQLType[T] and SQLType[list[T]]
        cls = types.new_class(name, (SQLType[py_type],), {"sql_type": sql_type, "default": is_default})
        types.new_class(
            name + "[]",
            (SQLType[list[py_type]],),
            {"sql_type": sql_type + "[]", "default": is_default},
        )

        if DOCS_BUILDING:

            @property
            def _(cls):
                return cls

            _.__doc__ = f"Represents the SQL ``{sql_type}`` type."

            if is_default:
                qualified_name = ""
                if py_type.__module__ != "builtins":
                    qualified_name = f"{py_type.__module__}."
                qualified_name += py_type.__name__
                _.__doc__ += f" Python class :class:`{qualified_name}`, can be used as a substitute."

        else:
            _ = cls

        setattr(SQLType, name, _)
        for alias in aliases:
            setattr(SQLType, alias, _)
