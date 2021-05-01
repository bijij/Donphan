from __future__ import annotations

import decimal
import datetime
from donphan.consts import ENUM_TYPES, POOLS
import ipaddress
import uuid

from types import new_class
from typing import Any, Generic, NamedTuple, Optional, TYPE_CHECKING, TypeVar

from .creatable import Creatable
from .enums import Enum
from .utils import normalise_name, not_creatable, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = (
    "SQLType",
    "CustomType",
    "EnumType",
)


T = TypeVar("T")
OT = TypeVar("OT")
ET = TypeVar("ET", bound=Enum)


class SQLType(Generic[T]):
    if TYPE_CHECKING:
        Integer = int
        Text = str

    py_type: type[T]
    sql_type: str
    __defaults: dict[type[Any], type[SQLType[Any]]] = {}
    __enum_types: dict[type[Any], type[SQLType[Any]]] = {}

    @classmethod
    def from_type(cls, type: type[OT]) -> type[SQLType[OT]]:
        if issubclass(type, Enum):
            if type in cls.__enum_types:
                return cls.__enum_types[type]

            enum_type = new_class(type.__name__, (EnumType[type],))
            cls.__enum_types[type] = enum_type
            return enum_type

        return cls.__defaults[type]

    def __init_subclass__(cls, *, sql_type: Optional[str] = None, default: bool = False, **kwargs: Any) -> None:
        cls.py_type = cls.__orig_bases__[0].__args__[0]  # type: ignore
        if default:
            cls.__defaults[cls.py_type] = cls
        super().__init_subclass__(**kwargs)
        cls.sql_type = sql_type or cls._name  # type: ignore


@not_creatable
class CustomType(SQLType[T], Creatable, sql_type=""):
    @classmethod
    def _query_drop(cls, if_exists: bool, cascade: bool) -> str:
        return super()._query_drop("TYPE", if_exists, cascade)


def _encode_enum(value: Enum) -> str:
    return value.name


@not_creatable
class EnumType(CustomType[ET], sql_type=""):
    @classmethod
    @query_builder
    def _query_create(cls, if_not_exists: bool) -> list[str]:
        builder = ["CREATE TYPE"]
        builder.append(cls._name)
        builder.append("AS ENUM (")

        for key in cls.py_type:  # type: ignore
            builder.append(f"'{key.name}'")
            builder.append(",")

        builder.pop(-1)

        builder.append(")")
        return builder

    @classmethod
    def _decoder(cls, value: str) -> ET:
        return getattr(cls.py_type, value)

    @classmethod
    async def _set_codec(cls, connection: Connection) -> None:
        await connection.set_type_codec(
            normalise_name(cls.__name__),
            schema=cls._schema,
            encoder=_encode_enum,
            decoder=cls._decoder,
            format="text",
        )

    @classmethod
    async def create(cls, connection: Connection, *args: Any, **kwargs: Any) -> None:
        await super().create(connection, *args, **kwargs)
        ENUM_TYPES.append(cls)

        for pool in POOLS:
            for holder in pool._holders:
                await cls._set_codec(holder._con)

    @classmethod
    async def drop(cls, connection: Connection, *args: Any, **kwargs: Any) -> None:
        ENUM_TYPES.remove(cls)

        for pool in POOLS:
            for holder in pool._holders:
                await holder._con.reset_type_codec(normalise_name(cls.__name__), schema=cls._schema)

        await super().drop(connection, *args, **kwargs)


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
        "JSONB": SQLTypeConfig(dict, "JSONB"),
    }.items():
        cls = new_class(name, (SQLType[py_type],), {"sql_type": sql_type, "default": is_default})
        new_class(name + "[]", (SQLType[list[py_type]],), {"sql_type": sql_type + "[]", "default": is_default})
        setattr(SQLType, name, cls)
        for alias in aliases:
            setattr(SQLType, alias, cls)
