from .abc import Creatable, ObjectMeta
from .connection import MaybeAcquire
from .consts import DEFAULT_SCHEMA
from .sqltype import SQLType

from asyncpg import Connection
from asyncpg.exceptions import DuplicateObjectError

from typing import List, Type


class CustomTypeMeta(ObjectMeta):
    _sql: str

    def __new__(cls, name, bases, attrs, **kwargs):
        obj = super().__new__(cls, name, bases, attrs, **kwargs)

        obj._sql = obj._name

        return obj


class CustomType(Creatable, SQLType, metaclass=CustomTypeMeta):

    @classmethod
    def _query_drop(cls, if_exists=True, cascade=False):
        return cls._base_query_drop('TYPE', if_exists, cascade)


class EnumMeta(CustomTypeMeta):
    _values: List[str]
    _python: Type

    def __new__(cls, name, bases, attrs, **kwargs):

        attrs.update({
            '_values': kwargs.get('values', [])
        })

        obj = super().__new__(cls, name, bases, attrs, **kwargs)

        if not obj._values:
            raise ValueError(f'Enum type {obj._name} cannot have 0 values.')

        for _name in obj._values:
            if obj._values.count(name) > 1:
                raise ValueError(f'Enum cannot have duplicate value {_name}.')

        obj._python = str

        return obj

    def __getattr__(cls, key):
        if key in cls._values:
            return key

        return super().__getattr__(key)


class Enum(CustomType):

    @classmethod
    def _query_create(cls, drop_if_exists=True, if_not_exists=True):
        builder = ['CREATE TYPE']

        builder.append(cls._name)
        builder.append('AS ENUM (')

        builder.append(', '.join(f"'{value}'" for value in cls._values))

        builder.append(')')
        return ' '.join(builder)


def enum(name: str, values: str, *, schema: str = DEFAULT_SCHEMA) -> Type[Enum]:
    """Create a new enum type.

    Args:
        name (str): The name of the new enum type.
        values (str): Space separated list of enum values.
        schema (str, optional): The schema for the type.
    Returns:
        (Type): The new enum type.
    """
    return EnumMeta(name, (Enum,), {}, values=values.split(), schema=schema)  # type: ignore


async def create_types(connection: Connection = None, drop_if_exists: bool = False, if_not_exists: bool = True):
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
            except DuplicateObjectError:
                if not if_not_exists:
                    raise
