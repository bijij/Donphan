from __future__ import annotations
from sys import is_finalizing

from typing import Protocol, TYPE_CHECKING

from .consts import DEFAULT_SCHEMA, NOT_CREATABLE
from .utils import MISSING, normalise_name

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = ("Creatable",)


class Creatable(Protocol):
    _schema: str
    _name: str

    def __init_subclass__(cls, schema: str = MISSING) -> None:
        if schema is MISSING:
            schema = DEFAULT_SCHEMA

        name = normalise_name(cls.__name__)

        cls._schema = schema
        cls._name = f"{schema}.{name}"
        super().__init_subclass__()

    # region query generation

    @classmethod
    def _query_create(cls, if_not_exists: bool) -> str:
        raise NotImplementedError()

    @classmethod
    def _query_drop(cls, cascade: bool) -> str:
        raise NotImplementedError()

    # endregion

    # region public methods

    @classmethod
    async def create(cls, connection: Connection, *, if_not_exists: bool = False) -> None:
        query = cls._query_create(if_not_exists)
        await connection.execute(query)

    @classmethod
    async def create_all(cls, connection: Connection, *, if_not_exists: bool = False) -> None:
        for subcls in cls.__subclasses__():
            if subcls in NOT_CREATABLE:
                await subcls.create_all(connection, if_not_exists=if_not_exists)
            else:
                print(subcls._query_create(if_not_exists))
                await subcls.create(connection, if_not_exists=if_not_exists)

    @classmethod
    async def drop(cls, connection: Connection, *, cascade: bool = False) -> None:
        query = cls._query_drop(cascade)
        await connection.execute(query)

    # endregion
