from __future__ import annotations

from typing import Protocol, TYPE_CHECKING

from .consts import DEFAULT_SCHEMA, NOT_CREATABLE
from .utils import MISSING, normalise_name, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection


__all__ = ("Creatable",)


class Creatable(Protocol):
    _schema: str
    _name: str

    def __init_subclass__(
        cls,
        schema: str = MISSING,
    ) -> None:
        if schema is MISSING:
            schema = DEFAULT_SCHEMA

        name = normalise_name(cls.__name__)

        cls._schema = schema
        cls._name = f"{schema}.{name}"
        super().__init_subclass__()

    # region: query generation

    @classmethod
    def _query_create(
        cls,
        if_not_exists: bool,
    ) -> str:
        raise NotImplementedError()

    @classmethod
    @query_builder
    def _query_drop(
        cls,
        type: str,
        if_exists: bool,
        cascade: bool,
    ) -> list[str]:
        builder = ["DROP", type]

        if if_exists:
            builder.append("IF EXISTS")

        builder.append(cls._name)

        if cascade:
            builder.append("CASCADE")

        return builder

    # endregion

    # region: public methods

    @classmethod
    async def create(
        cls,
        connection: Connection,
        *,
        if_not_exists: bool = False,
    ) -> None:
        """|coro|

        Creates this database object.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection`
            The database connection to use for transactions.
        if_not_exists: :class:`bool`
            Sets whether creation should continue if the object already exists.
            Defaults to ``False``.
        """
        query = cls._query_create(if_not_exists)
        await connection.execute(query)

    @classmethod
    async def create_all(
        cls,
        connection: Connection,
        /,
        *,
        if_not_exists: bool = False,
    ) -> None:
        """|coro|

        Creates all subclasses of this database object.

        Parameters
        ----------
        connection: :class:`asyncpg.Connection`
            The database connection to use for transactions.
        if_not_exists: :class:`bool`
            Sets whether creation should continue if the object already exists.
            Defaults to ``False``.
        """
        for subcls in cls.__subclasses__():
            if subcls in NOT_CREATABLE:
                await subcls.create_all(connection, if_not_exists=if_not_exists)
            else:
                await subcls.create(connection, if_not_exists=if_not_exists)

    @classmethod
    async def drop(
        cls,
        connection: Connection,
        /,
        *,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        """|coro|

        Drops this object from the database.

        Parameters
        ----------
        if_exists: :class:`bool`
            Sets whether dropping should not error if the object does not exist.
            Defaults to ``True``.
        cascade: :class:`bool`
            Sets whether dropping should cascade.
            Defaults to ``False``.
        """
        query = cls._query_drop(if_exists, cascade)
        await connection.execute(query)

    # endregion
