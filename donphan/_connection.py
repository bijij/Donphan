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
import json
from collections.abc import Callable
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    Optional,
    TextIO,
    TypeVar,
    Union,
    overload,
)

import asyncpg

from ._consts import CUSTOM_TYPES, DEFAULT_SCHEMA, POOLS
from .utils import DOCS_BUILDING, write_to_file, optional_transaction

if TYPE_CHECKING:
    from asyncpg import Connection, Pool

    from ._object import Object

__all__ = (
    "create_pool",
    "create_db",
    "export_db",
    "MaybeAcquire",
    "TYPE_CODECS",
    "TypeCodec",
    "OPTIONAL_CODECS",
)


T = TypeVar("T")
BE = TypeVar("BE", bound=BaseException)
OT = TypeVar("OT", bound="Object")


# Y2K_DT = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
Y2K_EPOCH: Final[int] = 946684800000000


class TypeCodec(tuple[T]):
    """
    A NamedTuple defining a custom type codec.

    See :meth:`asyncpg.Connection.set_type_codec <asyncpg.connection.Connection.set_type_codec>` for more information.


    Parameters
    ----------
    format: Literal[``"text"``, ``"binary"``, ``"tuple"``]
        The type of decoder/encoder to use.

    encoder: collections.abc.Callable[``...``, Any]
        A callable which given a python object returns an encoded value.

    decoder: collections.abc.Callable[``...``, Any]
        A callable which given an encoded value returns a python object.
    """

    if TYPE_CHECKING:
        format: Literal["text", "binary", "tuple"]
        encoder: Callable[[T], Any]
        decoder: Callable[..., T]

    def __new__(
        cls: type[TypeCodec[T]],
        format: Literal["text", "binary", "tuple"],
        encoder: Callable[[T], Any],
        decoder: Callable[..., T],
    ) -> TypeCodec[T]:
        new_cls = super().__new__(cls, (format, encoder, decoder))  # type: ignore
        new_cls.format = format
        new_cls.encoder = encoder
        new_cls.decoder = decoder
        return new_cls


def _encode_datetime(value: datetime.datetime) -> tuple[int]:
    if value.tzinfo is None:
        value = value.astimezone(datetime.timezone.utc)
    return (round(value.timestamp() * 1_000_000) - Y2K_EPOCH,)


def _decode_timestamp(value: tuple[int]) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(round(value[0] + Y2K_EPOCH) / 1_000_000, datetime.timezone.utc)


TYPE_CODECS: dict[str, TypeCodec[Any]] = {
    "json": TypeCodec[dict]("text", json.dumps, json.loads),
    "jsonb": TypeCodec[dict]("text", json.dumps, json.loads),
}

OPTIONAL_CODECS: dict[str, TypeCodec[Any]] = {
    "timestamp": TypeCodec[datetime.datetime]("tuple", _encode_datetime, _decode_timestamp),
}

if DOCS_BUILDING and not TYPE_CHECKING:

    class TYPE_CODECS(dict[str, TypeCodec[Any]]):
        """
        A dictionary of pre-defined custom type-codecs.
        """

        @property
        def json(cls):
            """Automatically conveters a dictionary to and from json data to be stored in the database."""
            ...

        jsonb = json

    class OPTIONAL_CODECS(dict[str, TypeCodec[Any]]):
        """
        A dictionary of optional custom type-codecs.
        """

        @property
        def timestamp(cls):
            """Ensures all timestamps are timezone aware relative to UTC."""
            ...


async def create_pool(
    dsn: str,
    codecs: dict[str, TypeCodec[Any]] = {},
    *,
    set_as_default: bool = False,
    **kwargs: Any,
) -> Pool:
    r"""|coro|

    Creates the database connection pool.

    Parameters
    ----------
    dsn: :class:`str`
        A database connection string.
    codecs: Dict[:class:`str`, :class:`.TypeCodec`]
        A mapping of type to encoder and decoder for custom type codecs.
        A pre-defined set of codecs is provided in :class:`TYPE_CODECS` is used by default.
        As well as a set of :class:`OPTIONAL_CODECS` is provided.
    set_as_default: :class:`bool`
        Sets whether the pool should be used as the default connection pool for database operations.
        Defaults to ``False``.
    \*\*kwargs: Any
        Extra keyword arguments to pass to :func:`asyncpg.create_pool <asyncpg.pool.create_pool>`

    Returns
    -------
    :class:`asyncpg.Pool <asyncpg.pool.Pool>`
        The new pool which was created.
    """
    codecs = TYPE_CODECS | codecs

    async def init(connection: Connection) -> None:
        for type, codec in codecs.items():
            await connection.set_type_codec(
                type,
                schema="pg_catalog",
                encoder=codec.encoder,
                decoder=codec.decoder,
                format=codec.format,
            )

        for type in CUSTOM_TYPES.values():
            await type._set_codec(connection)

    pool = await asyncpg.create_pool(dsn, init=init, **kwargs)
    if pool is None:
        raise RuntimeError("Could not create pool.")
    POOLS.append(pool)

    if set_as_default:
        # this is a hack because >circular imports<
        from ._object import Object

        Object.set_pool(pool)

    return pool


async def create_db(
    connection: Connection,
    if_not_exists: bool = True,
    automatic_migrations: bool = False,
    with_transaction: bool = True,
) -> None:
    """|coro|

    A helper function to create all objects in the database.

    Parameters
    ----------
    connection: :class:`asyncpg.Connection <asyncpg.connection.Connection>`
            The database connection to use for transactions.
    if_not_exists: :class:`bool`
        Sets whether creation should continue if the object already exists.
        Defaults to ``True``.
    automatic_migrations: :class:`bool`
        Sets whether automatic migrations should be run.
        Defaults to ``False``.
    with_transaction: :class:`bool`
        Sets whether the database should be wrapped in a transaction.
        Defaults to ``True``.
    """
    # this is a hack because >circular imports<
    from ._creatable import Creatable
    from ._custom_types import CustomType
    from ._table import Table
    from ._view import View

    async with optional_transaction(connection, with_transaction):

        for schema in Creatable._find_schemas():
            if schema._schema != DEFAULT_SCHEMA:
                await schema.create(connection, if_not_exists=if_not_exists)

    await CustomType.create_all(
        connection,
        if_not_exists=if_not_exists,
        create_schema=False,
        automatic_migrations=automatic_migrations,
        with_transaction=False,
    )

    async with optional_transaction(connection, with_transaction):

        for type in (Table, View):
            await type.create_all(
                connection,
                if_not_exists=if_not_exists,
                create_schema=False,
                automatic_migrations=automatic_migrations,
                with_transaction=False,
            )


@overload
def export_db(*, if_not_exists: bool = ..., fp: None = ...) -> str:
    ...


@overload
def export_db(*, if_not_exists: bool = ..., fp: Union[str, TextIO] = ...) -> TextIO:
    ...


def export_db(*, if_not_exists: bool = False, fp: Optional[Union[str, TextIO]] = None) -> Union[TextIO, str]:
    """
    A helper function which exports all objects in the database.

    Parameters
    ----------
    if_not_exists: :class:`bool`
        Sets whether the if_not_exists clause should be set on
        exported objects. Defaults to ``False``.
    fp: Optional[:class:`os.PathLike`, :class:`io.TextIOBase`]
        A file-like object opened in text mode and write mode.
        or a filename representing a file on disk to write to.

        .. note::
            If the file-like object passed is opened via :func:`open`
            ensure the object is in a text-writing mode such as ``"w"``.

            If the file-like object passed is a path it will be opened in
            write-mode ``"w"``.

    Returns
    -------
    Union[:class:`io.TextIOBase`, :class:`str`]
        The file-like object which was provided or a string containing the
        exported database.
    """
    # this is a hack because >circular imports<
    from ._creatable import Creatable
    from ._custom_types import CustomType
    from ._table import Table
    from ._view import View

    output = ""

    for schema in Creatable._find_schemas():
        if schema._schema != DEFAULT_SCHEMA:
            output += schema.export_schema(if_not_exists=if_not_exists)
            output += "\n\n"

    for type in (CustomType, Table, View):
        output += type.export_all(if_not_exists=if_not_exists, export_schema=False)

    if fp is None:
        return output
    return write_to_file(fp, output)


class MaybeAcquire:
    """Async helper for acquiring a connection to the database.

    .. container:: operations

        .. describe:: async with x as c

            Yields a database connection to use,
            if a new one was created it will be closed on exit.

    Parameters
    ----------
        connection: Optional[:class:`asyncpg.Connection <asyncpg.connection.Connection>`]
            A database connection to use.
            If none is supplied a connection will be acquired from the pool.
        pool: Optional[:class:`asyncpg.Pool <asyncpg.pool.Pool>`]
            A connection pool to use.

    Attributes
    ----------
        connection: Optional[:class:`asyncpg.Connection <asyncpg.connection.Connection>`]
            The supplied database connection, if provided.
        pool: Optional[:class:`asyncpg.Pool <asyncpg.pool.Pool>`]
            The connection pool used to acquire new connections.
    """

    @overload
    def __init__(self, connection: Connection = ..., /, *, pool: Optional[Pool] = ...):
        ...

    @overload
    def __init__(self, connection: None = ..., /, *, pool: Pool = ...):
        ...

    def __init__(self, connection: Optional[Connection] = None, /, *, pool: Optional[Pool] = None):
        self.connection: Optional[Connection] = connection
        self.pool: Optional[Pool] = pool
        self._cleanup: bool = False

    async def __aenter__(self) -> Connection:
        if self.connection is None:
            self._cleanup = True
            self._connection = c = await self.pool.acquire()  # type: ignore
            return c
        return self.connection

    async def __aexit__(self, exc_type: type[BE], exc_val: BE, exc_tb: TracebackType) -> None:
        if self._cleanup:
            await self.pool.release(self._connection)  # type: ignore
