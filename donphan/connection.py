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
from typing import Any, TYPE_CHECKING, NamedTuple, TypeVar, Literal

import asyncpg

from .consts import CUSTOM_TYPES, POOLS

if TYPE_CHECKING:
    from asyncpg import Connection
    from asyncpg.pool import Pool

__all__ = (
    "create_pool",
    "MaybeAcquire",
    "TYPE_CODECS",
    "OPTIONAL_CODECS",
)


T = TypeVar("T")


# Y2K_DT = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
Y2K_EPOCH = 946684800000000


class TypeCodec(NamedTuple):
    format: Literal["text", "binary", "tuple"]
    encoder: Callable[..., Any]
    decoder: Callable[..., Any]


def _encode_datetime(value: datetime.datetime) -> tuple[int]:
    if value.tzinfo is None:
        value = value.astimezone(datetime.timezone.utc)
    return (round(value.timestamp() * 1_000_000) - Y2K_EPOCH,)


def _decode_timestamp(value: tuple[int]) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(round(value[0] + Y2K_EPOCH) / 1_000_000, datetime.timezone.utc)


TYPE_CODECS: dict[str, TypeCodec] = {
    "json": TypeCodec("text", json.dumps, json.loads),
    "jsonb": TypeCodec("text", json.dumps, json.loads),
}

OPTIONAL_CODECS: dict[str, TypeCodec] = {
    "timestamp": TypeCodec("tuple", _encode_datetime, _decode_timestamp),
}


async def create_pool(dsn: str, codecs: dict[str, TypeCodec] = {}, **kwargs) -> Pool:
    r"""|coro|

    Creates the database connection pool.

    Parameters
    ----------
    dsn: :class:`str`
        A database connection string.
    codecs: Dict[:class:`str`, :class:`.TypeCodec`]
        A mapping of type to encoder and decoder for custom type codecs.
    \*\*kwargs: Any
        Extra keyword arguments to pass to :meth:`asyncpg.create_pool`

    Returns
    -------
    :class:`asyncpg.pool.Pool`
        The new pool which was created.
    """
    codecs |= TYPE_CODECS

    async def init(connection: Connection) -> None:
        for type, codec in codecs.items():
            await connection.set_type_codec(
                type, schema="pg_catalog", encoder=codec.encoder, decoder=codec.decoder, format=codec.format
            )

        for type in CUSTOM_TYPES.values():
            await type._set_codec(connection)

    pool = await asyncpg.create_pool(dsn, init=init, **kwargs)
    if pool is None:
        raise RuntimeError("Could not create pool.")
    POOLS.append(pool)
    return pool


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
        pool: Optional[:class:`asyncpg.pool.Pool`]
            A connection pool to use.

    Attributes
    ----------
        connection: Optional[:class:`asyncpg.Connection <asyncpg.connection.Connection>`]
            The supplied database connection, if provided.
        pool: Optional[:class:`asyncpg.pool.Pool`]
            The connection pool used to acquire new connections.
    """

    def __init__(self, connection: Connection = None, /, *, pool: Pool):
        self.connection = connection
        self.pool = pool
        self._cleanup = False

    async def __aenter__(self) -> Connection:
        if self.connection is None:
            self._cleanup = True
            self._connection = c = await self.pool.acquire()
            return c
        return self.connection

    async def __aexit__(self, *args):
        if self._cleanup:
            await self.pool.release(self._connection)
