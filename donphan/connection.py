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

import datetime
import json

from typing import (
    Any,
    cast,
    Callable,
    Dict,
    NamedTuple,
    Tuple,
    TypeVar,
)

import asyncpg

__all__ = (
    "create_pool",
    "MaybeAcquire",
    "TYPE_CODECS",
    "OPTIONAL_CODECS",
)


T = TypeVar("T")


# Y2K_DT = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
Y2K_EPOCH = 946684800000000


_pool: asyncpg.pool.Pool = None


class TypeCodec(NamedTuple):
    format: str  # Literal['text', 'binary', 'tuple']
    encoder: Callable[..., Any]
    decoder: Callable[..., Any]


def _encode_datetime(value: datetime.datetime) -> Tuple[int]:
    if value.tzinfo is None:
        value = value.astimezone(datetime.timezone.utc)
    return (round(value.timestamp() * 1_000_000) - Y2K_EPOCH,)


def _decode_timestamp(value: Tuple[int]) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(round(value[0] + Y2K_EPOCH) / 1_000_000, datetime.timezone.utc)


TYPE_CODECS: Dict[str, TypeCodec] = {
    "json": TypeCodec("text", json.dumps, json.loads),
    "jsonb": TypeCodec("text", json.dumps, json.loads),
}

OPTIONAL_CODECS: Dict[str, TypeCodec] = {
    "timestamp": TypeCodec("tuple", _encode_datetime, _decode_timestamp),
}


async def create_pool(dsn: str, codecs: Dict[str, TypeCodec] = TYPE_CODECS, **kwargs) -> asyncpg.pool.Pool:
    """Creates the database connection pool.

    Args:
        dsn (str): A database connection string.
        codecs (Dict[str, TypeCodec]): A mapping of type to
            encoder and decoder for custom type codecs,
            defaults to encoders for JSON and JSONB.
    """
    global _pool

    async def init(connection: asyncpg.Connection) -> None:
        for type, codec in codecs.items():
            await connection.set_type_codec(
                type, schema="pg_catalog", encoder=codec.encoder, decoder=codec.decoder, format=codec.format
            )

    _pool = p = cast(asyncpg.pool.Pool, await asyncpg.create_pool(dsn, init=init, **kwargs))
    return p


class MaybeAcquire:
    """Async helper for acquiring a connection to the database.

    Args:
        connection (asyncpg.Connection, optional): A database connection to use
                If none is supplied a connection will be acquired from the pool.
    Kwargs:
        pool (asyncpg.pool.Pool, optional): A connection pool to use.
            If none is supplied the default pool will be used.
    """

    def __init__(self, connection: asyncpg.Connection = None, *, pool=None):
        self.connection = connection
        self.pool = pool or _pool
        self._cleanup = False

    async def __aenter__(self) -> asyncpg.Connection:
        if self.connection is None:
            self._cleanup = True
            self._connection = c = await self.pool.acquire()
            return c
        return self.connection

    async def __aexit__(self, *args):
        if self._cleanup:
            await self.pool.release(self._connection)
