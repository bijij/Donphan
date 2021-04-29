from __future__ import annotations

import datetime
import json

from collections.abc import Callable
from typing import Any, TYPE_CHECKING, NamedTuple, TypeVar

import asyncpg


if TYPE_CHECKING:
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
    format: str  # Literal['text', 'binary', 'tuple']
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


async def create_pool(dsn: str, codecs: dict[str, TypeCodec] = TYPE_CODECS, **kwargs) -> asyncpg.pool.Pool:
    """Creates the database connection pool.
    Args:
        dsn (str): A database connection string.
        codecs (Dict[str, TypeCodec]): A mapping of type to
            encoder and decoder for custom type codecs,
            defaults to encoders for JSON and JSONB.
    """

    async def init(connection: asyncpg.Connection) -> None:
        for type, codec in codecs.items():
            await connection.set_type_codec(
                type, schema="pg_catalog", encoder=codec.encoder, decoder=codec.decoder, format=codec.format
            )

    return await asyncpg.create_pool(dsn, init=init, **kwargs)  # type: ignore


class MaybeAcquire:
    """Async helper for acquiring a connection to the database.
    Args:
        connection (asyncpg.Connection, optional): A database connection to use
                If none is supplied a connection will be acquired from the pool.
    Kwargs:
        pool (asyncpg.pool.Pool, optional): A connection pool to use.
            If none is supplied the default pool will be used.
    """

    def __init__(self, connection: asyncpg.Connection = None, *, pool: Pool):
        self.connection = connection
        self.pool = pool
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
