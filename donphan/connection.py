import json

import asyncpg
from asyncpg import Connection, Record  # noqa: F401
from asyncpg.pool import Pool


_pool: Pool = None  # type: ignore


async def create_pool(dsn: str, **kwargs) -> Pool:
    """Creates the database connection pool."""
    global _pool

    def _encode_json(value):
        return json.dumps(value)

    def _decode_json(value):
        return json.loads(value)

    async def init(connection: asyncpg.Connection):
        await connection.set_type_codec('json', schema='pg_catalog', encoder=_encode_json, decoder=_decode_json, format='text')
        await connection.set_type_codec('jsonb', schema='pg_catalog', encoder=_encode_json, decoder=_decode_json, format='text')

    _pool = p = await asyncpg.create_pool(dsn, init=init, **kwargs)
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

    async def __aenter__(self) -> Connection:
        if self.connection is None:
            self._cleanup = True
            self._connection = c = await self.pool.acquire()
            return c
        return self.connection

    async def __aexit__(self, *args):
        if self._cleanup:
            await self.pool.release(self._connection)
