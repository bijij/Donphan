import json

import asyncpg

_pool = None


async def create_pool(dsn: str, **kwargs):
    """Creates the database connection pool."""
    global _pool

    def _encode_json(value):
        return json.dumps(value)

    def _decode_json(value):
        return json.loads(value)

    async def init(connection: asyncpg.Connection):
        await connection.set_type_codec('json', schema='pg_catalog', encoder=_encode_json, decoder=_decode_json, format='text')
        await connection.set_type_codec('jsonb', schema='pg_catalog', encoder=_encode_json, decoder=_decode_json, format='text')

    _pool = await asyncpg.create_pool(dsn, init=init, **kwargs)


class MaybeAcquire:
    """Async helper for acquiring a connection to the database."""

    def __init__(self, connection: asyncpg.Connection = None, *, pool=None):
        """
        Args:
            connection (asyncpg.Connection, optional): A database connection to use
                    If none is supplied a connection will be acquired from the pool.

        Kwargs:
            pool (asyncpg.pool.Pool, optional): A connection pool to use.
                If none is supplied the default pool will be used.
        """
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
