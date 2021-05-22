from donphan.connection import MaybeAcquire
from unittest import TestCase

import asyncpg
from donphan import create_pool

from .env import POSTGRES_DSN
from .utils import async_test, set_pool, with_pool


class ConnectionTest(TestCase):
    @async_test
    async def test_a_create_pool(self):
        pool = await create_pool(POSTGRES_DSN)
        set_pool(pool)

        self.assertIsInstance(pool, asyncpg.pool.Pool)

    @async_test
    @with_pool
    async def test_b_maybeacquire(self, pool):
        async with MaybeAcquire(None, pool=pool) as conn:
            self.assertIsInstance(conn, asyncpg.Connection)
