from donphan import MaybeAcquire
from unittest import TestCase

import asyncpg
from donphan import create_pool

from .env import POSTGRES_DSN
from .utils import async_test, set_pool, with_pool


class ConnectionTest(TestCase):
    @async_test
    async def test_a_create_pool(self):
        pool = await create_pool(POSTGRES_DSN, set_as_default=True)
        set_pool(pool)

        assert isinstance(pool, asyncpg.Pool)

    @async_test
    @with_pool
    async def test_b_maybeacquire(self, pool):
        async with MaybeAcquire(None, pool=pool) as conn:
            assert isinstance(conn, asyncpg.Connection)
