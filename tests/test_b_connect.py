from unittest import TestCase

from donphan import create_pool

from .utils import async_test
from .env import POSTGRES_DSN


class TestConnect(TestCase):

    @async_test
    async def test_connecting(self):
        await create_pool(dsn=POSTGRES_DSN)
