import random

from tests.utils import async_test, with_connection
from donphan import Column, Table, SQLType
from unittest import TestCase


NUM_ITEMS = random.randint(3, 10)


class _TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)


class ViewTest(TestCase):
    def test_query_create(self):
        self.assertEqual(
            _TestTable._query_create(True),
            "CREATE TABLE IF NOT EXISTS public.__test_table ( a INTEGER , PRIMARY KEY ( a ) )",
        )

    @async_test
    async def test_a_table_create(self):
        await _TestTable.create(None)

    @async_test
    async def test_b_table_insert(self):
        for x in range(NUM_ITEMS):
            await _TestTable.insert(None, a=x)

    @async_test
    async def test_c_table_fetch(self):
        records = list(await _TestTable.fetch(None))
        self.assertEqual(len(records), NUM_ITEMS)

    @async_test
    async def test_d_table_delete(self):
        await _TestTable.drop(None)
