import random

from tests.utils import async_test
from donphan import Column, Table, SQLType
from unittest import TestCase


NUM_ITEMS = random.randint(3, 10)


class _TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)
    b: Column[SQLType.Integer] = Column(nullable=True)


class ViewTest(TestCase):
    def test_query_fetch_null(self):
        where = _TestTable._build_where_clause({"b": None})

        self.assertEqual(
            _TestTable._build_query_fetch(where, None, None),
            "SELECT * FROM public.__test_table WHERE b IS NULL",
        )

        where = _TestTable._build_where_clause({"b__ne": None})
        self.assertEqual(
            _TestTable._build_query_fetch(where, None, None),
            "SELECT * FROM public.__test_table WHERE b IS NOT NULL",
        )

    @async_test
    async def test_a_table_create(self):
        await _TestTable.create(None)

    @async_test
    async def test_b_table_insert(self):
        for x in range(NUM_ITEMS):
            await _TestTable.insert(None, a=x, b=None if x % 2 == 0 else x)

    @async_test
    async def test_c_table_fetch(self):
        total = sum(x % 2 == 0 for x in range(NUM_ITEMS))
        records = await _TestTable.fetch(None, b=None)
        assert len(records) == total  # type: ignore

    @async_test
    async def test_f_table_delete(self):
        await _TestTable.drop(None)
