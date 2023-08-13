import random

from tests.utils import async_test
from donphan import Column, Table, SQLType
from unittest import TestCase


NUM_ITEMS = random.randint(3, 10)


class _TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)
    b: Column[list[SQLType.Double]]


class ViewTest(TestCase):
    def test_query_create(self):
        assert (
            _TestTable._query_create(True)
            == "CREATE TABLE IF NOT EXISTS public.__test_table ( a INTEGER , b DOUBLE PRECISION[] , PRIMARY KEY ( a ) )"
        )

    def test_query_fetch_in(self):
        where = _TestTable._build_where_clause({"a__in": (1, 2, 3)})

        assert (
            _TestTable._build_query_fetch(where, None, None)
            == "SELECT * FROM public.__test_table WHERE a = any($1::INTEGER[])"
        )

    @async_test
    async def test_a_table_create(self):
        await _TestTable.create(None)

    @async_test
    async def test_b_table_insert(self):
        for x in range(NUM_ITEMS):
            await _TestTable.insert(a=x)

    @async_test
    async def test_c_table_fetch(self):
        records = await _TestTable.fetch(None)
        assert len(list(records)) == NUM_ITEMS

        record = await _TestTable.fetch_row(a=1)
        assert record is not None
        assert record["a"] == 1

        value = await _TestTable.fetch_value(None, _TestTable.a, a=2)
        assert value == 2

    @async_test
    async def test_d_table_insert_returning(self):
        record = await _TestTable.insert(a=10, returning="*")
        assert record["a"] == 10

        record = await _TestTable.insert(a=11, returning=["a"])
        assert record["a"] == 11

        record = await _TestTable.insert(a=12, returning=[_TestTable.a])
        assert record["a"] == 12

    @async_test
    async def test_e_table_fetch_in(self):
        records = await _TestTable.fetch(a__in=(10, 11, 12, 13))
        assert len(list(records)) == 3

    @async_test
    async def test_f_table_insert_many(self):
        await _TestTable.insert_many(None, [_TestTable.a], (-1,), (-2,))

    @async_test
    async def test_g_table_drop(self):
        await _TestTable.drop(None)
