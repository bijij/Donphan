import random

from tests.utils import async_test, with_connection
from donphan import Column, Table, SQLType
from unittest import TestCase


NUM_ITEMS = random.randint(3, 10)


class _TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)


class _TestTable2(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True, references=_TestTable.a)


class ForeignKeyTest(TestCase):
    def test_query_create(self):
        assert (
            _TestTable2._query_create(True)
            == "CREATE TABLE IF NOT EXISTS public.__test_table2 ( a INTEGER NOT NULL REFERENCES public.__test_table ( a ) , PRIMARY KEY ( a ) )"
        )

    @async_test
    @with_connection
    async def test_a_table_create(self, conn):
        await _TestTable.create(conn)
        await _TestTable2.create(conn)

    @async_test
    @with_connection
    async def test_b_table_insert(self, conn):
        for x in range(NUM_ITEMS):
            await _TestTable.insert(conn, a=x)
            await _TestTable2.insert(conn, a=x)

    @async_test
    @with_connection
    async def test_c_table_fetch(self, conn):
        records = list(await _TestTable.fetch(conn))

    @async_test
    @with_connection
    async def test_d_table_delete(self, conn):
        await _TestTable2.drop(conn)
        await _TestTable.drop(conn)
