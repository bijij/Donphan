import random

from tests.utils import async_test, with_connection
from donphan import Column, Table, SQLType, View, ViewColumn
from unittest import TestCase


NUM_ITEMS = random.randint(3, 10)


class _TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)


class _TestView(View):
    a: ViewColumn = ViewColumn(select="COUNT(*)")
    _query = f"FROM {_TestTable._name}"


class ViewTest(TestCase):
    def test_query_create(self):
        self.assertEqual(
            _TestView._query_create(True),
            'CREATE OR REPLACE VIEW "public.__test_view" ( a ) AS SELECT COUNT(*) AS a FROM "public.__test_table"',
        )

    @async_test
    @with_connection
    async def test_a_view_create(self, conn):
        await _TestTable.create(conn)
        await _TestView.create(conn)

        for x in range(NUM_ITEMS):
            await _TestTable.insert(conn, a=x)

    @async_test
    @with_connection
    async def test_b_view_fetch(self, conn):
        record = await _TestView.fetch_row(conn)
        self.assertEqual(record["a"], NUM_ITEMS)

    @async_test
    @with_connection
    async def test_c_view_delete(self, conn):
        await _TestView.drop(conn)
        await _TestTable.drop(conn)
