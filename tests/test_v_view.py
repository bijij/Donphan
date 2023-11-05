import random
from unittest import TestCase

from donphan import Column, SQLType, Table, View, ViewColumn
from tests.utils import async_test, with_connection

NUM_ITEMS = random.randint(3, 10)


class _TestViewTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)


class _TestView(View):
    a: ViewColumn = ViewColumn(select="COUNT(*)")
    _query = f"FROM {_TestViewTable._name}"


class ViewTest(TestCase):
    def test_query_create(self):
        assert (
            _TestView._query_create(True)
            == "CREATE OR REPLACE VIEW public.__test_view ( a ) AS SELECT COUNT(*) AS a FROM public.__test_view_table"
        )

    @async_test
    @with_connection
    async def test_a_view_create(self, conn):
        await _TestViewTable.create(conn)
        await _TestView.create(conn)

        for x in range(NUM_ITEMS):
            await _TestViewTable.insert(conn, a=x)

    @async_test
    @with_connection
    async def test_b_view_fetch(self, conn):
        record = await _TestView.fetch_row(conn)
        assert record is not None
        assert record["a"] == NUM_ITEMS

    @async_test
    @with_connection
    async def test_c_view_delete(self, conn):
        await _TestView.drop(conn)
        await _TestViewTable.drop(conn)
