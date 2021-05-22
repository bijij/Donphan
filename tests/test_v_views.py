from tests.utils import async_test, with_connection
from donphan import Column, Table, SQLType, View, ViewColumn
from unittest import TestCase


class TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)


class TestView(View):
    a: ViewColumn = ViewColumn(select="COUNT(*)")
    _query = f"FROM {TestTable._name}"


class ViewTest(TestCase):
    def test_query_create(self):
        self.assertEqual(
            TestView._query_create(True),
            "CREATE OR REPLACE VIEW public.test_view ( a ) AS SELECT COUNT(*) AS a FROM public.test_table",
        )

    @async_test
    @with_connection
    async def test_a_view_create(self, conn):
        await TestTable.create(conn)
        await TestView.create(conn)

        for x in range(10):
            await TestTable.insert(conn, a=x)

    @async_test
    @with_connection
    async def test_b_view_fetch(self, conn):
        await TestTable.create(conn)
        await TestView.create(conn)

        record = await TestView.fetch_row(conn)

        self.assertEqual(record["a"], 10)

    @async_test
    @with_connection
    async def test_c_view_delete(self, conn):
        await TestView.drop(conn)
        await TestTable.drop(conn)
