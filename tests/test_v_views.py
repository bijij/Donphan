from donphan import Column, Table, SQLType, View, ViewColumn
from unittest import TestCase

import asyncpg

pool: asyncpg.pool.Pool


class TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)


class TestView(View):
    a: ViewColumn = ViewColumn(select="COUNT(a)")
    _query = f"FROM {TestTable._name} GROUP BY a"


class ViewTest(TestCase):
    def test_query_create(self):
        self.assertEqual(
            TestView._query_create(True),
            "CREATE OR REPLACE VIEW public.test_view ( a ) AS SELECT COUNT(a) FROM public.test_table GROUP BY a",
        )
