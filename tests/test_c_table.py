from unittest import TestCase

from donphan import Column, SQLType, Table

from .utils import async_test


class Table(Table, schema='test'):
    a: int = Column(primary_key=True)
    b: SQLType.Integer


class TestTableCreate(TestCase):

    def test_query_create(self):
        self.assertEqual(Table._query_create(), 'CREATE TABLE IF NOT EXISTS test.table ( a INTEGER, b INTEGER, PRIMARY KEY (a) )')

    def test_query_drop(self):
        self.assertEqual(Table._query_drop(), 'DROP TABLE IF EXISTS test.table')

    @async_test
    async def test_create(self):
        await Table.create()

    @async_test
    async def test_drop(self):
        await Table.drop()
