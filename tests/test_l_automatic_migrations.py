import random
from donphan.utils import not_creatable

from tests.utils import async_test
from donphan import Column, Table, SQLType
from unittest import TestCase


class _TestAutomaticMigrationsTable(Table):  # type: ignore
    a: Column[SQLType.Text] = Column(primary_key=True)
    c: Column[SQLType.Text]


class AutomaticMigrationsTest(TestCase):
    @async_test
    async def test_a_table_create(self):
        await _TestAutomaticMigrationsTable.create(None)

    @async_test
    async def test_b_table_insert(self):
        await _TestAutomaticMigrationsTable.insert(None, a="a", c="c")

    @async_test
    async def test_c_test_automatic_migration(self):
        global _TestAutomaticMigrationsTable

        class _TestAutomaticMigrationsTable(Table):
            a: Column[SQLType.Text] = Column(primary_key=True)
            b: Column[SQLType.Text] = Column(default="'b'")

        await _TestAutomaticMigrationsTable.create(None, automatic_migrations=True)

    @async_test
    async def test_d_table_fetch(self):
        record = await _TestAutomaticMigrationsTable.fetch_row(None, a="a")
        assert record is not None
        assert record["b"] == "b"
        assert "c" not in record

    @async_test
    async def test_e_table_delete(self):
        await _TestAutomaticMigrationsTable.drop(None)
