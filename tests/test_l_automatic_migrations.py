from unittest import TestCase

from donphan import Column, SQLType, Table
from tests.utils import async_test, with_connection


class _TestAutomaticMigrationsTable(Table):  # type: ignore
    a: Column[SQLType.Text] = Column(primary_key=True)
    c: Column[SQLType.Text]


class AutomaticMigrationsTest(TestCase):
    @async_test
    @with_connection
    async def test_a_table_create(self, conn):
        await _TestAutomaticMigrationsTable.create(conn)

    @async_test
    @with_connection
    async def test_b_table_insert(self, conn):
        await _TestAutomaticMigrationsTable.insert(conn, a="a", c="c")

    @async_test
    @with_connection
    async def test_c_test_automatic_migration(self, conn):
        global _TestAutomaticMigrationsTable

        class _TestAutomaticMigrationsTable(Table):
            a: Column[SQLType.Text] = Column(primary_key=True)
            b: Column[SQLType.Text] = Column(default="'b'")

        await _TestAutomaticMigrationsTable.create(conn, automatic_migrations=True)

    @async_test
    @with_connection
    async def test_d_table_fetch(self, conn):
        record = await _TestAutomaticMigrationsTable.fetch_row(conn, a="a")
        assert record is not None
        assert record["b"] == "b"
        assert "c" not in record

    @async_test
    @with_connection
    async def test_e_table_delete(self, conn):
        await _TestAutomaticMigrationsTable.drop(conn)
