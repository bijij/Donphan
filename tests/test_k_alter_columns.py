from unittest import TestCase

from donphan import Column, SQLType, Table
from donphan.utils import not_creatable
from tests.utils import async_test, with_connection


class _TestAlterColumnsTable(Table):
    a: Column[SQLType.Text] = Column(primary_key=True)


class AlterColumnsTest(TestCase):
    def test_query_drop_column(self):
        assert (
            _TestAlterColumnsTable._query_drop_column(_TestAlterColumnsTable._columns_dict["b"])
            == r"ALTER TABLE public.__test_alter_columns_table DROP COLUMN b"
        )

    def test_query_add_column(self):
        column = Column.create("b", SQLType.Text)  # type: ignore
        assert (
            _TestAlterColumnsTable._query_add_column(column)
            == r"ALTER TABLE public.__test_alter_columns_table ADD COLUMN b TEXT NOT NULL"
        )

    @async_test
    @with_connection
    async def test_a_table_create(self, conn):
        await _TestAlterColumnsTable.create(conn)

    @async_test
    @with_connection
    async def test_c_table_add_column(self, conn):
        column = Column.create("b", SQLType.Text)  # type: ignore
        await _TestAlterColumnsTable.add_column(conn, column)
        column = Column.create("c", SQLType.Text)  # type: ignore
        await _TestAlterColumnsTable.add_column(conn, column)

    @async_test
    @with_connection
    async def test_d_table_drop_column(self, conn):
        await _TestAlterColumnsTable.drop_column(conn, _TestAlterColumnsTable._columns_dict["b"])

    @async_test
    @with_connection
    async def test_e_table_migrate(self, conn):
        @not_creatable
        class Migrator(Table, _name="__test_alter_columns_table"):
            a: Column[SQLType.Text] = Column(primary_key=True)
            b: Column[SQLType.Text]

        await _TestAlterColumnsTable.migrate_to(conn, Migrator)

    @async_test
    @with_connection
    async def test_f_table_delete(self, conn):
        await _TestAlterColumnsTable.drop(conn)
