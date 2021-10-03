import random
from donphan.utils import not_creatable

from tests.utils import async_test
from donphan import Column, Table, SQLType
from unittest import TestCase


class _TestAlterColumnsTable(Table):
    a: Column[SQLType.Text] = Column(primary_key=True)


class AlterColumnsTest(TestCase):
    def test_query_drop_column(self):
        assert (
            _TestAlterColumnsTable._query_drop_column(_TestAlterColumnsTable._columns_dict["b"])
            == r"ALTER TABLE public.__test_alter_columns_table DROP COLUMN b"
        )

    def test_query_add_column(self):
        column = Column.create("b", SQLType.Text)
        assert (
            _TestAlterColumnsTable._query_add_column(column)
            == r"ALTER TABLE public.__test_alter_columns_table ADD COLUMN b TEXT"
        )

    @async_test
    async def test_a_table_create(self):
        await _TestAlterColumnsTable.create(None)

    @async_test
    async def test_c_table_add_column(self):
        column = Column.create("b", SQLType.Text)
        await _TestAlterColumnsTable.add_column(None, column)
        column = Column.create("c", SQLType.Text)
        await _TestAlterColumnsTable.add_column(None, column)

    @async_test
    async def test_d_table_drop_column(self):
        await _TestAlterColumnsTable.drop_column(None, _TestAlterColumnsTable._columns_dict["b"])

    @async_test
    async def test_e_table_migrate(self):
        @not_creatable
        class Migrator(Table, _name="__test_alter_columns_table"):
            a: Column[SQLType.Text] = Column(primary_key=True)
            b: Column[SQLType.Text]

        await _TestAlterColumnsTable.migrate_to(None, Migrator)

    @async_test
    async def test_f_table_delete(self):
        await _TestAlterColumnsTable.drop(None)
