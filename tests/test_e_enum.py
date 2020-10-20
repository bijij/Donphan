from unittest import TestCase

from donphan import Column, enum, Enum, Table

from .utils import async_test

_Test_Enum = enum("_Test_Enum", "a b")


class _Test_Table(Table):
    col_a: _Test_Enum = Column(primary_key=True)
    col_b: int


class TestEnum(TestCase):

    # test queries

    def test_enum_type(self):
        assert issubclass(_Test_Enum, Enum)

    @async_test
    async def test_a_create(self):
        await _Test_Enum.create()

    @async_test
    async def test_b_create_table_with_enum(self):
        await _Test_Table.create()

    @async_test
    async def test_c_insert_with_enum(self):
        await _Test_Table.insert(col_a=_Test_Enum.b, col_b=3)
        record = await _Test_Table.fetchrow(col_b=3)
        assert record['col_a'] == _Test_Enum.b

    @async_test
    async def test_d_drop(self):
        await _Test_Table.drop(cascade=True)
        await _Test_Enum.drop(cascade=True)
