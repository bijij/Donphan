import random

from tests.utils import async_test, with_connection
from donphan import Column, Table, Enum
from unittest import TestCase


class _TestEnum(Enum):
    a = 1
    b = 2
    c = 3


VALUE = _TestEnum.try_value(random.randint(1, 3))


class _TestTable(Table):
    a: Column[_TestEnum] = Column(primary_key=True)


class ViewTest(TestCase):
    def test_query_create(self):
        self.assertEqual(
            _TestTable.a.sql_type._query_create(True), "CREATE TYPE public.__test_enum AS ENUM ( 'a' , 'b' , 'c' )"
        )

    @async_test
    @with_connection
    async def test_a_enum_create(self, conn):
        await _TestTable.a.sql_type.create(conn)
        await _TestTable.create(conn)

    @async_test
    @with_connection
    async def test_b_enum_insert(self, conn):
        await _TestTable.insert(conn, a=VALUE)

    @async_test
    @with_connection
    async def test_c_enum_fetch(self, conn):
        record = await _TestTable.fetch_row(conn)
        self.assertEqual(record["a"], VALUE)

    @async_test
    @with_connection
    async def test_d_enum_delete(self, conn):
        await _TestTable.drop(conn)
        await _TestTable.a.sql_type.drop(conn)
