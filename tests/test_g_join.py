import random

from tests.utils import async_test, with_connection
from donphan import Column, Table, SQLType
from unittest import TestCase


NUM_ITEMS = random.randint(3, 10)


class _TestTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)
    b: Column[SQLType.Integer]


class _TestTable2(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True, references=_TestTable.a)
    c: Column[SQLType.Integer]


class _TestTable3(Table):
    i: Column[SQLType.Serial] = Column(primary_key=True)
    b: Column[SQLType.Integer]
    d: Column[SQLType.Integer]


class ViewTest(TestCase):
    @async_test
    @with_connection
    async def test_a_table_create(self, conn):
        await _TestTable.create(conn)
        await _TestTable2.create(conn)
        await _TestTable3.create(conn)

    @async_test
    @with_connection
    async def test_b_table_insert(self, conn):
        for x in range(NUM_ITEMS):
            b = random.randint(1, 100)
            await _TestTable.insert(conn, a=x, b=b)
            await _TestTable2.insert(conn, a=x, c=random.randint(1, 100))
            await _TestTable3.insert(conn, b=b, d=random.randint(1, 100))

    def test_join(self):
        join = _TestTable.inner_join(_TestTable2, (_TestTable.a, _TestTable2.a))

        a_alias = join._aliases[_TestTable]
        b_alias = join._aliases[_TestTable2]

        self.assertEqual(
            join._name,
            f'( SELECT {a_alias}.a AS a , {a_alias}.b AS b , {b_alias}.c AS c FROM public.__test_table AS {a_alias} INNER JOIN public.__test_table2 AS {b_alias} ON {a_alias}.a = {b_alias}.a ) AS {join._alias}',
        )

    @async_test
    @with_connection
    async def test_c_inner_join(self, conn):
        join = _TestTable.inner_join(_TestTable2, (_TestTable.a, _TestTable2.a))
        records = list(await join.fetch(conn, a=0))
        self.assertEqual(len(records), 1)

    @async_test
    @with_connection
    async def test_d_left_join(self, conn):
        join = _TestTable.a.left_join(_TestTable2.a)
        records = list(await join.fetch(conn, a=0))
        self.assertEqual(len(records), 1)

    @async_test
    @with_connection
    async def test_e_right_join(self, conn):
        join = _TestTable.right_join(_TestTable2, (_TestTable.a, _TestTable2.a))
        records = list(await join.fetch(conn, a=0))
        self.assertEqual(len(records), 1)

    @async_test
    @with_connection
    async def test_f_full_outer_join(self, conn):
        join = _TestTable.a.full_outer_join(_TestTable2.a)
        records = list(await join.fetch(conn, a=0))
        self.assertEqual(len(records), 1)

    @async_test
    @with_connection
    async def test_g_join_shortcut(self, conn):
        join = _TestTable.a.inner_join(_TestTable2.a)
        records = list(await join.fetch(conn, a=0))
        self.assertEqual(len(records), 1)

    @async_test
    @with_connection
    async def test_g_chained_join(self, conn):
        join = _TestTable.a.inner_join(_TestTable2.a).inner_join(_TestTable3, (_TestTable.b, _TestTable3.b))
        records = list(await join.fetch(conn, a=0))
        self.assertGreaterEqual(len(records), 1)

    @async_test
    @with_connection
    async def test_i_table_delete(self, conn):
        await _TestTable2.drop(conn)
        await _TestTable.drop(conn)
        await _TestTable3.drop(conn)
