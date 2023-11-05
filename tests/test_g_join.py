import random
from unittest import TestCase

from donphan import Column, SQLType, Table
from tests.utils import async_test, with_connection

NUM_ITEMS = random.randint(3, 10)


class _TestJoinTable(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True)
    b: Column[SQLType.Integer]


class _TestJoinTable2(Table):
    a: Column[SQLType.Integer] = Column(primary_key=True, references=_TestJoinTable.a)
    c: Column[SQLType.Integer]


class _TestJoinTable3(Table):
    i: Column[SQLType.Serial] = Column(primary_key=True)
    b: Column[SQLType.Integer]
    d: Column[SQLType.Integer]


class JoinTest(TestCase):
    @async_test
    @with_connection
    async def test_a_table_create(self, conn):
        await _TestJoinTable.create(conn)
        await _TestJoinTable2.create(conn)
        await _TestJoinTable3.create(conn)

    @async_test
    @with_connection
    async def test_b_table_insert(self, conn):
        for x in range(NUM_ITEMS):
            b = random.randint(1, 100)
            await _TestJoinTable.insert(conn, a=x, b=b)
            await _TestJoinTable2.insert(conn, a=x, c=random.randint(1, 100))
            await _TestJoinTable3.insert(conn, b=b, d=random.randint(1, 100))

    def test_join(self):
        join = _TestJoinTable.inner_join(_TestJoinTable2, (_TestJoinTable.a, _TestJoinTable2.a))

        a_alias = join._aliases[_TestJoinTable]
        b_alias = join._aliases[_TestJoinTable2]

        assert (
            join._name
            == f"( SELECT {a_alias}.a AS a , {a_alias}.b AS b , {b_alias}.c AS c FROM public.__test_join_table AS {a_alias} INNER JOIN public.__test_join_table2 AS {b_alias} ON {a_alias}.a = {b_alias}.a ) AS {join._alias}"
        )

    @async_test
    @with_connection
    async def test_c_inner_join(self, conn):
        join = _TestJoinTable.inner_join(_TestJoinTable2, (_TestJoinTable.a, _TestJoinTable2.a))
        records = await join.fetch(conn, a=0)
        assert len(list(records)) == 1

    @async_test
    @with_connection
    async def test_d_left_join(self, conn):
        join = _TestJoinTable.a.left_join(_TestJoinTable2.a)
        records = await join.fetch(conn, a=0)
        assert len(list(records)) == 1

    @async_test
    @with_connection
    async def test_e_right_join(self, conn):
        join = _TestJoinTable.right_join(_TestJoinTable2, (_TestJoinTable.a, _TestJoinTable2.a))
        records = await join.fetch(conn, a=0)
        assert len(list(records)) == 1

    @async_test
    @with_connection
    async def test_f_full_outer_join(self, conn):
        join = _TestJoinTable.a.full_outer_join(_TestJoinTable2.a)
        records = await join.fetch(conn, a=0)
        assert len(list(records)) == 1

    @async_test
    @with_connection
    async def test_g_join_shortcut(self, conn):
        join = _TestJoinTable.a.inner_join(_TestJoinTable2.a)
        records = await join.fetch(conn, a=0)
        assert len(list(records)) == 1

    @async_test
    @with_connection
    async def test_g_chained_join(self, conn):
        join = _TestJoinTable.a.inner_join(_TestJoinTable2.a).inner_join(
            _TestJoinTable3, (_TestJoinTable.b, _TestJoinTable3.b)
        )
        records = await join.fetch(conn, a=0)
        assert len(list(records)) >= 1

    @async_test
    @with_connection
    async def test_i_table_delete(self, conn):
        await _TestJoinTable2.drop(conn)
        await _TestJoinTable.drop(conn)
        await _TestJoinTable3.drop(conn)
