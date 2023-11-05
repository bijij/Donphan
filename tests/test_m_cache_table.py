import random
from unittest import TestCase

from donphan import CachedTable, Column, SQLType
from tests.utils import async_test, with_connection

NUM_ITEMS = random.randint(3, 10)

B_VALUES = [random.random() for _ in range(random.randint(1, 5))]


class _TestCachedTable(CachedTable):
    a: Column[SQLType.Integer] = Column(primary_key=True)
    b: Column[list[SQLType.Double]] = Column(nullable=True)


class CachedTableTest(TestCase):
    @async_test
    @with_connection
    async def test_a_table_create(self, conn):
        await _TestCachedTable.create(conn)

    @async_test
    @with_connection
    async def test_b_table_insert(self, conn):
        for x in range(NUM_ITEMS):
            await _TestCachedTable.insert(conn, a=x)

    def test_c_cached_insert(self):
        record = _TestCachedTable.get_cached(0)
        assert record is not None
        assert record["a"] == 0 and record["b"] is None

    @async_test
    @with_connection
    async def test_d_table_update(self, conn):
        record = _TestCachedTable.get_cached(0)
        assert record is not None
        await _TestCachedTable.update_record(conn, record, b=B_VALUES)

    def test_e_cached_update(self):
        record = _TestCachedTable.get_cached(0)
        assert record is not None
        assert record["a"] == 0 and record["b"] == B_VALUES

    @async_test
    @with_connection
    async def test_f_table_insert_many_disabled(self, conn):
        with self.assertRaises(NotImplementedError):
            await _TestCachedTable.insert_many(conn, [_TestCachedTable.a], (-1,), (-2,))

    @async_test
    @with_connection
    async def test_g_fetch_row_cache_hit(self, conn):
        cached_record = _TestCachedTable.get_cached(0)
        record = await _TestCachedTable.fetch_row(conn, a=0)
        assert record is cached_record

    @async_test
    @with_connection
    async def test_h_fetch_row_cache_miss(self, conn):
        _TestCachedTable._delete_cached(0)
        assert _TestCachedTable.get_cached(0) is None
        record = await _TestCachedTable.fetch_row(conn, a=0)
        cached_record = _TestCachedTable.get_cached(0)
        assert record is cached_record

    @async_test
    @with_connection
    async def test_i_fetch_value_cache_hit(self, conn):
        cached_record = _TestCachedTable.get_cached(0)
        assert cached_record is not None
        record = await _TestCachedTable.fetch_value(conn, 'b', a=0)
        assert record is cached_record['b']

    @async_test
    @with_connection
    async def test_j_fetch_value_cache_miss(self, conn):
        _TestCachedTable._delete_cached(0)
        assert _TestCachedTable.get_cached(0) is None
        record = await _TestCachedTable.fetch_value(conn, 'b', a=0)
        assert record == B_VALUES
        cached_record = _TestCachedTable.get_cached(0)
        assert cached_record is not None
        assert cached_record['b'] is record

    @async_test
    @with_connection
    async def test_k_table_drop(self, conn):
        await _TestCachedTable.drop(conn)
