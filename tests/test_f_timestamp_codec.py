import datetime

from unittest import TestCase

from donphan import Table, SQLType
from donphan.connection import _encode_datetime, _decode_timestamp

from .utils import async_test


class _Test_Table(Table):
    col_a: SQLType.Timestamp(with_timezone=False)


time = datetime.datetime.now(tz=datetime.timezone.utc)
naivetime = datetime.datetime.utcnow() + datetime.timedelta(hours=2)
offsettime = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=1)))


class TestEnum(TestCase):

    # test queries

    def test_a_encode_decode(self):
        self.assertEqual(_decode_timestamp(_encode_datetime(time)), time)

    @async_test
    async def test_b_create_table_and_insert(self):
        print(_Test_Table._query_create())
        await _Test_Table.create()
        await _Test_Table.insert(col_a=time)
        await _Test_Table.insert(col_a=naivetime)
        await _Test_Table.insert(col_a=offsettime)

    @async_test
    async def test_c_fetch_timestamp(self):
        record = await _Test_Table.fetchrow(col_a=time)
        self.assertIsNotNone(record)
        self.assertEqual(record["col_a"], time)

    @async_test
    async def test_d_fetch_naive_timestamp(self):
        record = await _Test_Table.fetchrow(col_a=naivetime)
        self.assertIsNotNone(record)
        self.assertEqual(record["col_a"], naivetime.astimezone(datetime.timezone.utc))

    @async_test
    async def test_e_fetch_timestamp_with_offset(self):
        record = await _Test_Table.fetchrow(col_a=offsettime)
        self.assertIsNotNone(record)
        self.assertEqual(record["col_a"], offsettime)

    @async_test
    async def test_f_drop(self):
        await _Test_Table.drop(cascade=True)
