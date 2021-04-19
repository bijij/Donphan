from unittest import TestCase

from donphan import Column, SQLType, Table

from .utils import async_test


class Table(Table, schema="test"):
    a: SQLType.Integer = Column(primary_key=True)
    b: SQLType.Integer


class TestTableCreate(TestCase):

    # test queries

    def test_query_create(self):
        self.assertEqual(
            Table._query_create(), "CREATE TABLE IF NOT EXISTS test.table ( a INTEGER, b INTEGER, PRIMARY KEY (a) )"
        )

    def test_query_insert(self):
        # Test Base Case
        self.assertEqual(
            Table._query_insert(False, None, None, a=3, b=4)[0], "INSERT INTO test.table (a, b) VALUES ($1, $2)"
        )

        # Test ON CONFLICT DO NOTHING
        self.assertEqual(
            Table._query_insert(True, None, None, a=3, b=4)[0],
            "INSERT INTO test.table (a, b) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )

        # Test ON CONFLICT DO UPDATE
        self.assertEqual(
            Table._query_insert(False, Table.a, None, a=3, b=4)[0],
            "INSERT INTO test.table (a, b) VALUES ($1, $2) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a",
        )

        # Validate passing multiple on_conflict handlers raises
        with self.assertRaises(ValueError):
            Table._query_insert(True, Table.a, None, a=3, b=4)

        # Validate returning *
        self.assertEqual(
            Table._query_insert(False, None, "*", a=3, b=4)[0],
            "INSERT INTO test.table (a, b) VALUES ($1, $2) RETURNING *",
        )

        # Validate returning single column
        self.assertEqual(
            Table._query_insert(False, None, Table.a, a=3, b=4)[0],
            "INSERT INTO test.table (a, b) VALUES ($1, $2) RETURNING a",
        )

        # Validate returning multiple columns
        self.assertEqual(
            Table._query_insert(False, None, Table._columns, a=3, b=4)[0],
            "INSERT INTO test.table (a, b) VALUES ($1, $2) RETURNING a, b",
        )

        # Test returning on conflict
        self.assertEqual(
            Table._query_insert(False, Table.a, Table.a, a=3, b=4)[0],
            "INSERT INTO test.table (a, b) VALUES ($1, $2) ON CONFLICT (a) \
DO UPDATE SET a = EXCLUDED.a RETURNING a",
        )

    def test_query_drop(self):
        self.assertEqual(Table._query_drop(), "DROP TABLE IF EXISTS test.table")

    # test integration

    @async_test
    async def test_a_create(self):
        await Table.create()

    @async_test
    async def test_b_insert(self):
        await Table.insert(a=1, b=2)

    @async_test
    async def test_c_fetch(self):
        record = await Table.fetch(a=1)
        self.assertEqual(record[0]["b"], 2)

    @async_test
    async def test_d_fetchrow(self):
        record = await Table.fetchrow(a=1)
        self.assertEqual(record["b"], 2)

    @async_test
    async def test_e_update_record(self):
        record = await Table.fetchrow(a=1)
        self.assertEqual(record["b"], 2)

        await Table.update_record(record, b=3)
        record = await Table.fetchrow(a=1)
        self.assertEqual(record["b"], 3)

    @async_test
    async def test_f_delete_record(self):
        record = await Table.fetchrow(a=1)
        self.assertEqual(record["b"], 3)

        await Table.delete_record(record)
        record = await Table.fetchrow(a=1)
        self.assertEqual(record, None)

    @async_test
    async def test_g_drop(self):
        await Table.drop()
