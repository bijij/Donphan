from tests.utils import async_test
from donphan import Column, Table, SQLType
from unittest import TestCase


class _TestLikeTable(Table):
    a: Column[SQLType.Text] = Column(primary_key=True)


class ViewTest(TestCase):
    def test_query_fetch_null(self):
        where = _TestLikeTable._build_where_clause({"a__like": r"%foo"})

        assert (
            _TestLikeTable._build_query_fetch(where, None, None)
            == r"SELECT * FROM public.__test_like_table WHERE a LIKE $1"
        )

    @async_test
    async def test_a_table_create(self):
        await _TestLikeTable.create(None)

    @async_test
    async def test_b_table_insert(self):
        await _TestLikeTable.insert(a="foo")
        await _TestLikeTable.insert(a="bar")
        await _TestLikeTable.insert(a="foobar")

    @async_test
    async def test_c_table_fetch(self):
        records = await _TestLikeTable.fetch(a__like=r"foo%")
        assert len(records) == 2  # type: ignore

    @async_test
    async def test_d_table_fetch_insensitive(self):
        records = await _TestLikeTable.fetch(a__ilike=r"FoO%")
        assert len(records) == 2  # type: ignore

    @async_test
    async def test_e_table_delete(self):
        await _TestLikeTable.drop()
