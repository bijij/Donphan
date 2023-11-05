from unittest import TestCase

from donphan._consts import NOT_CREATABLE
from donphan.utils import MISSING, normalise_name, not_creatable, query_builder


class UtilsTest(TestCase):
    def test_a_normalise_name(self):
        assert normalise_name("test") == "test"
        assert normalise_name("Test") == "test"
        assert normalise_name("TestTest") == "test_test"
        assert normalise_name("Test_Test") == "test__test"

    def test_b_query_builder(self):
        assert query_builder(lambda: ["1", "2", "3", "4"])() == "1 2 3 4"
        assert query_builder(lambda: [1, 2, 3, 4])() == "1 2 3 4"

    def test_c_not_creatable(self):
        not_creatable(MISSING)
        assert MISSING in NOT_CREATABLE
