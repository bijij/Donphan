from unittest import TestCase

from donphan._consts import NOT_CREATABLE
from donphan.utils import normalise_name, query_builder, not_creatable, MISSING


class ConnectionTest(TestCase):
    def test_a_normalise_name(self):
        self.assertEqual(normalise_name("test"), "test")
        self.assertEqual(normalise_name("Test"), "test")
        self.assertEqual(normalise_name("TestTest"), "test_test")
        self.assertEqual(normalise_name("Test_Test"), "test__test")

    def test_b_query_builder(self):
        self.assertEqual(query_builder(lambda: ["1", "2", "3", "4"])(), "1 2 3 4")
        self.assertEqual(query_builder(lambda: [1, 2, 3, 4])(), "1 2 3 4")

    def test_c_not_creatable(self):
        not_creatable(MISSING)
        self.assertTrue(MISSING in NOT_CREATABLE)
