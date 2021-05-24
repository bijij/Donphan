from donphan.utils import not_creatable
from unittest import TestCase

from donphan import SQLType, CustomType, Enum, EnumType


class ConnectionTest(TestCase):
    def test_a_sql_type(self):
        a = SQLType._from_type(str)
        self.assertEqual(a.sql_type, "TEXT")
        self.assertEqual(a.py_type, str)

        b = SQLType._from_type(str)
        self.assertIs(a, b)

        c = SQLType.Integer
        self.assertEqual(c.py_type, int)

    def test_b_custom_type(self):
        @not_creatable
        class A(CustomType, _name="foo", schema="foo"):
            ...

        self.assertEqual(A._name, "foo.foo")
        self.assertEqual(A._schema, "foo")

    def test_c_enum_type(self):
        class E(Enum):
            a = 2

        class _E(EnumType[E], _name=E.__name__):
            ...

        self.assertIs(_E.py_type.a, E.a)
        self.assertEqual(_E.sql_type, _E._name)
