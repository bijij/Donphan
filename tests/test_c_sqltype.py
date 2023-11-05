from unittest import TestCase

from donphan import CustomType, Enum, EnumType, SQLType
from donphan.utils import not_creatable


class SQLTypeTest(TestCase):
    def test_a_sql_type(self):
        a = SQLType._from_type(str)  # type: ignore
        assert a.sql_type == "TEXT"
        assert a.py_type is str

        b = SQLType.Text
        assert a is b

        c = SQLType.Integer
        assert c.py_type is int  # type: ignore

    def test_b_custom_type(self):
        @not_creatable
        class A(CustomType, _name="foo", schema="foo"):
            ...

        assert A._name == "foo.foo"
        assert A._schema == "foo"

    def test_c_enum_type(self):
        class E(Enum):
            a = 2

        class _E(EnumType[E], _name=E.__name__):
            ...

        assert _E.py_type.a is E.a
        assert _E.sql_type == _E._name
