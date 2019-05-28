import asyncpg

import uuid

_defaults = {}


def default_for(python_type):
    """Sets a specified python type's default SQL type.

    Args:
        python_type (type): Python type to set the specified sqltype as default for.

    """
    def func(sql_type):
        _defaults[python_type] = sql_type
        return sql_type
    return func


class SQLType:
    python = NotImplemented
    sql = NotImplemented

    def __init__(self, python, sql):
        self.python = python
        self.sql = sql

    def __repr__(self):
        return f'<SQLType sql=\'{self.sql}\' python=\'{self.__name__}\'>'

    def __eq__(self, other) -> bool:
        return self.sql == other.sql

    @property
    def __name__(self) -> str:
        return self.python.__name__

    @classmethod
    @default_for(int)
    def Integer(cls):
        return cls(int, 'INTEGER')

    @classmethod
    def SmallInt(cls):
        return cls(int, 'SMALLINT')

    @classmethod
    def BigInt(cls):
        return cls(int, 'BIGINT')

    @classmethod
    @default_for(float)
    def Float(cls):
        return cls(float, 'FLOAT')

    @classmethod
    def DoublePercision(cls):
        return cls(float, 'DOUBLE PERCISION')

    @classmethod
    def Money(cls):
        return cls(str, 'MONEY')

    @classmethod
    @default_for(str)
    def Text(cls):
        return cls(str, 'TEXT')

    @classmethod
    @default_for(bool)
    def Boolean(cls):
        return cls(bool, 'BOOLEAN')

    @classmethod
    @default_for(uuid.UUID)
    def UUID(cls):
        return cls(uuid.UUID, 'UUID')

    @classmethod
    def JSON(cls):
        return cls(dict, 'JSON')

    @classmethod
    @default_for(dict)
    def JSONB(cls):
        return cls(dict, 'JSONB')

    @classmethod
    def from_python_type(cls, python_type: type):
        """Dynamically determines an SQL type given a pyton type.

        Args:
            python_type (type): The python type.
        """

        if _defaults.get(python_type):
            return _defaults[python_type](cls)

        raise TypeError(
            f'Could not find an applicable SQL type for Python type {python_type.__name__}.')
