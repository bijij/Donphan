import datetime
import decimal
import ipaddress
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

    # 8.1 Numeric

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
    @default_for(decimal.Decimal)
    def Numeric(cls):
        return cls(decimal.Decimal, 'NUMERIC')

    # 8.2 Monetary

    @classmethod
    def Money(cls):
        return cls(str, 'MONEY')

    # 8.3 Character

    @classmethod
    def CharacterVarying(cls, n: int = 2000):
        return cls(str, f'CHARACTER VARYING({n})')

    @classmethod
    def Character(cls):
        return cls(str, 'CHARACTER')

    @classmethod
    @default_for(str)
    def Text(cls):
        return cls(str, 'TEXT')

    # 8.4 Binary

    @classmethod
    @default_for(bytes)
    def Bytea(cls):
        return cls(bytes, 'BYTEA')

    # 8.5 Date/Time

    @classmethod
    @default_for(datetime.datetime)
    def Timestamp(cls):
        return cls(datetime.datetime, 'TIMESTAMP')

    @classmethod
    @default_for(datetime.date)
    def Date(cls):
        return cls(datetime.date, 'DATE')

    @classmethod
    @default_for(datetime.timedelta)
    def Interval(cls):
        return cls(datetime.datetime, 'INTERVAL')

    # 8.6 Boolean

    @classmethod
    @default_for(bool)
    def Boolean(cls):
        return cls(bool, 'BOOLEAN')

    # 8.9 Network Adress

    @classmethod
    @default_for(ipaddress.IPv4Network)
    @default_for(ipaddress.IPv6Network)
    def CIDR(cls):
        return cls(ipaddress._BaseNetwork, 'CIDR')

    @classmethod
    @default_for(ipaddress.IPv4Address)
    @default_for(ipaddress.IPv6Address)
    def Inet(cls):
        return cls(ipaddress._BaseNetwork, 'INET')

    @classmethod
    def MACAddr(cls):
        return cls(str, 'MACADDR')

    # 8.12 UUID

    @classmethod
    @default_for(uuid.UUID)
    def UUID(cls):
        return cls(uuid.UUID, 'UUID')

    # 8.14 JSON

    @classmethod
    def JSON(cls):
        return cls(dict, 'JSON')

    @classmethod
    @default_for(dict)
    def JSONB(cls):
        return cls(dict, 'JSONB')

    # Aliases
    Char = Character
    VarChar = CharacterVarying

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
