import inspect
from typing import Any, List, Iterable, Optional, Tuple

import asyncpg

from .connection import MaybeAcquire
from .column import Column
from .sqltype import SQLType


class _ObjectMeta(type):

    def __new__(cls, name, parents, dct, **kwargs):

        # Set the DB Schema
        dct.update({
            'schema': kwargs.get('schema', 'public'),
            '_columns': {}
        })

        table = super().__new__(cls, name, parents, dct)

        for _name, _type in dct.get('__annotations__', {}).items():

            # If the input type is an array
            is_array = False
            while isinstance(_type, list):
                is_array = True
                _type = _type[0]

            if inspect.ismethod(_type) and _type.__self__ is SQLType:
                _type = _type()
            elif not isinstance(_type, SQLType):
                _type = SQLType.from_python_type(_type)

            column = dct.get(_name, Column())
            column._update(table, _name, _type, is_array)

            table._columns[_name] = column

        return table

    def __getattr__(cls, key):
        if key == '__name__':
            return f'{cls.__name__.lower()}'

        if key == '_name':
            return f'{cls.schema}.{cls.__name__.lower()}'

        if key in cls._columns:
            return cls._columns[key]

        raise AttributeError(f'\'{cls.__name__}\' has no attribute \'{key}\'')


_default_operators = {
    'eq': '=',
    'ne': '!=',
    'lt': '<',
    'gt': '>',
    'le': '<=',
    'ge': '>='
}


class Object(metaclass=_ObjectMeta):

    @classmethod
    def _validate_kwargs(cls, primary_keys_only=False, **kwargs) -> List[Tuple[str, Any]]:
        """Validates passed kwargs against table"""
        verified = list()
        for kwarg, value in kwargs.items():

            # Strip Extra operators
            if kwarg.startswith('or_'):
                kwarg = kwarg[3:]
            if kwarg[-4:-2] == '__':
                kwarg = kwarg[:-4]

            # Check column is in Object
            if kwarg not in cls._columns:
                raise AttributeError(
                    f'Could not find column with name {kwarg} in table {cls._name}')
            column = cls._columns[kwarg]

            # Skip non primary when relevant
            if primary_keys_only and not column.primary_key:
                continue

            # Check passing null into a non nullable column
            if not column.nullable and value is None:
                raise TypeError(
                    f'Cannot pass None into non-nullable column {column.name}')

            def check_type(element):
                return isinstance(element, (column.type.python, type(None)))

            # If column is an array
            if column.is_array:

                def check_array(element):

                    # If not at the deepest level check elements in array
                    if isinstance(element, (List, Tuple)):
                        for item in element:
                            check_array(item)

                    # Otherwise check the type of the element
                    else:
                        if not check_type(element):
                            raise TypeError(
                                f'Column {column.name}; expected {column.type.__name__ }[], received {type(element).__name__}[]')

                # Check array depth is expected.
                check_array(value)

            # Otherwise check type of element
            elif not check_type(value):
                raise TypeError(
                    f'Column {column.name}; expected {column.type.__name__}, received {type(value).__name__}')

            verified.append((column.name, value))

        return verified

    # region SQL Queries

    @classmethod
    def _query_create_schema(cls) -> str:
        """Generates the CREATE SCHEMA stub."""
        return f'CREATE SCHEMA IF NOT EXISTS {cls.schema};'

    @classmethod
    def _query_drop(cls, cascade: bool = False) -> str:
        """Generates the DROP stub."""
        raise NotImplementedError

    @classmethod
    def _query_create(cls, drop_if_exists: bool = False) -> str:
        """Generates the CREATE stub."""
        raise NotImplementedError

    @classmethod
    def _query_fetch(cls, order_by, limit, **kwargs) -> Tuple[str, Iterable]:
        """Generates the SELECT FROM stub"""

        verified = cls._validate_kwargs(**kwargs)

        # AND / OR statement check
        statements = ['AND ' for _ in verified]
        # =, <, >, != check
        operators = ['=' for _ in verified]

        # Determine operators
        for i, (_key, (key, _)) in enumerate(zip(kwargs, verified)):

            # First statement has no boolean operator
            if i == 0:
                statements[i] = ''
            elif _key[:3] == 'or_':
                statements[i] = 'OR '

            if _key[-4:-2] == '__':
                try:
                    operators[i] = _default_operators[_key[-2:]]
                except KeyError:
                    raise AttributeError('Unknown operator type {_key[-2:]}')

        builder = [f'SELECT * FROM {cls._name}']

        # Set the WHERE clause
        if verified:
            builder.append('WHERE')
            checks = []
            for i, (key, _) in enumerate(verified):
                checks.append(f'{statements[i]}{key} {operators[i]} ${i+1}')
            builder.append(' '.join(checks))

        if order_by is not None:
            builder.append(f'ORDER BY {order_by}')

        if limit is not None:
            builder.append(f'LIMIT {limit}')

        return (" ".join(builder), (value for (_, value) in verified))

    @classmethod
    def _query_fetch_where(cls, query, order_by, limit) -> str:
        """Generates the SELECT FROM stub"""

        builder = [f'SELECT * FROM {cls._name} WHERE']
        builder.append(query)

        if order_by is not None:
            builder.append(f'ORDER BY {order_by}')

        if limit is not None:
            builder.append(f'LIMIT {limit}')

        return " ".join(builder)

    # endregion

    @classmethod
    async def drop(cls, connection: asyncpg.Connection = None):
        """Drops this object from the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        """
        async with MaybeAcquire(connection) as connection:
            await connection.execute(cls._query_drop(True))

    @classmethod
    async def create(cls, connection: asyncpg.Connection = None, drop_if_exists: bool = False):
        """Creates this object in the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            drop_if_exists (bool, optional): Specified wether this object should be
                first dropped from the database if it already exists.
        """
        async with MaybeAcquire(connection) as connection:
            if drop_if_exists:
                await cls.drop(connection)
            await connection.execute(cls._query_create_schema())
            try:
                await connection.execute(cls._query_create(drop_if_exists))
            except asyncpg.DuplicateTableError:
                pass

    @classmethod
    async def prepare(cls, query: str, connection: asyncpg.Connection = None) -> asyncpg.prepared_stmt.PreparedStatement:
        """Creates a :class:`asyncpg.prepared_stmt.PreparedStatement` based on the given query.

        Args:
            query (str): The SQL query to prepare.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.

        Returns:
            asyncpg.prepared_stmt.PreparedStatement: The prepared statement object.
        """
        async with MaybeAcquire(connection) as connection:
            return await connection.prepare(query)

    @classmethod
    async def fetch(cls, connection: asyncpg.Connection = None, order_by: str = None, limit: int = None, **kwargs) -> List[asyncpg.Record]:
        """Fetches a list of records from the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            order_by (str, optional): Sets the `ORDER BY` constraint.
            limit (int, optional): Sets the maximum number of records to fetch.
            **kwargs (any): Database :class:`Column` values to search for

        Returns:
            list(asyncpg.Record): A list of database records.
        """
        query, values = cls._query_fetch(order_by, limit, **kwargs)
        # print(query)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetch(query, *values)

    @classmethod
    async def fetchall(cls, connection: asyncpg.Connection = None, order_by: str = None, limit: int = None) -> List[asyncpg.Record]:
        """Fetches a list of all records from the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool
            order_by (str, optional): Sets the `ORDER BY` constraint
            limit (int, optional): Sets the maximum number of records to fetch

        Returns:
            list(asyncpg.Record): A list of database records.
        """
        query, values = cls._query_fetch(order_by, limit)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetch(query, *values)

    @classmethod
    async def fetch_where(cls, where: str, values: Optional[Tuple[Any]] = tuple(), connection: asyncpg.Connection = None,
                          order_by: str = None, limit: int = None) -> List[asyncpg.Record]:
        """Fetches a list of records from the database.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accompanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            order_by (str, optional): Sets the `ORDER BY` constraint.
            limit (int, optional): Sets the maximum number of records to fetch.

        Returns:
            list(asyncpg.Record): A list of database records.
        """
        query = cls._query_fetch_where(where, order_by, limit)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetch(query, *values)

    @classmethod
    async def fetchrow(cls, connection: asyncpg.Connection = None, order_by: str = None, **kwargs) -> asyncpg.Record:
        """Fetches a record from the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            order_by (str, optional): Sets the `ORDER BY` constraint.
            **kwargs (any): Database :class:`Column` values to search for

        Returns:
            asyncpg.Record: A record from the database.
        """

        query, values = cls._query_fetch(order_by, 1, **kwargs)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetchrow(query, *values)

    @classmethod
    async def fetchrow_where(cls, where: str, values: Optional[Tuple[Any]] = tuple(), connection: asyncpg.Connection = None,
                             order_by: str = None) -> List[asyncpg.Record]:
        """Fetches a record from the database.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accompanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            order_by (str, optional): Sets the `ORDER BY` constraint.

        Returns:
            asyncpg.Record: A record from the database.
        """
        query = cls._query_fetch_where(where, order_by, 1)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetchrow(query, *values)
