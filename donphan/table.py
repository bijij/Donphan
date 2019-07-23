import inspect
from typing import Any, Dict, Iterable, List, Optional, Tuple

import asyncpg

from .connection import MaybeAcquire
from .column import Column
from .sqltype import SQLType


class _TableMeta(type):

    def __new__(cls, name, parents, dct, **kwargs):

        # Set the DB Schema
        dct.update({
            'schema': kwargs.get('schema', 'public'),
            '_columns': {}
        })

        table = super().__new__(cls, name, parents, dct)

        for _name, _type in dct.get('__annotations__', {}).items():

            # If the input type is an array
            is_array = 0
            while isinstance(_type, list):
                is_array += 1
                _type = _type[0]

            if inspect.ismethod(_type) and _type.__self__ is SQLType:
                _type = _type()
            else:
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


class Table(metaclass=_TableMeta):

    @classmethod
    def _validate_kwargs(cls, primary_keys_only=False, **kwargs) -> Dict[str, Any]:
        """Validates passed kwargs against table"""
        verified = {}
        for kwarg, value in kwargs.items():

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

            # If column is an array
            if column.is_array:
                raise NotImplementedError(
                    'Inserting into columns with arrays is currently not supported')

            # Validate the type being passed in is correct
            elif not isinstance(value, (column.type.python, type(None))):
                raise TypeError(
                    f'Column {column.name}; expected {column.type.__name__}, recieved {type(value).__name__}')

            verified[column.name] = value

        return verified

    # region SQL Queries

    @classmethod
    def _query_drop_table(cls) -> str:
        """Generates the DROP TABLE stub."""
        return f'DROP TABLE IF EXISTS {cls._name}'

    @classmethod
    def _query_create_schema(cls) -> str:
        """Generates the CREATE SCHEMA stub."""
        return f'CREATE SCHEMA IF NOT EXISTS {cls.schema};'

    @classmethod
    def _query_create_table(cls) -> str:
        """Generates the CREATE TABLE stub."""
        builder = [f'CREATE TABLE IF NOT EXISTS {cls._name} (']

        primary_keys = []
        for column in cls._columns.values():
            if column.primary_key:
                primary_keys.append(column.name)

            builder.append(f'\t{column},')

        builder.append(f'\tPRIMARY KEY ({", ".join(primary_keys)})')

        builder.append(');')

        return "\n".join(builder)

    @classmethod
    def _query_insert(cls, returning, **kwargs) -> Tuple[str, Iterable]:
        """Generates the INSERT INTO stub."""
        verified = cls._validate_kwargs(**kwargs)

        builder = [f'INSERT INTO {cls._name}']
        builder.append(f'({", ".join(verified)})')
        builder.append('VALUES')

        values = []
        for i, _ in enumerate(verified, 1):
            values.append(f'${i}')
        builder.append(f'({", ".join(values)})')

        if returning:
            builder.append('RETURNING')

            if returning == '*':
                builder.append('*')

            else:

                # Convert to tuple if object is not iter
                try:
                    iter(returning)
                except TypeError:
                    returning = (returning,)

                for value in returning:
                    if not isinstance(value, Column):
                        raise TypeError(
                            f'Expected a volume for the returning value recieved {type(value).__name__}')
                    builder.append(value.name)

        return (" ".join(builder), verified.values())

    @classmethod
    def _query_fetch(cls, order_by, limit, **kwargs) -> Tuple[str, Iterable]:
        """Generates the SELECT FROM stub"""
        verified = cls._validate_kwargs(**kwargs)

        builder = [f'SELECT * FROM {cls._name}']

        # Set the WHERE clause
        if verified:
            builder.append('WHERE')
            checks = []
            for i, key in enumerate(verified, 1):
                checks.append(f'{key} = ${i}')
            builder.append(' AND '.join(checks))

        if order_by is not None:
            builder.append(f'ORDER BY {order_by}')

        if limit is not None:
            builder.append(f'LIMIT {limit}')

        return (" ".join(builder), verified.values())

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
    async def drop_table(cls, connection: asyncpg.Connection = None):
        """Drops this table from the database.

            Args:
                connection (asyncpg.Connection, optional): A database connection to use.
                    If none is supplied a connection will be acquired from the pool.
        """
        async with MaybeAcquire(connection) as connection:
            await connection.execute(cls._query_drop_table())

    @classmethod
    async def create_table(cls, connection: asyncpg.Connection = None, drop_if_exists: bool = False):
        """Creates this table in the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            drop_if_exists (bool, optional): Specified wether the table should be
                first dropped from the database if it already exists.
        """
        async with MaybeAcquire(connection) as connection:
            if drop_if_exists:
                await cls.drop_table(connection)
            await connection.execute(cls._query_create_schema())
            await connection.execute(cls._query_create_table())

    @classmethod
    async def prepare(cls, query: str, connection: asyncpg.Connection = None) -> asyncpg.prepared_stmt.PreparedStatement:
        """Creates a `class`:asyncpg.prepared_stmt.PreparedStatement: based on the given query.

        Args:
            query (str): The SQL query to prepare.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
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
        """
        query, values = cls._query_fetch(order_by, limit, **kwargs)
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
        """
        query, values = cls._query_fetch(order_by, limit)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetch(query, *values)

    @classmethod
    async def fetch_where(cls, where: str, values: Optional[Tuple[Any]] = tuple(), connection: asyncpg.Connection = None, order_by: str = None, limit: int = None) -> List[asyncpg.Record]:
        """Fetches a list of records from the database.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accomanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            order_by (str, optional): Sets the `ORDER BY` constraint.
            limit (int, optional): Sets the maximum number of records to fetch.
        """
        query = cls._query_fetch_where(where, order_by, limit)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetch(query, *values)

    @classmethod
    async def fetchrow(cls, connection: asyncpg.Connection = None, **kwargs) -> asyncpg.Record:
        """Fetches a record from the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        """

        query, values = cls._query_fetch(None, None, **kwargs)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetchrow(query, *values)

    @classmethod
    async def fetchrow_where(cls, where: str, values: Optional[Tuple[Any]] = tuple(), connection: asyncpg.Connection = None) -> List[asyncpg.Record]:
        """Fetches a record from the database.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accomanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        """
        query = cls._query_fetch_where(where, None, None)
        async with MaybeAcquire(connection) as connection:
            return await connection.fetchrow(query, *values)

    @classmethod
    async def insert(cls, connection: asyncpg.Connection = None, returning: Iterable[Column] = None, **kwargs):
        query, values = cls._query_insert(returning, **kwargs)
        async with MaybeAcquire(connection) as connection:
            if returning:
                return await connection.fetchrow(query, *values)
            await connection.execute(query, *values)
