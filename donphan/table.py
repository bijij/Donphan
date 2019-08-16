import inspect
from collections.abc import Iterable
from typing import Any, Dict, Iterable, List, Optional, Tuple

import asyncpg

from .abc import Object
from .connection import MaybeAcquire
from .column import Column
from .sqltype import SQLType


class Table(Object):
    """A Pythonic representation of a database table.

    Attributes:
        _name (str): The tables full name in `schema.table_name` format.

    """

    # region SQL Queries

    @classmethod
    def _query_drop(cls, cascade: bool = False) -> str:
        return f'DROP TABLE IF EXISTS {cls._name}{" CASCADE" if cascade else ""}'

    @classmethod
    def _query_create(cls, drop_if_exists: bool = False) -> str:
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
                if not isinstance(returning, Iterable):
                    returning = (returning,)

                returning_builder = []

                for value in returning:
                    if not isinstance(value, Column):
                        raise TypeError(
                            f'Expected a volume for the returning value recieved {type(value).__name__}')
                    returning_builder.append(value.name)

                builder.append(', '.join(returning_builder))

        return (" ".join(builder), verified.values())

    @classmethod
    def _query_insert_many(cls, columns) -> str:
        """Generates the INSERT INTO stub."""
        builder = [f'INSERT INTO {cls._name}']
        builder.append(f'({", ".join(column.name for column in columns)})')
        builder.append('VALUES')
        builder.append(
            f'({", ".join(f"${n+1}" for n in range(len(columns)))})')

        return " ".join(builder)

    @classmethod
    def _query_update_record(cls, record, **kwargs) -> Tuple[str, List[Any]]:
        '''Generates the UPDATE stub'''
        verified = cls._validate_kwargs(**kwargs)

        builder = [f'UPDATE {cls._name} SET']

        # Set the values
        sets = []
        for i, key in enumerate(verified, 1):
            sets.append(f'{key} = ${i}')
        builder.append(', '.join(sets))

        # Set the QUERY
        record_keys = cls._validate_kwargs(primary_keys_only=True, **record)

        builder.append('WHERE')
        checks = []
        for i, key in enumerate(record_keys, i+1):
            checks.append(f'{key} = ${i}')
        builder.append(' AND '.join(checks))

        return (" ".join(builder), list(verified.values()) + list(record_keys.values()))

    @classmethod
    def _query_update_where(cls, query, values, **kwargs) -> Tuple[str, List[Any]]:
        '''Generates the UPDATE stub'''
        verified = cls._validate_kwargs(**kwargs)

        builder = [f'UPDATE {cls._name} SET']

        # Set the values
        sets = []
        for i, key in enumerate(verified, len(values) + 1):
            sets.append(f'{key} = ${i}')
        builder.append(', '.join(sets))

        # Set the QUERY
        builder.append('WHERE')
        builder.append(query)

        return (" ".join(builder), values + tuple(verified.values()))

    @classmethod
    def _query_delete_record(cls, record) -> Tuple[str, List[Any]]:
        '''Generates the DELETE stub'''

        builder = [f'DELETE FROM {cls._name}']

        # Set the QUERY
        record_keys = cls._validate_kwargs(primary_keys_only=True, **record)

        builder.append('WHERE')
        checks = []
        for i, key in enumerate(record_keys, 1):
            checks.append(f'{key} = ${i}')
        builder.append(' AND '.join(checks))

        return (" ".join(builder), record_keys.values())

    @classmethod
    def _query_delete_where(cls, query) -> str:
        '''Generates the UPDATE stub'''

        builder = [f'DELETE FROM {cls._name}']

        # Set the QUERY
        builder.append('WHERE')
        builder.append(query)

        return " ".join(builder)

    # endregion

    @classmethod
    async def insert(cls, connection: asyncpg.Connection = None, returning: Iterable[Column] = None, **kwargs) -> Optional[asyncpg.Record]:
        """Inserts a new record into the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            returning (list(Column), optional): A list of columns from this record to return
            **kwargs (any): The records column values.

        Returns:
            (asyncpy.Record, optional): The record inserted into the database
        """
        query, values = cls._query_insert(returning, **kwargs)
        async with MaybeAcquire(connection) as connection:
            if returning:
                return await connection.fetchrow(query, *values)
            await connection.execute(query, *values)

    @classmethod
    async def insert_many(cls, columns: Iterable[Column], values: Iterable[Iterable[Any]], connection: asyncpg.Connection = None):
        """Inserts multiple records into the database.

        Args:
            columns (list(Column)): The list of columns to insert based on.
            values (list(list)): The list of values to insert into the database. 

            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        """
        query = cls._query_insert_many(columns)

        async with MaybeAcquire(connection) as connection:
            await connection.executemany(query, values)

    @classmethod
    async def update_record(cls, record: asyncpg.Record, connection: asyncpg.Connection = None, **kwargs):
        """Updates a record in the database.

        Args:	
            record (asyncpg.Record): The database record to update
            connection (asyncpg.Connection, optional): A database connection to use.	
                If none is supplied a connection will be acquired from the pool	
            **kwargs: Values to update	
        """
        query, values = cls._query_update_record(record, **kwargs)
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)

    @classmethod
    async def update_where(cls, where: str, values: Optional[Tuple[Any]] = tuple(), connection: asyncpg.Connection = None, **kwargs):
        """Updates any record in the database which satisfies the query.

        Args:	
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accomanying values.
            connection (asyncpg.Connection, optional): A database connection to use.	
                If none is supplied a connection will be acquired from the pool	
            **kwargs: Values to update	
        """

        query, values = cls._query_update_where(where, values, **kwargs)
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)

    @classmethod
    async def delete_record(cls, record: asyncpg.Record, connection: asyncpg.Connection = None):
        """Deletes a record in the database.

        Args:
            record (asyncpg.Record): The database record to delete
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool

        """
        query, values = cls._query_delete_record(record)
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)

    @classmethod
    async def delete_where(cls, where: str, values: Optional[Tuple[Any]] = tuple(), connection: asyncpg.Connection = None):
        """Deletes any record in the database which statisfies the query.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accomanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool

        """
        query = cls._query_delete_where(where)
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)


async def create_tables(connection: asyncpg.Connection = None, drop_if_exists: bool = False):
    """Create all defined tables.

    Args:
        connection (asyncpg.Connection, optional): A database connection to use.
            If none is supplied a connection will be acquired from the pool.
        drop_if_exists (bool, optional): Specifies wether the tables should be
                first dropped from the database if they already exists.
    """
    async with MaybeAcquire(connection=connection) as connection:
        for table in Table.__subclasses__():
            await table.create(connection=connection, drop_if_exists=drop_if_exists)
