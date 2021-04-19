"""
MIT License

Copyright (c) 2019-present Josh B

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import abc
import inspect

from typing import (
    Any,
    cast,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import asyncpg  # type: ignore

from .connection import MaybeAcquire
from .column import Column
from .consts import DEFAULT_SCHEMA
from .meta import ObjectMeta
from .sqltype import SQLType


DEFAULT_OPERATORS = {
    "eq": "=",
    "ne": "!=",
    "lt": "<",
    "gt": ">",
    "le": "<=",
    "ge": ">=",
}


class Creatable(metaclass=ObjectMeta):
    _name: str
    _schema: str

    @classmethod
    def _query_create_schema(cls, if_not_exists: bool = True) -> str:
        """Generates a CREATE SCHEMA stub."""
        builder = ["CREATE SCHEMA"]

        if if_not_exists:
            builder.append("IF NOT EXISTS")

        builder.append(cls._schema)  # type: ignore

        return " ".join(builder)

    @classmethod
    @abc.abstractmethod
    def _query_create(cls, drop_if_exists: bool = True, if_not_exists: bool = True) -> str:
        """Generates a CREATE stub."""
        raise NotImplementedError

    @classmethod
    def _base_query_drop(cls, type: str, if_exists: bool = True, cascade: bool = False) -> str:
        """Generates a DROP stub."""
        builder = ["DROP", type]

        if if_exists:
            builder.append("IF EXISTS")

        builder.append(cls._name)  # type: ignore

        if cascade:
            builder.append("CASCADE")

        return " ".join(builder)

    @classmethod
    @abc.abstractmethod
    def _query_drop(cls, if_exists: bool = True, cascade: bool = False) -> str:
        """Generates a DROP stub."""
        raise NotImplementedError

    @classmethod
    async def create(cls, *, connection=None, drop_if_exists=True, if_not_exists=True):
        """Creates this object in the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            if_not_exists (bool, optional): TODO
        """
        async with MaybeAcquire(connection) as connection:

            if if_not_exists:
                await connection.execute(cls._query_create_schema())

            await connection.execute(cls._query_create(drop_if_exists, if_not_exists))

    @classmethod
    async def drop(cls, *, connection=None, if_exists: bool = False, cascade: bool = False):
        """Drops this object from the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            if_exists (bool, optional): TODO
            cascade (bool, optional): TODO
        """
        async with MaybeAcquire(connection) as connection:
            await connection.execute(cls._query_drop(if_exists, cascade))


class FetchableMeta(ObjectMeta):
    _columns: List[Column]
    _columns_dict: Dict[str, Column]

    def __new__(cls, name, bases, attrs, **kwargs):

        attrs.update({"_columns": list(), "_columns_dict": dict()})

        obj = cast(Type[FetchableMeta], super().__new__(cls, name, bases, attrs, **kwargs))

        for _name, _type in attrs.get("__annotations__", {}).items():

            # Ignore annotations with leading underscore
            if _name[0] == "_":
                continue

            # Raise an exception when an invalid column name is set.
            if _name.startswith("or_") or _name[-4:-2] == "__":
                raise NameError(f"Column {_name}'s name is invalid.")

            if isinstance(_type, str):
                _type = eval(_type)

            # If the input type is an array
            is_array = False
            while isinstance(_type, list):
                is_array = True
                _type = _type[0]

            if inspect.ismethod(_type) and _type.__self__ is SQLType:
                _type = _type()
            elif not isinstance(_type, SQLType):
                if not issubclass(_type, SQLType):
                    _type = SQLType._from_python_type(_type)

            column = attrs.get(_name, Column())._update(obj, _name, _type, is_array)
            obj._columns.append(column)
            obj._columns_dict[_name] = column

        return obj

    def __getattr__(cls, key):
        if key in cls._columns_dict:
            return cls._columns_dict[key]

        return super().__getattr__(key)


class Fetchable(Creatable, metaclass=FetchableMeta):
    @classmethod
    def _validate_kwargs(cls, primary_keys_only: bool = False, **kwargs: Any) -> Dict[Column, Any]:
        """Validates passed kwargs against table"""
        verified: Dict[Column, Any] = dict()
        for kwarg, value in kwargs.items():

            # Strip Extra operators
            if kwarg.startswith("or_"):
                kwarg = kwarg[3:]
            if kwarg[-4:-2] == "__":
                kwarg = kwarg[:-4]

            # Check column is in Object
            if kwarg not in cls._columns_dict:
                raise AttributeError(f"Could not find column with name {kwarg} in table {cls._name}")

            column = cls._columns_dict[kwarg]

            # Skip non primary when relevant
            if primary_keys_only and not column.primary_key:
                continue

            # Check passing null into a non nullable column
            if not column.nullable and value is None:
                raise TypeError(f"Cannot pass None into non-nullable column {column.name}")

            def check_type(element: Any) -> bool:
                return isinstance(element, (column.type._python, type(None)))  # type: ignore

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
                                f"Column {column.name}; expected {column.type._python }[], received {type(element)}[]"
                            )

                # Check array depth is expected.
                check_array(value)

            # Otherwise check type of element
            elif not check_type(value):
                raise TypeError(f"Column {column.name}; expected {column.type._python}, received {type(value)}")

            verified[column] = value

        return verified

    @classmethod
    def _query_fetch(cls, order_by: Optional[str], limit: Optional[int], **kwargs) -> Tuple[str, Iterable]:
        """Generates a SELECT FROM stub"""
        verified = cls._validate_kwargs(**kwargs)

        # AND / OR statement check
        statements = ["AND " for _ in verified]
        # =, <, >, != check
        operators = ["=" for _ in verified]

        # Determine operators
        for i, key in enumerate(kwargs):

            # First statement has no boolean operator
            if i == 0:
                statements[i] = ""
            elif key[:3] == "or_":
                statements[i] = "OR "

            if key[-4:-2] == "__":
                try:
                    operators[i] = DEFAULT_OPERATORS[key[-2:]]
                except KeyError:
                    raise AttributeError(f"Unknown operator type {key[-2:]}")

        builder = [f"SELECT * FROM {cls._name}"]

        # Set the WHERE clause
        if verified:
            builder.append("WHERE")
            checks = [
                f"{statements[i]}{column.name} {operators[i]} ${i+1}" for i, column in enumerate(verified.keys())
            ]

            builder.append(" ".join(checks))

        if order_by is not None:
            builder.append(f"ORDER BY {order_by}")

        if limit is not None:
            builder.append(f"LIMIT {limit}")

        return (" ".join(builder), verified.values())

    @classmethod
    def _query_fetch_where(cls, query: str, order_by: Optional[str], limit: Optional[int]) -> str:
        """Generates a SELECT FROM stub"""

        builder = [f"SELECT * FROM {cls._name} WHERE", query]
        if order_by is not None:
            builder.append(f"ORDER BY {order_by}")

        if limit is not None:
            builder.append(f"LIMIT {limit}")

        return " ".join(builder)

    @classmethod
    async def fetch(
        cls,
        *,
        connection: Optional[asyncpg.Connection] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> List[asyncpg.Record]:
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
        async with MaybeAcquire(connection) as connection:
            return await connection.fetch(query, *values)

    @classmethod
    async def fetchall(
        cls,
        *,
        connection: Optional[asyncpg.Connection] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[asyncpg.Record]:
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
    async def fetchrow(
        cls, *, connection: Optional[asyncpg.Connection] = None, order_by: Optional[str] = None, **kwargs
    ) -> Optional[asyncpg.Record]:
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
    async def fetch_where(
        cls,
        where: str,
        *values,
        connection: Optional[asyncpg.Connection] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[asyncpg.Record]:
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
    async def fetchrow_where(
        cls, where: str, *values: Any, connection: Optional[asyncpg.Connection] = None, order_by: Optional[str] = None
    ) -> List[asyncpg.Record]:
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
            return await connection.fetchrow(query, *values)  # type: ignore


class Insertable(Fetchable):
    @classmethod
    def _query_update_on_conflict(cls, conflict_keys: Collection[Column], column: Column) -> str:
        return f'ON CONFLICT ({",".join(col.name for col in conflict_keys)}) DO UPDATE SET {column.name} = EXCLUDED.{column.name}'

    @classmethod
    def _query_insert(
        cls,
        ignore_on_conflict: bool,
        update_on_conflict: Optional[Column],
        returning: Optional[Union[str, Column, Iterable[Column]]],
        **kwargs,
    ) -> Tuple[str, Iterable]:
        """Generates the INSERT INTO stub."""
        verified = cls._validate_kwargs(**kwargs)

        builder = [
            f"INSERT INTO {cls._name}",
            f'({", ".join(column.name for column in verified.keys())})',
            "VALUES",
        ]

        values = [f"${i}" for i, _ in enumerate(verified, 1)]
        builder.append(f'({", ".join(values)})')

        if ignore_on_conflict and update_on_conflict is not None:
            raise ValueError("Conflicting ON CONFLICT settings.")

        elif ignore_on_conflict:
            builder.append("ON CONFLICT DO NOTHING")

        elif update_on_conflict is not None:
            if update_on_conflict.table != cls:
                raise ValueError("Supplied column for different table.")

            conflict_keys = [column for column in verified.keys() if column.primary_key]
            builder.append(cls._query_update_on_conflict(conflict_keys, update_on_conflict))

        if returning:
            builder.append("RETURNING")

            if returning == "*":
                builder.append("*")

            else:

                # Convert to tuple if object is not iter
                if not isinstance(returning, Iterable):
                    returning = (returning,)

                returning_builder = []

                for value in returning:
                    if not isinstance(value, Column):
                        raise TypeError(f"Expected a volume for the returning value received {type(value)}")
                    if value.table != cls:
                        raise ValueError("Supplied column for different table.")

                    returning_builder.append(value.name)

                builder.append(", ".join(returning_builder))

        return (" ".join(builder), verified.values())

    @classmethod
    def _query_insert_many(
        cls, columns: Collection[Column], ignore_on_conflict: bool, update_on_conflict: Optional[Column]
    ) -> str:
        """Generates the INSERT INTO stub."""
        builder = [
            f"INSERT INTO {cls._name}",
            f'({", ".join(column.name for column in columns)})',
            "VALUES",
            f'({", ".join(f"${n+1}" for n in range(len(columns)))})',
        ]

        if ignore_on_conflict and update_on_conflict:
            raise ValueError("Conflicting ON CONFLICT settings.")

        elif ignore_on_conflict:
            builder.append("ON CONFLICT DO NOTHING")

        elif update_on_conflict:
            conflict_keys = [column for column in columns if column.primary_key]
            builder.append(cls._query_update_on_conflict(conflict_keys, update_on_conflict))

        return " ".join(builder)

    @classmethod
    def _query_update_record(cls, record, **kwargs) -> Tuple[str, List[Any]]:
        """Generates the UPDATE stub"""
        verified = cls._validate_kwargs(**kwargs)

        builder = [f"UPDATE {cls._name} SET"]

        # Set the values
        sets = [f"{column.name} = ${i}" for i, column in enumerate(verified.keys(), 1)]
        builder.append(", ".join(sets))

        # Set the QUERY
        record_keys = cls._validate_kwargs(primary_keys_only=True, **record)

        builder.append("WHERE")
        checks = [f"{column.name} = ${i}" for i, column in enumerate(record_keys.keys(), len(sets) + 1)]

        builder.append(" AND ".join(checks))

        return (" ".join(builder), list(verified.values()) + list(record_keys.values()))

    @classmethod
    def _query_update_where(cls, query, values, **kwargs) -> Tuple[str, List[Any]]:
        """Generates the UPDATE stub"""
        verified = cls._validate_kwargs(**kwargs)

        builder = [f"UPDATE {cls._name} SET"]

        # Set the values
        sets = [f"{column.name} = ${i}" for i, column in enumerate(verified.keys(), len(values) + 1)]

        builder.append(", ".join(sets))

        # Set the QUERY
        builder.append("WHERE")
        builder.append(query)

        return (" ".join(builder), values + tuple(verified.values()))

    @classmethod
    def _query_delete(cls, **kwargs) -> Tuple[str, List[Any]]:
        """Generates the DELETE stub"""
        verified = cls._validate_kwargs(**kwargs)

        # AND / OR statement check
        statements = ["AND " for _ in verified]
        # =, <, >, != check
        operators = ["=" for _ in verified]

        # Determine operators
        for i, key in enumerate(kwargs):

            # First statement has no boolean operator
            if i == 0:
                statements[i] = ""
            elif key[:3] == "or_":
                statements[i] = "OR "

            if key[-4:-2] == "__":
                try:
                    operators[i] = DEFAULT_OPERATORS[key[-2:]]
                except KeyError:
                    raise AttributeError(f"Unknown operator type {key[-2:]}")

        builder = [f"DELETE FROM {cls._name}"]

        # Set the WHERE clause
        if verified:
            builder.append("WHERE")
            checks = [
                f"{statements[i]}{column.name} {operators[i]} ${i+1}" for i, column in enumerate(verified.keys())
            ]

            builder.append(" ".join(checks))

        return (" ".join(builder), list(verified.values()))

    @classmethod
    def _query_delete_record(cls, record) -> Tuple[str, List[Any]]:
        """Generates the DELETE stub"""

        builder = [f"DELETE FROM {cls._name}"]

        # Set the QUERY
        record_keys = cls._validate_kwargs(primary_keys_only=True, **record)

        builder.append("WHERE")
        checks = [f"{column.name} = ${i}" for i, column in enumerate(record_keys.keys(), 1)]

        builder.append(" AND ".join(checks))

        return (" ".join(builder), list(record_keys.values()))

    @classmethod
    def _query_delete_where(cls, query) -> str:
        """Generates the UPDATE stub"""

        builder = [f"DELETE FROM {cls._name}"]

        # Set the QUERY
        builder.append("WHERE")
        builder.append(query)

        return " ".join(builder)

    @classmethod
    async def insert(
        cls,
        *,
        connection: asyncpg.Connection = None,
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Column] = None,
        returning: Iterable[Column] = None,
        **kwargs,
    ) -> Optional[asyncpg.Record]:
        """Inserts a new record into the database.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            ignore_on_conflict (bool): Sets whether inserting conflicting values should be ignored.
                Defaults to False.
            update_on_conflict (Column, optional): Sets whether a value should be updated on conflict.
                Defaults to None.
            returning (list(Column), optional): A list of columns from this record to return
            **kwargs (any): The records column values.
        Returns:
            (asyncpg.Record, optional): The record inserted into the database
        """
        query, values = cls._query_insert(ignore_on_conflict, update_on_conflict, returning, **kwargs)
        async with MaybeAcquire(connection) as connection:
            if returning:
                return await connection.fetchrow(query, *values)
            await connection.execute(query, *values)
        return None

    @classmethod
    async def insert_many(
        cls,
        columns: Collection[Column],
        *values: Iterable[Iterable[Any]],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Column] = None,
        connection: asyncpg.Connection = None,
    ):
        """Inserts multiple records into the database.
        Args:
            columns (list(Column)): The list of columns to insert based on.
            values (list(list)): The list of values to insert into the database.
            ignore_on_conflict (bool): Sets whether inserting conflicting values should be ignored.
                Defaults to False.
            update_on_conflict (Column, optional): Sets whether a value should be updated on conflict.
                Defaults to None.
            connection (asyncpg.asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        """
        query = cls._query_insert_many(columns, ignore_on_conflict, update_on_conflict)

        async with MaybeAcquire(connection) as connection:
            await connection.executemany(query, values)

    @classmethod
    async def update_record(cls, record: asyncpg.Record, *, connection: asyncpg.Connection = None, **kwargs):
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
    async def update_where(cls, where: str, *values: Any, connection: asyncpg.Connection = None, **kwargs):
        """Updates any record in the database which satisfies the query.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accompanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool
            **kwargs: Values to update
        """

        query, values = cls._query_update_where(where, values, **kwargs)  # type: ignore
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)

    @classmethod
    async def delete(cls, *, connection: asyncpg.Connection = None, **kwargs):
        """Deletes any records in the database which satisfy the supplied kwargs.

        Args:
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
            **kwargs (any): Database :class:`Column` values to filter by when deleting.
        """
        query, values = cls._query_delete(**kwargs)
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)

    @classmethod
    async def delete_record(cls, record: asyncpg.Record, *, connection: asyncpg.Connection = None):
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
    async def delete_where(cls, where: str, *values: Optional[Tuple[Any]], connection: asyncpg.Connection = None):
        """Deletes any record in the database which satisfies the query.

        Args:
            where (str): An SQL Query to pass
            values (tuple, optional): A tuple containing accompanying values.
            connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool
        """
        query = cls._query_delete_where(where)
        async with MaybeAcquire(connection) as connection:
            await connection.execute(query, *values)
