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

from typing import Optional

import asyncpg

from .abc import Insertable
from .connection import MaybeAcquire

__all__ = (
    "create_tables",
    "Table",
)


class Table(Insertable):
    @classmethod
    def _query_create(cls, drop_if_exists=True, if_not_exists=True):
        builder = ["CREATE TABLE"]

        if if_not_exists:
            builder.append("IF NOT EXISTS")

        builder.append(cls._name)
        builder.append("(")

        primary_keys = []
        for column in cls._columns:
            if column.primary_key:
                primary_keys.append(column.name)
            builder.append(f"{column},")

        if primary_keys:
            builder.append(f'PRIMARY KEY ({", ".join(primary_keys)})')
        else:
            builder[-1] = builder[-1][:-1]

        builder.append(")")

        return " ".join(builder)

    @classmethod
    def _query_drop(cls, if_exists=True, cascade=False):
        return cls._base_query_drop("TABLE", if_exists, cascade)


async def create_tables(
    connection: Optional[asyncpg.Connection] = None, drop_if_exists: bool = False, if_not_exists: bool = True
) -> None:
    """Create all defined tables.

    Args:
        connection (asyncpg.Connection, optional): A database connection to use.
                If none is supplied a connection will be acquired from the pool.
        drop_if_exists (bool, optional): Specifies wether the tables should be
                first dropped from the database if they already exists.
    """
    async with MaybeAcquire(connection=connection) as connection:
        for table in Table.__subclasses__():
            if drop_if_exists:
                await table.drop(connection=connection, if_exists=True, cascade=True)
            await table.create(connection=connection, if_not_exists=if_not_exists)
