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

from .abc import Fetchable
from .connection import MaybeAcquire


__all__ = (
    "create_views",
    "View",
)


class View(Fetchable):
    _select: str
    _query: str

    @classmethod
    def _query_create(cls, drop_if_exists=True, if_not_exists=True):
        builder = ["CREATE"]

        if drop_if_exists:
            builder.append("OR REPLACE")

        builder.append(f"VIEW {cls._name} AS")

        builder.append("SELECT")

        if hasattr(cls, "_select"):
            builder.append(cls._select)
        else:
            for i, column in enumerate(cls._columns, 1):
                builder.append(f'\t{column.name}{"," if i != len(cls._columns) else ""}')

        builder.append(cls._query)

        return "\n".join(builder)

    @classmethod
    def _query_drop(cls, if_exists=True, cascade=False):
        return cls._base_query_drop("VIEW", if_exists, cascade)


async def create_views(connection: Optional[asyncpg.Connection] = None, drop_if_exists: bool = False):
    """Create all defined views.

    Args:
        connection (asyncpg.Connection, optional): A database connection to use.
            If none is supplied a connection will be acquired from the pool.
        drop_if_exists (bool, optional): Specifies wether the views should be
                first dropped from the database if they already exists.
    """
    async with MaybeAcquire(connection=connection) as connection:
        for view in View.__subclasses__():
            await view.create(connection=connection, drop_if_exists=drop_if_exists)
