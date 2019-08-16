import asyncpg

from .abc import Object
from .connection import MaybeAcquire


class View(Object):
    """A Pythonic representation of a database view.

    Attributes:
        _name (str): The view full name in `schema.view_name` format.
        _select (str, optional): The `SELECT` query stub to use.
        _query (str): The `FROM ... WHERE ...` query stub to use.

    """

    @classmethod
    def _query_drop(cls, cascade: bool = False) -> str:
        return f'DROP TABLE IF EXISTS {cls._name}{" CASCADE" if cascade else ""}'

    @classmethod
    def _query_create(cls, drop_if_exists: bool = False) -> str:
        builder = ['CREATE']

        if drop_if_exists:
            builder.append('OR REPLACE')

        builder.append(F'VIEW {cls._name} AS')

        builder.append('SELECT')

        if hasattr(cls, '_select'):
            builder.append(cls._select)
        else:
            for i, column in enumerate(cls._columns.values(), 1):
                builder.append(
                    f'\t{column.name}{"," if i != len(cls._columns) else ""}')

        builder.append(cls._query)

        return "\n".join(builder)


async def create_views(connection: asyncpg.Connection = None, drop_if_exists: bool = False):
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
