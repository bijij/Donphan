from .abc import Fetchable
from .connection import Connection, MaybeAcquire


class View(Fetchable):

    @classmethod
    def _query_create(cls, drop_if_exists=True, if_not_exists=True):
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

    @classmethod
    def _query_drop(cls, if_exists=True, cascade=False):
        return cls._base_query_drop('VIEW', if_exists, cascade)


async def create_views(connection: Connection = None, drop_if_exists: bool = False):
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
