from .abc import Insertable
from .connection import MaybeAcquire

from asyncpg import Connection


class Table(Insertable):

    @classmethod
    def _query_create(cls, drop_if_exists=True, if_not_exists=True):
        builder = ['CREATE TABLE']

        if if_not_exists:
            builder.append('IF NOT EXISTS')

        builder.append(cls._name)
        builder.append('(')

        primary_keys = list()
        for column in cls._columns:
            if column.primary_key:
                primary_keys.append(column.name)
            builder.append(f'{column},')

        builder.append(f'PRIMARY KEY ({", ".join(primary_keys)})')

        builder.append(')')

        return ' '.join(builder)

    @classmethod
    def _query_drop(cls, if_exists=True, cascade=False):
        return cls._base_query_drop('TABLE', if_exists, cascade)


async def create_tables(connection: Connection = None, drop_if_exists: bool = False, if_not_exists: bool = True):
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
