:orphan:

.. _quickstart:

.. currentmodule:: donphan

Quickstart Guide
================

Creating a Table
----------------

Tables are simple to define.

.. code-block:: python3

    import asyncio
    from donphan import create_pool, Column, Table, MaybeAcquire, SQLType

    class ExampleTable(Table):
        id: Column[SQLType.Serial] = Column(primary_key=True)            # example auto increment primary key
        created_at: Column[SQLType.Timestamp] = Column(default='NOW()')  # example column with a default value
        some_text: Column[str]                                           # example simple text column                
        a_list_of_numbers: Column[list[int]                              # example coulmn with a foreign key

    async def main():
        pool = await create_pool(os.getenv("POSTGRES_DSN"))              # Connects to postgres
        async with MaybeAcquire(pool=pool) as connection:                # Acquire a connection from the pool
            await ExampleTable.create(connection)                        # Create the Example Table 

This code shows how one could define a simple table using donphan.

:class:`.Column` names are defined as class atributes, types are specified 
as type hints, supplied types can either be defined as a built in python type
or using a :class:`.SQLType` class.

Additional :class:`.Column` properties such as wether the column is a primary key
can be set via creating a :class:`.Column` instance.


Interacting with a Table
------------------------

Once a table has been defined and created it can be interacted with using asynchronous
classmethods, A list of applicable methods can be found here: :class:`Table`

The following shows an inserting a record into a predefined table

.. code-block:: python3

    await ExampleTable.insert(
        connection,
        some_text='This is some text',
        a_list_of_numbers=[1,2,3],
    )

Records can be fetched from the table in a similar way

.. code-block:: python3

    records = await ExampleTable.fetch(connection, a_list_of_numbers=[1,2,3])

In this example the variable `records` will hold a list of all records in the table where
the value of the column `some_other_thing` is equal to `2`.

Records returned are instances of :class:`asyncpg.Record`.

One can check if a value does not equal, is less or greater than and their or equal counterparts by appending `__ne`, `__lt`, `__gt`, `__le`, and `__ge`
to the end of the keyword argument for each respective column.

.. code-block:: python3

    records = await ExampleTable.fetch(connection, created_at__lt=datetime.datetime.utcnow())

By default all keword arguments applied are assumed to be an SQL `AND` statement. However it is possible
to use an `OR` statement by appending `or_` to the beginning of a keyword argument for a respective column.

.. code-block:: python3

    records = await ExampleTable.fetch(connection, created_at__lt=datetime.datetime.utcnow(), or_some_other_thing = 2)

If desired a pure SQL where clause may be used. With value subtitution where needed.

.. code-block:: python3

    records = await ExampleTable.fetch_where(connection, 'created_at < NOW() OR some_other_thing = $1', 2)

It is possible to obtain a single record from the database as well.

.. code-block:: python3

    record = await ExampleTable.fetch_row(connection, id=1)

Using a :class:`asyncpg.Record` instance we can simply delete a record in a table.

.. code-block:: python3

    await ExampleTable.delete_record(connection, record)
