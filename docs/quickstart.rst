Quick Start Guide
=================

Creating a Table
----------------

Tables are simple to define.

.. code-block:: python3

    import asyncio
    from donphan import create_pool, create_tables, Column, Table, SQLType

    class Example_Table(Table):
        id: int = Column(primary_key=True, auto_increment=True)     # example auto increment primary key
        created_at: SQLType.Timestamp() = Column(default='NOW()')   # example column with a default value
        some_text: str                                              # example simple text column                
        some_other_thing: int = Column(references=Other_Table.id)   # example coulmn with a foreign key

    if __name__ == '__main__':
        run = asyncio.get_event_loop().run_until_complete

        run(create_pool('your dsn here')) # Connects to postgres
        run(create_tables()) # Creates all tables defined. Tables can also be individually created.


This code shows how one could define a simple table using donphan.

:class:`donphan.Column` names are defined as class atributes, types are specified 
as type hints, supplied types can either be defined as a built in python type
or using a :class:`donphan.SQLType` classmethod.

Additional :class:`donphan.Column` properties such as wether the column is a primary key
can be set by assigning a value to the class attribute.


Interacting with a Table
------------------------

Once a table has been defined and created it can be interacted with using asynchronous
classmethods, A list of applicable methods can be found here: :class:`donphan.Table`

The following shows an inserting a record into a predefined table

.. code-block:: python3

    await Example_Table.insert(
        some_text = 'This is some text',
        some_other_thing = 2
    )

Records can be fetched from the table in a similar way

.. code-block:: python3

    records = await Example_Table.fetch(
        some_other_thing = 2
    )

In this example the variable `records` will hold a list of all records in the table where
the value of the column `some_other_thing` is equal to `2`.

Records returned are instances of :class:`asyncpg.Record`.

One can check if a value does not equal, is less or greater than and their or equal counterparts by appending `__ne`, `__lt`, `__gt`, `__le`, and `__ge`
to the end of the keyword argument for each respective column.

.. code-block:: python3

    records = await Example_Table.fetch(
        created_at__lt = 'NOW() - INTERVAL \'30 days\'

By default all keword arguments applied are assumed to be an SQL `AND` statement. However it is possible
to use an `OR` statement by appending `or_` to the beginning of a keyword argument for a respective column.

.. code-block:: python3

    records = await Example_Table.fetch(
        created_at__lt = 'NOW() - INTERVAL \'30 days\'
        or_some_other_thing = 2

If desired a pure SQL where clause may be used.

.. code-block:: python3

    record = await Example_Table.fetchrow_where(
        'created_at < NOW() - INTERVAL \'30 days\' OR some_other_thing = 2'
    )

In this instance the varaible `record` will hold the first result of the query or :class:`None`.

Using a :class:`asyncpg.Record` instance we can simply delete a record in a table.

.. code-block:: python3

    await Example_Table.delete_record(record)


Views
-----

Views are virtual tables which display the result of a SQL Query. In some instances using a view can help
improve database recall performance especially on complicated queries which may be executed often over a long
period of time.

Views can be defined as such:

.. code-block:: python3

    class Example_View(View):
        _select = '*'
        _query = f'FROM {Example_Table._name} WHERE some_text LIKE \'%abc%\''

Views share some functionality with Tables, allowing for fetch methods to be called on them in a similar fashion.