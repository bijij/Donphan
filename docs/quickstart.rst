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
        created_at: SQLType.Timestamp() = Column(default="NOW()")   # example column with a default value
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

.. code-block:: python3