:orphan:

.. currentmodule:: donphan

.. _migrating_to_v4:

Migrating to v4.0.0
======================

v4.0 is the largest change to the library due to a complete redesign.


Python Minumum Version Change
-----------------------------

In order to allow for more advanced use of type-hinting older python version support has been dropped.
**Donphan now requires python 3.9 at a minimum**.


Database Model Object Changes
-----------------------------

Table definition redesign
~~~~~~~~~~~~~~~~~~~~~~~~~

With support for more advanced type hinting Database Model Objects such as :class:`.Table` have had substantial changes.

As an example:

.. code-block:: python3

    # before
    class MyTable(Table):
        id: int = Column(primary_key=True, auto_increment=True)
        created_at: SQLType.Timestamp = Column(default="NOW()")
        some_text: str
        some_other_thing: int = Column(references=OtherTable.id)

    # after
    class MyTable(Table):
        id: Column[SQLType.Serial] = Column(primary_key=True)
        created_at: Column[SQLType.Timestamp] = Column(default="NOW()")
        some_text: Column[str]
        some_other_thing: Column[int] = Column(references=OtherTable.id)

You can read more about the new :class:`.SQLType`, :class:`.Column`, and :class:`.Table` in the :doc:`api`

Table name normalisation changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Table names are now normalised differently to allow for more pythonic class naming,
Custom names can be set using a ``_name=`` keyword argument when defining the class.

.. code-Block:: python3

    #before
    class MyTable(Table):
        ...

    >>> MyTable._name
    "public.mytable"

    #after
    class MyTable(Table):
        ...

    >>> MyTable._name
    "public.my_table"

Table class methods now require a connection.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To allow for multiple database connection pools all Table helper methods require a connection to be passed as
the first parameter.

``Table.fetchrow`` now ``Table.fetch_row``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To be more in-line with other methods ``Table.fetchrow`` has been renamed to :attr:`.Table.fetch_row`.


Views
-----

Views have been reworked to allow for named columns simular to tables.
