.. currentmodule:: donphan

API Reference
=============

The following page outlines the donphan API.

Connection Helpers
------------------

.. autofunction:: create_pool

.. autoclass:: MaybeAcquire
    :members:

Type Codecs
~~~~~~~~~~~

.. autoclass:: TYPE_CODECS()
    :members:

.. autoclass:: OPTIONAL_CODECS
    :members:

Database Creation Helpers
-------------------------

.. autofunction:: create_db


SQL Types
---------

SQLType
~~~~~~~
.. autoclass:: SQLType
    :members: py_type, sql_type

Supported Types
~~~~~~~~~~~~~~~

The following types are supported by donphan.

Numeric Types
*************
.. autoattribute:: SQLType.Integer

.. autoattribute:: SQLType.SmallInt

.. autoattribute:: SQLType.BigInt

.. autoattribute:: SQLType.Serial

.. autoattribute:: SQLType.Float

.. autoattribute:: SQLType.DoublePrecision

.. autoattribute:: SQLType.Numeric

Monetary Types
**************
.. autoattribute:: SQLType.Money

Character Types
***************
.. autoattribute:: SQLType.Character

.. autoattribute:: SQLType.Text

Binary Type
***********
.. autoattribute:: SQLType.Bytea

Date/Time Types
***************
.. autoattribute:: SQLType.Timestamp

.. autoattribute:: SQLType.AwareTimestamp

.. autoattribute:: SQLType.Date

.. autoattribute:: SQLType.Interval

Boolean Type
************
.. autoattribute:: SQLType.Boolean

Network Adress Types
********************
.. autoattribute:: SQLType.CIDR

.. autoattribute:: SQLType.Inet

.. autoattribute:: SQLType.MACAddr

UUID Type
*********
.. autoattribute:: SQLType.UUID

JSON Types
*************************
.. autoattribute:: SQLType.JSON

.. autoattribute:: SQLType.JSONB




Database Objects
----------------

Below are types which are used to interact with the dtabase.

BaseColumn
~~~~~~~~~~
.. autoclass:: BaseColumn()
    :members:

Column
~~~~~~
.. autoclass:: Column
    :members:


Table
~~~~~
.. autoclass:: Table()
    :members:
    :inherited-members:


ViewColumn
~~~~~~~~~~
.. autoclass:: ViewColumn
    :members:


View
~~~~
.. autoclass:: View()
    :members:
    :inherited-members:


Utility Functions / Classes
---------------------------

Below are utility some utility functions and classes which interact with donphan.


.. autofunction:: export_db


Enum
~~~~
.. autoclass:: Enum()
    

    A fully-featured Enum class with support for custom types, see :class:`enum.Enum` for documentation.

    :resource:`Usage Example <examples-custom-enum>`: ::
    
        from donphan import Column, Enum, Table


        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3


        class MyTable(Table):
            key: Column[str] = Column(primary_key=True)
            value: Column[MyEnum]


Type Validation Helpers
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TypeCodec
