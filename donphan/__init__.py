__title__ = 'donphan'
__author__ = 'Bijij'
__license__ = 'MIT'
__copyright__ = 'Copyright 2020 Bijij'
__version__ = '2.0.0'

from .column import Column
from .connection import create_pool, MaybeAcquire
from .table import create_tables, Table
from .sqltype import SQLType
from .view import create_views, View


class _Test_Table(Table):
    id: int = Column(auto_increment=True)
