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

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Optional, TYPE_CHECKING, TypeVar, Union

from .sqltype import SQLType
from .utils import MISSING

if TYPE_CHECKING:
    from .selectable import Selectable


__all__ = ("Column",)


T = TypeVar("T")


@dataclass
class Column(Generic[T]):
    """A representation of a SQL Database Table Column

    Parameters
    ----------
        primary_key: :class:`bool`
            Whether the column is a primary key column, defaults to ``False``.
        index: :class:`bool`
            Whether to create an index for the given column, defaults to ``False``.

    Attributes
    ----------
        name: :class:`str`
            The column's name.
        table: :class:`~.Table`
            The Table the column is a part of.
        py_type: Type
            The python type associated with the column.
        sql_type: Type[:class:`~.SQLType`]
            The SQL type associated with the column.
        primary_key: :class:`bool`
            Whether the column is a primary key column.
        index: :class:`bool`
            Whether the column is indexed.
        nullable: :class:`bool`
            Whether the column is nullable.
        unique: :class:`bool`
            Whether the column has a unique constraint.
        default: Any
            The default value of the column.
        references: Optional[:class:`Column`]
            The column which this column references, if set.
        cascade: :class:`bool`
            Whether deletions / updates the referenced column should cascace. Defaults to ``False``.
    """

    if TYPE_CHECKING:
        name: str
        table: type[Selectable]
        _sql_type: type[SQLType[T]]

    primary_key: bool = False
    index: bool = False
    nullable: bool = True
    unique: bool = False
    cascade: bool = False
    default: Optional[Union[str, T]] = MISSING
    references: Optional[Column[T]] = None

    @property
    def py_type(self) -> type[T]:
        return self._sql_type.py_type

    @property
    def sql_type(self) -> type[SQLType[T]]:
        return self._sql_type

    @classmethod
    def _with_type(cls, type: type[SQLType[T]], **options: Any) -> Column[T]:
        column = cls(**options)
        column._sql_type = type
        return column
