from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Optional, TYPE_CHECKING, TypeVar, Union

from .types import SQLType
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
        default:
            The default value of the column.
        references: Optional[:class:`.Column`]
            The column which this column references, if set.
    """

    if TYPE_CHECKING:
        name: str
        table: type[Selectable]
        _sql_type: type[SQLType[T]]

    primary_key: bool = False
    index: bool = False
    nullable: bool = True
    unique: bool = False
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
