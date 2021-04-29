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
    if TYPE_CHECKING:
        name: str
        table: type[Selectable]
        _sql_type: type[SQLType[T]]

    primary_key: bool = False
    index: bool = False
    nullable: bool = True
    default: Optional[Union[str, T]] = MISSING
    references: Optional[Column[T]] = None

    @property
    def py_type(self) -> type[T]:
        return self._sql_type.py_type

    @property
    def sql_type(self) -> type[SQLType[T]]:
        return self._sql_type

    @classmethod
    def with_type(cls, type: type[SQLType[T]], **options: Any) -> Column[T]:
        column = cls(**options)
        column._sql_type = type
        return column
