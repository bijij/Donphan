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
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union

from ._consts import Operators
from ._sqltype import SQLType
from .utils import MISSING, query_builder

if TYPE_CHECKING:
    from ._join import Join
    from ._selectable import Selectable
    from ._view import View


__all__ = (
    "BaseColumn",
    "Column",
    "JoinColumn",
    "OnClause",
    "ViewColumn",
)


_T = TypeVar("_T")


class OnClause(tuple["BaseColumn", "BaseColumn", Operators]):
    """
    A NamedTuple defining an on clause for a join.


    Parameters
    ----------
    a: :class:`BaseColumn`
        the primary column to join to.

    b: :class:`BaseColumn`
        the secondary column to join to.

    operator: Literal[``"eq"``, ``"lt"``, ``"le"``, ``"ne"``, ``"ge"``, ``"gt"``]
        An opterator to use to compare columns, defaults to eq.
    """

    if TYPE_CHECKING:
        a: BaseColumn
        b: BaseColumn
        operator: Operators

    def __new__(
        cls: type[OnClause],
        a: BaseColumn,
        b: BaseColumn,
        operator: Operators = "eq",
    ) -> OnClause:
        new_cls = super().__new__(cls, (a, b, operator))
        new_cls.a = a
        new_cls.b = b
        new_cls.operator = operator
        return new_cls


class BaseColumn:
    """A Base class which all Column types inherit.

    Attributes
    ----------
        name: :class:`str`
            The column's name.

    """

    if TYPE_CHECKING:
        name: str
        _selectable: type[Selectable]

    def inner_join(self, other: BaseColumn) -> type[Join]:
        """A shortcut for creating an INNER JOIN between two columns.

        Parameters
        ----------
        other: :class:`BaseColumn`
            The other column to join to.

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return self._selectable.inner_join(other._selectable, OnClause(self, other))

    def left_join(self, other: BaseColumn) -> type[Join]:
        """A shortcut for creating an LEFT JOIN between two columns.

        Parameters
        ----------
        other: :class:`BaseColumn`
            The other column to join to.

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return self._selectable.left_join(other._selectable, OnClause(self, other))

    def right_join(self, other: BaseColumn) -> type[Join]:
        """A shortcut for creating an RIGHT JOIN between two columns.

        Parameters
        ----------
        other: :class:`BaseColumn`
            The other column to join to.

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return self._selectable.right_join(other._selectable, OnClause(self, other))

    def full_outer_join(self, other: BaseColumn) -> type[Join]:
        """A shortcut for creating an FULL OUTER JOIN between two columns.

        Parameters
        ----------
        other: :class:`BaseColumn`
            The other column to join to.

        Returns
        -------
        :class:`Join`
            A representation of a join of which fetch methods can be applied to.
        """
        return self._selectable.full_outer_join(other._selectable, OnClause(self, other))


@dataclass
class Column(BaseColumn, Generic[_T]):
    """A representation of a SQL Database Table Column

    Parameters
    ----------
        primary_key: :class:`bool`
            Whether the column is a primary key column, defaults to ``False``.
        index: :class:`bool`
            Whether to create an index for the given column, defaults to ``False``.
        nullable: :class:`bool`
            Whether the column is nullable.
        unique: :class:`bool`
            Whether the column has a unique constraint.
        default: Any
            The default value of the column.
        references: Optional[:class:`Column`]
            The column which this column references.
        cascade: :class:`bool`
            Whether deletions / updates the referenced column should cascace. Defaults to ``False``.

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
            Whether deletions / updates the referenced column should cascace.
    """

    if TYPE_CHECKING:

        @property
        def name(self) -> str:
            ...

        @name.setter
        def name(self, value: str) -> None:
            ...

        @name.deleter
        def name(self) -> None:
            ...

        @property
        def table(self) -> type[Selectable]:
            ...

        @table.setter
        def table(self, value: type[Selectable]) -> None:
            ...

        @table.deleter
        def table(self) -> None:
            ...

        @property
        def _sql_type(self) -> type[SQLType[_T]]:
            ...

        @_sql_type.setter
        def _sql_type(self, value: type[SQLType[_T]]) -> None:
            ...

        @_sql_type.deleter
        def _sql_type(self) -> None:
            ...

    primary_key: bool = False
    index: bool = False
    nullable: bool = True
    unique: bool = False
    cascade: bool = False
    default: Optional[Union[str, _T]] = MISSING
    references: Optional[Column[_T]] = None

    @property
    def py_type(self) -> type[_T]:
        return self._sql_type.py_type

    @property
    def sql_type(self) -> type[SQLType[_T]]:
        return self._sql_type

    @property
    def _selectable(self) -> type[Selectable]:
        return self.table

    @query_builder
    def _query(self) -> list[str]:
        builder = []

        builder.append(self.name)

        builder.append(self.sql_type.sql_type)

        if not self.nullable:
            builder.append("NOT NULL")

        if self.default is not MISSING:
            builder.append("DEFAULT (")
            builder.append(str(self.default))
            builder.append(")")

        if self.references is not None:
            builder.append("REFERENCES")
            builder.append(self.references.table._name)
            builder.append("(")
            builder.append(self.references.name)
            builder.append(")")

            if self.cascade:
                builder.append("ON DELETE CASCADE ON UPDATE CASCADE")

        return builder

    def _copy(self: Column[_T]) -> Column[_T]:
        copy = self.__class__(
            primary_key=self.primary_key,
            index=self.index,
            nullable=self.nullable,
            unique=self.unique,
            cascade=self.cascade,
            default=self.default,
            references=self.references,
        )
        copy._sql_type = self._sql_type
        copy.name = self.name
        return copy

    @classmethod
    def _with_type(cls, type: type[SQLType[_T]], **options: Any) -> Column[_T]:
        column = cls(**options)
        column._sql_type = type
        return column

    @classmethod
    def create(cls, name: str, type: type[SQLType[_T]], **options: Any) -> Column[_T]:
        """A shortcut for creating a column with a given type.

        Parameters
        ----------
        name: :class:`str`
            The name of the column.
        type: type[:class:`SQLType`]
            The type of the column.
        """
        column = cls._with_type(type, **options)
        column.name = name
        return column


@dataclass
class JoinColumn(BaseColumn):
    """A representation of a column in an Join.

    Attributes
    ----------
        name: :class:`str`
            The column's name.
        join: :class:`~.Join`
            The Join the column is a part of.
    """

    if TYPE_CHECKING:

        @property
        def name(self) -> str:
            ...

        @name.setter
        def name(self, value: str) -> None:
            ...

        @name.deleter
        def name(self) -> None:
            ...

        @property
        def join(self) -> type[Join]:
            ...

        @join.setter
        def join(self, value: type[Join]) -> None:
            ...

        @join.deleter
        def join(self) -> None:
            ...

        @property
        def source(self) -> type[Selectable]:
            ...

        @source.setter
        def source(self, value: type[Selectable]) -> None:
            ...

        @source.deleter
        def source(self) -> None:
            ...

    @property
    def _selectable(self) -> type[Selectable]:
        return self.join


@dataclass
class ViewColumn(BaseColumn):
    """A representation of a column in an SQL View object.

    Parameters
    ----------
        select: :class:`str`
            the attribute to SELECT from the view QUERY for this column.
            Defaults to the column name.

    Attributes
    ----------
        name: :class:`str`
            The column's name.
        view: :class:`~.View`
            The View the column is a part of.
        select: :class:`str`
            the attribute to SELECT from the view QUERY for this column.

    """

    if TYPE_CHECKING:

        @property
        def name(self) -> str:
            ...

        @name.setter
        def name(self, value: str) -> None:
            ...

        @name.deleter
        def name(self) -> None:
            ...

        @property
        def view(self) -> type[View]:
            ...

        @view.setter
        def view(self, value: type[View]) -> None:
            ...

        @view.deleter
        def view(self) -> None:
            ...

    @property
    def _selectable(self) -> type[Selectable]:
        return self.view

    select: str = MISSING
