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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from ._column import BaseColumn, JoinColumn, OnClause
from ._consts import OPERATORS
from ._selectable import Selectable
from .utils import generate_alias, query_builder

JoinType = Literal["INNER", "LEFT", "RIGHT", "FULL OUTER"]

__all__ = ("Join",)


class Join(Selectable):
    """Base class for representations of SQL Database Tables.

    .. note::
        Subclasses of this class should never be created, instead
        join methods such as :meth:`Table.inner_join` should be used
        to create this model.

    Attributes
    ----------
        _name: :class:`str`
            The name of the table.
        _schema: :class:`str`
            The tables schema.
        _columns: Iterable[:class:`~.ViewColumn`]
            The columns contained in the table.
        _columns_dict: Dict[:class:`str`, :class:`~.ViewColumn`]
            A mapping between a column's name and itself.
        _query: :class:`str`
            The query used to create the view.
    """

    if TYPE_CHECKING:
        _columns: ClassVar[list[JoinColumn]]
        _columns_dict: ClassVar[dict[str, JoinColumn]]

    if TYPE_CHECKING:
        _type: ClassVar[JoinType]
        _alias: ClassVar[str]
        _selectables: tuple[type[Selectable], type[Selectable]]
        _aliases: dict[type[Selectable], str]
        _on: ClassVar[Iterable[OnClause]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._set_columns()
        cls._overridden_name = cls._build_select()

    @classmethod
    def set_column(cls, source: BaseColumn) -> None:
        column = JoinColumn()
        column.name = source.name
        column.join = cls
        column.source = source._selectable

        cls._columns_dict[column.name] = column
        setattr(cls, column.name, column)

    @classmethod
    def _set_columns(cls) -> None:
        a, b = cls._selectables

        for column in a._columns:
            cls.set_column(column)

        for column in b._columns:
            if column.name in cls._columns_dict:
                continue
            cls.set_column(column)

    @classmethod
    @query_builder
    def _build_select(cls) -> list[str]:
        builder = ["( SELECT"]

        a, b = cls._selectables

        # Generate aliases
        if issubclass(a, Join):
            alias_a = a._alias
        else:
            alias_a = generate_alias()

        # Generate aliases
        if issubclass(b, Join):
            alias_b = b._alias
        else:
            alias_b = generate_alias()

        cls._aliases = {
            a: alias_a,
            b: alias_b,
        }

        for column in cls._columns:
            alias = cls._aliases[column.source]

            builder.append(f"{alias}.{column.name}")
            builder.append("AS")
            builder.append(column.name)
            builder.append(",")

        builder.pop(-1)

        builder.append("FROM")

        builder.append(a._name)
        if not issubclass(a, Join):
            builder.append("AS")
            builder.append(alias_a)

        builder.append(cls._type)
        builder.append("JOIN")

        builder.append(b._name)
        if not issubclass(b, Join):
            builder.append("AS")
            builder.append(alias_b)

        builder.append("ON")

        for clause in cls._on:
            if len(clause) == 2:
                col_a, col_b = clause  # type: ignore
                operator = "eq"
            else:
                col_a, col_b, operator = clause

            builder.append(f"{alias_a}.{col_a.name}")

            builder.append(OPERATORS[operator])

            builder.append(f"{alias_b}.{col_b.name}")

            builder.append("AND")

        builder.pop(-1)

        builder.append(") AS")

        builder.append(cls._alias)

        return builder
