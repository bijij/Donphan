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
from donphan.column import ViewColumn

from typing import Any, ClassVar, TYPE_CHECKING

from .selectable import Selectable
from .utils import MISSING, not_creatable, query_builder


__all__ = ("View",)


@not_creatable
class View(Selectable):
    """Base class for creating representations of SQL Database Tables.

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
        _columns: ClassVar[list[ViewColumn]]
        _columns_dict: ClassVar[dict[str, ViewColumn]]

    _query: ClassVar[str]

    @classmethod
    def _setup_column(
        cls,
        name: str,
        type: Any,
        globals: dict[str, Any],
        locals: dict[str, Any],
        cache: dict[str, Any],
    ) -> None:
        if not hasattr(cls, name):
            column = ViewColumn()
        else:
            column = getattr(cls, name)
            if not isinstance(column, ViewColumn):
                raise ValueError("Column values must be an instance of ViewColumn.")

        column.name = name

        if column.select is MISSING:
            column.select = column.name

        cls._columns.append(column)
        cls._columns_dict[name] = column

    @classmethod
    @query_builder
    def _query_create(cls, if_not_exists: bool) -> list[str]:
        builder = ["CREATE"]

        if if_not_exists:
            builder.append("OR REPLACE")

        builder.append("VIEW")

        builder.append(cls._name)
        builder.append("(")

        for column in cls._columns:
            builder.append(column.name)
            builder.append(",")

        builder.pop(-1)
        builder.append(")")

        builder.append("AS SELECT")

        for column in cls._columns:
            builder.append(column.select)
            builder.append("AS")
            builder.append(column.name)
            builder.append(",")

        builder.pop(-1)

        builder.append(cls._query)

        return builder

    @classmethod
    def _query_drop(cls, if_exists: bool, cascade: bool) -> str:
        return super()._query_drop("VIEW", if_exists, cascade)
