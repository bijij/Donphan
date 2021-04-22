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

from json import dumps

from typing import (
    Any,
    cast,
    TYPE_CHECKING,
    Optional,
    Type,
    TypeVar,
)

from .sqltype import BaseSQLType, _SQLType, SQLType

if TYPE_CHECKING:
    from .table import Table


__all__ = ("Column",)


T = TypeVar("T", bound="_Column")


class _Column(BaseSQLType):  # subclasses SQLType to appease type checkers
    """Sets Database Table Column Properties.

    Args:
        index (bool, optional): Create an index for this column
        primary_key (bool, optional): Sets this column to be a primary key
        unique (bool, optional): Sets the `UNIQUE` constraint
        auto_increment (bool, optional): Sets this column to `AUTO INCREMENT`
        nullable (bool, optional): Sets the `NOT NULL` constraint
        default (Any, optional): Sets the `DEFAULT` value of a column.
            Value can be either a pythonic value or a SQL QUERY
        references (Column, optional): Sets the `FOREIGN KEY` constraint.
    """

    def __init__(
        self,
        *,
        index: bool = False,
        primary_key: bool = False,
        unique: bool = False,
        auto_increment: bool = False,
        nullable: bool = True,
        default: Any = NotImplemented,
        references: "_Column" = None,
    ) -> None:
        self.index = index
        self.primary_key = primary_key
        self.unique = unique
        self.auto_increment = auto_increment
        self.nullable = nullable
        self.default = default
        self.references = references

    def _update(self: T, table: Table, name: str, sqltype: Type[_SQLType], is_array: bool) -> T:
        self.table = table
        self.name = name
        self.type = sqltype
        self.is_array = is_array

        # Validate column properties
        if (
            self.references
            and self.type != self.references.type
            and (not self.references.auto_increment or self.type._python != int)
        ):
            raise AttributeError(
                f"Column {self} does not match types with referenced column; expected: {self.references.type}, received: {self.type}"
            )

        if self.auto_increment:
            if self.type._python == int:
                self.type = cast(Type[_SQLType], SQLType.Serial)
            else:
                raise TypeError(
                    f"Column {self} is auto_increment and must have a supporting type; expected: {SQLType.Serial}, received: {self.type}"
                )

        return self

    def __str__(self) -> str:
        builder = [f"{self.name}", self.type._sql]

        if self.is_array:
            builder.append("[]" * self.is_array)

        if not self.nullable:
            builder.append("NOT NULL")

        if self.unique:
            builder.append("UNIQUE")

        if self.default is not NotImplemented:
            builder.append("DEFAULT")
            if isinstance(self.default, str) and self.type == SQLType.Text:
                builder.append(f"'{self.default}'")
            elif isinstance(self.default, bool) and self.type == SQLType.Boolean:
                builder.append(str(self.default).upper())
            elif isinstance(self.default, dict) and self.type == SQLType.JSONB:
                builder.append(f"'{dumps(self.default)}'::jsonb")
            else:
                builder.append(f"({self.default})")

        if self.references is not None:
            builder.append("REFERENCES")
            builder.append(f"{self.references.table._name}({self.references.name})")

        return " ".join(builder)


def Column(
    *,
    index: bool = False,
    primary_key: bool = False,
    unique: bool = False,
    auto_increment: bool = False,
    nullable: bool = True,
    default: Any = NotImplemented,
    references: Optional[_Column] = None,
) -> Any:
    """Sets Database Table Column Properties.

    Args:
        index (bool, optional): Create an index for this column
        primary_key (bool, optional): Sets this column to be a primary key
        unique (bool, optional): Sets the `UNIQUE` constraint
        auto_increment (bool, optional): Sets this column to `AUTO INCREMENT`
        nullable (bool, optional): Sets the `NOT NULL` constraint
        default (Any, optional): Sets the `DEFAULT` value of a column.
            Value can be either a pythonic value or a SQL QUERY
        references (Column, optional): Sets the `FOREIGN KEY` constraint.
    """
    return _Column(
        index=index,
        primary_key=primary_key,
        unique=unique,
        auto_increment=auto_increment,
        nullable=nullable,
        default=default,
        references=references,
    )
