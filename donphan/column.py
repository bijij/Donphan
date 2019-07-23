from typing import Any

from .sqltype import SQLType


class Column:
    """SQL Table Column"""
    name = None
    type = None
    table = None
    is_array = False

    def __init__(self, *, index: bool = False, primary_key: bool = False,
                 unique: bool = False, auto_increment: bool = False,
                 nullable: bool = True, default: Any = NotImplemented,
                 references: "Column" = None):
        """Sets Database Table Column Properties.

        Kwargs:
            index (bool, optional): Create an index for this column
            primary_key (bool, optional): Sets this column to be a primary key
            unique (bool, optional): Sets the `UNIQUE` constraint
            auto_increment (bool, optional): Sets this column to `AUTO INCREMENT`
            nullable (bool, optional): Sets the `NOT NULL` constraint
            default (Any, optional): Sets the `DEFAULT` value of a column.
                Value can be either a pythonic value or a SQL QUERY
            references (Column, optional): Sets the `FOREIGN KEY` constraint

        """
        self.index = index
        self.primary_key = primary_key
        self.unique = unique
        self.auto_increment = auto_increment
        self.nullable = nullable
        self.default = default
        self.references = references

    def _update(self, table: 'Table', name: str, type: SQLType, is_array: bool):
        """Sets Additional Column Properties known after table creation."""
        self.table = table
        self.name = name
        self.type = type
        self.is_array = is_array

        # Validate column properties
        if self.references:
            if self.type != self.references.type:
                raise AttributeError(
                    f'Column {self} does not match types with referenced column; expected: {self.references.type}, recieved: {self.type}')

    def __repr__(self) -> str:
        return f'<Column "{self}" >'

    def __str__(self) -> str:
        builder = []

        builder.append(f"{self.name}")
        builder.append(self.type.sql)

        if self.is_array:
            builder.append('[]' * self.is_array)

        if not self.nullable:
            builder.append('NOT NULL')

        if self.unique:
            builder.append('UNOQUE')

        if self.default is not NotImplemented:
            builder.append('DEFAULT')
            if isinstance(self.default, str) and self.type == SQLType.Text():
                builder.append(f'\'{self.default}\'')
            elif isinstance(self.default, bool) and self.type == SQLType.Boolean():
                builder.append(str(self.default).upper())
            else:
                builder.append(f'({self.default})')

        if self.auto_increment:
            builder.append('AUTO INCREMENT')

        if self.references is not None:
            builder.append('REFERENCES')
            builder.append(
                f'{self.references.table._name}({self.references.name})')

        return " ".join(builder)
