from __future__ import annotations

from collections.abc import Iterable

from typing import Any, cast, Optional, TYPE_CHECKING, Union, overload

from .column import Column
from .selectable import Selectable
from .utils import not_creatable, query_builder

if TYPE_CHECKING:
    from asyncpg import Connection, Record  # type: ignore


__all__ = ("Insertable",)


@not_creatable
class Insertable(Selectable):
    # region query generation

    @classmethod
    def _get_columns(
        cls,
        values: dict[str, Any],
    ) -> Iterable[Column]:
        return [cls._columns_dict[column] for column in values]

    @classmethod
    def _get_primary_keys(
        cls,
        record: Record,
    ) -> dict[str, Any]:
        return {column.name: record[column.name] for column in cls._primary_keys}

    @classmethod
    @query_builder
    def _build_query_insert(
        cls,
        columns: Iterable[Column],
        ignore_on_conflict: bool,
        update_on_conflict: Iterable[Column],
        returning: Iterable[Column],
    ) -> list[str]:
        builder = [f"INSERT INTO {cls._name} ("]

        for column in columns:
            builder.append(column.name)
            builder.append(",")

        builder.pop(-1)

        builder.append(") VALUES (")

        for i, _ in enumerate(columns, 1):
            builder.append(f"${i}")
            builder.append(",")

        builder.pop(-1)

        builder.append(")")

        if ignore_on_conflict and update_on_conflict:
            raise ValueError("")

        elif ignore_on_conflict:
            builder.append("ON CONFLICT DO NOTHING")

        elif update_on_conflict:
            builder.append("ON CONFLICT (")

            for column in cls._primary_keys:
                builder.append(column.name)
                builder.append(",")

            builder.pop(-1)

            builder.append(") DO UPDATE SET")

            for column in update_on_conflict:
                builder.append(f"{column.name} = EXCLUDED.{column.name}")
                builder.append(",")

            builder.pop(-1)

        if returning:
            builder.append("RETURNING")

            for column in returning:
                builder.append(column.name)
                builder.append(",")

            builder.pop(-1)

        return builder

    @classmethod
    @query_builder
    def _build_query_update(
        cls,
        where: str,
        offset: int,
        columns: Iterable[Column],
    ) -> list[str]:
        builder = [f"UPDATE", cls._name, "SET"]

        for i, column in enumerate(columns, offset):
            builder.append(column.name)
            builder.append(f"= ${i}")
            builder.append(",")

        builder.pop(-1)

        builder.append("WHERE")
        builder.append(where)

        return builder

    @classmethod
    @query_builder
    def _build_query_delete(
        cls,
        where: str,
    ) -> list[str]:
        builder = ["DELETE FROM", cls._name]
        if where:
            builder.append("WHERE")
            builder.append(where)
        return builder

    # endregion

    # region public methods

    @overload
    @classmethod
    async def insert(
        cls,
        connection: Connection,
        *,
        ignore_on_conflict: bool = ...,
        update_on_conflict: Optional[Iterable[Column]] = ...,
        returning: Iterable[Column],
        **values: Any,
    ) -> Record:
        ...

    @overload
    @classmethod
    async def insert(
        cls,
        connection: Connection,
        *,
        ignore_on_conflict: bool = ...,
        update_on_conflict: Optional[Iterable[Column]] = ...,
        returning: None,
        **values: Any,
    ) -> None:
        ...

    @classmethod
    async def insert(
        cls,
        connection: Connection,
        *,
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Iterable[Column]] = None,
        returning: Optional[Iterable[Column]],
        **values: Any,
    ) -> Optional[Record]:
        columns = cls._get_columns(values)
        query = cls._build_query_insert(columns, ignore_on_conflict, update_on_conflict, returning or [])
        if returning is not None:
            return await connection.fetchrow(query, *values.values())
        await connection.execute(query, *values.values())

    @overload
    @classmethod
    async def insert_many(
        cls,
        connection: Connection,
        columns: Iterable[Column],
        *values: Iterable[Any],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Iterable[Column]] = None,
    ) -> None:
        ...

    @overload
    @classmethod
    async def insert_many(
        cls,
        connection: Connection,
        columns: None,
        *values: dict[str, Any],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Iterable[Column]] = None,
    ) -> None:
        ...

    @classmethod
    async def insert_many(
        cls,
        connection: Connection,
        columns: Optional[Iterable[Column]],
        *values: Union[Iterable[Any], dict[str, Any]],
        ignore_on_conflict: bool = False,
        update_on_conflict: Optional[Iterable[Column]] = None,
    ) -> None:
        if columns is None:
            values = cast(tuple[dict[str, Any], ...], values)
            columns = cls._get_columns(values[0])
            values = cast(tuple[Iterable[Any]], (value.values() for value in values))

        query = cls._build_query_insert(columns, ignore_on_conflict, update_on_conflict or [])
        await connection.executemany(query, values)

    @classmethod
    async def update_where(
        cls,
        connection: Connection,
        where: str,
        *values: Any,
        **_values: Any,
    ) -> None:
        columns = cls._get_columns(_values)
        query = cls._build_query_update(where, len(_values) + 1, columns)
        await connection.execute(query, *values, *_values.values())

    @classmethod
    async def update_record(
        cls,
        connection: Connection,
        record: Record,
        **values: Any,
    ) -> None:
        primary_keys = cls._get_primary_keys(record)
        where = cls._build_where_clause(primary_keys)
        return await cls.update_where(connection, where, *primary_keys.values(), **values)

    @classmethod
    async def delete_where(
        cls,
        connection: Connection,
        where: str,
        *values: Any,
    ) -> None:
        query = cls._build_query_delete(where)
        await connection.execute(query, *values)

    @classmethod
    async def delete(
        cls,
        connection: Connection,
        **values: Any,
    ) -> None:
        where = cls._build_where_clause(values)
        return await cls.delete_where(connection, where, *values.values())

    @classmethod
    async def delete_record(
        cls,
        connection: Connection,
        record: Record,
    ) -> None:
        primary_keys = cls._get_primary_keys(record)
        where = cls._build_where_clause(primary_keys)
        return await cls.delete_where(connection, where, *primary_keys.values())

    # endregion
