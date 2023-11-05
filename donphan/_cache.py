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
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THEdefault
SOFTWARE.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from copy import deepcopy
from typing import TYPE_CHECKING, Any, NoReturn

from ._column import Column
from ._table import Table
from .utils import DOCS_BUILDING, LRUDict, not_creatable, with_lock

if TYPE_CHECKING:
    from asyncpg import Connection, Record


__all__ = ("CachedTable",)


@not_creatable
class CachedTable(Table):
    """Base class for creating representations of SQL Database tables that cache their data in memory.

    .. note::
        The cache is only used for the following methods:
            - :meth:`.fetch_row_where`
            - :meth:`.fetch_where`
            - :meth:`.fetch_value_where`
            - :meth:`.fetch_value`

    .. warning::
        The cache assumes that no other process is modifying the table.

    """

    _lock: asyncio.Lock
    _cache: dict[tuple[Any, ...], dict[str, Any]]

    def __init_subclass__(cls, max_cache_size: int | None = None, **kwargs: Any) -> None:
        cls._lock = asyncio.Lock()
        if max_cache_size is None:
            cls._cache = {}
        else:
            cls._cache = LRUDict(max_cache_size)

        super().__init_subclass__(**kwargs)

    @classmethod
    def _get_primary_key_values(
        cls,
        record: dict[str, Any],
    ) -> tuple[Any, ...]:
        return tuple(cls._get_primary_keys(record).values())

    @classmethod
    def _store_cached(cls, *args: Any, record: dict[str, Any]) -> None:
        cls._cache[args] = record

    @classmethod
    def _delete_cached(cls, *args: Any) -> None:
        cls._cache.pop(args, None)

    @classmethod
    def get_cached(cls, **kwargs: Any) -> dict[str, Any] | None:
        """Get a cached record from the table.

        .. note::
            The returned record is a copy of the cached record, so modifying it will not modify the cached record.

        """
        key = cls._get_primary_key_values(kwargs)
        record = cls._cache.get(key)

        if record is not None:
            return deepcopy(record)

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the cache."""
        cls._cache.clear()

    if not TYPE_CHECKING and not DOCS_BUILDING:

        @classmethod
        @with_lock
        async def fetch_where(cls, *args: Any, **kwargs: Any) -> Iterable[Record]:
            result = await super().fetch_where(*args, **kwargs)
            for record in result:
                cls._store_cached(*cls._get_primary_key_values(record), record=record)
            return result

        @classmethod
        @with_lock
        async def fetch_row_where(cls, *args: Any, **kwargs: Any) -> Record | None:
            cached_result = cls.get_cached(**kwargs)
            if cached_result is not None:
                return cached_result

            result = await super().fetch_row_where(*args, **kwargs)
            if result:
                cls._store_cached(*cls._get_primary_key_values(result), record=result)
            return result

        @classmethod
        @with_lock
        async def fetch_value_where(
            cls, connection: Connection, /, column: Column[Any] | str, *args: Any, **kwargs: Any
        ) -> Any:
            if isinstance(column, str):
                column = cls._columns_dict[column]

            cached_result = cls.get_cached(**kwargs)
            if cached_result is not None:
                return cached_result[column.name]

            result = await super().fetch_row_where(connection, *args, **kwargs)
            if result:
                cls._store_cached(*cls._get_primary_key_values(result), record=result)
                return result[column.name]

        @classmethod
        @with_lock
        async def insert(cls, *args: Any, **kwargs: Any) -> Record:
            kwargs["returning"] = "*"
            result = await super().insert(*args, **kwargs)
            cls._store_cached(*cls._get_primary_key_values(result), record=result)
            return result

        @classmethod
        @with_lock
        async def update_where(cls, *args: Any, **kwargs: Any) -> list[Record]:
            kwargs["returning"] = "*"
            result = await super().update_where(*args, **kwargs)
            for record in result:
                cls._store_cached(*cls._get_primary_key_values(record), record=record)
            return result

        @classmethod
        @with_lock
        async def delete_where(cls, *args: Any, **kwargs: Any) -> list[Record]:
            kwargs["returning"] = "*"
            result = await super().delete_where(*args, **kwargs)
            for record in result:
                cls._delete_cached(*cls._get_primary_key_values(record))
            return result

    @classmethod
    async def insert_many(cls, *args: Any, **kwargs: Any) -> NoReturn:
        """|coro|

        Not supported for cached tables.
        """
        raise NotImplementedError
