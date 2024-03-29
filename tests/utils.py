from __future__ import annotations

import asyncio
from donphan import MaybeAcquire

from functools import wraps

from collections.abc import Callable
from typing import Any, Awaitable, Coroutine, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Concatenate, ParamSpec

    from asyncpg import Connection, Pool

    P = ParamSpec("P")


T = TypeVar("T")
U = TypeVar("U")

Coro = Coroutine[Any, Any, T]


pool: Pool


def async_test(func: Callable[..., Awaitable[Any]]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any) -> Callable[..., Any]:
        return asyncio.get_event_loop().run_until_complete(func(self, *args, **kwargs))

    return wrapper


def set_pool(_pool: Pool):
    global pool
    pool = _pool


def get_pool() -> Pool:
    return pool


def with_pool(func: Callable[Concatenate[U, Pool, P], Coro[T]]) -> Callable[Concatenate[U, P], Coro[T]]:
    @wraps(func)
    def wrapper(self, *args: P.args, **kwargs: P.kwargs) -> Coro[T]:
        pool = get_pool()
        return func(self, pool, *args, **kwargs)

    return wrapper


def with_connection(func: Callable[Concatenate[U, Connection, P], Coro[T]]) -> Callable[Concatenate[U, P], Coro[T]]:
    @wraps(func)
    @with_pool
    async def wrapper(self: U, pool: Pool, *args: P.args, **kwargs: P.kwargs) -> T:
        async with MaybeAcquire(pool=pool) as connection:
            return await func(self, connection, *args, **kwargs)

    return wrapper
