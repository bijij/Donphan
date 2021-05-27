import asyncio
from donphan import MaybeAcquire

from functools import wraps

from collections.abc import Callable
from typing import Any, Awaitable

import asyncpg

pool: asyncpg.Pool


def async_test(func: Callable[..., Awaitable[Any]]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any) -> Callable[..., Any]:
        return asyncio.get_event_loop().run_until_complete(func(self, *args, **kwargs))

    return wrapper


def set_pool(_pool: asyncpg.Pool):
    global pool
    pool = _pool


def get_pool() -> asyncpg.Pool:
    return pool


def with_pool(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any) -> Callable[..., Any]:
        pool = get_pool()
        return func(self, pool, *args, **kwargs)

    return wrapper


def with_connection(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(func)
    @with_pool
    async def wrapper(self, pool, *args: Any, **kwargs: Any) -> Callable[..., Awaitable[Any]]:
        async with MaybeAcquire(pool=pool) as connection:
            return await func(self, connection, *args, **kwargs)

    return wrapper
