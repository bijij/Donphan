import asyncio

from functools import wraps

from collections.abc import Callable
from typing import Any, Awaitable


def async_test(func: Callable[..., Awaitable[Any]]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Callable[..., Any]:
        return asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper
