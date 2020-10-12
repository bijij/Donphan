import asyncio

from functools import wraps


def async_test(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper
