import asyncio
from donphan.utils import decorator


@decorator
def async_test(func):
    def wrapper(*args, **kwargs):
        asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper
