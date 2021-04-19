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

from typing import Any, Callable, Optional, TypeVar, Union

T = TypeVar("T", bound=Callable[..., Any])


def _process_decorator(deco):
    def _process_wrapped(*args, **kwargs):
        def wrap(wrapped):
            return deco(wrapped, *args, **kwargs)

        if args and isinstance(args[0], Callable):
            # decorator was called without parens.
            wrapped, *args = args
            return wrap(wrapped)

        # decorator was called with parens.
        return wrap

    return _process_wrapped


def decorator(deco: Optional[T] = None) -> Union[Callable[[Any], Any], T]:
    """A helper function that simplifies handling arguments in decorators.

    Parameters
    ----------
    deco: Callable[..., Any]
        A decorator function to wrap. Note however decorators wrapped with this
        function cannot accept a callable as their first argument.
    """

    if deco is None:
        # We were called with parens.
        return _process_decorator

    # We were called witout parens.
    return _process_decorator(deco)
