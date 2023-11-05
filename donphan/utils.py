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

import asyncio
import io
import os
import string
import sys
import types
from collections.abc import Callable, Coroutine, Iterable
from functools import wraps
from typing import TYPE_CHECKING, Any, ForwardRef, Literal, Protocol, TextIO, TypeVar, Union, overload

from ._consts import NOT_CREATABLE

if TYPE_CHECKING:
    from types import TracebackType

    from asyncpg import Connection
    from asyncpg.transaction import Transaction
    from typing_extensions import Concatenate, ParamSpec

    from ._creatable import Creatable
    from ._object import Object

    P = ParamSpec("P")
else:
    P = TypeVar("P")

__all__ = ("not_creatable",)


T = TypeVar("T")
V = TypeVar("V")
T_co = TypeVar("T_co", covariant=True)
CT = TypeVar("CT", bound="Creatable")
OT = TypeVar("OT", bound="Object")


class WithLock(Protocol):
    _lock: asyncio.Lock


L = TypeVar("L", bound=WithLock)

BE = TypeVar("BE", bound=BaseException)

Coro = Coroutine[Any, Any, T]


class _MissingSentinel:
    def __repr__(self):
        return "MISSING"


MISSING: Any = _MissingSentinel()


_normalisation: dict[int, str] = str.maketrans(
    {u: f"_{l}" for u, l in zip(string.ascii_uppercase, string.ascii_lowercase)}
)


def normalise_name(name: str) -> str:
    return name[0].lower() + name[1:].translate(_normalisation)


def generate_alias() -> str:
    n = int.from_bytes(os.urandom(4), "little")
    alias = ""

    while n > 0:
        n, d = divmod(n, 26)
        alias += string.ascii_lowercase[d]

    return alias


def query_builder(func: Callable[P, list[Any]]) -> Callable[P, str]:
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> str:
        return " ".join(str(e) for e in func(*args, **kwargs))

    return wrapper


def not_creatable(cls: type[CT]) -> type[CT]:
    """Marks a type as non-creatable."""
    if cls not in NOT_CREATABLE:
        NOT_CREATABLE.append(cls)
    return cls


def write_to_file(fp: str | TextIO, data: str) -> TextIO:
    if isinstance(fp, io.TextIOBase):
        if not fp.writeable():  # type: ignore
            raise ValueError(f"File buffer {fp!r} must be writable")
    elif isinstance(fp, str):
        fp = open(fp, "w")
    else:
        raise TypeError(f"Could not determine type of file-like object.")

    fp.write(data)
    return fp  # type: ignore


class VoidContextManager:
    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type: type[BE], exc_val: BE, exc_tb: TracebackType) -> None:
        pass

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, exc_type: type[BE], exc_val: BE, exc_tb: TracebackType) -> None:
        pass


@overload
def optional_transaction(connection: Connection, use_transaction: Literal[True]) -> Transaction:
    ...


@overload
def optional_transaction(connection: Connection, use_transaction: Literal[False]) -> VoidContextManager:
    ...


@overload
def optional_transaction(connection: Connection, use_transaction: bool) -> VoidContextManager | Transaction:
    ...


def optional_transaction(connection: Connection, use_transaction: bool) -> VoidContextManager | Transaction:
    if use_transaction:
        return connection.transaction()
    return VoidContextManager()


PY_310 = sys.version_info >= (3, 10)


def flatten_literal_params(parameters: Iterable[Any]) -> tuple[Any, ...]:
    params = []
    literal_cls = type(Literal[0])
    for p in parameters:
        if isinstance(p, literal_cls):
            params.extend(p.__args__)
        else:
            params.append(p)
    return tuple(params)


def normalise_optional_params(parameters: Iterable[Any]) -> tuple[Any, ...]:
    none_cls = type(None)
    return tuple(p for p in parameters if p is not none_cls) + (none_cls,)


def evaluate_annotation(
    tp: Any,
    globals: dict[str, Any],
    locals: dict[str, Any],
    cache: dict[str, Any],
    *,
    implicit_str: bool = True,
) -> Any:
    if isinstance(tp, ForwardRef):
        tp = tp.__forward_arg__
        # ForwardRefs always evaluate their internals
        implicit_str = True

    if implicit_str and isinstance(tp, str):
        if tp in cache:
            return cache[tp]
        evaluated = eval(tp, globals, locals)
        cache[tp] = evaluated
        return evaluate_annotation(evaluated, globals, locals, cache)

    if hasattr(tp, "__args__"):
        implicit_str = True
        is_literal = False
        args = tp.__args__
        if not hasattr(tp, "__origin__"):
            if PY_310 and tp.__class__ is types.UnionType:
                converted = [args]
                return evaluate_annotation(converted, globals, locals, cache)

            return tp
        if tp.__origin__ is Union:
            try:
                if args.index(type(None)) != len(args) - 1:
                    args = normalise_optional_params(tp.__args__)
            except ValueError:
                pass
        if tp.__origin__ is Literal:
            if not PY_310:
                args = flatten_literal_params(tp.__args__)
            implicit_str = False
            is_literal = True

        evaluated_args = tuple(
            evaluate_annotation(arg, globals, locals, cache, implicit_str=implicit_str) for arg in args
        )

        if is_literal and not all(isinstance(x, (str, int, bool, type(None))) for x in evaluated_args):
            raise TypeError("Literal arguments must be of type str, int, bool, or NoneType.")

        if evaluated_args == args:
            return tp

        try:
            return tp.copy_with(evaluated_args)
        except AttributeError:
            return tp.__origin__[evaluated_args]

    return tp


def resolve_annotation(
    annotation: Any,
    globalns: dict[str, Any],
    localns: dict[str, Any] | None,
    cache: dict[str, Any] | None,
) -> Any:
    if annotation is None:
        return type(None)
    if isinstance(annotation, str):
        annotation = ForwardRef(annotation)

    locals = globalns if localns is None else localns
    if cache is None:
        cache = {}
    return evaluate_annotation(annotation, globalns, locals, cache)


def with_lock(func: Callable[Concatenate[L, P], Coro[T]]) -> Callable[Concatenate[L, P], Coro[T]]:
    @wraps(func)
    async def wrapper(cls: L, *args: P.args, **kwargs: P.kwargs) -> T:
        async with cls._lock:
            return await func(cls, *args, **kwargs)

    return wrapper


class LRUDict(dict[T, V]):
    def __init__(self, max_size: int = 1024, *args, **kwargs):
        if max_size <= 0:
            raise ValueError("Maximum cache size must be greater than 0.")
        self.max_size = max_size
        super().__init__(*args, **kwargs)
        self.__cleanup()

    def __cleanup(self):
        while len(self) > self.max_size:
            del self[next(iter(self))]

    def __getitem__(self, key: Any) -> Any:
        value = super().__getitem__(key)
        self.__cleanup()
        return value

    def __setitem__(self, key: Any, value: Any):
        super().__setitem__(key, value)
        self.__cleanup()


try:
    DOCS_BUILDING: Literal[True] = __sphinx_building__  # type: ignore
except NameError:
    DOCS_BUILDING: Literal[False] = False
