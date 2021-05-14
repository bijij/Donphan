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

import sys
import types
import string
from functools import wraps

from collections.abc import Callable, Iterable
from typing import Any, ForwardRef, Optional, Literal, Union, TypeVar, TYPE_CHECKING

from .consts import NOT_CREATABLE

if TYPE_CHECKING:
    from .creatable import Creatable


__all__ = (
    "MISSING",
    "normalise_name",
    "query_builder",
    "not_creatable",
    "resolve_annotation",
    "BUILDING_DOCS",
)


T = TypeVar("T")
CT = TypeVar("CT", bound="Creatable")


class _MissingSentinel:
    def __repr__(self):
        return "MISSING"


MISSING: Any = _MissingSentinel()


_normalisation = str.maketrans({u: f"_{l}" for u, l in zip(string.ascii_uppercase, string.ascii_lowercase)})


def normalise_name(name: str) -> str:
    return name[0].lower() + name[1:].translate(_normalisation)


def query_builder(func: Callable[..., list[Any]]) -> Callable[..., str]:
    @wraps(func)
    def wrapper(*args, **kwargs):
        return " ".join(str(e) for e in func(*args, **kwargs))

    return wrapper


def not_creatable(cls: type[CT]) -> type[CT]:
    NOT_CREATABLE.append(cls)
    return cls


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
):
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
            if PY_310 and tp.__class__ is types.Union:  # type: ignore
                converted = Union[args]  # type: ignore
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
    localns: Optional[dict[str, Any]],
    cache: Optional[dict[str, Any]],
) -> Any:
    if annotation is None:
        return type(None)
    if isinstance(annotation, str):
        annotation = ForwardRef(annotation)

    locals = globalns if localns is None else localns
    if cache is None:
        cache = {}
    return evaluate_annotation(annotation, globalns, locals, cache)


DOCS_BUILDING: bool
try:
    DOCS_BUILDING = __sphinx_building__  # type: ignore
except NameError:
    DOCS_BUILDING = False
