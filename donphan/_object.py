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

from typing import ClassVar, Protocol

from ._consts import DEFAULT_SCHEMA
from .utils import MISSING, normalise_name

__all__ = ("Object",)


class Object(Protocol):
    _schema: ClassVar[str]
    _name: ClassVar[str]

    def __init_subclass__(
        cls,
        *,
        _name: str = MISSING,
        schema: str = MISSING,
    ) -> None:
        if schema is MISSING:
            schema = DEFAULT_SCHEMA

        if _name is MISSING:
            _name = normalise_name(cls.__name__)

        cls._schema = schema
        cls._name = f"{schema}.{_name}"
        super().__init_subclass__()