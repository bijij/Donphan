from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from .consts import DEFAULT_SCHEMA


class ObjectMeta(abc.ABCMeta):
    if TYPE_CHECKING:
        _schema: str

    def __new__(cls, name, bases, attrs, *, schema: str = DEFAULT_SCHEMA):

        attrs.update({"_schema": schema})

        return super().__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key == "__name__":
            return f"{cls.__name__.lower()}"

        if key == "_name":
            return f"{cls._schema}.{cls.__name__.lower()}"

        raise AttributeError(f"'{cls.__name__}' has no attribute '{key}'")
