from __future__ import annotations

import abc

from .consts import DEFAULT_SCHEMA


class ObjectMeta(abc.ABCMeta):
    def __new__(cls, name, bases, attrs, **kwargs):

        attrs.update({"_schema": kwargs.get("schema", DEFAULT_SCHEMA)})

        return super().__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key == "__name__":
            return f"{cls.__name__.lower()}"

        if key == "_name":
            return f"{cls._schema}.{cls.__name__.lower()}"

        raise AttributeError(f"'{cls.__name__}' has no attribute '{key}'")
