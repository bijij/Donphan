from __future__ import annotations
from donphan.utils import not_creatable, query_builder

from types import new_class
from typing import Any, Generic, TYPE_CHECKING, TypeVar

from .creatable import Creatable
from .enums import Enum

__all__ = (
    "SQLType",
    "CustomType",
)


T = TypeVar("T")
OT = TypeVar("OT")
ET = TypeVar("ET", bound=Enum)


class SQLType(Generic[T]):
    if TYPE_CHECKING:
        Integer = int
        Text = str

    py_type: type[T]
    __defaults: dict[type[Any], type[SQLType[Any]]] = {}
    __enum_types: dict[type[Any], type[SQLType[Any]]] = {}

    @classmethod
    def from_type(cls, type: type[OT]) -> type[SQLType[OT]]:
        if issubclass(type, Enum):
            if type in cls.__enum_types:
                return cls.__enum_types[type]

            enum_type = new_class(type.__name__, (EnumType[type],), {"default": False})
            cls.__enum_types[type] = enum_type
            return enum_type

        return cls.__defaults[type]

    def __init_subclass__(cls, *, default: bool = False) -> None:
        cls.py_type = cls.__orig_bases__[0].__args__[0]  # type: ignore
        if default:
            cls.__defaults[cls.py_type] = cls
        super().__init_subclass__()
        setattr(SQLType, cls.__name__, cls)


@not_creatable
class CustomType(SQLType[T], Creatable):
    ...


@not_creatable
class EnumType(CustomType[ET]):
    @classmethod
    @query_builder
    def _query_create(cls, if_not_exists: bool) -> list[str]:
        builder = ["CREATE TYPE"]
        builder.append(cls._name)
        builder.append("AS ENUM (")

        for key in cls.py_type:  # type: ignore
            builder.append(f"'{key.name}'")
            builder.append(",")

        builder.pop(-1)

        builder.append(")")
        return builder


if not TYPE_CHECKING:
    for name, (py_type, is_default) in {
        "Integer": (int, True),
        "Text": (str, True),
    }.items():
        new_class(name, (SQLType[py_type],), {"default": is_default})
        new_class(name + "[]", (SQLType[list[py_type]],), {"default": is_default})
