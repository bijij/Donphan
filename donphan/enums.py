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

---

This project bundles code from https://github.com/Rapptz/discord.py 
which is available under an MIT license.


The MIT License (MIT)

Copyright (c) 2015-present Rapptz

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

"""

from __future__ import annotations

import types

from typing import Any, NamedTuple, TYPE_CHECKING, TypeVar

__all__ = ("Enum",)


class _EnumValue(NamedTuple):
    name: str
    value: Any


if TYPE_CHECKING:
    from enum import Enum as _Enum

    class Enum(_Enum):
        @classmethod
        def try_value(cls: type[ET], value: Any) -> ET:
            ...


else:

    def _create_value_cls(name):
        cls = types.new_class(f"_EnumValue_{name}", (_EnumValue,))
        cls.__repr__ = lambda self: f"<{name}.{self.name}: {self.value!r}>"
        cls.__str__ = lambda self: f"{name}.{self.name}"
        return cls

    def _is_descriptor(obj):
        return hasattr(obj, "__get__") or hasattr(obj, "__set__") or hasattr(obj, "__delete__")

    class EnumMeta(type):
        def __new__(cls, name, bases, attrs):
            value_mapping = {}
            member_mapping = {}
            member_names = []

            value_cls = _create_value_cls(name)
            for key, value in list(attrs.items()):
                is_descriptor = _is_descriptor(value)
                if key[0] == "_" and not is_descriptor:
                    continue

                # Special case classmethod to just pass through
                if isinstance(value, classmethod):
                    continue

                if is_descriptor:
                    setattr(value_cls, key, value)
                    del attrs[key]
                    continue

                try:
                    new_value = value_mapping[value]
                except KeyError:
                    new_value = value_cls(name=key, value=value)
                    value_mapping[value] = new_value
                    member_names.append(key)

                member_mapping[key] = new_value
                attrs[key] = new_value

            attrs["_enum_value_map_"] = value_mapping
            attrs["_enum_member_map_"] = member_mapping
            attrs["_enum_member_names_"] = member_names
            attrs["_enum_value_cls_"] = value_cls
            actual_cls = super().__new__(cls, name, bases, attrs)
            value_cls._actual_enum_cls_ = actual_cls
            return actual_cls

        def __iter__(cls):
            return (cls._enum_member_map_[name] for name in cls._enum_member_names_)

        def __reversed__(cls):
            return (cls._enum_member_map_[name] for name in reversed(cls._enum_member_names_))

        def __len__(cls):
            return len(cls._enum_member_names_)

        def __repr__(cls):
            return f"<enum {cls.__name__}>"

        @property
        def __members__(cls):
            return types.MappingProxyType(cls._enum_member_map_)

        def __call__(cls, value):
            try:
                return cls._enum_value_map_[value]
            except (KeyError, TypeError):
                raise ValueError(f"{value!r} is not a valid {cls.__name__}")

        def __getitem__(cls, key):
            return cls._enum_member_map_[key]

        def __setattr__(cls, name, value):
            raise TypeError("Enums are immutable.")

        def __delattr__(cls, attr):
            raise TypeError("Enums are immutable")

        def __instancecheck__(self, instance):
            # isinstance(x, Y)
            # -> __instancecheck__(Y, x)
            try:
                return instance._actual_enum_cls_ is self
            except AttributeError:
                return False

    class Enum(metaclass=EnumMeta):
        @classmethod
        def try_value(cls, value):
            try:
                return cls._enum_value_map_[value]
            except (KeyError, TypeError):
                return value


ET = TypeVar("ET", bound=Enum)


def create_unknown_value(cls: type[ET], val: Any) -> ET:
    value_cls = cls._enum_value_cls_  # type: ignore
    name = f"unknown_{val}"
    return value_cls(name=name, value=val)


def try_enum(cls: type[ET], val: Any) -> ET:
    """A function that tries to turn the value into enum ``cls``.
    If it fails it returns a proxy invalid value instead.
    """

    try:
        return cls._enum_value_map_[val]  # type: ignore
    except (KeyError, TypeError, AttributeError):
        return create_unknown_value(cls, val)
