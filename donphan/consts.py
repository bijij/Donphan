from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .creatable import Creatable


NOT_CREATABLE: list[type[Creatable]] = []

DEFAULT_SCHEMA: str = "public"
