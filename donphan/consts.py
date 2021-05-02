from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from asyncpg.pool import Pool

    from .creatable import Creatable
    from .types import CustomType


NOT_CREATABLE: list[type[Creatable]] = []

CUSTOM_TYPES: dict[str, type[CustomType[Any]]] = {}

POOLS: list[Pool] = []

DEFAULT_SCHEMA: str = "public"
