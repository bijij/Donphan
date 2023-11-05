from collections.abc import Iterable

import asyncpg

from donphan import Column, OnClause, Table


class Users(Table):
    id: Column[int] = Column(primary_key=True)
    name: Column[str]


class Items(Table):
    id: Column[int] = Column(primary_key=True)
    name: Column[str]


class Inventories(Table):
    user_id: Column[int] = Column(primary_key=True, references=Users.id)
    item_id: Column[int] = Column(primary_key=True, references=Items.id)
    ammount: Column[int] = Column(default=1)


async def get_user_items(connection: asyncpg.Connection, user_id: int) -> Iterable[asyncpg.Record]:
    join = Users.id.inner_join(Inventories.user_id).inner_join(Items, OnClause(Inventories.item_id, Items.id))
    return await join.fetch(connection, user_id=user_id)
