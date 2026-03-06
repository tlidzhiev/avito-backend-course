from dataclasses import dataclass
from typing import Callable

import asyncpg


@dataclass(frozen=True)
class ItemsRepository:
    pool_getter: Callable[[], asyncpg.Pool | None]

    async def create_item(
        self,
        item_id: int,
        seller_id: int,
        name: str,
        description: str,
        category: int,
        images_qty: int,
    ) -> None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'INSERT INTO advertisements (id, seller_id, name, description, category, images_qty, is_closed) '
            'VALUES ($1, $2, $3, $4, $5, $6, FALSE) '
            'ON CONFLICT (id) DO UPDATE SET '
            'seller_id = EXCLUDED.seller_id, '
            'name = EXCLUDED.name, '
            'description = EXCLUDED.description, '
            'category = EXCLUDED.category, '
            'images_qty = EXCLUDED.images_qty, '
            'is_closed = FALSE'
        )
        async with pool.acquire() as conn:
            await conn.execute(query, item_id, seller_id, name, description, category, images_qty)

    async def get_item_by_id(self, item_id: int) -> dict | None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'SELECT a.id AS item_id, a.seller_id, a.name, a.description, a.category, a.images_qty, '
            'u.is_verified_seller '
            'FROM advertisements a '
            'JOIN users u ON u.id = a.seller_id '
            'WHERE a.id = $1 AND a.is_closed = FALSE'
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, item_id)

        if row is None:
            return None

        return {
            'seller_id': row['seller_id'],
            'is_verified_seller': row['is_verified_seller'],
            'item_id': row['item_id'],
            'name': row['name'],
            'description': row['description'],
            'category': row['category'],
            'images_qty': row['images_qty'],
        }

    async def close_item(self, item_id: int) -> bool:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('DELETE FROM moderation_results WHERE item_id = $1', item_id)
                result = await conn.execute('DELETE FROM advertisements WHERE id = $1', item_id)

        return result.endswith('1')
