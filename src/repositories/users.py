from dataclasses import dataclass
from typing import Callable

import asyncpg


@dataclass(frozen=True)
class UsersRepository:
    pool_getter: Callable[[], asyncpg.Pool | None]

    async def create_user(self, seller_id: int, is_verified_seller: bool) -> None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'INSERT INTO users (id, is_verified_seller) VALUES ($1, $2) '
            'ON CONFLICT (id) DO UPDATE SET is_verified_seller = EXCLUDED.is_verified_seller'
        )
        async with pool.acquire() as conn:
            await conn.execute(query, seller_id, is_verified_seller)

    async def get_user_by_id(self, seller_id: int) -> dict | None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = 'SELECT id, is_verified_seller FROM users WHERE id = $1'
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, seller_id)

        if row is None:
            return None

        return {'seller_id': row['id'], 'is_verified_seller': row['is_verified_seller']}
