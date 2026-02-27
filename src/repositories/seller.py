import asyncpg


class SellerRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_by_id(self, seller_id: int) -> dict | None:
        row = await self._pool.fetchrow(
            'SELECT id, name, is_verified FROM sellers WHERE id = $1', seller_id
        )
        if row is None:
            return None
        return dict(row)

    async def create(self, name: str, is_verified: bool) -> dict:
        row = await self._pool.fetchrow(
            'INSERT INTO sellers (name, is_verified) VALUES ($1, $2) RETURNING id, name, is_verified',
            name,
            is_verified,
        )
        return dict(row)
