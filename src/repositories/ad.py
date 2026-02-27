import asyncpg


class AdRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_by_id(self, ad_id: int) -> dict | None:
        row = await self._pool.fetchrow(
            'SELECT id, seller_id, name, description, category, images_qty FROM ads WHERE id = $1',
            ad_id,
        )
        if row is None:
            return None
        return dict(row)

    async def create(
        self, seller_id: int, name: str, description: str, category: int, images_qty: int
    ) -> dict:
        row = await self._pool.fetchrow(
            'INSERT INTO ads (seller_id, name, description, category, images_qty) '
            'VALUES ($1, $2, $3, $4, $5) '
            'RETURNING id, seller_id, name, description, category, images_qty',
            seller_id,
            name,
            description,
            category,
            images_qty,
        )
        return dict(row)
