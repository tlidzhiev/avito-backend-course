import os
import time

import asyncpg
import pytest

from src.repositories.items import ItemsRepository
from src.repositories.moderation_results import ModerationResultsRepository
from src.repositories.users import UsersRepository


@pytest.fixture
def anyio_backend():
    return 'asyncio'


async def _create_pool(database_url: str) -> asyncpg.Pool | None:
    try:
        return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=1, timeout=2)
    except Exception:
        return None


@pytest.mark.integration
@pytest.mark.anyio
async def test_repositories_work_with_real_postgres():
    database_url = os.getenv('DATABASE_URL', 'postgresql://avito:avito@localhost:5432/avito')
    pool = await _create_pool(database_url)
    if pool is None:
        pytest.skip(f'PostgreSQL is not available at {database_url}')

    suffix = int(time.time() * 1000)
    seller_id = 900000000 + suffix % 1000000
    item_id = 800000000 + suffix % 1000000

    users_repo = UsersRepository(lambda: pool)
    items_repo = ItemsRepository(lambda: pool)
    moderation_repo = ModerationResultsRepository(lambda: pool)

    async with pool.acquire() as conn:
        has_is_closed = await conn.fetchval(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'advertisements' AND column_name = 'is_closed'"
            ")"
        )
    if not has_is_closed:
        await pool.close()
        pytest.skip('Migration with advertisements.is_closed is not applied')

    try:
        await users_repo.create_user(seller_id=seller_id, is_verified_seller=True)
        await items_repo.create_item(
            item_id=item_id,
            seller_id=seller_id,
            name='Item',
            description='Desc',
            category=1,
            images_qty=1,
        )

        task_id = await moderation_repo.create_pending(item_id=item_id)
        await moderation_repo.update_completed(task_id=task_id, is_violation=False, probability=0.2)

        item = await items_repo.get_item_by_id(item_id)
        assert item is not None
        assert item['item_id'] == item_id

        assert await items_repo.close_item(item_id=item_id) is True
        assert await items_repo.get_item_by_id(item_id) is None

        await moderation_repo.delete_by_item(item_id=item_id)
        assert await moderation_repo.get_by_id(task_id=task_id) is None
    finally:
        async with pool.acquire() as conn:
            await conn.execute('DELETE FROM moderation_results WHERE item_id = $1', item_id)
            await conn.execute('DELETE FROM advertisements WHERE id = $1', item_id)
            await conn.execute('DELETE FROM users WHERE id = $1', seller_id)
        await pool.close()
