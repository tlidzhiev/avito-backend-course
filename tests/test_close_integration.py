import json
import os
import uuid

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from src.main import app


@pytest.fixture
def anyio_backend():
    return 'asyncio'


async def _create_pool(database_url: str) -> asyncpg.Pool | None:
    try:
        return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=1, timeout=2)
    except Exception:
        return None


async def _redis_available(redis_url: str) -> bool:
    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
        return True
    except Exception:
        return False
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_close_endpoint_removes_item_results_and_cache(monkeypatch):
    database_url = os.getenv('DATABASE_URL', 'postgresql://avito:avito@localhost:5432/avito')
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

    pool = await _create_pool(database_url)
    if pool is None:
        pytest.skip(f'PostgreSQL is not available at {database_url}')
    if not await _redis_available(redis_url):
        await pool.close()
        pytest.skip(f'Redis is not available at {redis_url}')

    redis_client = Redis.from_url(redis_url, decode_responses=True)

    suffix = uuid.uuid4().hex
    seller_id = int(suffix[:8], 16) % 1_000_000_000 + 1_000_000_000
    item_id = int(suffix[8:16], 16) % 1_000_000_000 + 1_000_000_000

    task_id = None
    simple_key = f'moderation:simple:item:{item_id}'
    task_ids_key = f'moderation:item:{item_id}:task_ids'

    async with pool.acquire() as conn:
        has_is_closed = await conn.fetchval(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'advertisements' AND column_name = 'is_closed'"
            ")"
        )
    if not has_is_closed:
        await redis_client.aclose()
        await pool.close()
        pytest.skip('Migration with advertisements.is_closed is not applied')

    try:
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO users (id, is_verified_seller) VALUES ($1, $2)',
                seller_id,
                True,
            )
            await conn.execute(
                (
                    'INSERT INTO advertisements (id, seller_id, name, description, '
                    'category, images_qty, is_closed) '
                    'VALUES ($1, $2, $3, $4, $5, $6, FALSE)'
                ),
                item_id,
                seller_id,
                'Item for close',
                'Item description',
                1,
                2,
            )
            task_id = await conn.fetchval(
                "INSERT INTO moderation_results (item_id, status, is_violation, probability) "
                "VALUES ($1, 'completed', $2, $3) "
                'RETURNING id',
                item_id,
                False,
                0.11,
            )

        task_key = f'moderation:task:{task_id}'
        await redis_client.set(
            simple_key,
            json.dumps({'is_violation': False, 'probability': 0.11}),
            ex=300,
        )
        await redis_client.set(
            task_key,
            json.dumps(
                {
                    'task_id': task_id,
                    'status': 'completed',
                    'is_violation': False,
                    'probability': 0.11,
                    'error_message': None,
                }
            ),
            ex=300,
        )
        await redis_client.sadd(task_ids_key, task_id)
        await redis_client.expire(task_ids_key, 300)

        monkeypatch.setenv('DATABASE_URL', database_url)
        monkeypatch.setenv('REDIS_URL', redis_url)
        monkeypatch.delenv('KAFKA_BOOTSTRAP_SERVERS', raising=False)

        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url='http://test') as client:
                response = await client.post('/moderation/close', params={'item_id': item_id})

        assert response.status_code == 200
        assert response.json() == {'item_id': item_id, 'status': 'closed'}

        async with pool.acquire() as conn:
            item_count = await conn.fetchval(
                'SELECT COUNT(*) FROM advertisements WHERE id = $1',
                item_id,
            )
            results_count = await conn.fetchval(
                'SELECT COUNT(*) FROM moderation_results WHERE item_id = $1',
                item_id,
            )
        assert item_count == 0
        assert results_count == 0

        assert await redis_client.get(simple_key) is None
        assert await redis_client.get(task_key) is None
        assert await redis_client.smembers(task_ids_key) == set()
    finally:
        if task_id is not None:
            task_key = f'moderation:task:{task_id}'
            await redis_client.delete(simple_key, task_ids_key, task_key)
        await redis_client.aclose()
        async with pool.acquire() as conn:
            await conn.execute('DELETE FROM moderation_results WHERE item_id = $1', item_id)
            await conn.execute('DELETE FROM advertisements WHERE id = $1', item_id)
            await conn.execute('DELETE FROM users WHERE id = $1', seller_id)
        await pool.close()
