import os
import uuid

import pytest
from redis.asyncio import Redis

from src.repositories.cache import CacheRepository


@pytest.fixture
def anyio_backend():
    return 'asyncio'


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
async def test_cache_repository_roundtrip_with_real_redis():
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    if not await _redis_available(redis_url):
        pytest.skip(f'Redis is not available at {redis_url}')

    client = Redis.from_url(redis_url, decode_responses=True)
    suffix = uuid.uuid4().hex
    item_id = int(suffix[:8], 16)
    task_id = int(suffix[8:16], 16)

    repository = CacheRepository(lambda: client, ttl_seconds=120)
    try:
        await repository.set_simple_prediction(
            item_id=item_id,
            payload={'is_violation': True, 'probability': 0.91},
        )
        await repository.set_moderation_result(
            task_id=task_id,
            item_id=item_id,
            payload={
                'task_id': task_id,
                'status': 'completed',
                'is_violation': True,
                'probability': 0.91,
                'error_message': None,
            },
        )

        simple = await repository.get_simple_prediction(item_id=item_id)
        result = await repository.get_moderation_result(task_id=task_id)
        assert simple == {'is_violation': True, 'probability': 0.91}
        assert result['task_id'] == task_id
        assert result['status'] == 'completed'

        await repository.delete_item_predictions(item_id=item_id)
        assert await repository.get_simple_prediction(item_id=item_id) is None
        assert await repository.get_moderation_result(task_id=task_id) is None
    finally:
        await client.aclose()
