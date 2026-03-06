import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from src.repositories.cache import CacheRepository


def test_cache_repository_get_simple_prediction_returns_payload():
    redis_client = MagicMock()
    redis_client.get = AsyncMock(
        return_value=json.dumps({'is_violation': True, 'probability': 0.77})
    )

    repository = CacheRepository(lambda: redis_client, ttl_seconds=300)
    payload = asyncio.run(repository.get_simple_prediction(item_id=100))

    assert payload == {'is_violation': True, 'probability': 0.77}
    redis_client.get.assert_awaited_once_with('moderation:simple:item:100')


def test_cache_repository_set_simple_prediction_uses_expected_key_and_ttl():
    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=True)

    repository = CacheRepository(lambda: redis_client, ttl_seconds=120)
    asyncio.run(
        repository.set_simple_prediction(
            item_id=100,
            payload={'is_violation': False, 'probability': 0.12},
        )
    )

    redis_client.set.assert_awaited_once_with(
        'moderation:simple:item:100',
        json.dumps({'is_violation': False, 'probability': 0.12}),
        ex=120,
    )


def test_cache_repository_get_moderation_result_returns_payload():
    redis_client = MagicMock()
    redis_client.get = AsyncMock(
        return_value=json.dumps(
            {
                'task_id': 7,
                'status': 'completed',
                'is_violation': True,
                'probability': 0.95,
                'error_message': None,
            }
        )
    )

    repository = CacheRepository(lambda: redis_client, ttl_seconds=300)
    payload = asyncio.run(repository.get_moderation_result(task_id=7))

    assert payload == {
        'task_id': 7,
        'status': 'completed',
        'is_violation': True,
        'probability': 0.95,
        'error_message': None,
    }
    redis_client.get.assert_awaited_once_with('moderation:task:7')


def test_cache_repository_set_moderation_result_uses_pipeline_with_expected_keys():
    pipe = MagicMock()
    pipe.set.return_value = pipe
    pipe.sadd.return_value = pipe
    pipe.expire.return_value = pipe
    pipe.execute = AsyncMock(return_value=[True, 1, True])

    pipe_ctx = MagicMock()
    pipe_ctx.__aenter__ = AsyncMock(return_value=pipe)
    pipe_ctx.__aexit__ = AsyncMock(return_value=False)

    redis_client = MagicMock()
    redis_client.pipeline.return_value = pipe_ctx

    repository = CacheRepository(lambda: redis_client, ttl_seconds=300)
    asyncio.run(
        repository.set_moderation_result(
            task_id=7,
            item_id=100,
            payload={'task_id': 7, 'status': 'completed'},
        )
    )

    pipe.set.assert_called_once_with(
        'moderation:task:7',
        json.dumps({'task_id': 7, 'status': 'completed'}),
        ex=300,
    )
    pipe.sadd.assert_called_once_with('moderation:item:100:task_ids', 7)
    pipe.expire.assert_called_once_with('moderation:item:100:task_ids', 300)
    pipe.execute.assert_awaited_once()


def test_cache_repository_delete_item_predictions_deletes_all_related_keys():
    redis_client = MagicMock()
    redis_client.smembers = AsyncMock(return_value={'1', '2'})
    redis_client.delete = AsyncMock(return_value=4)

    repository = CacheRepository(lambda: redis_client, ttl_seconds=300)
    asyncio.run(repository.delete_item_predictions(item_id=100))

    redis_client.smembers.assert_awaited_once_with('moderation:item:100:task_ids')
    redis_client.delete.assert_awaited_once_with(
        'moderation:simple:item:100',
        'moderation:item:100:task_ids',
        'moderation:task:1',
        'moderation:task:2',
    )
