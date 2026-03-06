import json
from dataclasses import dataclass
from typing import Callable

from redis.asyncio import Redis


@dataclass(frozen=True)
class CacheRepository:
    redis_getter: Callable[[], Redis | None]
    ttl_seconds: int = 300

    def _simple_key(self, item_id: int) -> str:
        return f'moderation:simple:item:{item_id}'

    def _task_key(self, task_id: int) -> str:
        return f'moderation:task:{task_id}'

    def _task_ids_by_item_key(self, item_id: int) -> str:
        return f'moderation:item:{item_id}:task_ids'

    async def get_simple_prediction(self, item_id: int) -> dict | None:
        redis_client = self.redis_getter()
        if redis_client is None:
            return None

        payload = await redis_client.get(self._simple_key(item_id))
        if payload is None:
            return None
        return json.loads(payload)

    async def set_simple_prediction(self, item_id: int, payload: dict) -> None:
        redis_client = self.redis_getter()
        if redis_client is None:
            return

        await redis_client.set(self._simple_key(item_id), json.dumps(payload), ex=self.ttl_seconds)

    async def get_moderation_result(self, task_id: int) -> dict | None:
        redis_client = self.redis_getter()
        if redis_client is None:
            return None

        payload = await redis_client.get(self._task_key(task_id))
        if payload is None:
            return None
        return json.loads(payload)

    async def set_moderation_result(self, task_id: int, item_id: int, payload: dict) -> None:
        redis_client = self.redis_getter()
        if redis_client is None:
            return

        task_key = self._task_key(task_id)
        task_ids_key = self._task_ids_by_item_key(item_id)

        async with redis_client.pipeline(transaction=True) as pipe:
            await (
                pipe.set(task_key, json.dumps(payload), ex=self.ttl_seconds)
                .sadd(task_ids_key, task_id)
                .expire(task_ids_key, self.ttl_seconds)
                .execute()
            )

    async def delete_item_predictions(self, item_id: int) -> None:
        redis_client = self.redis_getter()
        if redis_client is None:
            return

        task_ids_key = self._task_ids_by_item_key(item_id)
        task_ids = await redis_client.smembers(task_ids_key)

        sorted_task_ids = sorted(int(task_id) for task_id in task_ids)
        task_keys = [self._task_key(task_id) for task_id in sorted_task_ids]

        keys_to_delete = [self._simple_key(item_id), task_ids_key, *task_keys]
        if keys_to_delete:
            await redis_client.delete(*keys_to_delete)
