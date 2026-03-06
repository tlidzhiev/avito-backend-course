import logging
import os
from typing import Optional

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

_redis_client: Optional[Redis] = None


async def init_redis(redis_url: str | None = None) -> None:
    global _redis_client
    if _redis_client is not None:
        return

    url = redis_url or os.getenv('REDIS_URL')
    if not url:
        logger.warning('REDIS_URL is not set. Cache features are disabled.')
        return

    client = Redis.from_url(url, decode_responses=True)
    try:
        await client.ping()
        _redis_client = client
        logger.info('Redis client initialized')
    except Exception as exc:
        logger.warning('Redis initialization failed: %s', exc)
        await client.aclose()


async def close_redis() -> None:
    global _redis_client
    if _redis_client is None:
        return

    await _redis_client.aclose()
    _redis_client = None
    logger.info('Redis client closed')


def get_redis_client() -> Optional[Redis]:
    return _redis_client
