import logging
import os
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

_db_pool: Optional[asyncpg.Pool] = None


async def init_db(database_url: str | None = None) -> None:
    global _db_pool
    if _db_pool is not None:
        return

    db_url = database_url or os.getenv('DATABASE_URL')
    if not db_url:
        logger.warning('DATABASE_URL is not set. DB features are disabled.')
        return

    try:
        _db_pool = await asyncpg.create_pool(dsn=db_url, timeout=2)
        logger.info('Database pool initialized')
    except Exception as exc:
        logger.warning('Database pool initialization failed: %s', exc)
        _db_pool = None


async def close_db() -> None:
    global _db_pool
    if _db_pool is None:
        return

    await _db_pool.close()
    _db_pool = None
    logger.info('Database pool closed')


def get_db_pool() -> Optional[asyncpg.Pool]:
    return _db_pool
