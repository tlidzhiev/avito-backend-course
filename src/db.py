import os

import asyncpg


async def init_db_pool() -> asyncpg.Pool:
    dsn = os.getenv('DATABASE_URL', 'postgresql://avito:avito@localhost:5432/avito')
    pool = await asyncpg.create_pool(dsn)
    return pool


async def close_db_pool(pool: asyncpg.Pool) -> None:
    await pool.close()
