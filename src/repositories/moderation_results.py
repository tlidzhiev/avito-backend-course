from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable

import asyncpg


@dataclass(frozen=True)
class ModerationResultsRepository:
    pool_getter: Callable[[], asyncpg.Pool | None]

    async def create_pending(self, item_id: int) -> int:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'INSERT INTO moderation_results (item_id, status, created_at) '
            "VALUES ($1, 'pending', $2) "
            'RETURNING id'
        )
        async with pool.acquire() as conn:
            task_id = await conn.fetchval(query, item_id, datetime.now(UTC).replace(tzinfo=None))

        return int(task_id)

    async def get_by_id(self, task_id: int) -> dict | None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'SELECT id, item_id, status, is_violation, probability, '
            'error_message, created_at, processed_at '
            'FROM moderation_results '
            'WHERE id = $1'
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, task_id)

        if row is None:
            return None

        return {
            'task_id': row['id'],
            'item_id': row['item_id'],
            'status': row['status'],
            'is_violation': row['is_violation'],
            'probability': row['probability'],
            'error_message': row['error_message'],
            'created_at': row['created_at'],
            'processed_at': row['processed_at'],
        }

    async def get_pending_task_id_by_item(self, item_id: int) -> int | None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'SELECT id FROM moderation_results '
            "WHERE item_id = $1 AND status = 'pending' "
            'ORDER BY created_at ASC '
            'LIMIT 1'
        )
        async with pool.acquire() as conn:
            task_id = await conn.fetchval(query, item_id)

        return int(task_id) if task_id is not None else None

    async def update_completed(
        self, task_id: int, is_violation: bool, probability: float
    ) -> None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'UPDATE moderation_results '
            "SET status = 'completed', is_violation = $2, probability = $3, "
            'error_message = NULL, processed_at = $4 '
            'WHERE id = $1'
        )
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                task_id,
                is_violation,
                probability,
                datetime.now(UTC).replace(tzinfo=None),
            )

    async def update_failed(self, task_id: int, error_message: str) -> None:
        pool = self.pool_getter()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        query = (
            'UPDATE moderation_results '
            "SET status = 'failed', error_message = $2, processed_at = $3 "
            'WHERE id = $1'
        )
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                task_id,
                error_message,
                datetime.now(UTC).replace(tzinfo=None),
            )
