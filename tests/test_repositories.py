import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repositories.items import ItemsRepository
from src.repositories.users import UsersRepository


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock()

    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = conn
    pool.acquire.return_value = acquire_ctx

    return pool, conn


@pytest.fixture
def mock_pool_getter(mock_pool):
    pool, conn = mock_pool
    return lambda: pool


def test_create_user(mock_pool, mock_pool_getter):
    pool, conn = mock_pool

    repo = UsersRepository(pool_getter=mock_pool_getter)
    asyncio.run(repo.create_user(seller_id=1, is_verified_seller=True))

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert 'INSERT INTO users' in call_args[0][0]
    assert call_args[0][1] == 1
    assert call_args[0][2] is True


def test_get_user_by_id(mock_pool, mock_pool_getter):
    pool, conn = mock_pool
    conn.fetchrow.return_value = {'id': 1, 'is_verified_seller': False}

    repo = UsersRepository(pool_getter=mock_pool_getter)
    result = asyncio.run(repo.get_user_by_id(1))

    assert result == {'seller_id': 1, 'is_verified_seller': False}
    conn.fetchrow.assert_called_once()
    call_args = conn.fetchrow.call_args
    assert 'SELECT' in call_args[0][0]
    assert call_args[0][1] == 1


def test_get_user_by_id_not_found(mock_pool, mock_pool_getter):
    pool, conn = mock_pool
    conn.fetchrow.return_value = None

    repo = UsersRepository(pool_getter=mock_pool_getter)
    result = asyncio.run(repo.get_user_by_id(999))

    assert result is None


def test_create_item(mock_pool, mock_pool_getter):
    pool, conn = mock_pool

    repo = ItemsRepository(pool_getter=mock_pool_getter)
    asyncio.run(
        repo.create_item(
            item_id=1,
            seller_id=10,
            name='Test Ad',
            description='Description',
            category=5,
            images_qty=3,
        )
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert 'INSERT INTO advertisements' in call_args[0][0]
    assert call_args[0][1] == 1
    assert call_args[0][2] == 10
    assert call_args[0][3] == 'Test Ad'
    assert call_args[0][4] == 'Description'
    assert call_args[0][5] == 5
    assert call_args[0][6] == 3


def test_get_item_by_id(mock_pool, mock_pool_getter):
    pool, conn = mock_pool
    conn.fetchrow.return_value = {
        'seller_id': 10,
        'is_verified_seller': True,
        'item_id': 1,
        'name': 'Test Ad',
        'description': 'Description',
        'category': 5,
        'images_qty': 3,
    }

    repo = ItemsRepository(pool_getter=mock_pool_getter)
    result = asyncio.run(repo.get_item_by_id(1))

    assert result == {
        'seller_id': 10,
        'is_verified_seller': True,
        'item_id': 1,
        'name': 'Test Ad',
        'description': 'Description',
        'category': 5,
        'images_qty': 3,
    }
    conn.fetchrow.assert_called_once()
    call_args = conn.fetchrow.call_args
    assert 'SELECT' in call_args[0][0]
    assert call_args[0][1] == 1


def test_get_item_by_id_not_found(mock_pool, mock_pool_getter):
    pool, conn = mock_pool
    conn.fetchrow.return_value = None

    repo = ItemsRepository(pool_getter=mock_pool_getter)
    result = asyncio.run(repo.get_item_by_id(999))

    assert result is None
