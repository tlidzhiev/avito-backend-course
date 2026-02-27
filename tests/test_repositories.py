import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repositories.ad import AdRepository
from src.repositories.seller import SellerRepository


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    pool.fetchrow = AsyncMock()
    return pool


def test_create_seller(mock_pool):
    mock_pool.fetchrow.return_value = {'id': 1, 'name': 'Test Seller', 'is_verified': True}

    repo = SellerRepository(mock_pool)
    result = asyncio.new_event_loop().run_until_complete(
        repo.create(name='Test Seller', is_verified=True)
    )

    assert result == {'id': 1, 'name': 'Test Seller', 'is_verified': True}
    mock_pool.fetchrow.assert_called_once()
    call_args = mock_pool.fetchrow.call_args
    assert 'INSERT INTO sellers' in call_args[0][0]
    assert call_args[0][1] == 'Test Seller'
    assert call_args[0][2] is True


def test_get_seller_by_id(mock_pool):
    mock_pool.fetchrow.return_value = {'id': 1, 'name': 'Test Seller', 'is_verified': False}

    repo = SellerRepository(mock_pool)
    result = asyncio.new_event_loop().run_until_complete(repo.get_by_id(1))

    assert result == {'id': 1, 'name': 'Test Seller', 'is_verified': False}
    mock_pool.fetchrow.assert_called_once()
    call_args = mock_pool.fetchrow.call_args
    assert 'SELECT' in call_args[0][0]
    assert call_args[0][1] == 1


def test_get_seller_by_id_not_found(mock_pool):
    mock_pool.fetchrow.return_value = None

    repo = SellerRepository(mock_pool)
    result = asyncio.new_event_loop().run_until_complete(repo.get_by_id(999))

    assert result is None


def test_create_ad(mock_pool):
    mock_pool.fetchrow.return_value = {
        'id': 1,
        'seller_id': 10,
        'name': 'Test Ad',
        'description': 'Description',
        'category': 5,
        'images_qty': 3,
    }

    repo = AdRepository(mock_pool)
    result = asyncio.new_event_loop().run_until_complete(
        repo.create(
            seller_id=10, name='Test Ad', description='Description', category=5, images_qty=3
        )
    )

    assert result == {
        'id': 1,
        'seller_id': 10,
        'name': 'Test Ad',
        'description': 'Description',
        'category': 5,
        'images_qty': 3,
    }
    mock_pool.fetchrow.assert_called_once()
    call_args = mock_pool.fetchrow.call_args
    assert 'INSERT INTO ads' in call_args[0][0]
    assert call_args[0][1] == 10
    assert call_args[0][2] == 'Test Ad'


def test_get_ad_by_id(mock_pool):
    mock_pool.fetchrow.return_value = {
        'id': 1,
        'seller_id': 10,
        'name': 'Test Ad',
        'description': 'Description',
        'category': 5,
        'images_qty': 3,
    }

    repo = AdRepository(mock_pool)
    result = asyncio.new_event_loop().run_until_complete(repo.get_by_id(1))

    assert result == {
        'id': 1,
        'seller_id': 10,
        'name': 'Test Ad',
        'description': 'Description',
        'category': 5,
        'images_qty': 3,
    }
    mock_pool.fetchrow.assert_called_once()
    call_args = mock_pool.fetchrow.call_args
    assert 'SELECT' in call_args[0][0]
    assert call_args[0][1] == 1


def test_get_ad_by_id_not_found(mock_pool):
    mock_pool.fetchrow.return_value = None

    repo = AdRepository(mock_pool)
    result = asyncio.new_event_loop().run_until_complete(repo.get_by_id(999))

    assert result is None
