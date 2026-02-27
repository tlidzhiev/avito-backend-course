from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def valid_ad_data():
    return {
        'seller_id': 1,
        'is_verified_seller': True,
        'item_id': 100,
        'name': 'Test Item',
        'description': 'Test Description',
        'category': 1,
        'images_qty': 3,
    }


def test_predict_success_violation(client, valid_ad_data):
    valid_ad_data['is_verified_seller'] = False
    valid_ad_data['images_qty'] = 0

    response = client.post('/moderation/predict', json=valid_ad_data)

    assert response.status_code == 200
    data = response.json()
    assert 'is_violation' in data
    assert 'probability' in data
    assert isinstance(data['is_violation'], bool)
    assert isinstance(data['probability'], float)
    assert 0.0 <= data['probability'] <= 1.0


def test_predict_success_no_violation(client, valid_ad_data):
    valid_ad_data['is_verified_seller'] = True
    valid_ad_data['images_qty'] = 5

    response = client.post('/moderation/predict', json=valid_ad_data)

    assert response.status_code == 200
    data = response.json()
    assert 'is_violation' in data
    assert 'probability' in data
    assert isinstance(data['is_violation'], bool)
    assert isinstance(data['probability'], float)
    assert 0.0 <= data['probability'] <= 1.0


@pytest.mark.parametrize(
    'field_name,invalid_value',
    [
        ('seller_id', 'invalid'),
        ('seller_id', None),
        ('is_verified_seller', 'invalid'),
        ('is_verified_seller', None),
        ('item_id', 'invalid'),
        ('item_id', None),
        ('name', 12345),
        ('name', None),
        ('description', 12345),
        ('description', None),
        ('category', 'invalid'),
        ('category', None),
        ('images_qty', 'invalid'),
        ('images_qty', None),
    ],
)
def test_predict_invalid_field_types(client, valid_ad_data, field_name, invalid_value):
    valid_ad_data[field_name] = invalid_value

    response = client.post('/moderation/predict', json=valid_ad_data)
    assert response.status_code == 422


@pytest.mark.parametrize(
    'excluded_field',
    ['seller_id', 'is_verified_seller', 'item_id', 'name', 'description', 'category', 'images_qty'],
)
def test_predict_missing_required_fields(client, valid_ad_data, excluded_field):
    del valid_ad_data[excluded_field]

    response = client.post('/moderation/predict', json=valid_ad_data)
    assert response.status_code == 422


@pytest.mark.parametrize('payload', [{}, None])
def test_predict_invalid_payload(client, payload):
    response = client.post('/moderation/predict', json=payload)
    assert response.status_code == 422


def test_predict_service_error(client, valid_ad_data, monkeypatch):
    from src.services.moderation import ModerationService

    async def mock_error(self, ad, model):
        raise Exception('Internal service error')

    monkeypatch.setattr(ModerationService, 'predict_moderation', mock_error)
    response = client.post('/moderation/predict', json=valid_ad_data)
    assert response.status_code == 500
    assert 'error' in response.json()['detail'].lower()


def test_predict_model_not_available(client, valid_ad_data, monkeypatch):
    from src.api import moderation

    def mock_get_model():
        return None

    monkeypatch.setattr(moderation, '_get_model_func', mock_get_model)
    response = client.post('/moderation/predict', json=valid_ad_data)
    assert response.status_code == 503
    assert 'not available' in response.json()['detail'].lower()


# --- simple_predict tests ---


def _mock_repos(monkeypatch, ad_data, seller_data):
    from src.repositories.ad import AdRepository
    from src.repositories.seller import SellerRepository

    monkeypatch.setattr(AdRepository, 'get_by_id', AsyncMock(return_value=ad_data))
    monkeypatch.setattr(SellerRepository, 'get_by_id', AsyncMock(return_value=seller_data))

    mock_pool = MagicMock()
    monkeypatch.setattr('src.api.moderation._db_pool', mock_pool)


def test_simple_predict_positive(client, monkeypatch):
    ad_data = {
        'id': 1,
        'seller_id': 10,
        'name': 'Test Ad',
        'description': 'Short',
        'category': 1,
        'images_qty': 0,
    }
    seller_data = {'id': 10, 'name': 'Seller', 'is_verified': False}
    _mock_repos(monkeypatch, ad_data, seller_data)

    response = client.post('/moderation/simple_predict', json={'item_id': 1})

    assert response.status_code == 200
    data = response.json()
    assert 'is_violation' in data
    assert 'probability' in data
    assert isinstance(data['is_violation'], bool)
    assert isinstance(data['probability'], float)
    assert data['is_violation'] is True


def test_simple_predict_negative(client, monkeypatch):
    ad_data = {
        'id': 2,
        'seller_id': 20,
        'name': 'Good Ad',
        'description': 'A very good and detailed description of the product',
        'category': 5,
        'images_qty': 5,
    }
    seller_data = {'id': 20, 'name': 'Verified Seller', 'is_verified': True}
    _mock_repos(monkeypatch, ad_data, seller_data)

    response = client.post('/moderation/simple_predict', json={'item_id': 2})

    assert response.status_code == 200
    data = response.json()
    assert 'is_violation' in data
    assert 'probability' in data
    assert isinstance(data['is_violation'], bool)
    assert isinstance(data['probability'], float)
    assert data['is_violation'] is False


def test_simple_predict_ad_not_found(client, monkeypatch):
    _mock_repos(monkeypatch, ad_data=None, seller_data=None)

    response = client.post('/moderation/simple_predict', json={'item_id': 999})

    assert response.status_code == 404
    assert 'not found' in response.json()['detail'].lower()


def test_simple_predict_invalid_item_id(client):
    response = client.post('/moderation/simple_predict', json={'item_id': 'invalid'})
    assert response.status_code == 422


def test_simple_predict_missing_item_id(client):
    response = client.post('/moderation/simple_predict', json={})
    assert response.status_code == 422
