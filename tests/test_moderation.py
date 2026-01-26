import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


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
