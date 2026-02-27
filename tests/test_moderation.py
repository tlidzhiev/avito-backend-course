import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app
from src.ml.model import load_model, train_model
from src.repositories.items import ItemsRepository
from src.repositories.users import UsersRepository
from src.schemas.ad import AdRequest
from src.services.moderation import ModerationService


class _SyncClient:
    def __init__(self, app_):
        self.app = app_

    async def _request(self, method: str, path: str, **kwargs):
        async with self.app.router.lifespan_context(self.app):
            transport = ASGITransport(app=self.app)
            async with AsyncClient(transport=transport, base_url='http://test') as client:
                return await client.request(method, path, **kwargs)

    def post(self, path: str, **kwargs):
        return asyncio.run(self._request('POST', path, **kwargs))

    def get(self, path: str, **kwargs):
        return asyncio.run(self._request('GET', path, **kwargs))


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeConnection:
    def __init__(self, pool):
        self.pool = pool

    async def execute(self, query, *args):
        if 'INSERT INTO users' in query:
            seller_id, is_verified_seller = args
            self.pool.users[seller_id] = {
                'id': seller_id,
                'is_verified_seller': is_verified_seller,
            }
            return 'INSERT 0 1'

        if 'INSERT INTO advertisements' in query:
            item_id, seller_id, name, description, category, images_qty = args
            self.pool.items[item_id] = {
                'item_id': item_id,
                'seller_id': seller_id,
                'name': name,
                'description': description,
                'category': category,
                'images_qty': images_qty,
            }
            return 'INSERT 0 1'

        if 'UPDATE moderation_results' in query and "status = 'completed'" in query:
            task_id, is_violation, probability, processed_at = args
            result = self.pool.moderation_results.get(task_id)
            if result is None:
                return 'UPDATE 0'
            result['status'] = 'completed'
            result['is_violation'] = is_violation
            result['probability'] = probability
            result['error_message'] = None
            result['processed_at'] = processed_at
            return 'UPDATE 1'

        if 'UPDATE moderation_results' in query and "status = 'failed'" in query:
            task_id, error_message, processed_at = args
            result = self.pool.moderation_results.get(task_id)
            if result is None:
                return 'UPDATE 0'
            result['status'] = 'failed'
            result['error_message'] = error_message
            result['processed_at'] = processed_at
            return 'UPDATE 1'

        raise ValueError(f'Unexpected query in execute: {query}')

    async def fetchval(self, query, *args):
        if 'INSERT INTO moderation_results' in query:
            item_id, created_at = args
            task_id = self.pool.next_task_id
            self.pool.next_task_id += 1
            self.pool.moderation_results[task_id] = {
                'id': task_id,
                'item_id': item_id,
                'status': 'pending',
                'is_violation': None,
                'probability': None,
                'error_message': None,
                'created_at': created_at,
                'processed_at': None,
            }
            return task_id

        if 'SELECT id FROM moderation_results' in query and "status = 'pending'" in query:
            item_id = args[0]
            for task_id in sorted(self.pool.moderation_results):
                result = self.pool.moderation_results[task_id]
                if result['item_id'] == item_id and result['status'] == 'pending':
                    return task_id
            return None

        raise ValueError(f'Unexpected query in fetchval: {query}')

    async def fetchrow(self, query, *args):
        if 'FROM users WHERE id = $1' in query:
            seller_id = args[0]
            user = self.pool.users.get(seller_id)
            if user is None:
                return None
            return {'id': user['id'], 'is_verified_seller': user['is_verified_seller']}

        if 'FROM advertisements a' in query and 'JOIN users u' in query:
            item_id = args[0]
            item = self.pool.items.get(item_id)
            if item is None:
                return None

            user = self.pool.users.get(item['seller_id'])
            if user is None:
                return None

            return {
                'item_id': item['item_id'],
                'seller_id': item['seller_id'],
                'name': item['name'],
                'description': item['description'],
                'category': item['category'],
                'images_qty': item['images_qty'],
                'is_verified_seller': user['is_verified_seller'],
            }

        if 'FROM moderation_results' in query and 'WHERE id = $1' in query:
            task_id = args[0]
            result = self.pool.moderation_results.get(task_id)
            if result is None:
                return None
            return result

        raise ValueError(f'Unexpected query in fetchrow: {query}')


class _FakePool:
    def __init__(self):
        self.users = {}
        self.items = {}
        self.moderation_results = {}
        self.next_task_id = 1

    def acquire(self):
        return _FakeAcquire(_FakeConnection(self))


class _FakeKafkaProducer:
    def __init__(self):
        self.sent_messages = []
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def send_moderation_request(self, item_id: int, task_id: int | None = None):
        message = {'type': 'moderation', 'item_id': item_id}
        if task_id is not None:
            message['task_id'] = task_id
        self.sent_messages.append(message)

    async def send_dlq_message(self, original_message, error_message: str, retry_count: int):
        self.sent_messages.append(
            {
                'type': 'dlq',
                'original_message': original_message,
                'error_message': error_message,
                'retry_count': retry_count,
            }
        )

    async def send_raw_message(self, topic: str, payload: dict):
        self.sent_messages.append({'type': 'raw', 'topic': topic, 'payload': payload})


def _get_real_model():
    return load_model('model.pkl') or train_model()


def _predict_with_model(model, ad: AdRequest) -> int:
    features = [
        [
            1.0 if ad.is_verified_seller else 0.0,
            ad.images_qty / 10.0,
            len(ad.description) / 1000.0,
            ad.category / 100.0,
        ]
    ]
    return int(model.predict(features)[0])


def _find_ad_by_prediction(model, target: int) -> AdRequest | None:
    for is_verified in [False, True]:
        for images_qty in [0, 1, 3, 5, 10]:
            for desc_len in [5, 50, 500, 1000]:
                for category in [1, 10, 50, 100]:
                    ad = AdRequest(
                        seller_id=1,
                        is_verified_seller=is_verified,
                        item_id=100,
                        name='Test',
                        description='x' * desc_len,
                        category=category,
                        images_qty=images_qty,
                    )
                    if _predict_with_model(model, ad) == target:
                        return ad
    return None


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv('DATABASE_URL', raising=False)
    monkeypatch.delenv('KAFKA_BOOTSTRAP_SERVERS', raising=False)
    monkeypatch.delenv('KAFKA_MODERATION_TOPIC', raising=False)
    monkeypatch.delenv('KAFKA_DLQ_TOPIC', raising=False)
    return _SyncClient(app)


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
    from src import main

    def mock_get_model():
        return None

    monkeypatch.setattr(main, 'get_model', mock_get_model)
    response = client.post('/moderation/predict', json=valid_ad_data)
    assert response.status_code == 503
    assert 'not available' in response.json()['detail'].lower()


def test_simple_predict_success_violation(client, monkeypatch):
    from src import main
    from src.services.moderation import ModerationService

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': False}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Test Item',
        'description': 'Test Description',
        'category': 1,
        'images_qty': 0,
    }

    async def mock_predict(self, ad, model):
        return True, 0.93

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)
    monkeypatch.setattr(ModerationService, 'predict_moderation', mock_predict)

    response = client.post('/moderation/simple_predict', params={'item_id': 100})
    assert response.status_code == 200
    assert response.json() == {'is_violation': True, 'probability': 0.93}


def test_simple_predict_success_no_violation(client, monkeypatch):
    from src import main
    from src.services.moderation import ModerationService

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': True}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Test Item',
        'description': 'Test Description',
        'category': 1,
        'images_qty': 5,
    }

    async def mock_predict(self, ad, model):
        return False, 0.07

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)
    monkeypatch.setattr(ModerationService, 'predict_moderation', mock_predict)

    response = client.post('/moderation/simple_predict', params={'item_id': 100})
    assert response.status_code == 200
    assert response.json() == {'is_violation': False, 'probability': 0.07}


def test_simple_predict_item_not_found(client, monkeypatch):
    from src import main

    monkeypatch.setattr(main, 'get_db_pool', lambda: _FakePool())

    response = client.post('/moderation/simple_predict', params={'item_id': 999})
    assert response.status_code == 404
    assert 'not found' in response.json()['detail'].lower()


def test_simple_predict_db_not_available(client, monkeypatch):
    from src import main

    monkeypatch.setattr(main, 'get_db_pool', lambda: None)

    response = client.post('/moderation/simple_predict', params={'item_id': 100})
    assert response.status_code == 503
    assert 'database' in response.json()['detail'].lower()


def test_simple_predict_invalid_item_id(client):
    response = client.post('/moderation/simple_predict', params={'item_id': 0})
    assert response.status_code == 422


def test_async_predict_success(client, monkeypatch):
    from src import main

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': True}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Async Item',
        'description': 'Async Description',
        'category': 1,
        'images_qty': 2,
    }
    fake_producer = _FakeKafkaProducer()

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)
    monkeypatch.setattr(main, 'get_kafka_moderation_producer', lambda: fake_producer)

    response = client.post('/moderation/async_predict', params={'item_id': 100})
    assert response.status_code == 200
    assert response.json() == {
        'task_id': 1,
        'status': 'pending',
        'message': 'Moderation request accepted',
    }
    assert fake_pool.moderation_results[1]['item_id'] == 100
    assert fake_pool.moderation_results[1]['status'] == 'pending'
    assert fake_producer.sent_messages == [{'type': 'moderation', 'item_id': 100, 'task_id': 1}]


def test_async_predict_item_not_found(client, monkeypatch):
    from src import main

    fake_pool = _FakePool()
    fake_producer = _FakeKafkaProducer()

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)
    monkeypatch.setattr(main, 'get_kafka_moderation_producer', lambda: fake_producer)

    response = client.post('/moderation/async_predict', params={'item_id': 404})
    assert response.status_code == 404
    assert 'not found' in response.json()['detail'].lower()


def test_async_predict_marks_task_failed_when_kafka_send_fails(client, monkeypatch):
    from src import main

    class _FailingKafkaProducer(_FakeKafkaProducer):
        async def send_moderation_request(self, item_id: int, task_id: int | None = None):
            raise RuntimeError('Kafka send failed')

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': True}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Async Item',
        'description': 'Async Description',
        'category': 1,
        'images_qty': 2,
    }
    failing_producer = _FailingKafkaProducer()

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)
    monkeypatch.setattr(main, 'get_kafka_moderation_producer', lambda: failing_producer)

    response = client.post('/moderation/async_predict', params={'item_id': 100})
    assert response.status_code == 500
    assert fake_pool.moderation_results[1]['item_id'] == 100
    assert fake_pool.moderation_results[1]['status'] == 'failed'
    assert 'kafka send failed' in fake_pool.moderation_results[1]['error_message'].lower()


def test_moderation_result_pending(client, monkeypatch):
    from src import main

    fake_pool = _FakePool()
    fake_pool.moderation_results[1] = {
        'id': 1,
        'item_id': 100,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
        'created_at': None,
        'processed_at': None,
    }

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)

    response = client.get('/moderation/moderation_result/1')
    assert response.status_code == 200
    assert response.json() == {
        'task_id': 1,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
    }


def test_moderation_result_not_found(client, monkeypatch):
    from src import main

    monkeypatch.setattr(main, 'get_db_pool', lambda: _FakePool())

    response = client.get('/moderation/moderation_result/1')
    assert response.status_code == 404
    assert 'not found' in response.json()['detail'].lower()


def test_users_repository_create_and_get():
    fake_pool = _FakePool()
    users_repo = UsersRepository(lambda: fake_pool)

    asyncio.run(users_repo.create_user(seller_id=10, is_verified_seller=True))
    user = asyncio.run(users_repo.get_user_by_id(10))

    assert user == {'seller_id': 10, 'is_verified_seller': True}


def test_items_repository_create_and_get():
    fake_pool = _FakePool()
    users_repo = UsersRepository(lambda: fake_pool)
    items_repo = ItemsRepository(lambda: fake_pool)

    asyncio.run(users_repo.create_user(seller_id=11, is_verified_seller=False))
    asyncio.run(
        items_repo.create_item(
            item_id=101,
            seller_id=11,
            name='Laptop',
            description='Used laptop',
            category=2,
            images_qty=2,
        )
    )

    item = asyncio.run(items_repo.get_item_by_id(101))

    assert item == {
        'seller_id': 11,
        'is_verified_seller': False,
        'item_id': 101,
        'name': 'Laptop',
        'description': 'Used laptop',
        'category': 2,
        'images_qty': 2,
    }


def test_moderation_service_positive_prediction():
    model = _get_real_model()
    ad = _find_ad_by_prediction(model, target=1)
    assert ad is not None, 'Could not find input with positive prediction for model.pkl'

    service = ModerationService()
    is_violation, probability = asyncio.run(service.predict_moderation(ad, model))

    assert is_violation is True
    assert 0.0 <= probability <= 1.0


def test_moderation_service_negative_prediction():
    model = _get_real_model()
    ad = _find_ad_by_prediction(model, target=0)
    assert ad is not None, 'Could not find input with negative prediction for model.pkl'

    service = ModerationService()
    is_violation, probability = asyncio.run(service.predict_moderation(ad, model))

    assert is_violation is False
    assert 0.0 <= probability <= 1.0


def test_worker_process_message_success():
    from src.repositories.moderation_results import ModerationResultsRepository
    from src.workers.moderation_worker import ModerationWorker

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': False}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Item',
        'description': 'Description',
        'category': 1,
        'images_qty': 0,
    }
    fake_pool.moderation_results[1] = {
        'id': 1,
        'item_id': 100,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
        'created_at': None,
        'processed_at': None,
    }

    worker = ModerationWorker()
    worker.model = _get_real_model()
    worker.kafka_producer = _FakeKafkaProducer()

    items_repo = ItemsRepository(lambda: fake_pool)
    moderation_results_repo = ModerationResultsRepository(lambda: fake_pool)

    message = json.dumps({'item_id': 100, 'task_id': 1, 'retry_count': 0}).encode('utf-8')
    asyncio.run(worker._process_message(message, items_repo, moderation_results_repo))

    assert fake_pool.moderation_results[1]['status'] == 'completed'
    assert isinstance(fake_pool.moderation_results[1]['is_violation'], bool)
    assert isinstance(fake_pool.moderation_results[1]['probability'], float)
    assert worker.kafka_producer.sent_messages == []


def test_worker_process_message_send_to_dlq_on_error():
    from src.repositories.moderation_results import ModerationResultsRepository
    from src.workers.moderation_worker import ModerationWorker

    fake_pool = _FakePool()
    fake_pool.moderation_results[1] = {
        'id': 1,
        'item_id': 999,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
        'created_at': None,
        'processed_at': None,
    }

    worker = ModerationWorker()
    worker.model = _get_real_model()
    worker.kafka_producer = _FakeKafkaProducer()

    items_repo = ItemsRepository(lambda: fake_pool)
    moderation_results_repo = ModerationResultsRepository(lambda: fake_pool)

    message = json.dumps({'item_id': 999, 'task_id': 1, 'retry_count': 1}).encode('utf-8')
    asyncio.run(worker._process_message(message, items_repo, moderation_results_repo))

    assert fake_pool.moderation_results[1]['status'] == 'failed'
    assert 'not found' in fake_pool.moderation_results[1]['error_message'].lower()
    assert len(worker.kafka_producer.sent_messages) == 1
    assert worker.kafka_producer.sent_messages[0]['type'] == 'dlq'


def test_worker_process_message_retries_on_temporary_error(monkeypatch):
    from src.repositories.moderation_results import ModerationResultsRepository
    from src.services.moderation import ModerationService
    from src.workers.moderation_worker import ModerationWorker

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': False}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Item',
        'description': 'Description',
        'category': 1,
        'images_qty': 0,
    }
    fake_pool.moderation_results[1] = {
        'id': 1,
        'item_id': 100,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
        'created_at': None,
        'processed_at': None,
    }

    worker = ModerationWorker()
    worker.model = _get_real_model()
    worker.kafka_producer = _FakeKafkaProducer()
    worker.retry_delay_seconds = 0

    async def _raise_timeout(*_args, **_kwargs):
        raise TimeoutError('temporary outage')

    monkeypatch.setattr(ModerationService, 'predict_moderation', _raise_timeout)

    items_repo = ItemsRepository(lambda: fake_pool)
    moderation_results_repo = ModerationResultsRepository(lambda: fake_pool)

    message = json.dumps({'item_id': 100, 'task_id': 1, 'retry_count': 0}).encode('utf-8')
    asyncio.run(worker._process_message(message, items_repo, moderation_results_repo))

    assert fake_pool.moderation_results[1]['status'] == 'pending'
    assert len(worker.kafka_producer.sent_messages) == 1
    assert worker.kafka_producer.sent_messages[0]['type'] == 'raw'
    assert worker.kafka_producer.sent_messages[0]['topic'] == worker.moderation_topic
    assert worker.kafka_producer.sent_messages[0]['payload']['item_id'] == 100
    assert worker.kafka_producer.sent_messages[0]['payload']['task_id'] == 1
    assert worker.kafka_producer.sent_messages[0]['payload']['retry_count'] == 1
    assert 'timestamp' in worker.kafka_producer.sent_messages[0]['payload']


def test_worker_process_message_uses_task_id_from_message():
    from src.repositories.moderation_results import ModerationResultsRepository
    from src.workers.moderation_worker import ModerationWorker

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': False}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Item',
        'description': 'Description',
        'category': 1,
        'images_qty': 0,
    }
    fake_pool.moderation_results[1] = {
        'id': 1,
        'item_id': 100,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
        'created_at': None,
        'processed_at': None,
    }
    fake_pool.moderation_results[2] = {
        'id': 2,
        'item_id': 100,
        'status': 'pending',
        'is_violation': None,
        'probability': None,
        'error_message': None,
        'created_at': None,
        'processed_at': None,
    }

    worker = ModerationWorker()
    worker.model = _get_real_model()
    worker.kafka_producer = _FakeKafkaProducer()

    items_repo = ItemsRepository(lambda: fake_pool)
    moderation_results_repo = ModerationResultsRepository(lambda: fake_pool)

    message = json.dumps({'item_id': 100, 'task_id': 2, 'retry_count': 0}).encode('utf-8')
    asyncio.run(worker._process_message(message, items_repo, moderation_results_repo))

    assert fake_pool.moderation_results[1]['status'] == 'pending'
    assert fake_pool.moderation_results[2]['status'] == 'completed'
