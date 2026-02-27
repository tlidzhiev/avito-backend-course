import asyncio
import json
import os
import uuid

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from httpx import ASGITransport, AsyncClient

from src.clients.kafka import KafkaModerationProducer
from src.main import app
from tests.test_moderation import _FakePool


@pytest.fixture
def anyio_backend():
    return 'asyncio'


async def _kafka_available(bootstrap_servers: str) -> bool:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    try:
        await producer.start()
        return True
    except Exception:
        return False
    finally:
        try:
            await producer.stop()
        except Exception:
            pass


async def _consume_one_message(topic: str, bootstrap_servers: str, timeout: float = 8.0) -> dict:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=f'test-group-{uuid.uuid4().hex}',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=timeout)
        return json.loads(message.value.decode('utf-8'))
    finally:
        await consumer.stop()


@pytest.mark.integration
@pytest.mark.anyio
async def test_async_predict_publishes_to_real_kafka(monkeypatch):
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    if not await _kafka_available(bootstrap_servers):
        pytest.skip(f'Kafka is not available at {bootstrap_servers}')

    moderation_topic = f'test_moderation_{uuid.uuid4().hex}'
    dlq_topic = f'test_moderation_dlq_{uuid.uuid4().hex}'

    monkeypatch.setenv('KAFKA_BOOTSTRAP_SERVERS', bootstrap_servers)
    monkeypatch.setenv('KAFKA_MODERATION_TOPIC', moderation_topic)
    monkeypatch.setenv('KAFKA_DLQ_TOPIC', dlq_topic)
    monkeypatch.delenv('DATABASE_URL', raising=False)

    fake_pool = _FakePool()
    fake_pool.users[1] = {'id': 1, 'is_verified_seller': True}
    fake_pool.items[100] = {
        'item_id': 100,
        'seller_id': 1,
        'name': 'Kafka Item',
        'description': 'Kafka Description',
        'category': 1,
        'images_qty': 2,
    }

    from src import main

    monkeypatch.setattr(main, 'get_db_pool', lambda: fake_pool)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url='http://test') as client:
            response = await client.post('/moderation/async_predict', params={'item_id': 100})
            assert response.status_code == 200
            payload = response.json()
            assert payload['status'] == 'pending'
            task_id = payload['task_id']

        message = await _consume_one_message(moderation_topic, bootstrap_servers)

    assert message['item_id'] == 100
    assert message['retry_count'] == 0
    assert 'timestamp' in message
    assert message['task_id'] == task_id


@pytest.mark.integration
@pytest.mark.anyio
async def test_kafka_producer_sends_real_dlq_message():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    if not await _kafka_available(bootstrap_servers):
        pytest.skip(f'Kafka is not available at {bootstrap_servers}')

    moderation_topic = f'test_moderation_{uuid.uuid4().hex}'
    dlq_topic = f'test_moderation_dlq_{uuid.uuid4().hex}'

    producer = KafkaModerationProducer(
        bootstrap_servers=bootstrap_servers,
        moderation_topic=moderation_topic,
        dlq_topic=dlq_topic,
    )
    await producer.start()
    try:
        await producer.send_dlq_message(
            original_message={'item_id': 777, 'task_id': 12},
            error_message='Test error',
            retry_count=3,
        )
    finally:
        await producer.stop()

    message = await _consume_one_message(dlq_topic, bootstrap_servers)
    assert message['original_message']['item_id'] == 777
    assert message['original_message']['task_id'] == 12
    assert message['error'] == 'Test error'
    assert message['retry_count'] == 3
    assert 'timestamp' in message
