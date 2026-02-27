import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaProducer


@dataclass
class KafkaModerationProducer:
    bootstrap_servers: str
    moderation_topic: str = 'moderation'
    dlq_topic: str = 'moderation_dlq'
    _producer: AIOKafkaProducer | None = None

    @classmethod
    def from_env(cls) -> 'KafkaModerationProducer | None':
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        if not bootstrap_servers:
            return None

        return cls(
            bootstrap_servers=bootstrap_servers,
            moderation_topic=os.getenv('KAFKA_MODERATION_TOPIC', 'moderation'),
            dlq_topic=os.getenv('KAFKA_DLQ_TOPIC', 'moderation_dlq'),
        )

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is None:
            return

        await self._producer.stop()
        self._producer = None

    async def send_moderation_request(self, item_id: int, task_id: int | None = None) -> None:
        message = {
            'item_id': item_id,
            'timestamp': datetime.now(UTC).isoformat(),
            'retry_count': 0,
        }
        if task_id is not None:
            message['task_id'] = task_id
        await self._send(self.moderation_topic, message)

    async def send_dlq_message(
        self,
        original_message: dict[str, Any],
        error_message: str,
        retry_count: int,
    ) -> None:
        dlq_message = {
            'original_message': original_message,
            'error': error_message,
            'timestamp': datetime.now(UTC).isoformat(),
            'retry_count': retry_count,
        }
        await self._send(self.dlq_topic, dlq_message)

    async def send_raw_message(self, topic: str, payload: dict[str, Any]) -> None:
        await self._send(topic, payload)

    async def _send(self, topic: str, payload: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError('Kafka producer is not started')

        await self._producer.send_and_wait(
            topic,
            json.dumps(payload).encode('utf-8'),
        )
