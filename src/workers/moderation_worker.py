import asyncio
import json
import logging
import os
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer

from src.clients.kafka import KafkaModerationProducer
from src.ml.model import load_model, save_model, train_model
from src.repositories.db import close_db, get_db_pool, init_db
from src.repositories.items import ItemsRepository
from src.repositories.moderation_results import ModerationResultsRepository
from src.schemas.ad import AdRequest
from src.services.moderation import ModerationService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_temporary_error(exc: Exception) -> bool:
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))


class ModerationWorker:
    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.moderation_topic = os.getenv('KAFKA_MODERATION_TOPIC', 'moderation')
        self.consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'moderation_workers')
        self.max_retry_count = int(os.getenv('KAFKA_MAX_RETRY_COUNT', '3'))
        self.retry_delay_seconds = float(os.getenv('KAFKA_RETRY_DELAY_SECONDS', '2'))

        self.model = None
        self.service = ModerationService()
        self.kafka_producer = KafkaModerationProducer(
            bootstrap_servers=self.bootstrap_servers,
            moderation_topic=self.moderation_topic,
            dlq_topic=os.getenv('KAFKA_DLQ_TOPIC', 'moderation_dlq'),
        )

    async def run(self) -> None:
        await init_db()

        self.model = load_model('model.pkl')
        if self.model is None:
            logger.info('Model file not found. Training a new model...')
            self.model = train_model()
            save_model(self.model, 'model.pkl')

        pool = get_db_pool()
        if pool is None:
            raise RuntimeError('Database pool is not available')

        await self.kafka_producer.start()

        consumer = AIOKafkaConsumer(
            self.moderation_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        )

        await consumer.start()
        logger.info('Moderation worker started. Listening topic %s', self.moderation_topic)

        items_repository = ItemsRepository(get_db_pool)
        moderation_results_repository = ModerationResultsRepository(get_db_pool)

        try:
            async for message in consumer:
                await self._process_message(
                    message_value=message.value,
                    items_repository=items_repository,
                    moderation_results_repository=moderation_results_repository,
                )
        finally:
            await consumer.stop()
            await self.kafka_producer.stop()
            await close_db()

    async def _process_message(
        self,
        message_value: bytes,
        items_repository: ItemsRepository,
        moderation_results_repository: ModerationResultsRepository,
    ) -> None:
        original_message: dict = {}
        task_id: int | None = None
        retry_count = 0

        try:
            original_message = json.loads(message_value.decode('utf-8'))
            item_id = int(original_message['item_id'])
            if item_id < 1:
                raise ValueError('Invalid item_id in message')

            retry_count = int(original_message.get('retry_count', 0))
            task_id = original_message.get('task_id')
            if task_id is not None:
                task_id = int(task_id)
            else:
                task_id = await moderation_results_repository.get_pending_task_id_by_item(item_id)

            if task_id is None:
                raise ValueError(f'Pending moderation task not found for item_id={item_id}')

            ad_data = await items_repository.get_item_by_id(item_id)
            if ad_data is None:
                raise ValueError(f'Item not found: {item_id}')

            request = AdRequest(**ad_data)
            is_violation, probability = await self.service.predict_moderation(request, self.model)
            await moderation_results_repository.update_completed(
                task_id=task_id,
                is_violation=is_violation,
                probability=probability,
            )
            logger.info('Task %s completed for item_id=%s', task_id, item_id)
        except Exception as exc:
            logger.error('Error processing message: %s', exc, exc_info=True)

            if _is_temporary_error(exc) and retry_count < self.max_retry_count:
                retry_message = {
                    **original_message,
                    'retry_count': retry_count + 1,
                    'timestamp': datetime.now(UTC).isoformat(),
                }
                await asyncio.sleep(self.retry_delay_seconds)
                await self.kafka_producer.send_raw_message(self.moderation_topic, retry_message)
                return

            if task_id is not None:
                try:
                    await moderation_results_repository.update_failed(
                        task_id=task_id,
                        error_message=str(exc),
                    )
                except Exception:
                    logger.exception('Failed to update moderation status for task_id=%s', task_id)

            await self.kafka_producer.send_dlq_message(
                original_message=original_message,
                error_message=str(exc),
                retry_count=retry_count,
            )


async def main() -> None:
    worker = ModerationWorker()
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
