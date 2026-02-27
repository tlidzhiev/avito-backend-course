import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.routers import api_router
from src.clients.kafka import KafkaModerationProducer
from src.ml.model import load_model, save_model, train_model
from src.repositories.db import close_db, get_db_pool, init_db

logger = logging.getLogger(__name__)

ml_model = None
kafka_moderation_producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ml_model
    global kafka_moderation_producer
    logger.info('Loading ML model...')
    ml_model = load_model('model.pkl')
    if ml_model is None:
        logger.info('Model file not found. Training new model...')
        ml_model = train_model()
        save_model(ml_model, 'model.pkl')
        logger.info('Model trained and saved successfully')
    else:
        logger.info('Model loaded successfully')

    await init_db()

    kafka_moderation_producer = KafkaModerationProducer.from_env()
    if kafka_moderation_producer is not None:
        try:
            await kafka_moderation_producer.start()
            logger.info('Kafka producer initialized')
        except Exception as exc:
            logger.warning('Kafka producer initialization failed: %s', exc)
            kafka_moderation_producer = None
    else:
        logger.warning('Kafka producer is disabled: KAFKA_BOOTSTRAP_SERVERS is not set')

    from src.api.moderation import (
        set_db_pool_getter,
        set_kafka_producer_getter,
        set_model_getter,
    )

    set_model_getter(get_model)
    set_db_pool_getter(get_db_pool)
    set_kafka_producer_getter(get_kafka_moderation_producer)
    yield
    if kafka_moderation_producer is not None:
        await kafka_moderation_producer.stop()
        kafka_moderation_producer = None
    await close_db()
    logger.info('Shutting down...')


def get_model():
    return ml_model


def get_kafka_moderation_producer():
    return kafka_moderation_producer


app = FastAPI(title='Avito Moderation API', version='0.1.0', lifespan=lifespan)
app.include_router(api_router)


@app.get('/')
async def root():
    return {'message': 'Hello, world!'}
