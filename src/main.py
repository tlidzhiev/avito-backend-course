import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.routers import api_router
from src.ml.model import load_model, save_model, train_model

logger = logging.getLogger(__name__)

ml_model = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ml_model
    logger.info('Loading ML model...')
    ml_model = load_model('model.pkl')
    if ml_model is None:
        logger.info('Model file not found. Training new model...')
        ml_model = train_model()
        save_model(ml_model, 'model.pkl')
        logger.info('Model trained and saved successfully')
    else:
        logger.info('Model loaded successfully')

    from src.api.moderation import set_model_getter

    set_model_getter(get_model)
    yield
    logger.info('Shutting down...')


def get_model():
    return ml_model


app = FastAPI(title='Avito Moderation API', version='0.1.0', lifespan=lifespan)
app.include_router(api_router)


@app.get('/')
async def root():
    return {'message': 'Hello, world!'}
