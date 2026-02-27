import logging

from fastapi import APIRouter, HTTPException, Query

from src.clients.kafka import KafkaModerationProducer
from src.repositories.items import ItemsRepository
from src.repositories.moderation_results import ModerationResultsRepository
from src.schemas.ad import AdRequest, AdResponse
from src.schemas.moderation import AsyncModerationResponse, ModerationResultResponse
from src.services.moderation import ModerationService

logger = logging.getLogger(__name__)

router = APIRouter()
service = ModerationService()
INT32_MAX = 2_147_483_647

_get_model_func = None
_get_db_pool_func = None
_get_kafka_producer_func = None


def set_model_getter(func) -> None:
    global _get_model_func
    _get_model_func = func


def set_db_pool_getter(func) -> None:
    global _get_db_pool_func
    _get_db_pool_func = func


def set_kafka_producer_getter(func) -> None:
    global _get_kafka_producer_func
    _get_kafka_producer_func = func


def _get_db_pool():
    if _get_db_pool_func is None:
        return None
    return _get_db_pool_func()


def _get_kafka_producer() -> KafkaModerationProducer | None:
    if _get_kafka_producer_func is None:
        return None
    return _get_kafka_producer_func()


@router.post('/predict', response_model=AdResponse)
async def predict(request: AdRequest) -> AdResponse:
    if _get_model_func is None:
        logger.error('Model getter not initialized')
        raise HTTPException(status_code=503, detail='Model is not available')

    model = _get_model_func()
    if model is None:
        logger.error('Model is not loaded')
        raise HTTPException(status_code=503, detail='Model is not available')

    try:
        is_violation, probability = await service.predict_moderation(request, model)
        return AdResponse(is_violation=is_violation, probability=probability)
    except Exception as e:
        logger.error(f'Error during prediction: {str(e)}', exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error processing request: {str(e)}')


@router.post('/simple_predict', response_model=AdResponse)
async def simple_predict(item_id: int = Query(..., ge=1, le=INT32_MAX)) -> AdResponse:
    if _get_model_func is None:
        logger.error('Model getter not initialized')
        raise HTTPException(status_code=503, detail='Model is not available')

    model = _get_model_func()
    if model is None:
        logger.error('Model is not loaded')
        raise HTTPException(status_code=503, detail='Model is not available')

    pool = _get_db_pool()
    if pool is None:
        logger.error('Database pool is not available')
        raise HTTPException(status_code=503, detail='Database is not available')

    items_repository = ItemsRepository(lambda: pool)
    ad_data = await items_repository.get_item_by_id(item_id)
    if ad_data is None:
        raise HTTPException(status_code=404, detail='Item not found')

    request = AdRequest(**ad_data)
    try:
        is_violation, probability = await service.predict_moderation(request, model)
        return AdResponse(is_violation=is_violation, probability=probability)
    except Exception as e:
        logger.error(f'Error during prediction: {str(e)}', exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error processing request: {str(e)}')


@router.post('/async_predict', response_model=AsyncModerationResponse)
async def async_predict(item_id: int = Query(..., ge=1, le=INT32_MAX)) -> AsyncModerationResponse:
    pool = _get_db_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail='Database is not available')

    kafka_producer = _get_kafka_producer()
    if kafka_producer is None:
        raise HTTPException(status_code=503, detail='Kafka producer is not available')

    items_repository = ItemsRepository(lambda: pool)
    moderation_results_repository = ModerationResultsRepository(lambda: pool)

    item = await items_repository.get_item_by_id(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail='Item not found')

    task_id = None
    try:
        task_id = await moderation_results_repository.create_pending(item_id=item_id)
        await kafka_producer.send_moderation_request(item_id=item_id, task_id=task_id)
    except Exception as exc:
        if task_id is not None:
            try:
                await moderation_results_repository.update_failed(task_id=task_id, error_message=str(exc))
            except Exception:
                logger.exception('Failed to mark task as failed for task_id=%s', task_id)
        logger.error('Error creating async moderation task: %s', exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error processing request: {str(exc)}')

    return AsyncModerationResponse(
        task_id=task_id,
        status='pending',
        message='Moderation request accepted',
    )


@router.get('/moderation_result/{task_id}', response_model=ModerationResultResponse)
async def moderation_result(task_id: int) -> ModerationResultResponse:
    if task_id < 1:
        raise HTTPException(status_code=422, detail='task_id must be greater than or equal to 1')

    pool = _get_db_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail='Database is not available')

    moderation_results_repository = ModerationResultsRepository(lambda: pool)
    result = await moderation_results_repository.get_by_id(task_id=task_id)
    if result is None:
        raise HTTPException(status_code=404, detail='Task not found')

    return ModerationResultResponse(
        task_id=result['task_id'],
        status=result['status'],
        is_violation=result['is_violation'],
        probability=result['probability'],
        error_message=result['error_message'],
    )
