import logging

from fastapi import APIRouter, HTTPException

from src.repositories.ad import AdRepository
from src.repositories.seller import SellerRepository
from src.schemas.ad import AdRequest, AdResponse, SimpleAdRequest
from src.services.moderation import ModerationService

logger = logging.getLogger(__name__)

router = APIRouter()
service = ModerationService()

_get_model_func = None
_db_pool = None


def set_model_getter(func) -> None:
    global _get_model_func
    _get_model_func = func


def set_db_pool(pool) -> None:
    global _db_pool
    _db_pool = pool


def _get_model():
    if _get_model_func is None:
        logger.error('Model getter not initialized')
        raise HTTPException(status_code=503, detail='Model is not available')

    model = _get_model_func()
    if model is None:
        logger.error('Model is not loaded')
        raise HTTPException(status_code=503, detail='Model is not available')

    return model


@router.post('/predict', response_model=AdResponse)
async def predict(request: AdRequest) -> AdResponse:
    model = _get_model()

    try:
        is_violation, probability = await service.predict_moderation(request, model)
        return AdResponse(is_violation=is_violation, probability=probability)
    except Exception as e:
        logger.error(f'Error during prediction: {str(e)}', exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error processing request: {str(e)}')


@router.post('/simple_predict', response_model=AdResponse)
async def simple_predict(request: SimpleAdRequest) -> AdResponse:
    model = _get_model()

    if _db_pool is None:
        logger.error('Database pool not initialized')
        raise HTTPException(status_code=503, detail='Database is not available')

    ad_repo = AdRepository(_db_pool)
    ad = await ad_repo.get_by_id(request.item_id)
    if ad is None:
        raise HTTPException(status_code=404, detail=f'Ad with id {request.item_id} not found')

    seller_repo = SellerRepository(_db_pool)
    seller = await seller_repo.get_by_id(ad['seller_id'])
    if seller is None:
        raise HTTPException(status_code=404, detail=f'Seller with id {ad["seller_id"]} not found')

    ad_request = AdRequest(
        seller_id=seller['id'],
        is_verified_seller=seller['is_verified'],
        item_id=ad['id'],
        name=ad['name'],
        description=ad['description'],
        category=ad['category'],
        images_qty=ad['images_qty'],
    )

    try:
        is_violation, probability = await service.predict_moderation(ad_request, model)
        return AdResponse(is_violation=is_violation, probability=probability)
    except Exception as e:
        logger.error(f'Error during prediction: {str(e)}', exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error processing request: {str(e)}')
