import logging

from fastapi import APIRouter, HTTPException

from src.schemas.ad import AdRequest, AdResponse
from src.services.moderation import ModerationService

logger = logging.getLogger(__name__)

router = APIRouter()
service = ModerationService()

_get_model_func = None


def set_model_getter(func) -> None:
    global _get_model_func
    _get_model_func = func


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
