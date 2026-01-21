from fastapi import APIRouter, HTTPException

from src.schemas.ad import AdRequest, AdResponse
from src.services.moderation import ModerationService

router = APIRouter()
service = ModerationService()


@router.post('/predict', response_model=AdResponse)
async def predict(request: AdRequest) -> AdResponse:
    try:
        is_approved = await service.predict_moderation(request)
        return AdResponse(is_approved=is_approved)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error processing request: {str(e)}')
