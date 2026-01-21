from fastapi import APIRouter

from .moderation import router as moderation_router

api_router = APIRouter()
api_router.include_router(moderation_router, prefix='/moderation', tags=['moderation'])
