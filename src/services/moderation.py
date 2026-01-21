from dataclasses import dataclass

from src.schemas.ad import AdRequest


@dataclass(frozen=True)
class ModerationService:
    async def predict_moderation(self, ad: AdRequest):
        if ad.is_verified_seller:
            return True
        return ad.images_qty > 0
