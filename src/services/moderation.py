import logging
from dataclasses import dataclass

import numpy as np

from src.schemas.ad import AdRequest

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModerationService:
    async def predict_moderation(self, ad: AdRequest, model) -> tuple[bool, float]:
        logger.info(
            f'Processing moderation request - seller_id={ad.seller_id}, '
            f'item_id={ad.item_id}, is_verified={ad.is_verified_seller}, '
            f'images_qty={ad.images_qty}, description_len={len(ad.description)}, '
            f'category={ad.category}'
        )

        features = np.array(
            [
                [
                    1.0 if ad.is_verified_seller else 0.0,
                    ad.images_qty / 10.0,
                    len(ad.description) / 1000.0,
                    ad.category / 100.0,
                ]
            ]
        )

        prediction = model.predict(features)[0]
        probability = model.predict_proba(features)[0][1]
        is_violation = bool(prediction)
        logger.info(
            f'Prediction result - seller_id={ad.seller_id}, item_id={ad.item_id}, '
            f'is_violation={is_violation}, probability={probability:.4f}'
        )

        return is_violation, float(probability)
