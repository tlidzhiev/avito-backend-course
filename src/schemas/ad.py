from pydantic import BaseModel, Field

INT32_MAX = 2_147_483_647


class AdRequest(BaseModel):
    seller_id: int = Field(ge=1, le=INT32_MAX)
    is_verified_seller: bool
    item_id: int = Field(ge=1, le=INT32_MAX)
    name: str
    description: str
    category: int
    images_qty: int


class AdResponse(BaseModel):
    is_violation: bool
    probability: float
