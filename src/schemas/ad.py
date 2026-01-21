from pydantic import BaseModel


class AdRequest(BaseModel):
    seller_id: int
    is_verified_seller: bool
    item_id: int
    name: str
    description: str
    category: int
    images_qty: int


class AdResponse(BaseModel):
    is_approved: bool
