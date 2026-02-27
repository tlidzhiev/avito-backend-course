from pydantic import BaseModel


class AsyncModerationResponse(BaseModel):
    task_id: int
    status: str
    message: str


class ModerationResultResponse(BaseModel):
    task_id: int
    status: str
    is_violation: bool | None = None
    probability: float | None = None
    error_message: str | None = None
