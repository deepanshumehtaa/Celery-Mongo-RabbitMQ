from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class EnumStatus(str, Enum):
    """
    Valid lifecycle status enums for tasks in the platform.
    """
    STARTED = "STARTED"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    INPROGESS = "INPROGESS"


def get_now_utc_str() -> str:
    """
    Utility function returning ISO 8601 UTC string of the current time.
    """
    return datetime.now(timezone.utc).isoformat()


class OutputResponse(BaseModel):
    """
    Simplified model representing a task response/result, containing only unique fields.
    """
    task_response: Optional[Any] = None
    error_message: Optional[str] = None


class TaskRequestResponseDoc(BaseModel):
    """
    Primary MongoDB document schema containing task metadata, inputs, status, and nested output response.
    """
    task_id: str
    task_name: str
    trace_id: str
    lock_key: str
    input: Optional[Any] = None
    status: EnumStatus
    retry_count: int = 0
    created_at: str = Field(default_factory=get_now_utc_str)
    updated_at: str = Field(default_factory=get_now_utc_str)
    response: Optional[OutputResponse] = None

    model_config = {
        "use_enum_values": True
    }
