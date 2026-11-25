from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEW_EVENTS = {EventType.VIEW_STARTED, EventType.VIEW_FINISHED, EventType.VIEW_PAUSED, EventType.VIEW_RESUMED}


class EventIn(BaseModel):
    """Событие, которое присылают внешние сервисы. event_id и timestamp можно не передавать."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    device_type: DeviceType
    session_id: str = Field(min_length=1)
    progress_seconds: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def to_utc(cls, value: datetime) -> datetime:
        # время без часового пояса считаем UTC
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def check_progress(self):
        if self.event_type in VIEW_EVENTS and self.progress_seconds is None:
            raise ValueError(f"progress_seconds is required for {self.event_type.value}")
        return self

    def to_record(self) -> dict:
        """Запись в том виде, в каком её ждёт Avro-схема."""
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


class EventOut(BaseModel):
    event_id: UUID
