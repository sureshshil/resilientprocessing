"""Job document shape, statuses and request models."""
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

STAGES = ("transcription", "summary", "profiling", "compliance", "behaviour")
StageName = Literal["transcription", "summary", "profiling", "compliance", "behaviour"]
FaultType = Literal["llm_429", "llm_timeout", "llm_500", "llm_content_blocked", "bad_audio", "slow"]


class Status:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RecordingKey(BaseModel):
    opportunityId: str = Field(min_length=1)
    agentId: str = Field(min_length=1)
    clientId: str = Field(min_length=1)
    eventId: str = Field(min_length=1)
    sessionId: str = Field(min_length=1)
    recordingId: str = Field(min_length=1)
    recordingType: Literal["audio", "consent"]

    @property
    def job_id(self) -> str:
        # recordingId is only unique within a session.
        return f"{self.sessionId}:{self.recordingId}"


class FaultSpec(BaseModel):
    type: FaultType
    times: int = Field(default=1, ge=0)  # 0 = every try


class SubmitRequest(RecordingKey):
    faults: dict[StageName, FaultSpec] = Field(default_factory=dict)


def to_ms(value: datetime) -> datetime:
    """MongoDB stores milliseconds; truncating keeps equality filters on datetimes exact."""
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def utcnow() -> datetime:
    return to_ms(datetime.now(timezone.utc))


def new_job_document(req: SubmitRequest, now: datetime, send_after: datetime) -> dict:
    return {
        "_id": req.job_id,
        "key": req.model_dump(exclude={"faults"}),
        "status": Status.PENDING,
        "stages": {
            stage: {"status": Status.PENDING, "tries": 0, "output": None, "finishedAt": None}
            for stage in STAGES
        },
        "owner": None,
        "sendMe": True,
        "sendAfter": to_ms(send_after),
        "attempts": 0,
        "error": None,
        "faults": {stage: spec.model_dump() for stage, spec in req.faults.items()},
        "createdAt": now,
        "updatedAt": now,
    }
