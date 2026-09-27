import uuid

from app.models import SubmitRequest, new_job_document, utcnow


def make_request(**overrides) -> SubmitRequest:
    data = {
        "opportunityId": "OPP-1", "agentId": "AG-1", "clientId": "CL-1", "eventId": "EV-1",
        "sessionId": "S-1", "recordingId": f"rec-{uuid.uuid4().hex[:6]}", "recordingType": "audio",
    }
    data.update(overrides)
    return SubmitRequest(**data)


async def insert_job(repo, **overrides) -> dict:
    """Insert a PENDING job that is already due for sending."""
    now = utcnow()
    doc = new_job_document(make_request(**overrides), now, now)
    await repo.insert_job(doc)
    return doc
