from dataclasses import replace
from datetime import timedelta

import httpx
import pytest

from app.api import create_app
from app.models import utcnow
from app.publisher import publish_due
from tests.helpers import make_request


@pytest.fixture
async def client(repo, queue, settings):
    app = create_app(settings, repo=repo, sender=queue)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


def payload(**overrides) -> dict:
    return make_request(**overrides).model_dump()


async def test_submit_audio_creates_job_and_sends_it(client, queue):
    response = await client.post("/jobs", json=payload(sessionId="S-1", recordingId="rec-1"))
    assert response.status_code == 201
    body = response.json()
    assert body["id"] == "S-1:rec-1"
    assert body["status"] == "PENDING"
    assert body["sendMe"] is False
    assert queue.sent == ["S-1:rec-1"]


async def test_consent_recording_is_skipped(client, repo, queue):
    response = await client.post("/jobs", json=payload(sessionId="S-2", recordingId="c-1", recordingType="consent"))
    assert response.status_code == 200
    assert response.json() == {"status": "SKIPPED", "id": None}
    assert await repo.get("S-2:c-1") is None
    assert queue.sent == []


async def test_duplicate_submit_returns_the_existing_job(client, queue):
    data = payload()
    first = await client.post("/jobs", json=data)
    second = await client.post("/jobs", json=data)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["id"] == second.json()["id"]
    assert len(queue.sent) == 1


async def test_send_failure_leaves_the_job_for_the_publisher(client, queue, repo):
    queue.fail = True
    response = await client.post("/jobs", json=payload())
    assert response.status_code == 201
    assert response.json()["sendMe"] is True
    queue.fail = False
    job_id = response.json()["id"]
    await repo.jobs.update_one({"_id": job_id}, {"$set": {"sendAfter": utcnow() - timedelta(seconds=1)}})
    assert await publish_due(repo, queue) == 1
    assert queue.sent == [job_id]


async def test_get_job_and_404(client):
    job_id = (await client.post("/jobs", json=payload())).json()["id"]
    response = await client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert set(response.json()["stages"]) == {"transcription", "summary", "profiling", "compliance", "behaviour"}
    assert (await client.get("/jobs/missing")).status_code == 404


async def test_list_recent_jobs(client):
    await client.post("/jobs", json=payload())
    await client.post("/jobs", json=payload())
    assert len((await client.get("/jobs")).json()) == 2


async def test_unknown_fault_is_rejected(client):
    data = payload()
    data["faults"] = {"summary": {"type": "nope", "times": 1}}
    assert (await client.post("/jobs", json=data)).status_code == 422


async def test_database_down_returns_503(client, repo, settings):
    repo.settings = replace(settings, fault_cosmos_429_rate=1.0)
    assert (await client.post("/jobs", json=payload())).status_code == 503


async def test_ui_is_served(client):
    response = await client.get("/")
    assert response.status_code == 200
    assert "<html" in response.text
