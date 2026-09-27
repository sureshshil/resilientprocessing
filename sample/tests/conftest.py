import os
import uuid

import pytest
from pymongo import AsyncMongoClient

from app.config import Settings
from app.repository import JobRepository

MONGO_URL = os.environ.get("TEST_MONGO_URL", "mongodb://localhost:27017")


@pytest.fixture
def settings():
    return Settings(
        worker_id="test-worker",
        lease_seconds=30,
        heartbeat_seconds=0.05,
        stage_tries=3,
        stage_backoff_seconds=0,
        max_attempts=3,
        retry_base_seconds=30,
        retry_cap_seconds=300,
        stage_seconds=0,
        unavailable_pause_seconds=0,
        mongo_tries=3,
        mongo_backoff_seconds=0,
    )


@pytest.fixture
async def repo(settings):
    client = AsyncMongoClient(MONGO_URL, tz_aware=True, serverSelectionTimeoutMS=2000)
    db = client[f"fpa_test_{uuid.uuid4().hex[:8]}"]
    repository = JobRepository(db, settings)
    await repository.ensure_indexes()
    yield repository
    await client.drop_database(db.name)
    await client.close()


class FakeQueue:
    def __init__(self):
        self.sent: list[str] = []
        self.fail = False

    async def send(self, job_id: str) -> None:
        if self.fail:
            raise ConnectionError("queue down")
        self.sent.append(job_id)


@pytest.fixture
def queue():
    return FakeQueue()
