# Resilient Pipeline Sample Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `sample/`, a runnable local FPA post-recording pipeline that demonstrates checkpoints, lease + fencing, outbox, delayed retry, and a recovery sweeper.

**Architecture:** An API app (FastAPI) writes one Mongo document per audio recording, sends a Service Bus message at once, and runs a publisher loop (outbox) and a sweeper loop. A worker app consumes the queue, claims the job with a lease token, runs 5 fake stages with fenced checkpoint writes, and settles the message only after Mongo is updated. Spec: `docs/superpowers/specs/2026-09-27-resilient-pipeline-sample-design.md`.

**Tech Stack:** Python 3.11+ (developed on 3.14), FastAPI, pymongo async (`AsyncMongoClient`), azure-servicebus (aio), openai (exception classes only), pytest + pytest-asyncio, Docker Compose (MongoDB 7, Azure Service Bus emulator + SQL Server).

All commands run from `sample/` in PowerShell unless stated. Tests need `docker compose up -d mongo`.

---

### Task 1: Scaffold, infrastructure and config

**Files:**
- Create: `sample/requirements.txt`, `sample/pytest.ini`, `sample/docker-compose.yml`, `sample/servicebus-config.json`, `sample/.env.example`
- Create: `sample/app/__init__.py` (empty), `sample/app/config.py`
- Create: `sample/tests/__init__.py` (empty), `sample/tests/test_config.py`

- [ ] **Step 1: Create project files**

`sample/requirements.txt`:
```
fastapi>=0.115
uvicorn>=0.30
pymongo>=4.13
azure-servicebus>=7.14
azure-identity>=1.19
openai>=1.50
httpx>=0.27
python-dotenv>=1.0
pytest>=8.3
pytest-asyncio>=0.24
```

`sample/pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
asyncio_default_fixture_loop_scope = function
pythonpath = .
testpaths = tests
```

`sample/docker-compose.yml`:
```yaml
name: fpa-sample
services:
  mongo:
    image: mongo:7
    ports:
      - "27017:27017"

  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "${ACCEPT_EULA:-N}"
      MSSQL_SA_PASSWORD: "${MSSQL_SA_PASSWORD:-Fpa_Sample_Passw0rd!}"

  servicebus:
    image: mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
    depends_on:
      - mssql
    ports:
      - "5672:5672"
      - "5300:5300"
    environment:
      SQL_SERVER: mssql
      MSSQL_SA_PASSWORD: "${MSSQL_SA_PASSWORD:-Fpa_Sample_Passw0rd!}"
      ACCEPT_EULA: "${ACCEPT_EULA:-N}"
    volumes:
      - ./servicebus-config.json:/ServiceBus_Emulator/ConfigFiles/Config.json
```

`sample/servicebus-config.json`:
```json
{
  "UserConfig": {
    "Namespaces": [
      {
        "Name": "sbemulatorns",
        "Queues": [
          {
            "Name": "fpa-pipeline",
            "Properties": {
              "DeadLetteringOnMessageExpiration": false,
              "DefaultMessageTimeToLive": "PT1H",
              "DuplicateDetectionHistoryTimeWindow": "PT20S",
              "ForwardDeadLetteredMessagesTo": "",
              "ForwardTo": "",
              "LockDuration": "PT1M",
              "MaxDeliveryCount": 10,
              "RequiresDuplicateDetection": false,
              "RequiresSession": false
            }
          }
        ],
        "Topics": []
      }
    ],
    "Logging": {
      "Type": "Console"
    }
  }
}
```

`sample/.env.example`:
```
# App settings (read by app/config.py) — copy to .env
MONGO_URL=mongodb://localhost:27017
MONGO_DB=fpa_sample
SERVICEBUS_CONNECTION_STRING=Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
SERVICEBUS_QUEUE=fpa-pipeline

# Timings in seconds (demo-friendly defaults)
LEASE_SECONDS=60
HEARTBEAT_SECONDS=20
STAGE_TRIES=3
MAX_ATTEMPTS=5
RETRY_BASE_SECONDS=30
RETRY_CAP_SECONDS=300
STAGE_SECONDS=3
FAULT_COSMOS_429_RATE=0.0

# Docker Compose (Service Bus emulator + SQL Server).
# Set ACCEPT_EULA=Y only after reading the EULAs linked in README.md.
ACCEPT_EULA=N
MSSQL_SA_PASSWORD=Fpa_Sample_Passw0rd!
```

- [ ] **Step 2: Create venv, install, start Mongo**

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
docker compose up -d mongo
```
Expected: install succeeds; `docker compose ps` shows `mongo` running.

- [ ] **Step 3: Write failing config test**

`sample/tests/test_config.py`:
```python
from app.config import load_settings


def test_env_overrides_defaults(monkeypatch):
    monkeypatch.setenv("LEASE_SECONDS", "90")
    monkeypatch.setenv("STAGE_TRIES", "5")
    monkeypatch.setenv("WORKER_ID", "w1")
    settings = load_settings()
    assert settings.lease_seconds == 90.0
    assert settings.stage_tries == 5
    assert settings.worker_id == "w1"


def test_worker_id_has_a_default(monkeypatch):
    monkeypatch.delenv("WORKER_ID", raising=False)
    assert load_settings().worker_id
```

Run: `.\.venv\Scripts\python -m pytest tests/test_config.py -v` → FAIL (`ModuleNotFoundError: app.config`).

- [ ] **Step 4: Implement config**

`sample/app/config.py`:
```python
"""Settings loaded from environment variables (and an optional .env file)."""
import os
import socket
from dataclasses import dataclass, fields

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "fpa_sample"
    servicebus_connection_string: str = ""
    servicebus_namespace: str = ""
    servicebus_queue: str = "fpa-pipeline"
    worker_id: str = ""
    lease_seconds: float = 60
    heartbeat_seconds: float = 20
    stage_tries: int = 3
    stage_backoff_seconds: float = 1
    max_attempts: int = 5
    retry_base_seconds: float = 30
    retry_cap_seconds: float = 300
    publish_interval_seconds: float = 2
    publish_grace_seconds: float = 5
    sweep_interval_seconds: float = 60
    stuck_running_seconds: float = 120
    stuck_unsent_seconds: float = 600
    stage_seconds: float = 3
    unavailable_pause_seconds: float = 10
    mongo_tries: int = 3
    mongo_backoff_seconds: float = 0.5
    fault_cosmos_429_rate: float = 0.0


def load_settings() -> Settings:
    """Each field can be overridden by the upper-case environment variable of the same name."""
    load_dotenv()
    values = {}
    for field in fields(Settings):
        raw = os.environ.get(field.name.upper())
        if raw:
            values[field.name] = field.type(raw)
    values.setdefault("worker_id", f"{socket.gethostname()}-{os.getpid()}")
    return Settings(**values)
```

- [ ] **Step 5: Run test** → PASS. **Step 6: Commit** `git add sample && git commit -m "sample: scaffold, docker compose and settings"`

---

### Task 2: Models and errors

**Files:** Create `sample/app/models.py`, `sample/app/errors.py`, `sample/tests/helpers.py`, `sample/tests/test_models.py`

- [ ] **Step 1: Failing tests**

`sample/tests/helpers.py`:
```python
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
```

`sample/tests/test_models.py`:
```python
import pytest
from pydantic import ValidationError

from app.errors import PermanentError, TransientError, is_transient, safe_error
from app.models import STAGES, Status, new_job_document, utcnow
from tests.helpers import make_request


def test_job_id_combines_session_and_recording():
    assert make_request(sessionId="S-9", recordingId="rec-1").job_id == "S-9:rec-1"


def test_new_job_document_starts_pending_and_flagged_for_send():
    now = utcnow()
    doc = new_job_document(make_request(), now, now)
    assert list(doc["stages"]) == list(STAGES)
    assert all(s["status"] == Status.PENDING and s["tries"] == 0 for s in doc["stages"].values())
    assert doc["status"] == Status.PENDING
    assert doc["sendMe"] is True
    assert doc["owner"] is None
    assert doc["attempts"] == 0
    assert "faults" not in doc["key"]


def test_utcnow_has_millisecond_precision():
    assert utcnow().microsecond % 1000 == 0


def test_unknown_fault_type_is_rejected():
    with pytest.raises(ValidationError):
        make_request(faults={"summary": {"type": "nope"}})


def test_classification_of_our_errors():
    assert is_transient(TransientError("x")) is True
    assert is_transient(RuntimeError("unknown")) is True
    assert is_transient(PermanentError("x")) is False


def test_safe_error_keeps_only_our_messages():
    assert safe_error(PermanentError("audio is empty")) == "PermanentError: audio is empty"
    assert safe_error(RuntimeError("customer said something private")) == "RuntimeError"
```

Run: `.\.venv\Scripts\python -m pytest tests/test_models.py -v` → FAIL (imports).

- [ ] **Step 2: Implement**

`sample/app/models.py`:
```python
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
```

`sample/app/errors.py`:
```python
"""Error types and the transient/permanent decision."""
import openai


class TransientError(Exception):
    """A failure that may succeed if tried again later."""


class PermanentError(Exception):
    """A failure that will never succeed for this input."""


class LeaseLost(Exception):
    """This worker no longer owns the job; another worker has claimed it."""


class RepositoryUnavailable(Exception):
    """MongoDB/Cosmos kept failing after the repository's own retries."""


def is_transient(exc: BaseException) -> bool:
    """Rate limits, timeouts, 5xx and unknown errors are retried (bounded by the attempt budget).
    Invalid input and content-filter rejections are not."""
    return not isinstance(exc, (PermanentError, openai.BadRequestError))


def safe_error(exc: BaseException) -> str:
    """Short text for the job document. Provider messages can contain customer data,
    so only our own exceptions keep their message."""
    if isinstance(exc, (TransientError, PermanentError)):
        return f"{type(exc).__name__}: {exc}"[:300]
    return type(exc).__name__
```

- [ ] **Step 3: Run** `.\.venv\Scripts\python -m pytest tests/test_models.py -v` → PASS. **Step 4: Commit** `sample: job document model and error classification`

---

### Task 3: Repository

**Files:** Create `sample/app/repository.py`, `sample/tests/conftest.py`, `sample/tests/test_repository.py`

- [ ] **Step 1: Fixtures**

`sample/tests/conftest.py`:
```python
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
```

- [ ] **Step 2: Failing tests**

`sample/tests/test_repository.py`:
```python
from dataclasses import replace
from datetime import timedelta

import pytest
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from app.errors import LeaseLost, RepositoryUnavailable
from app.models import Status, utcnow
from tests.helpers import insert_job


async def test_insert_duplicate_returns_false(repo):
    doc = await insert_job(repo)
    assert await repo.insert_job(doc) is False


async def test_claim_sets_owner_and_running(repo):
    doc = await insert_job(repo)
    claimed = await repo.claim(doc["_id"], "w1", utcnow())
    assert claimed["status"] == Status.RUNNING
    assert claimed["owner"]["workerId"] == "w1"
    assert claimed["owner"]["token"]
    assert claimed["sendMe"] is False


async def test_claim_refused_while_lease_valid_then_allowed_after_expiry(repo, settings):
    doc = await insert_job(repo)
    now = utcnow()
    assert await repo.claim(doc["_id"], "w1", now) is not None
    assert await repo.claim(doc["_id"], "w2", now) is None
    later = now + timedelta(seconds=settings.lease_seconds + 1)
    assert (await repo.claim(doc["_id"], "w2", later))["owner"]["workerId"] == "w2"


async def test_claim_refused_for_finished_job(repo):
    doc = await insert_job(repo)
    await repo.jobs.update_one({"_id": doc["_id"]}, {"$set": {"status": Status.COMPLETED}})
    assert await repo.claim(doc["_id"], "w1", utcnow()) is None


async def test_stale_owner_write_is_rejected(repo, settings):
    doc = await insert_job(repo)
    now = utcnow()
    first = await repo.claim(doc["_id"], "w1", now)
    await repo.claim(doc["_id"], "w2", now + timedelta(seconds=settings.lease_seconds + 1))
    with pytest.raises(LeaseLost):
        await repo.complete_stage(doc["_id"], first["owner"]["token"], "transcription", "x", utcnow())
    stored = await repo.get(doc["_id"])
    assert stored["stages"]["transcription"]["status"] == Status.PENDING


async def test_start_stage_counts_tries(repo):
    doc = await insert_job(repo)
    token = (await repo.claim(doc["_id"], "w1", utcnow()))["owner"]["token"]
    assert await repo.start_stage(doc["_id"], token, "summary", utcnow()) == 1
    assert await repo.start_stage(doc["_id"], token, "summary", utcnow()) == 2
    assert (await repo.get(doc["_id"]))["stages"]["summary"]["status"] == Status.RUNNING


async def test_heartbeat_extends_lease_and_detects_loss(repo, settings):
    doc = await insert_job(repo)
    now = utcnow()
    token = (await repo.claim(doc["_id"], "w1", now))["owner"]["token"]
    later = now + timedelta(seconds=10)
    assert await repo.heartbeat(doc["_id"], token, later) is True
    stored = await repo.get(doc["_id"])
    assert stored["owner"]["expiresAt"] == later + timedelta(seconds=settings.lease_seconds)
    assert await repo.heartbeat(doc["_id"], "not-my-token", later) is False


async def test_schedule_retry_releases_owner_and_flags_send(repo):
    doc = await insert_job(repo)
    token = (await repo.claim(doc["_id"], "w1", utcnow()))["owner"]["token"]
    send_after = utcnow() + timedelta(seconds=30)
    await repo.schedule_retry(doc["_id"], token, "summary", "InternalServerError", send_after, utcnow())
    stored = await repo.get(doc["_id"])
    assert stored["status"] == Status.RETRYING
    assert stored["owner"] is None
    assert stored["sendMe"] is True
    assert stored["sendAfter"] == send_after
    assert stored["attempts"] == 1
    assert stored["stages"]["summary"]["status"] == Status.PENDING


async def test_due_for_send_and_mark_sent(repo):
    doc = await insert_job(repo)
    due = await repo.due_for_send(utcnow())
    assert [d["_id"] for d in due] == [doc["_id"]]
    assert await repo.mark_sent(doc["_id"], due[0]["sendAfter"], utcnow()) is True
    assert await repo.due_for_send(utcnow()) == []


async def test_mark_sent_ignores_a_changed_send_after(repo):
    doc = await insert_job(repo)
    due = await repo.due_for_send(utcnow())
    await repo.jobs.update_one(
        {"_id": doc["_id"]}, {"$set": {"sendAfter": utcnow() + timedelta(minutes=1)}}
    )
    assert await repo.mark_sent(doc["_id"], due[0]["sendAfter"], utcnow()) is False


async def test_throttling_is_retried(repo):
    calls = 0

    async def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OperationFailure("Request rate is large", code=16500)
        return "ok"

    assert await repo._call(flaky) == "ok"
    assert calls == 3


async def test_persistent_outage_raises_unavailable(repo):
    async def down():
        raise ServerSelectionTimeoutError("no servers")

    with pytest.raises(RepositoryUnavailable):
        await repo._call(down)


async def test_other_errors_are_not_retried(repo):
    calls = 0

    async def broken():
        nonlocal calls
        calls += 1
        raise OperationFailure("bad query", code=2)

    with pytest.raises(OperationFailure):
        await repo._call(broken)
    assert calls == 1


async def test_injected_cosmos_throttling(repo, settings):
    doc = await insert_job(repo)
    repo.settings = replace(settings, fault_cosmos_429_rate=1.0)
    with pytest.raises(RepositoryUnavailable):
        await repo.get(doc["_id"])
```

Run: `.\.venv\Scripts\python -m pytest tests/test_repository.py -v` → FAIL (import).

- [ ] **Step 3: Implement**

`sample/app/repository.py`:
```python
"""All MongoDB access. Every write for a running job is fenced by the lease token."""
import asyncio
import logging
import random
import uuid
from datetime import datetime, timedelta

from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.errors import AutoReconnect, DuplicateKeyError, OperationFailure

from app.config import Settings
from app.errors import LeaseLost, RepositoryUnavailable
from app.models import Status

logger = logging.getLogger(__name__)

THROTTLED = 16500  # Cosmos DB for MongoDB: "request rate is large"


def _retryable(exc: Exception) -> bool:
    # AutoReconnect covers NetworkTimeout and ServerSelectionTimeoutError.
    if isinstance(exc, AutoReconnect):
        return True
    return isinstance(exc, OperationFailure) and exc.code == THROTTLED


class JobRepository:
    def __init__(self, db, settings: Settings, rng: random.Random | None = None):
        self.jobs = db["jobs"]
        self.settings = settings
        self.rng = rng or random.Random()

    async def _call(self, operation):
        """Run `operation` (a zero-argument coroutine function), retrying throttling and
        network errors. Fenced updates are safe to repeat; a repeated `$inc` may over-count
        `tries`, which is acceptable for this sample."""
        tries = self.settings.mongo_tries
        for attempt in range(tries):
            try:
                if self.rng.random() < self.settings.fault_cosmos_429_rate:
                    raise OperationFailure("Injected: request rate is large", code=THROTTLED)
                return await operation()
            except Exception as exc:
                if not _retryable(exc):
                    raise
                if attempt == tries - 1:
                    raise RepositoryUnavailable(type(exc).__name__) from exc
                logger.warning("Mongo call failed (%s); retrying", type(exc).__name__)
                await asyncio.sleep(self.settings.mongo_backoff_seconds * 2**attempt)

    async def ensure_indexes(self) -> None:
        # Cosmos DB for MongoDB (RU) indexes only _id by default.
        await self._call(lambda: self.jobs.create_indexes([
            IndexModel([("sendMe", ASCENDING), ("sendAfter", ASCENDING)]),
            IndexModel([("status", ASCENDING), ("owner.expiresAt", ASCENDING)]),
            IndexModel([("status", ASCENDING), ("updatedAt", ASCENDING)]),
            IndexModel([("createdAt", DESCENDING)]),
        ]))

    async def insert_job(self, doc: dict) -> bool:
        """False when a job with the same id already exists."""
        try:
            await self._call(lambda: self.jobs.insert_one(doc))
        except DuplicateKeyError:
            return False
        return True

    async def get(self, job_id: str) -> dict | None:
        return await self._call(lambda: self.jobs.find_one({"_id": job_id}))

    async def recent(self, limit: int = 20) -> list[dict]:
        return await self._call(
            lambda: self.jobs.find().sort("createdAt", DESCENDING).limit(limit).to_list()
        )

    # --- outbox ---------------------------------------------------------------------------

    async def due_for_send(self, now: datetime, limit: int = 50) -> list[dict]:
        return await self._call(lambda: self.jobs.find(
            {"sendMe": True, "sendAfter": {"$lte": now}}, {"sendAfter": 1}
        ).limit(limit).to_list())

    async def mark_sent(self, job_id: str, send_after: datetime, now: datetime) -> bool:
        """Clear the flag only if nobody asked for a newer send in the meantime."""
        result = await self._call(lambda: self.jobs.update_one(
            {"_id": job_id, "sendMe": True, "sendAfter": send_after},
            {"$set": {"sendMe": False, "updatedAt": now}},
        ))
        return result.modified_count == 1

    # --- ownership ------------------------------------------------------------------------

    async def claim(self, job_id: str, worker_id: str, now: datetime) -> dict | None:
        """Take ownership if nobody holds a valid lease. Returns the claimed document."""
        owner = {
            "token": uuid.uuid4().hex,
            "workerId": worker_id,
            "expiresAt": now + timedelta(seconds=self.settings.lease_seconds),
        }
        return await self._call(lambda: self.jobs.find_one_and_update(
            {
                "_id": job_id,
                "status": {"$in": [Status.PENDING, Status.RETRYING, Status.RUNNING]},
                "$or": [{"owner": None}, {"owner.expiresAt": {"$lt": now}}],
            },
            {"$set": {"status": Status.RUNNING, "owner": owner, "sendMe": False, "updatedAt": now}},
            return_document=ReturnDocument.AFTER,
        ))

    async def _fenced(self, job_id: str, token: str, update: dict, now: datetime) -> None:
        update.setdefault("$set", {})["updatedAt"] = now
        result = await self._call(
            lambda: self.jobs.update_one({"_id": job_id, "owner.token": token}, update)
        )
        if result.matched_count == 0:
            raise LeaseLost(job_id)

    async def heartbeat(self, job_id: str, token: str, now: datetime) -> bool:
        expires = now + timedelta(seconds=self.settings.lease_seconds)
        try:
            await self._fenced(job_id, token, {"$set": {"owner.expiresAt": expires}}, now)
        except LeaseLost:
            return False
        return True

    # --- stage checkpoints ----------------------------------------------------------------

    async def start_stage(self, job_id: str, token: str, stage: str, now: datetime) -> int:
        """Mark the stage RUNNING and return its total try count, including this one."""
        doc = await self._call(lambda: self.jobs.find_one_and_update(
            {"_id": job_id, "owner.token": token},
            {
                "$set": {f"stages.{stage}.status": Status.RUNNING, "updatedAt": now},
                "$inc": {f"stages.{stage}.tries": 1},
            },
            projection={f"stages.{stage}.tries": 1},
            return_document=ReturnDocument.AFTER,
        ))
        if doc is None:
            raise LeaseLost(job_id)
        return doc["stages"][stage]["tries"]

    async def complete_stage(self, job_id, token, stage, output, now) -> None:
        await self._fenced(job_id, token, {"$set": {
            f"stages.{stage}.status": Status.COMPLETED,
            f"stages.{stage}.output": output,
            f"stages.{stage}.finishedAt": now,
        }}, now)

    # --- final outcomes -------------------------------------------------------------------

    async def finish_completed(self, job_id, token, now) -> None:
        await self._fenced(job_id, token, {"$set": {
            "status": Status.COMPLETED, "owner": None, "error": None,
        }}, now)

    async def finish_failed(self, job_id, token, stage, error, now) -> None:
        await self._fenced(job_id, token, {"$set": {
            "status": Status.FAILED, f"stages.{stage}.status": Status.FAILED,
            "owner": None, "error": error,
        }}, now)

    async def schedule_retry(self, job_id, token, stage, error, send_after, now) -> None:
        """One atomic write: RETRYING + due time + outbox flag + release ownership."""
        await self._fenced(job_id, token, {
            "$set": {
                "status": Status.RETRYING, f"stages.{stage}.status": Status.PENDING,
                "owner": None, "error": error, "sendMe": True, "sendAfter": send_after,
            },
            "$inc": {"attempts": 1},
        }, now)

    # --- recovery -------------------------------------------------------------------------

    async def sweep(self, now: datetime, running_before: datetime, unsent_before: datetime) -> int:
        """Flag stuck jobs for the publisher. Never sends and never changes status."""
        flag = {"$set": {"sendMe": True, "sendAfter": now, "updatedAt": now}}
        dead_owner = await self._call(lambda: self.jobs.update_many({
            "status": Status.RUNNING, "sendMe": False,
            "owner.expiresAt": {"$lt": running_before}, "updatedAt": {"$lt": running_before},
        }, flag))
        lost_message = await self._call(lambda: self.jobs.update_many({
            "status": {"$in": [Status.PENDING, Status.RETRYING]}, "sendMe": False,
            "updatedAt": {"$lt": unsent_before},
        }, flag))
        return dead_owner.modified_count + lost_message.modified_count
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `sample: Mongo repository with lease fencing and outbox queries`

---

### Task 4: Faults and fake stages

**Files:** Create `sample/app/faults.py`, `sample/app/stages.py`, `sample/tests/test_faults.py`

- [ ] **Step 1: Failing tests**

`sample/tests/test_faults.py`:
```python
import openai
import pytest

from app.errors import PermanentError, is_transient, safe_error
from app.faults import apply_fault, build_error
from app.stages import run_stage


@pytest.mark.parametrize("fault, transient", [
    ("llm_429", True), ("llm_timeout", True), ("llm_500", True),
    ("llm_content_blocked", False), ("bad_audio", False),
])
def test_fault_classification(fault, transient):
    assert is_transient(build_error(fault)) is transient


def test_provider_messages_are_not_stored():
    assert safe_error(build_error("llm_500")) == "InternalServerError"


async def test_fault_fires_only_for_first_n_tries():
    spec = {"type": "llm_429", "times": 2}
    for tries in (1, 2):
        with pytest.raises(openai.RateLimitError):
            await apply_fault(spec, tries)
    await apply_fault(spec, 3)


async def test_times_zero_means_every_try():
    with pytest.raises(PermanentError):
        await apply_fault({"type": "bad_audio", "times": 0}, 99)


async def test_no_fault_and_slow_fault_do_not_raise():
    await apply_fault(None, 1)
    await apply_fault({"type": "slow", "times": 1}, 1, slow_seconds=0)


async def test_stage_uses_the_job_fault():
    job = {"_id": "S-1:rec-1", "faults": {"summary": {"type": "llm_timeout", "times": 1}}}
    with pytest.raises(openai.APITimeoutError):
        await run_stage("summary", job, 1, stage_seconds=0)
    assert "summary" in await run_stage("summary", job, 2, stage_seconds=0)
```

- [ ] **Step 2: Implement**

`sample/app/faults.py`:
```python
"""Demo fault injection: makes a stage fail the way a real LLM call would."""
import asyncio

import httpx
import openai

from app.errors import PermanentError

SLOW_SECONDS = 60
_REQUEST = httpx.Request("POST", "https://fake-llm.invalid/v1/chat/completions")


def _response(status_code: int) -> httpx.Response:
    return httpx.Response(status_code, request=_REQUEST)


def build_error(fault_type: str) -> Exception:
    """The same exception classes the real OpenAI SDK raises, so retry code can't tell the difference."""
    if fault_type == "llm_429":
        return openai.RateLimitError("Injected rate limit", response=_response(429), body=None)
    if fault_type == "llm_timeout":
        return openai.APITimeoutError(request=_REQUEST)
    if fault_type == "llm_500":
        return openai.InternalServerError("Injected server error", response=_response(500), body=None)
    if fault_type == "llm_content_blocked":
        return openai.BadRequestError("Injected content filter", response=_response(400), body=None)
    if fault_type == "bad_audio":
        return PermanentError("audio is empty or unreadable")
    raise ValueError(f"Unknown fault type: {fault_type}")


async def apply_fault(spec: dict | None, tries: int, slow_seconds: float = SLOW_SECONDS) -> None:
    """Fail (or stall) this try if the job's fault spec says so.
    `tries` is the stage's total try count, so faults survive worker restarts."""
    if not spec:
        return
    times = spec.get("times", 1)
    if times and tries > times:
        return
    if spec["type"] == "slow":
        await asyncio.sleep(slow_seconds)
        return
    raise build_error(spec["type"])
```

`sample/app/stages.py`:
```python
"""Five fake stages. Replace `run_stage` with the real FPA calls; keep the contract:
return the stage output, or raise (see app.errors.is_transient for how errors are treated)."""
import asyncio

from app.faults import apply_fault


async def run_stage(stage: str, job: dict, tries: int, stage_seconds: float) -> str:
    await apply_fault(job.get("faults", {}).get(stage), tries)
    await asyncio.sleep(stage_seconds)
    return f"{stage} result for {job['_id']} (try {tries})"
```

- [ ] **Step 3: Run** `pytest tests/test_faults.py -v` → PASS. **Step 4: Commit** `sample: fault injection and fake stages`

---

### Task 5: Runner

**Files:** Create `sample/app/runner.py`, `sample/tests/test_runner.py`

- [ ] **Step 1: Failing tests**

`sample/tests/test_runner.py`:
```python
from datetime import timedelta

from app.models import Status, utcnow
from app.runner import Lease, run_stages
from tests.helpers import insert_job


async def claim(repo, doc, worker="w1"):
    claimed = await repo.claim(doc["_id"], worker, utcnow())
    return claimed, Lease(doc["_id"], claimed["owner"]["token"])


async def test_happy_path_completes_all_stages(repo, settings):
    job, lease = await claim(repo, await insert_job(repo))
    outcome = await run_stages(job, lease, repo, settings)
    assert outcome.kind == "completed"
    stored = await repo.get(job["_id"])
    assert all(s["status"] == Status.COMPLETED and s["tries"] == 1 for s in stored["stages"].values())


async def test_resume_skips_completed_stages(repo, settings):
    doc = await insert_job(repo)
    await repo.jobs.update_one({"_id": doc["_id"]}, {"$set": {
        "stages.transcription.status": Status.COMPLETED, "stages.summary.status": Status.COMPLETED,
    }})
    job, lease = await claim(repo, doc)
    ran = []

    async def stage_fn(stage, job, tries):
        ran.append(stage)
        return "ok"

    assert (await run_stages(job, lease, repo, settings, stage_fn)).kind == "completed"
    assert ran == ["profiling", "compliance", "behaviour"]


async def test_rate_limit_is_retried_inside_the_stage(repo, settings):
    doc = await insert_job(repo, faults={"summary": {"type": "llm_429", "times": 2}})
    job, lease = await claim(repo, doc)
    assert (await run_stages(job, lease, repo, settings)).kind == "completed"
    assert (await repo.get(doc["_id"]))["stages"]["summary"]["tries"] == 3


async def test_repeated_transient_error_asks_for_a_delayed_retry(repo, settings):
    doc = await insert_job(repo, faults={"summary": {"type": "llm_500", "times": 0}})
    job, lease = await claim(repo, doc)
    outcome = await run_stages(job, lease, repo, settings)
    assert (outcome.kind, outcome.stage, outcome.error) == ("retry", "summary", "InternalServerError")
    stored = await repo.get(doc["_id"])
    assert stored["stages"]["transcription"]["status"] == Status.COMPLETED
    assert stored["stages"]["summary"]["tries"] == settings.stage_tries


async def test_permanent_error_fails_without_retry(repo, settings):
    doc = await insert_job(repo, faults={"compliance": {"type": "llm_content_blocked", "times": 0}})
    job, lease = await claim(repo, doc)
    outcome = await run_stages(job, lease, repo, settings)
    assert (outcome.kind, outcome.stage) == ("failed", "compliance")
    assert (await repo.get(doc["_id"]))["stages"]["compliance"]["tries"] == 1


async def test_known_lost_lease_stops_before_running(repo, settings):
    job, lease = await claim(repo, await insert_job(repo))
    lease.lost = True
    assert (await run_stages(job, lease, repo, settings)).kind == "lease_lost"
    assert (await repo.get(job["_id"]))["stages"]["transcription"]["tries"] == 0


async def test_stale_worker_cannot_commit_its_result(repo, settings):
    doc = await insert_job(repo)
    job, lease = await claim(repo, doc)

    async def stage_fn(stage, job, tries):
        # While this worker is "frozen", its lease expires and another worker claims the job.
        later = utcnow() + timedelta(seconds=settings.lease_seconds + 1)
        await repo.claim(doc["_id"], "w2", later)
        return "stale result"

    assert (await run_stages(job, lease, repo, settings, stage_fn)).kind == "lease_lost"
    stored = await repo.get(doc["_id"])
    assert stored["owner"]["workerId"] == "w2"
    assert stored["stages"]["transcription"]["output"] is None
```

- [ ] **Step 2: Implement**

`sample/app/runner.py`:
```python
"""Runs the five stages for one claimed job, skipping completed checkpoints.
Knows nothing about Service Bus: it returns an Outcome for the worker to act on."""
import asyncio
import logging
import random
from dataclasses import dataclass

from app.config import Settings
from app.errors import LeaseLost, is_transient, safe_error
from app.models import STAGES, Status, utcnow
from app.repository import JobRepository
from app.stages import run_stage

logger = logging.getLogger(__name__)


@dataclass
class Lease:
    job_id: str
    token: str
    lost: bool = False  # set by the heartbeat when another worker took over


@dataclass
class Outcome:
    kind: str  # "completed" | "failed" | "retry" | "lease_lost"
    stage: str | None = None
    error: str | None = None


async def run_stages(job: dict, lease: Lease, repo: JobRepository, settings: Settings,
                     stage_fn=None) -> Outcome:
    """`job` must be the document returned by the claim, so checkpoints are current.
    RepositoryUnavailable is not caught here; the worker handles it."""
    if stage_fn is None:
        async def stage_fn(stage, job, tries):
            return await run_stage(stage, job, tries, settings.stage_seconds)

    try:
        for stage in STAGES:
            if job["stages"][stage]["status"] == Status.COMPLETED:
                continue
            outcome = await _run_stage(stage, job, lease, repo, settings, stage_fn)
            if outcome is not None:
                return outcome
    except LeaseLost:
        logger.warning("Lost the lease on %s; stopping without writing", lease.job_id)
        return Outcome("lease_lost")
    return Outcome("completed")


async def _run_stage(stage, job, lease, repo, settings, stage_fn) -> Outcome | None:
    """None when the stage completed; otherwise the outcome that ends this run."""
    last_error = None
    for attempt in range(settings.stage_tries):
        if lease.lost:
            raise LeaseLost(lease.job_id)
        tries = await repo.start_stage(lease.job_id, lease.token, stage, utcnow())
        try:
            output = await stage_fn(stage, job, tries)
        except Exception as exc:
            if not is_transient(exc):
                return Outcome("failed", stage, safe_error(exc))
            last_error = safe_error(exc)
            logger.warning("Stage %s of %s failed (%s), try %d/%d", stage, lease.job_id,
                           last_error, attempt + 1, settings.stage_tries)
            if attempt < settings.stage_tries - 1:
                await asyncio.sleep(settings.stage_backoff_seconds * 2**attempt * random.uniform(0.5, 1.5))
            continue
        await repo.complete_stage(lease.job_id, lease.token, stage, output, utcnow())
        return None
    return Outcome("retry", stage, last_error)
```

- [ ] **Step 3: Run** `pytest tests/test_runner.py -v` → PASS. **Step 4: Commit** `sample: stage runner with checkpoints and in-stage retry`

---

### Task 6: Message processing (worker logic)

**Files:** Create `sample/app/processing.py`, `sample/tests/test_processing.py`

(Kept separate from `worker.py` so it can be tested without Service Bus.)

- [ ] **Step 1: Failing tests**

`sample/tests/test_processing.py`:
```python
import asyncio
import json
from dataclasses import replace
from datetime import timedelta

from app.models import Status, utcnow
from app.processing import COMPLETE, Settlement, process_message
from tests.helpers import insert_job


def body(job_id: str) -> str:
    return json.dumps({"jobId": job_id})


async def make_due(repo, job_id):
    await repo.jobs.update_one({"_id": job_id}, {"$set": {"sendAfter": utcnow() - timedelta(seconds=1)}})


async def test_invalid_messages_are_dead_lettered(repo, settings):
    for raw in ["not json", "[]", '{"jobId": ""}', '{"other": 1}']:
        assert await process_message(raw, repo, settings) == Settlement("dead_letter", "INVALID_MESSAGE")


async def test_unknown_job_is_dead_lettered(repo, settings):
    assert await process_message(body("nope"), repo, settings) == Settlement("dead_letter", "JOB_NOT_FOUND")


async def test_completes_job_then_message(repo, settings):
    doc = await insert_job(repo)
    assert await process_message(body(doc["_id"]), repo, settings) == COMPLETE
    stored = await repo.get(doc["_id"])
    assert stored["status"] == Status.COMPLETED
    assert stored["owner"] is None


async def test_duplicate_message_for_finished_job_does_nothing(repo, settings):
    doc = await insert_job(repo)
    await process_message(body(doc["_id"]), repo, settings)
    assert await process_message(body(doc["_id"]), repo, settings) == COMPLETE
    assert (await repo.get(doc["_id"]))["stages"]["summary"]["tries"] == 1


async def test_transient_failure_schedules_a_delayed_retry(repo, settings):
    doc = await insert_job(repo, faults={"summary": {"type": "llm_500", "times": 0}})
    before = utcnow()
    assert await process_message(body(doc["_id"]), repo, settings) == COMPLETE
    stored = await repo.get(doc["_id"])
    assert stored["status"] == Status.RETRYING
    assert stored["attempts"] == 1
    assert stored["sendMe"] is True
    assert stored["owner"] is None
    assert stored["sendAfter"] >= before + timedelta(seconds=settings.retry_base_seconds)


async def test_retry_message_arriving_early_is_ignored(repo, settings):
    doc = await insert_job(repo, faults={"summary": {"type": "llm_500", "times": 0}})
    await process_message(body(doc["_id"]), repo, settings)
    assert await process_message(body(doc["_id"]), repo, settings) == COMPLETE
    stored = await repo.get(doc["_id"])
    assert stored["attempts"] == 1
    assert stored["stages"]["summary"]["tries"] == settings.stage_tries


async def test_retry_later_succeeds_and_skips_done_stages(repo, settings):
    doc = await insert_job(repo, faults={"summary": {"type": "llm_500", "times": 3}})
    await process_message(body(doc["_id"]), repo, settings)
    await make_due(repo, doc["_id"])
    await process_message(body(doc["_id"]), repo, settings)
    stored = await repo.get(doc["_id"])
    assert stored["status"] == Status.COMPLETED
    assert stored["stages"]["transcription"]["tries"] == 1
    assert stored["stages"]["summary"]["tries"] == 4


async def test_gives_up_after_max_attempts(repo, settings):
    doc = await insert_job(repo, faults={"summary": {"type": "llm_500", "times": 0}})
    for _ in range(settings.max_attempts):
        await process_message(body(doc["_id"]), repo, settings)
        await make_due(repo, doc["_id"])
    stored = await repo.get(doc["_id"])
    assert stored["status"] == Status.FAILED
    assert stored["error"].startswith(f"gave up after {settings.max_attempts} attempts")


async def test_permanent_failure_marks_job_failed(repo, settings):
    doc = await insert_job(repo, faults={"transcription": {"type": "bad_audio", "times": 0}})
    assert await process_message(body(doc["_id"]), repo, settings) == COMPLETE
    stored = await repo.get(doc["_id"])
    assert stored["status"] == Status.FAILED
    assert stored["error"] == "PermanentError: audio is empty or unreadable"
    assert stored["stages"]["transcription"]["status"] == Status.FAILED


async def test_message_skipped_when_another_worker_owns_the_job(repo, settings):
    doc = await insert_job(repo)
    await repo.claim(doc["_id"], "other-worker", utcnow())
    assert await process_message(body(doc["_id"]), repo, settings) == COMPLETE
    stored = await repo.get(doc["_id"])
    assert stored["owner"]["workerId"] == "other-worker"
    assert stored["stages"]["transcription"]["tries"] == 0


async def test_mongo_outage_abandons_the_message(repo, settings):
    doc = await insert_job(repo)
    repo.settings = replace(settings, fault_cosmos_429_rate=1.0)
    assert await process_message(body(doc["_id"]), repo, settings) == Settlement(
        "abandon", "REPOSITORY_UNAVAILABLE")


async def test_heartbeat_keeps_the_lease_during_a_long_run(repo, settings):
    fast = replace(settings, lease_seconds=0.3, heartbeat_seconds=0.05, stage_seconds=0.2)
    repo.settings = fast
    doc = await insert_job(repo)
    task = asyncio.create_task(process_message(body(doc["_id"]), repo, fast))
    await asyncio.sleep(0.6)  # past the original lease
    assert await repo.claim(doc["_id"], "intruder", utcnow()) is None
    assert await task == COMPLETE
    assert (await repo.get(doc["_id"]))["status"] == Status.COMPLETED
```

- [ ] **Step 2: Implement**

`sample/app/processing.py`:
```python
"""What the worker does with one queue message. Returns how to settle the message."""
import asyncio
import json
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta

from app.config import Settings
from app.errors import LeaseLost, RepositoryUnavailable
from app.models import Status, to_ms, utcnow
from app.repository import JobRepository
from app.runner import Lease, Outcome, run_stages

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settlement:
    action: str  # "complete" | "dead_letter" | "abandon"
    reason: str = ""


COMPLETE = Settlement("complete")


def parse_job_id(body: str) -> str | None:
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    job_id = payload.get("jobId") if isinstance(payload, dict) else None
    return job_id if isinstance(job_id, str) and job_id.strip() else None


def retry_delay(attempts_done: int, settings: Settings) -> float:
    return min(settings.retry_base_seconds * 2**attempts_done, settings.retry_cap_seconds)


async def process_message(body: str, repo: JobRepository, settings: Settings,
                          stage_fn=None) -> Settlement:
    job_id = parse_job_id(body)
    if job_id is None:
        return Settlement("dead_letter", "INVALID_MESSAGE")
    try:
        return await _process(job_id, repo, settings, stage_fn)
    except RepositoryUnavailable:
        # Nothing can be saved. Pause, then hand the message back for redelivery.
        logger.error("Mongo unavailable while processing %s; abandoning", job_id)
        await asyncio.sleep(settings.unavailable_pause_seconds)
        return Settlement("abandon", "REPOSITORY_UNAVAILABLE")


async def _process(job_id, repo, settings, stage_fn) -> Settlement:
    job = await repo.get(job_id)
    if job is None:
        return Settlement("dead_letter", "JOB_NOT_FOUND")
    now = utcnow()
    if job["status"] in (Status.COMPLETED, Status.FAILED):
        return COMPLETE  # duplicate delivery
    if job["status"] == Status.RETRYING and job["sendAfter"] > now:
        return COMPLETE  # early duplicate; the publisher sends again when due

    job = await repo.claim(job_id, settings.worker_id, now)
    if job is None:
        logger.info("Job %s is owned by another worker; skipping", job_id)
        return COMPLETE

    lease = Lease(job_id, job["owner"]["token"])
    heartbeat = asyncio.create_task(_heartbeat(lease, repo, settings))
    try:
        outcome = await run_stages(job, lease, repo, settings, stage_fn)
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat

    try:
        await _record_outcome(job, lease, outcome, repo, settings)
    except LeaseLost:
        logger.warning("Lost the lease on %s before recording the outcome", job_id)
    return COMPLETE


async def _record_outcome(job: dict, lease: Lease, outcome: Outcome, repo, settings) -> None:
    now = utcnow()
    if outcome.kind == "completed":
        await repo.finish_completed(lease.job_id, lease.token, now)
    elif outcome.kind == "failed":
        await repo.finish_failed(lease.job_id, lease.token, outcome.stage, outcome.error, now)
    elif outcome.kind == "retry":
        attempt_number = job["attempts"] + 1
        if attempt_number >= settings.max_attempts:
            error = f"gave up after {attempt_number} attempts: {outcome.error}"
            await repo.finish_failed(lease.job_id, lease.token, outcome.stage, error, now)
        else:
            send_after = to_ms(now + timedelta(seconds=retry_delay(job["attempts"], settings)))
            await repo.schedule_retry(lease.job_id, lease.token, outcome.stage, outcome.error,
                                      send_after, now)
    # "lease_lost": nothing to write; the new owner is responsible for the job.


async def _heartbeat(lease: Lease, repo: JobRepository, settings: Settings) -> None:
    while True:
        await asyncio.sleep(settings.heartbeat_seconds)
        try:
            if not await repo.heartbeat(lease.job_id, lease.token, utcnow()):
                logger.warning("Heartbeat found another owner for %s", lease.job_id)
                lease.lost = True
                return
        except RepositoryUnavailable:
            logger.warning("Heartbeat for %s failed; will try again", lease.job_id)
```

- [ ] **Step 3: Run** `pytest tests/test_processing.py -v` → PASS. **Step 4: Commit** `sample: message processing with lease, heartbeat and retry budget`

---

### Task 7: Publisher, sweeper and loop helper

**Files:** Create `sample/app/loops.py`, `sample/app/publisher.py`, `sample/app/sweeper.py`, `sample/tests/test_publisher.py`, `sample/tests/test_sweeper.py`

- [ ] **Step 1: Failing tests**

`sample/tests/test_publisher.py`:
```python
from datetime import timedelta

from app.models import utcnow
from app.publisher import publish_due
from tests.helpers import insert_job


async def test_publishes_due_jobs_once(repo, queue):
    doc = await insert_job(repo)
    assert await publish_due(repo, queue) == 1
    assert queue.sent == [doc["_id"]]
    assert await publish_due(repo, queue) == 0


async def test_jobs_not_yet_due_wait(repo, queue):
    doc = await insert_job(repo)
    await repo.jobs.update_one({"_id": doc["_id"]}, {"$set": {"sendAfter": utcnow() + timedelta(minutes=1)}})
    assert await publish_due(repo, queue) == 0


async def test_queue_down_keeps_the_job_flagged(repo, queue):
    doc = await insert_job(repo)
    queue.fail = True
    assert await publish_due(repo, queue) == 0
    assert (await repo.get(doc["_id"]))["sendMe"] is True
    queue.fail = False
    assert await publish_due(repo, queue) == 1
```

`sample/tests/test_sweeper.py`:
```python
from datetime import timedelta

from app.models import Status, utcnow
from app.sweeper import sweep_stuck
from tests.helpers import insert_job


async def test_flags_running_job_whose_worker_died(repo, settings):
    doc = await insert_job(repo)
    long_ago = utcnow() - timedelta(seconds=settings.lease_seconds + settings.stuck_running_seconds + 10)
    await repo.claim(doc["_id"], "dead-worker", long_ago)
    assert await sweep_stuck(repo, settings) == 1
    stored = await repo.get(doc["_id"])
    assert stored["sendMe"] is True
    assert stored["status"] == Status.RUNNING


async def test_flags_old_unsent_pending_job(repo, settings):
    doc = await insert_job(repo)
    old = utcnow() - timedelta(seconds=settings.stuck_unsent_seconds + 10)
    await repo.jobs.update_one({"_id": doc["_id"]}, {"$set": {"sendMe": False, "updatedAt": old}})
    assert await sweep_stuck(repo, settings) == 1
    assert (await repo.get(doc["_id"]))["sendMe"] is True


async def test_ignores_healthy_jobs(repo, settings):
    fresh = await insert_job(repo)
    await repo.jobs.update_one({"_id": fresh["_id"]}, {"$set": {"sendMe": False}})
    running = await insert_job(repo)
    await repo.claim(running["_id"], "live-worker", utcnow())
    done = await insert_job(repo)
    old = utcnow() - timedelta(days=1)
    await repo.jobs.update_one({"_id": done["_id"]}, {"$set": {
        "status": Status.COMPLETED, "sendMe": False, "updatedAt": old}})
    assert await sweep_stuck(repo, settings) == 0
```

- [ ] **Step 2: Implement**

`sample/app/loops.py`:
```python
"""Background loop helper for the publisher and sweeper."""
import asyncio
import logging
from contextlib import suppress

logger = logging.getLogger(__name__)


async def run_periodically(name: str, interval: float, fn, stop: asyncio.Event) -> None:
    """Call `fn()` every `interval` seconds until `stop` is set. A failing tick is logged, not fatal."""
    while not stop.is_set():
        try:
            await fn()
        except Exception:
            logger.exception("%s tick failed", name)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)
```

`sample/app/publisher.py`:
```python
"""Outbox publisher: sends every job whose sendMe flag is set and due."""
import logging

from app.models import utcnow
from app.repository import JobRepository

logger = logging.getLogger(__name__)


async def publish_due(repo: JobRepository, sender) -> int:
    sent = 0
    for job in await repo.due_for_send(utcnow()):
        try:
            await sender.send(job["_id"])
        except Exception as exc:
            logger.warning("Publisher could not send %s (%s); will retry", job["_id"], type(exc).__name__)
            break  # the queue is probably down; try again next tick
        # A crash here means a second send later; the worker treats duplicates safely.
        await repo.mark_sent(job["_id"], job["sendAfter"], utcnow())
        sent += 1
    return sent
```

`sample/app/sweeper.py`:
```python
"""Recovery sweeper: re-flags stuck jobs so the publisher sends them again."""
import logging
from datetime import timedelta

from app.config import Settings
from app.models import utcnow
from app.repository import JobRepository

logger = logging.getLogger(__name__)


async def sweep_stuck(repo: JobRepository, settings: Settings) -> int:
    now = utcnow()
    count = await repo.sweep(
        now,
        running_before=now - timedelta(seconds=settings.stuck_running_seconds),
        unsent_before=now - timedelta(seconds=settings.stuck_unsent_seconds),
    )
    if count:
        logger.warning("Sweeper re-flagged %d stuck job(s)", count)
    return count
```

- [ ] **Step 3: Run** `pytest tests/test_publisher.py tests/test_sweeper.py -v` → PASS. **Step 4: Commit** `sample: outbox publisher and recovery sweeper`

---

### Task 8: Queue wrapper and API app

**Files:** Create `sample/app/queue.py`, `sample/app/api.py`, `sample/app/static/index.html` (placeholder replaced in Task 10), `sample/tests/test_api.py`

- [ ] **Step 1: Failing tests**

`sample/tests/test_api.py`:
```python
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
```

- [ ] **Step 2: Implement**

`sample/app/queue.py`:
```python
"""Service Bus access: the local emulator (connection string) or Azure (DefaultAzureCredential)."""
import json
from contextlib import asynccontextmanager

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient

from app.config import Settings


@asynccontextmanager
async def servicebus_client(settings: Settings):
    if settings.servicebus_connection_string:
        async with ServiceBusClient.from_connection_string(settings.servicebus_connection_string) as client:
            yield client
    else:
        async with DefaultAzureCredential() as credential:
            async with ServiceBusClient(settings.servicebus_namespace, credential) as client:
                yield client


class QueueSender:
    def __init__(self, sender):
        self._sender = sender

    async def send(self, job_id: str) -> None:
        message = ServiceBusMessage(json.dumps({"jobId": job_id}), content_type="application/json")
        await self._sender.send_messages(message)
```

`sample/app/api.py`:
```python
"""API app: submit and read jobs, serve the UI, and run the publisher and sweeper loops."""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse
from pymongo import AsyncMongoClient

from app.config import Settings, load_settings
from app.errors import RepositoryUnavailable
from app.loops import run_periodically
from app.models import STAGES, SubmitRequest, new_job_document, utcnow
from app.publisher import publish_due
from app.queue import QueueSender, servicebus_client
from app.repository import JobRepository
from app.sweeper import sweep_stuck

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def job_view(doc: dict) -> dict:
    return {
        "id": doc["_id"],
        "key": doc["key"],
        "status": doc["status"],
        "stages": {
            name: {k: doc["stages"][name][k] for k in ("status", "tries", "output", "finishedAt")}
            for name in STAGES
        },
        "attempts": doc["attempts"],
        "error": doc["error"],
        "sendMe": doc["sendMe"],
        "sendAfter": doc["sendAfter"],
        "owner": doc["owner"]["workerId"] if doc["owner"] else None,
        "faults": doc.get("faults", {}),
        "createdAt": doc["createdAt"],
        "updatedAt": doc["updatedAt"],
    }


def create_app(settings: Settings | None = None, repo: JobRepository | None = None,
               sender=None) -> FastAPI:
    """Pass `repo` and `sender` (tests) to skip real connections and background loops."""
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if repo is not None:
            yield
            return
        mongo = AsyncMongoClient(settings.mongo_url, tz_aware=True, serverSelectionTimeoutMS=5000)
        app.state.repo = JobRepository(mongo[settings.mongo_db], settings)
        await app.state.repo.ensure_indexes()
        stop = asyncio.Event()
        async with servicebus_client(settings) as sb, sb.get_queue_sender(settings.servicebus_queue) as sb_sender:
            app.state.sender = QueueSender(sb_sender)
            loops = [
                asyncio.create_task(run_periodically(
                    "publisher", settings.publish_interval_seconds,
                    lambda: publish_due(app.state.repo, app.state.sender), stop)),
                asyncio.create_task(run_periodically(
                    "sweeper", settings.sweep_interval_seconds,
                    lambda: sweep_stuck(app.state.repo, settings), stop)),
            ]
            try:
                yield
            finally:
                stop.set()
                await asyncio.gather(*loops, return_exceptions=True)
        await mongo.close()

    app = FastAPI(title="FPA pipeline sample API", lifespan=lifespan)
    if repo is not None:
        app.state.repo, app.state.sender = repo, sender

    @app.exception_handler(RepositoryUnavailable)
    async def database_unavailable(request, exc):
        return JSONResponse({"detail": "Database unavailable, try again"}, status_code=503)

    @app.post("/jobs", status_code=201)
    async def submit(req: SubmitRequest, response: Response):
        if req.recordingType == "consent":
            response.status_code = 200
            return {"status": "SKIPPED", "id": None}

        now = utcnow()
        # sendAfter gives our own send a head start so the publisher doesn't race it.
        doc = new_job_document(req, now, now + timedelta(seconds=settings.publish_grace_seconds))
        if not await app.state.repo.insert_job(doc):
            response.status_code = 200
            return job_view(await app.state.repo.get(req.job_id))

        try:
            await app.state.sender.send(doc["_id"])
            await app.state.repo.mark_sent(doc["_id"], doc["sendAfter"], utcnow())
        except Exception as exc:
            logger.warning("Immediate send failed for %s (%s); the publisher will send it",
                           doc["_id"], type(exc).__name__)
        return job_view(await app.state.repo.get(doc["_id"]))

    @app.get("/jobs")
    async def list_jobs():
        return [job_view(doc) for doc in await app.state.repo.recent()]

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str):
        doc = await app.state.repo.get(job_id)
        if doc is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job_view(doc)

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
```

`sample/app/static/index.html` (temporary, replaced in Task 10):
```html
<!doctype html><html lang="en"><head><meta charset="utf-8"><title>FPA Pipeline Sample</title></head><body></body></html>
```

- [ ] **Step 3: Run** `pytest tests/test_api.py -v` → PASS. **Step 4: Commit** `sample: API app with immediate send and background loops`

---

### Task 9: Worker app

**Files:** Create `sample/app/worker.py`

No unit tests (the logic is in `processing.py`); verified end-to-end in Task 11.

- [ ] **Step 1: Implement**

`sample/app/worker.py`:
```python
"""Worker app: consumes the queue one message at a time and settles each message
only after the job's state has been saved."""
import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from azure.servicebus import ServiceBusReceiveMode
from azure.servicebus.aio import AutoLockRenewer
from azure.servicebus.exceptions import ServiceBusError
from fastapi import FastAPI, HTTPException
from pymongo import AsyncMongoClient

from app.config import Settings, load_settings
from app.processing import Settlement, process_message
from app.queue import servicebus_client
from app.repository import JobRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAX_LOCK_RENEWAL_SECONDS = 3600
SHUTDOWN_GRACE_SECONDS = 30


async def consume(receiver, repo: JobRepository, settings: Settings, stop: asyncio.Event) -> None:
    async with AutoLockRenewer() as renewer:
        while not stop.is_set():
            try:
                messages = await receiver.receive_messages(max_message_count=1, max_wait_time=5)
                for message in messages:
                    renewer.register(receiver, message, max_lock_renewal_duration=MAX_LOCK_RENEWAL_SECONDS)
                    settlement = await process_message(str(message), repo, settings)
                    await _settle(receiver, message, settlement)
            except ServiceBusError as exc:
                logger.error("Service Bus error (%s); retrying in 5s", type(exc).__name__)
                await asyncio.sleep(5)


async def _settle(receiver, message, settlement: Settlement) -> None:
    try:
        if settlement.action == "complete":
            await receiver.complete_message(message)
        elif settlement.action == "dead_letter":
            await receiver.dead_letter_message(
                message, reason=settlement.reason, error_description="See worker logs.")
        else:
            await receiver.abandon_message(message)
    except ServiceBusError as exc:
        # e.g. the lock was lost: the message will be redelivered, which is safe.
        logger.warning("Could not %s message (%s)", settlement.action, type(exc).__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        mongo = AsyncMongoClient(settings.mongo_url, tz_aware=True, serverSelectionTimeoutMS=5000)
        repo = JobRepository(mongo[settings.mongo_db], settings)
        stop = asyncio.Event()
        async with servicebus_client(settings) as sb, sb.get_queue_receiver(
            queue_name=settings.servicebus_queue,
            receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
            prefetch_count=0,
        ) as receiver:
            task = asyncio.create_task(consume(receiver, repo, settings, stop))
            app.state.consumer = task
            logger.info("Worker %s consuming %s", settings.worker_id, settings.servicebus_queue)
            try:
                yield
            finally:
                stop.set()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=SHUTDOWN_GRACE_SECONDS)
                except asyncio.TimeoutError:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
        await mongo.close()

    app = FastAPI(title="FPA pipeline sample worker", lifespan=lifespan)

    @app.get("/ready")
    async def ready():
        if app.state.consumer.done():
            raise HTTPException(status_code=503, detail="Queue consumer stopped")
        return {"status": "ready", "workerId": settings.worker_id}

    return app


app = create_app()
```

- [ ] **Step 2: Import check** `.\.venv\Scripts\python -c "import app.worker"` → no error. **Step 3: Commit** `sample: worker app with peek-lock consumer`

---

### Task 10: UI

**Files:** Replace `sample/app/static/index.html`

- [ ] **Step 1: Write the page** — a form with the 7 key fields (random ids prefilled) plus fault stage/type/times; a table of recent jobs with a coloured box per stage (tries in brackets), status, attempts, next send time, owner and error; polls `GET /jobs` every 2s. Full content is in the committed file.
- [ ] **Step 2:** `pytest tests/test_api.py -k ui` → PASS. **Step 3: Commit** `sample: status UI`

---

### Task 11: README, CLAUDE.md and end-to-end check

**Files:** Create `sample/README.md`; modify `CLAUDE.md`

- [ ] **Step 1:** README: what it demonstrates, setup (`.env`, EULA, `docker compose up -d`), run both apps, manual scenarios (fault buttons, Ctrl+C mid-`slow` stage, two workers, `docker compose stop mongo`, stopping the worker for 10+ minutes), and where real FPA code plugs in (`app/stages.py`).
- [ ] **Step 2:** CLAUDE.md: add the sample's commands and module map.
- [ ] **Step 3:** Full test run: `.\.venv\Scripts\python -m pytest -v` → all pass.
- [ ] **Step 4 (needs EULA acceptance by the user):** `docker compose up -d`, start API and worker, submit a job, and watch it complete; try `llm_429` and `llm_500`.
- [ ] **Step 5: Commit** `sample: README and CLAUDE.md`
