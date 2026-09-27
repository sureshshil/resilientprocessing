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
