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
