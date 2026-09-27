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
