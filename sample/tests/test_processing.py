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
