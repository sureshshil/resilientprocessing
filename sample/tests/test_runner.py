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
