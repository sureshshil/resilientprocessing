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
