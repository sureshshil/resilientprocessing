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
