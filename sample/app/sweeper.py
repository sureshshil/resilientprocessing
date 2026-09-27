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
